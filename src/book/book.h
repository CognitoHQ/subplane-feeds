#pragma once

// A local order book (absolute quantities per price, best first) and the
// strategy interface that keeps it in sync with an exchange's protocol. The
// mapping vocabulary cannot express snapshot+diff reconciliation, so each
// protocol is code, selected by name in the pack (`book: binance_diff`).

#include "pack.h"

#include <simdjson.h>

#include <climits>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace feeds {

struct Level { int64_t px = 0, qty = 0; };

enum class Side : uint8_t { Unknown = 0, Buy = 1, Sell = 2 };
enum class Action : uint8_t { Add = 1, Update = 2, Delete = 3, Clear = 4 };

class Book {
public:
    // Applies an absolute quantity at a price (0 deletes). Returns the action and
    // the 0-based level index at which it happened (the former index for deletes),
    // 255 when deeper than the top ten. `changed` is false when nothing happened
    // (delete of an absent level).
    struct Result { Action action; uint32_t level; bool changed; };
    Result apply(Side side, int64_t px, int64_t qty) noexcept;
    void clear() noexcept { bids_.clear(); asks_.clear(); }
    // The best `max` levels of a side, best first.
    void top(Side side, size_t max, std::vector<Level>& out) const;
    size_t depth(Side side) const noexcept { return (side == Side::Buy ? bids_ : asks_).size(); }
    std::vector<Level> bids() const { std::vector<Level> v; top(Side::Buy, SIZE_MAX, v); return v; }   // descending price
    std::vector<Level> asks() const { std::vector<Level> v; top(Side::Sell, SIZE_MAX, v); return v; }  // ascending price
private:
    // INTENT: an exchange book holds a thousand levels a side and a depth frame
    // touches a dozen; a sorted vector costs a 16 KB memmove per touch, which was
    // the whole gateway-to-agent latency for book updates (measured 2026-10-13).
    // A map keyed by signed price (bids negated so both sides iterate best first)
    // makes a touch O(log n), and the level index is only walked up to ten.
    using SideMap = std::map<int64_t, int64_t>;
    static int64_t key(Side side, int64_t px) noexcept { return side == Side::Buy ? -px : px; }
    SideMap bids_, asks_;
};

struct BookEmit {
    enum Kind : uint8_t { UPDATE, SNAPSHOT } kind;
    Side     side = Side::Unknown;
    Action   action = Action::Update;
    uint8_t  level = 255;        // 0..9 when within the top ten, else 255
    int64_t  px = 0, qty = 0;
    uint64_t exch_ts = 0;
    bool     resynced = false;   // SNAPSHOT after a (re)sync
};

struct SnapshotRequest { std::string venue_symbol; std::string url; };

class BookStrategy {
public:
    virtual ~BookStrategy() = default;
    // One stream event (record) for this instrument. `kind` is the frame type the
    // stream matched (route value, wrapper type, or the `kind` role). Emits go to `out` in order.
    virtual bool on_event(simdjson::ondemand::object& rec, std::string_view kind, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) = 0;
    // A snapshot body (REST response, or a recorded one on replay).
    virtual bool on_snapshot(simdjson::ondemand::object& body, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) = 0;
    // True once per needed snapshot; the gateway then fetches `snapshot_url()`.
    virtual bool take_snapshot_request() noexcept = 0;
    virtual void on_disconnect() noexcept = 0;
    virtual const Book& book() const noexcept = 0;
    virtual uint64_t resyncs() const noexcept = 0;
};

// Shared by the strategies.
struct LevelRow { Side side = Side::Unknown; int64_t px = 0, qty = 0; };
// Reads the array at the role's path with the role's level layout. Side is Unknown unless the layout has one.
bool parse_levels(simdjson::ondemand::object& rec, const FieldMap& role, std::vector<LevelRow>& out, std::string& err) noexcept;
// Reads the optional time role: false only when present and malformed; 0 when absent.
bool parse_time_role(simdjson::ondemand::object& rec, const FieldMap* role, uint64_t& out, std::string& err) noexcept;

// Emits one UPDATE per changed level and counts towards the periodic snapshot.
class BookBase : public BookStrategy {
public:
    const Book& book() const noexcept override { return book_; }
    uint64_t resyncs() const noexcept override { return resyncs_; }
protected:
    explicit BookBase(uint32_t snapshot_every) : snapshot_every_(snapshot_every) {}
    void apply_rows(Side side, const std::vector<LevelRow>& rows, uint64_t ts, std::vector<BookEmit>& out) noexcept; // side overrides Unknown rows
    void emit_snapshot(uint64_t ts, bool resynced, std::vector<BookEmit>& out) noexcept;
    void maybe_periodic_snapshot(uint64_t ts, std::vector<BookEmit>& out) noexcept;
    Book book_;
    uint32_t snapshot_every_;
    uint64_t resyncs_ = 0, updates_since_snapshot_ = 0;
};

// The strategy named by `sd.book`; null (with err) for an unknown name.
std::unique_ptr<BookStrategy> make_strategy(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every, std::string& err);

} // namespace feeds
