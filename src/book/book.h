#pragma once

// A local order book (absolute quantities per price, best first) and the
// strategy interface that keeps it in sync with an exchange's protocol. The
// mapping vocabulary cannot express snapshot+diff reconciliation, so each
// protocol is code, selected by name in the pack (`book: binance_diff`).

#include "pack.h"

#include <simdjson.h>

#include <cstdint>
#include <string>
#include <vector>

namespace feeds {

struct Level { int64_t px = 0, qty = 0; };

enum class Side : uint8_t { Unknown = 0, Buy = 1, Sell = 2 };
enum class Action : uint8_t { Add = 1, Update = 2, Delete = 3, Clear = 4 };

class Book {
public:
    // Applies an absolute quantity at a price (0 deletes). Returns the action and
    // the 0-based level index at which it happened (the former index for deletes).
    // `changed` is false when nothing happened (delete of an absent level).
    struct Result { Action action; uint32_t level; bool changed; };
    Result apply(Side side, int64_t px, int64_t qty) noexcept;
    void clear() noexcept { bids_.clear(); asks_.clear(); }
    const std::vector<Level>& bids() const noexcept { return bids_; }   // descending price
    const std::vector<Level>& asks() const noexcept { return asks_; }   // ascending price
private:
    std::vector<Level> bids_, asks_;
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
    // One stream event for this instrument. Emits go to `out` in order.
    virtual bool on_event(simdjson::ondemand::object& data, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) = 0;
    // A snapshot body (REST response, or a recorded one on replay).
    virtual bool on_snapshot(simdjson::ondemand::object& body, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) = 0;
    // True once per needed snapshot; the gateway then fetches `snapshot_url()`.
    virtual bool take_snapshot_request() noexcept = 0;
    virtual void on_disconnect() noexcept = 0;
    virtual const Book& book() const noexcept = 0;
    virtual uint64_t resyncs() const noexcept = 0;
};

} // namespace feeds
