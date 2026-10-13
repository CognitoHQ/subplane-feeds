#pragma once

// Binance "diff depth" protocol: buffer events, fetch a REST snapshot with
// lastUpdateId, drop events with u <= lastUpdateId, the first applied event
// must satisfy U <= lastUpdateId + 1 <= u, then every event must satisfy
// U == previous u + 1; anything else is a resync.

#include "book.h"

#include <deque>
#include <memory>

namespace feeds {

class BinanceDiff final : public BookStrategy {
public:
    // `sd` supplies the roles (first_id, last_id, bids, asks, ExchangeTs) and the snapshot roles.
    BinanceDiff(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every);

    bool on_event(simdjson::ondemand::object& data, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) override;
    bool on_snapshot(simdjson::ondemand::object& body, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) override;
    bool take_snapshot_request() noexcept override;
    void on_disconnect() noexcept override;
    const Book& book() const noexcept override { return book_; }
    uint64_t resyncs() const noexcept override { return resyncs_; }
    bool synced() const noexcept { return synced_; }

    struct Event {
        uint64_t first_id = 0, last_id = 0, exch_ts = 0;
        std::vector<Level> bids, asks;
    };
    // Exposed for tests: the pure sync logic over parsed events.
    bool apply_event(const Event& ev, std::vector<BookEmit>& out) noexcept;
    bool apply_snapshot(uint64_t last_id, const std::vector<Level>& bids, const std::vector<Level>& asks, std::vector<BookEmit>& out) noexcept;

private:
    bool parse_event(simdjson::ondemand::object& data, Event& ev, std::string& err) noexcept;
    bool parse_levels(simdjson::ondemand::object& obj, const FieldMap& role, std::vector<Level>& out, std::string& err) noexcept;
    void apply_levels(Side side, const std::vector<Level>& lv, uint64_t ts, std::vector<BookEmit>& out) noexcept;
    void emit_snapshot(uint64_t ts, bool resynced, std::vector<BookEmit>& out) noexcept;
    void start_resync() noexcept;

    const FieldMap *r_first_, *r_last_, *r_bids_, *r_asks_, *r_ts_;
    const FieldMap *s_last_, *s_bids_, *s_asks_;
    uint32_t snapshot_every_;
    Book book_;
    std::deque<Event> buffer_;        // events seen while waiting for a snapshot
    bool synced_ = false, request_pending_ = false, snapshot_wanted_ = false;
    uint64_t last_u_ = 0, resyncs_ = 0, updates_since_snapshot_ = 0;
    static constexpr size_t kMaxBuffered = 10000;
};

} // namespace feeds
