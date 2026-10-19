#pragma once

// Binance "diff depth" protocol: buffer events, fetch a REST snapshot with
// lastUpdateId, drop events with u <= lastUpdateId, the first applied event
// must satisfy U <= lastUpdateId + 1 <= u, then every event must satisfy
// U == previous u + 1; anything else is a resync.

#include "book.h"

#include <deque>

namespace feeds {

class BinanceDiff final : public BookBase {
public:
    // `sd` supplies the roles (first_id, last_id, bids, asks, ExchangeTs) and the snapshot roles.
    BinanceDiff(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every);

    bool on_event(simdjson::ondemand::object& rec, std::string_view kind, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) override;
    bool on_snapshot(simdjson::ondemand::object& body, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) override;
    bool take_snapshot_request() noexcept override;
    void on_disconnect() noexcept override;
    bool synced() const noexcept override { return synced_; }

    struct Event {
        uint64_t first_id = 0, last_id = 0, exch_ts = 0;
        std::vector<LevelRow> bids, asks;
    };
    // Exposed for tests: the pure sync logic over parsed events.
    bool apply_event(const Event& ev, std::vector<BookEmit>& out) noexcept;
    bool apply_snapshot(uint64_t last_id, const std::vector<LevelRow>& bids, const std::vector<LevelRow>& asks, std::vector<BookEmit>& out) noexcept;

private:
    bool parse_event(simdjson::ondemand::object& rec, Event& ev, std::string& err) noexcept;
    void start_resync() noexcept;

    const FieldMap *r_first_, *r_last_, *r_bids_, *r_asks_, *r_ts_;
    const FieldMap *s_last_, *s_bids_, *s_asks_;
    std::deque<Event> buffer_;        // events seen while waiting for a snapshot
    bool synced_ = false, request_pending_ = false, snapshot_wanted_ = false;
    uint64_t last_u_ = 0;
    static constexpr size_t kMaxBuffered = 10000;
};

} // namespace feeds
