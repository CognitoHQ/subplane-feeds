#pragma once

// Every event is the whole book (Bitstamp order_book_*): replace it and emit one
// BookSnapshot. Roles: bids, asks, optional ExchangeTs.

#include "book.h"

namespace feeds {

class SnapshotOnly final : public BookBase {
public:
    SnapshotOnly(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every, uint32_t max_levels = Book::kDefaultMaxLevels)
        : BookBase(snapshot_every, max_levels), r_bids_(pack.role(sd.fields, "bids")), r_asks_(pack.role(sd.fields, "asks")), r_ts_(pack.role(sd.fields, "ExchangeTs")) {}
    bool on_event(simdjson::ondemand::object& rec, std::string_view, uint64_t, std::vector<BookEmit>& out, std::string& err) override {
        uint64_t ts = 0;
        if (!parse_time_role(rec, r_ts_, ts, err)) return false;
        book_.clear();
        synced_ = false;   // overflow: Stale until an event fits, since the next event is the whole book again
        if (!parse_levels(rec, *r_bids_, rows_, err)) return false;
        if (!load(Side::Buy, rows_)) { book_.clear(); return false; }
        if (!parse_levels(rec, *r_asks_, rows_, err)) return false;
        if (!load(Side::Sell, rows_)) { book_.clear(); return false; }
        emit_snapshot(ts, false, out);
        synced_ = true;
        return true;
    }
    bool on_snapshot(simdjson::ondemand::object&, uint64_t, std::vector<BookEmit>&, std::string& err) override { err = "snapshot_only takes no REST snapshot"; return false; }
    bool take_snapshot_request() noexcept override { return false; }
    void on_disconnect() noexcept override { book_.clear(); synced_ = false; }
    bool synced() const noexcept override { return synced_; }
    bool full_depth() const noexcept override { return false; }
private:
    bool synced_ = false;
    const FieldMap *r_bids_, *r_asks_, *r_ts_;
    std::vector<LevelRow> rows_;
};

} // namespace feeds
