#include "l2_inband.h"

namespace feeds {

L2Inband::L2Inband(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every)
    : BookBase(snapshot_every), r_bids_(pack.role(sd.fields, "bids")), r_asks_(pack.role(sd.fields, "asks")),
      r_changes_(pack.role(sd.fields, "changes")), r_ts_(pack.role(sd.fields, "ExchangeTs")), snapshot_kind_(sd.snapshot_kind) {}

bool L2Inband::on_event(simdjson::ondemand::object& rec, std::string_view kind, uint64_t, std::vector<BookEmit>& out, std::string& err) {
    uint64_t ts = 0;
    if (!parse_time_role(rec, r_ts_, ts, err)) return false;
    if (kind == snapshot_kind_) {
        if (!r_bids_ || !r_asks_) { err = "snapshot event but the stream has no bids/asks roles"; return false; }
        book_.clear();
        if (!parse_levels(rec, *r_bids_, rows_, err)) return false;
        for (const LevelRow& l : rows_) book_.apply(Side::Buy, l.px, l.qty);
        if (!parse_levels(rec, *r_asks_, rows_, err)) return false;
        for (const LevelRow& l : rows_) book_.apply(Side::Sell, l.px, l.qty);
        // INTENT: a snapshot after the first one means the exchange (or a reconnect)
        // restarted the stream; readers see the resync flag and drop their state.
        emit_snapshot(ts, had_snapshot_, out);
        if (had_snapshot_) ++resyncs_;
        had_snapshot_ = true;
        synced_ = true;
        return true;
    }
    if (!synced_) { ++dropped_; return true; }   // updates before the snapshot describe a book we do not have
    if (r_changes_) {
        if (!parse_levels(rec, *r_changes_, rows_, err)) return false;
        for (const LevelRow& l : rows_) if (l.side == Side::Unknown) { err = "changes row without a side"; return false; }
        apply_rows(Side::Unknown, rows_, ts, out);
    } else {
        if (!parse_levels(rec, *r_bids_, rows_, err)) return false;
        apply_rows(Side::Buy, rows_, ts, out);
        if (!parse_levels(rec, *r_asks_, rows_, err)) return false;
        apply_rows(Side::Sell, rows_, ts, out);
    }
    maybe_periodic_snapshot(ts, out);
    return true;
}

} // namespace feeds
