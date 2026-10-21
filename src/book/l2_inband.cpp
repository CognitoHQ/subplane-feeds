#include "l2_inband.h"

namespace feeds {

L2Inband::L2Inband(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every, uint32_t max_levels)
    : BookBase(snapshot_every, max_levels), r_bids_(pack.role(sd.fields, "bids")), r_asks_(pack.role(sd.fields, "asks")),
      r_changes_(pack.role(sd.fields, "changes")), r_ts_(pack.role(sd.fields, "ExchangeTs")), snapshot_kind_(sd.snapshot_kind) {}

// INTENT: the only snapshot is the one the socket sends on subscribe, so an
// overflowed book can only be rebuilt by reconnecting; until then it is Stale
// and its updates are dropped, as before the first snapshot.
void L2Inband::overflowed() noexcept {
    synced_ = false;
    book_.clear();
    reconnect_ = true;
}

bool L2Inband::on_event(simdjson::ondemand::object& rec, std::string_view kind, uint64_t, std::vector<BookEmit>& out, std::string& err) {
    uint64_t ts = 0;
    if (!parse_time_role(rec, r_ts_, ts, err)) return false;
    if (kind == snapshot_kind_) {
        if (!r_bids_ || !r_asks_) { err = "snapshot event but the stream has no bids/asks roles"; return false; }
        book_.clear();
        if (!parse_levels(rec, *r_bids_, rows_, err)) return false;
        if (!load(Side::Buy, rows_)) { overflowed(); return false; }
        if (!parse_levels(rec, *r_asks_, rows_, err)) return false;
        if (!load(Side::Sell, rows_)) { overflowed(); return false; }
        // INTENT: a snapshot after the first one means the exchange (or a reconnect)
        // restarted the stream; readers see the resync flag and drop their state.
        emit_sync(ts, had_snapshot_, out);
        if (had_snapshot_) ++resyncs_;
        had_snapshot_ = true;
        synced_ = true;
        return true;
    }
    if (!synced_) { ++dropped_; return true; }   // updates before the snapshot describe a book we do not have
    if (r_changes_) {
        if (!parse_levels(rec, *r_changes_, rows_, err)) return false;
        for (const LevelRow& l : rows_) if (l.side == Side::Unknown) { err = "changes row without a side"; return false; }
        if (!apply_rows(Side::Unknown, rows_, ts, out)) { overflowed(); return false; }
    } else {
        if (!parse_levels(rec, *r_bids_, rows_, err)) return false;
        if (!apply_rows(Side::Buy, rows_, ts, out)) { overflowed(); return false; }
        if (!parse_levels(rec, *r_asks_, rows_, err)) return false;
        if (!apply_rows(Side::Sell, rows_, ts, out)) { overflowed(); return false; }
    }
    maybe_periodic_snapshot(ts, out);
    return true;
}

} // namespace feeds
