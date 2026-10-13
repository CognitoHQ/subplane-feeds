#include "binance_diff.h"

namespace feeds {

BinanceDiff::BinanceDiff(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every)
    : BookBase(snapshot_every),
      r_first_(pack.role(sd.fields, "first_id")), r_last_(pack.role(sd.fields, "last_id")),
      r_bids_(pack.role(sd.fields, "bids")), r_asks_(pack.role(sd.fields, "asks")), r_ts_(pack.role(sd.fields, "ExchangeTs")),
      s_last_(pack.role(sd.snapshot.fields, "last_id")), s_bids_(pack.role(sd.snapshot.fields, "bids")),
      s_asks_(pack.role(sd.snapshot.fields, "asks")) {
    start_resync();
    resyncs_ = 0; // the initial sync is not a resync
}

void BinanceDiff::start_resync() noexcept {
    synced_ = false;
    snapshot_wanted_ = true;
    request_pending_ = false;
    buffer_.clear();
    book_.clear();
    last_u_ = 0;
    ++resyncs_;
}

bool BinanceDiff::take_snapshot_request() noexcept {
    if (!snapshot_wanted_ || request_pending_) return false;
    request_pending_ = true;
    return true;
}

void BinanceDiff::on_disconnect() noexcept {
    start_resync();
}

bool BinanceDiff::parse_event(simdjson::ondemand::object& rec, Event& ev, std::string& err) noexcept {
    simdjson::ondemand::value v;
    if (!walk(rec, r_first_->path, v) || !read_uint64(v, ev.first_id)) { err = "first_id missing or not an integer"; return false; }
    if (!walk(rec, r_last_->path, v) || !read_uint64(v, ev.last_id)) { err = "last_id missing or not an integer"; return false; }
    if (!parse_time_role(rec, r_ts_, ev.exch_ts, err)) return false;
    if (!parse_levels(rec, *r_bids_, ev.bids, err)) return false;
    if (!parse_levels(rec, *r_asks_, ev.asks, err)) return false;
    return true;
}

bool BinanceDiff::apply_event(const Event& ev, std::vector<BookEmit>& out) noexcept {
    if (!synced_) {
        if (buffer_.size() >= kMaxBuffered) buffer_.pop_front();
        buffer_.push_back(ev);
        return true;
    }
    // INTENT: a gap in update ids means a lost diff; the book is unknowable until
    // a fresh snapshot, so drop it and start again rather than publish a wrong book.
    if (ev.first_id != last_u_ + 1) {
        start_resync();
        buffer_.push_back(ev);
        return false;
    }
    apply_rows(Side::Buy, ev.bids, ev.exch_ts, out);
    apply_rows(Side::Sell, ev.asks, ev.exch_ts, out);
    last_u_ = ev.last_id;
    maybe_periodic_snapshot(ev.exch_ts, out);
    return true;
}

bool BinanceDiff::apply_snapshot(uint64_t last_id, const std::vector<LevelRow>& bids, const std::vector<LevelRow>& asks, std::vector<BookEmit>& out) noexcept {
    request_pending_ = false;
    book_.clear();
    for (const LevelRow& l : bids) book_.apply(Side::Buy, l.px, l.qty);
    for (const LevelRow& l : asks) book_.apply(Side::Sell, l.px, l.qty);
    // Drop everything the snapshot already contains.
    while (!buffer_.empty() && buffer_.front().last_id <= last_id) buffer_.pop_front();
    if (!buffer_.empty() && buffer_.front().first_id > last_id + 1) {
        // INTENT: the snapshot is older than our oldest buffered diff: we cannot
        // bridge the gap, so ask for another snapshot and keep buffering.
        snapshot_wanted_ = true;
        return false;
    }
    synced_ = true;
    snapshot_wanted_ = false;
    last_u_ = last_id;
    const bool resynced = resyncs_ > 0;
    uint64_t ts = 0;
    std::deque<Event> pending;
    pending.swap(buffer_);
    for (const Event& ev : pending) {
        // Buffered events overlapping the snapshot (U <= last_id + 1 <= u) apply in full: quantities are absolute.
        if (ev.first_id > last_u_ + 1) { start_resync(); return false; }
        apply_rows(Side::Buy, ev.bids, ev.exch_ts, out);
        apply_rows(Side::Sell, ev.asks, ev.exch_ts, out);
        last_u_ = ev.last_id;
        ts = ev.exch_ts;
    }
    // Updates emitted before the snapshot would describe a book the reader has
    // not seen yet; the snapshot states the book, so drop them and emit it last.
    out.clear();
    emit_snapshot(ts, resynced, out);
    return true;
}

bool BinanceDiff::on_event(simdjson::ondemand::object& rec, std::string_view, uint64_t, std::vector<BookEmit>& out, std::string& err) {
    Event ev;
    if (!parse_event(rec, ev, err)) return false;
    return apply_event(ev, out);
}

bool BinanceDiff::on_snapshot(simdjson::ondemand::object& body, uint64_t, std::vector<BookEmit>& out, std::string& err) {
    simdjson::ondemand::value v;
    uint64_t last = 0;
    if (!walk(body, s_last_->path, v) || !read_uint64(v, last)) { err = "snapshot last_id missing"; return false; }
    std::vector<LevelRow> bids, asks;
    if (!parse_levels(body, *s_bids_, bids, err)) return false;
    if (!parse_levels(body, *s_asks_, asks, err)) return false;
    return apply_snapshot(last, bids, asks, out);
}

} // namespace feeds
