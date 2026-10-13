#include "binance_diff.h"

#include <algorithm>

namespace feeds {

Book::Result Book::apply(Side side, int64_t px, int64_t qty) noexcept {
    std::vector<Level>& v = side == Side::Buy ? bids_ : asks_;
    // bids descend, asks ascend: find the first level not better than px.
    auto it = side == Side::Buy
        ? std::lower_bound(v.begin(), v.end(), px, [](const Level& l, int64_t p) { return l.px > p; })
        : std::lower_bound(v.begin(), v.end(), px, [](const Level& l, int64_t p) { return l.px < p; });
    const uint32_t idx = static_cast<uint32_t>(it - v.begin());
    if (it != v.end() && it->px == px) {
        if (qty == 0) { v.erase(it); return {Action::Delete, idx, true}; }
        it->qty = qty;
        return {Action::Update, idx, true};
    }
    if (qty == 0) return {Action::Delete, idx, false};
    v.insert(it, Level{px, qty});
    return {Action::Add, idx, true};
}

BinanceDiff::BinanceDiff(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every)
    : r_first_(pack.role(sd.fields, "first_id")), r_last_(pack.role(sd.fields, "last_id")),
      r_bids_(pack.role(sd.fields, "bids")), r_asks_(pack.role(sd.fields, "asks")), r_ts_(pack.role(sd.fields, "ExchangeTs")),
      s_last_(pack.role(sd.snapshot.fields, "last_id")), s_bids_(pack.role(sd.snapshot.fields, "bids")),
      s_asks_(pack.role(sd.snapshot.fields, "asks")), snapshot_every_(snapshot_every) {
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

bool BinanceDiff::parse_levels(simdjson::ondemand::object& obj, const FieldMap& role, std::vector<Level>& out, std::string& err) noexcept {
    out.clear();
    simdjson::ondemand::value v;
    if (!walk(obj, role.path, v)) { err = role.target + ": path '" + role.path.text + "' not found"; return false; }
    simdjson::ondemand::array arr;
    if (v.get_array().get(arr) != simdjson::SUCCESS) { err = role.target + ": not an array"; return false; }
    for (auto el : arr) {
        simdjson::ondemand::array pair;
        if (el.get_array().get(pair) != simdjson::SUCCESS) { err = role.target + ": level is not an array"; return false; }
        Level l;
        int i = 0;
        for (auto x : pair) {
            simdjson::ondemand::value xv;
            if (x.get(xv) != simdjson::SUCCESS) { err = role.target + ": bad level element"; return false; }
            int64_t d;
            if (!read_decimal(xv, d)) { err = role.target + ": level element is not a decimal"; return false; }
            if (i == 0) l.px = d; else if (i == 1) l.qty = d;
            ++i;
        }
        if (i < 2) { err = role.target + ": level needs [price, qty]"; return false; }
        out.push_back(l);
    }
    return true;
}

bool BinanceDiff::parse_event(simdjson::ondemand::object& data, Event& ev, std::string& err) noexcept {
    simdjson::ondemand::value v;
    if (!walk(data, r_first_->path, v) || !read_uint64(v, ev.first_id)) { err = "first_id missing or not an integer"; return false; }
    if (!walk(data, r_last_->path, v) || !read_uint64(v, ev.last_id)) { err = "last_id missing or not an integer"; return false; }
    ev.exch_ts = 0;
    if (r_ts_) {
        uint64_t t;
        if (!walk(data, r_ts_->path, v) || !read_uint64(v, t)) { err = "ExchangeTs missing or not an integer"; return false; }
        ev.exch_ts = to_ns(t, r_ts_->unit);
    }
    if (!parse_levels(data, *r_bids_, ev.bids, err)) return false;
    if (!parse_levels(data, *r_asks_, ev.asks, err)) return false;
    return true;
}

void BinanceDiff::apply_levels(Side side, const std::vector<Level>& lv, uint64_t ts, std::vector<BookEmit>& out) noexcept {
    for (const Level& l : lv) {
        const Book::Result r = book_.apply(side, l.px, l.qty);
        if (!r.changed) continue;
        BookEmit e;
        e.kind = BookEmit::UPDATE; e.side = side; e.action = r.action;
        e.level = r.level < 10 ? static_cast<uint8_t>(r.level) : 255;
        e.px = l.px; e.qty = l.qty; e.exch_ts = ts;
        out.push_back(e);
        ++updates_since_snapshot_;
    }
}

void BinanceDiff::emit_snapshot(uint64_t ts, bool resynced, std::vector<BookEmit>& out) noexcept {
    BookEmit e;
    e.kind = BookEmit::SNAPSHOT; e.exch_ts = ts; e.resynced = resynced;
    out.push_back(e);
    updates_since_snapshot_ = 0;
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
    apply_levels(Side::Buy, ev.bids, ev.exch_ts, out);
    apply_levels(Side::Sell, ev.asks, ev.exch_ts, out);
    last_u_ = ev.last_id;
    if (snapshot_every_ && updates_since_snapshot_ >= snapshot_every_) emit_snapshot(ev.exch_ts, false, out);
    return true;
}

bool BinanceDiff::apply_snapshot(uint64_t last_id, const std::vector<Level>& bids, const std::vector<Level>& asks, std::vector<BookEmit>& out) noexcept {
    request_pending_ = false;
    book_.clear();
    for (const Level& l : bids) book_.apply(Side::Buy, l.px, l.qty);
    for (const Level& l : asks) book_.apply(Side::Sell, l.px, l.qty);
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
        apply_levels(Side::Buy, ev.bids, ev.exch_ts, out);
        apply_levels(Side::Sell, ev.asks, ev.exch_ts, out);
        last_u_ = ev.last_id;
        ts = ev.exch_ts;
    }
    // Updates emitted before the snapshot would describe a book the reader has
    // not seen yet; the snapshot states the book, so drop them and emit it last.
    out.clear();
    emit_snapshot(ts, resynced, out);
    return true;
}

bool BinanceDiff::on_event(simdjson::ondemand::object& data, uint64_t, std::vector<BookEmit>& out, std::string& err) {
    Event ev;
    if (!parse_event(data, ev, err)) return false;
    return apply_event(ev, out);
}

bool BinanceDiff::on_snapshot(simdjson::ondemand::object& body, uint64_t, std::vector<BookEmit>& out, std::string& err) {
    simdjson::ondemand::value v;
    uint64_t last = 0;
    if (!walk(body, s_last_->path, v) || !read_uint64(v, last)) { err = "snapshot last_id missing"; return false; }
    std::vector<Level> bids, asks;
    if (!parse_levels(body, *s_bids_, bids, err)) return false;
    if (!parse_levels(body, *s_asks_, asks, err)) return false;
    return apply_snapshot(last, bids, asks, out);
}

} // namespace feeds
