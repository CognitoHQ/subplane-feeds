#include "gateway.h"

#include "canonical_encode.h"

#include <algorithm>
#include <cstdio>
#include <cstddef>
#include <cstring>

namespace feeds {
namespace {
constexpr uint64_t kOverflowHoldNs = 60'000'000'000ull;

void replace_all(std::string& s, std::string_view from, std::string_view to) {
    for (size_t pos = s.find(from); pos != std::string::npos; pos = s.find(from, pos + to.size())) s.replace(pos, from.size(), to);
}
// A placeholder appears quoted in the emitted JSON ("{streams}"); the replacement is a JSON value, so the quotes go too.
void replace_value(std::string& s, std::string_view name, std::string_view json) {
    replace_all(s, "\"{" + std::string(name) + "}\"", json);
    replace_all(s, "{" + std::string(name) + "}", json);
}
// A placeholder inside a string ("{channel}", "live_trades_{symbol}") takes the text itself.
void replace_text(std::string& s, std::string_view name, std::string_view text) {
    replace_all(s, "{" + std::string(name) + "}", text);
}
bool has_full(const std::vector<BookEmit>& v) noexcept {
    for (const BookEmit& e : v) if (e.kind == BookEmit::FULL) return true;
    return false;
}
std::string json_quoted(std::string_view s) { return "\"" + std::string(s) + "\""; }
std::string array_of(const std::vector<std::string>& items) {
    std::string a = "[";
    for (size_t i = 0; i < items.size(); ++i) { if (i) a += ','; a += items[i]; }
    return a + ']';
}

} // namespace

Gateway::Gateway(const Pack& pack, const Instruments& instruments, Publisher& pub, GatewayOptions opts, Recorder* rec, Publisher* state)
    : pack_(pack), ins_(instruments), pub_(pub), opts_(opts), rec_(rec), state_(state) {
    for (const StreamDef& sd : pack_.streams) {
        if (sd.book.empty()) continue;
        for (size_t i = 0; i < ins_.all().size(); ++i) {
            BookSlot slot;
            std::string err;
            slot.strategy = make_strategy(pack_, sd, opts_.snapshot_every, err);
            if (!slot.strategy) continue;   // the pack loader rejects unknown strategies
            slot.sd = &sd;
            slot.ins = &ins_.all()[i];
            slot.idx = i;
            books_.push_back(std::move(slot));
        }
    }
    states_.resize(ins_.all().size());
    count_states();
    buf_.reserve(65536 + simdjson::SIMDJSON_PADDING);
    emits_.reserve(1024);
    top_.reserve(10);
    batch_.reserve(opts_.batch_bytes ? opts_.batch_bytes : 512);
}

std::string Gateway::expand(std::string_view tpl, std::string_view stream_symbol, std::string_view venue_symbol, const StreamDef* sd) const {
    return expand_symbol(tpl, stream_symbol, venue_symbol, sd ? sd->channel : std::string_view());
}

std::vector<std::string> Gateway::stream_names() const {
    std::vector<std::string> out;
    for (const StreamDef& sd : pack_.streams)
        for (const Instrument& in : ins_.all()) out.push_back(expand(sd.name_tpl, in.stream_symbol, in.symbol, &sd));
    return out;
}

std::vector<std::string> Gateway::subscribe_messages() const {
    std::vector<std::string> symbols, streams;
    for (const Instrument& in : ins_.all()) symbols.push_back(json_quoted(in.symbol));
    for (const StreamDef& sd : pack_.streams)
        for (const Instrument& in : ins_.all()) {
            if (pack_.connect.stream_object.empty()) streams.push_back(json_quoted(expand(sd.name_tpl, in.stream_symbol, in.symbol, &sd)));
            else streams.push_back(expand(pack_.connect.stream_object, in.stream_symbol, in.symbol, &sd));
        }
    std::vector<std::string> out;
    for (const std::string& tpl : pack_.connect.subscribe) {
        if (tpl.find("{stream}") != std::string::npos) {
            // Once per stream name (Bitstamp: one channel per message).
            for (const StreamDef& sd : pack_.streams)
                for (const Instrument& in : ins_.all()) {
                    std::string m = tpl;
                    replace_text(m, "stream", expand(sd.name_tpl, in.stream_symbol, in.symbol, &sd));
                    replace_value(m, "symbols", array_of(symbols));
                    out.push_back(m);
                }
        } else if (tpl.find("{channel}") != std::string::npos) {
            // Once per stream definition, with every symbol (Kraken-style per-channel subscribes).
            for (const StreamDef& sd : pack_.streams) {
                std::string m = tpl;
                replace_text(m, "channel", sd.channel);
                replace_value(m, "symbols", array_of(symbols));
                out.push_back(m);
            }
        } else {
            std::string m = tpl;
            replace_value(m, "streams", array_of(streams));
            replace_value(m, "symbols", array_of(symbols));
            out.push_back(m);
        }
    }
    return out;
}

uint64_t Gateway::resyncs() const noexcept {
    uint64_t n = 0;
    for (const auto& b : books_) n += b.strategy->resyncs();
    return n;
}

void Gateway::log_error(const char* what, const std::string& err) noexcept {
    if (!opts_.log_errors || errors_logged_ >= 20) return;
    ++errors_logged_;
    std::fprintf(stderr, "spwsgw: %s: %s%s\n", what, err.c_str(), errors_logged_ == 20 ? " (further errors not logged)" : "");
}

// INTENT: batching never waits. A batch is what one input frame (or one timer tick)
// produced; it is offered when that unit ends, or earlier when the next message
// would not fit. So the last message of a frame leaves at the same moment it would
// have without batching, and the Aeron frame header is paid once per batch.
void Gateway::publish(uint8_t* msg, size_t len, OneWriter& counter, const Instrument* in) noexcept {
    const uint64_t seq = ++seq_;
    std::memcpy(msg + kPrefixSequence, &seq, 8);
    // INTENT: InstrumentSeq counts this instrument's messages, 1, 2, 3 ... with
    // no holes. It is the topic's count, whatever stream carries the message, so
    // an agent drops a copy of it from another stream and finds a gap per
    // instrument. Every message about the instrument counts, definitions too.
    if (in != nullptr) {
        const uint64_t iseq = ++states_[static_cast<size_t>(in - ins_.all().data())].iseq;
        std::memcpy(msg + kPrefixInstrumentSeq, &iseq, 8);
    }
    ++counter;
    if (opts_.batch_bytes == 0) { offer(msg, len, 1); return; }
    if (!batch_.empty() && batch_.size() + len > opts_.batch_bytes) flush();
    batch_.insert(batch_.end(), msg, msg + len);
    ++pending_;
}

void Gateway::offer(const uint8_t* msg, size_t len, uint64_t messages) noexcept {
    ++stats_.offers; stats_.offer_bytes += len;
    stats_.last_sequence.set(seq_);
    switch (pub_.publish(msg, len)) {
    case Offer::OK: stats_.published += messages; return;
    case Offer::BACK_PRESSURED: stats_.dropped_back_pressured += messages; break;
    case Offer::NOT_CONNECTED: stats_.dropped_not_connected += messages; break;
    case Offer::FAILED: stats_.dropped_failed += messages; break;
    }
    stats_.dropped += messages;
}

void Gateway::flush() noexcept {
    if (batch_.empty()) return;
    offer(batch_.data(), batch_.size(), pending_);
    batch_.clear();
    pending_ = 0;
}

void Gateway::publish_heartbeat(uint64_t now) noexcept {
    uint8_t buf[sp_canonical::Heartbeat::kSize];
    const CanonicalMessage* m = find_message("Heartbeat");
    begin_message(buf, *m, 0, pack_.venue_id, 0, now);
    put_u64(buf, kPrefixExchangeTs, now);
    // INTENT: the stream says what it is, so an agent wired to the wrong stream
    // counts stream_mismatch instead of silently routing another topic's data.
    std::memcpy(buf + offsetof(sp_canonical::Heartbeat, StreamId), &opts_.stream_stamp, 4);
    publish(buf, sizeof buf, stats_.heartbeats);
    last_heartbeat_ = now;
}

void Gateway::publish_definition(const Instrument& in, uint64_t now) noexcept {
    const CanonicalMessage* m = find_message("InstrumentDefinition");
    uint8_t buf[sp_canonical::InstrumentDefinition::kSize];
    begin_message(buf, *m, in.id, pack_.venue_id, 0, now);
    put_u64(buf, kPrefixExchangeTs, now);
    auto chars = [&](const char* name, const std::string& v) {
        const CanonicalField* f = find_field(*m, name);
        std::memcpy(buf + f->offset, v.data(), std::min<size_t>(v.size(), f->size));
    };
    chars("Symbol", in.symbol);
    chars("Exchange", pack_.exchange);
    chars("Base", in.base);
    chars("Quote", in.quote);
    put(buf, find_field(*m, "Status")->offset, static_cast<uint8_t>(1)); // Trading
    publish(buf, sizeof buf, stats_.definitions, &in);
}

void Gateway::publish_definitions(uint64_t now) noexcept {
    for (const Instrument& in : ins_.all()) publish_definition(in, now);
    last_definitions_ = now;
}

// INTENT: the state stream carries only SPP, never canonical data, so recovery
// stays independent of the data schema. Sequence is the last data sequence
// assigned; callers flush the data batch first, so a subscriber that has seen
// Sequence on the data stream has seen everything the state speaks about.
void Gateway::publish_state(uint64_t instrument, sp_proto::SourceStateCode code, sp_proto::SourceReason reason, uint64_t correlation) noexcept {
    if (!state_) return;
    uint8_t buf[4 + 8 + sp_proto::SourceStateEncoder::kBlockLength];
    sp_proto::SourceStateEncoder e(buf + 4, sizeof buf - 4);
    e.CorrelationId(correlation).SourceEpoch(opts_.source_epoch).Instrument(instrument).Sequence(seq_)
        .Venue(pack_.venue_id).State(code).Reason(reason);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    std::memcpy(buf, &n, 4);   // SPP framing: a u32 length per message
    ++stats_.state_messages;
    if (state_->publish(buf, 4 + n) != Offer::OK) ++stats_.state_dropped;
}

bool Gateway::instrument_ok(size_t i) const noexcept {
    if (!connected_) return false;
    for (const BookSlot& b : books_) if (b.idx == i && !b.strategy->synced()) return false;
    return true;
}

void Gateway::refresh_state(size_t i, sp_proto::SourceReason why, bool force, uint64_t correlation) noexcept {
    using sp_proto::SourceStateCode;
    using sp_proto::SourceReason;
    InsState& st = states_[i];
    const bool ok = instrument_ok(i);
    const SourceStateCode want = ok ? SourceStateCode::Ok : st.ever_ok ? SourceStateCode::Stale : SourceStateCode::Unknown;
    if (want == st.code && !force) return;
    SourceReason r = why;
    if (ok) r = why == SourceReason::Republish ? why : st.ever_ok ? SourceReason::Resync : SourceReason::Startup;
    else if (want == st.code) r = st.reason;   // forced and unchanged: repeat why it is not Ok
    st.code = want;
    st.reason = r;
    st.ever_ok = st.ever_ok || ok;
    flush();
    publish_state(ins_.all()[i].id, want, r, correlation);
    count_states();
}

void Gateway::count_states() noexcept {
    uint64_t ok = 0, stale = 0, unknown = 0;
    for (const InsState& st : states_) {
        if (st.code == sp_proto::SourceStateCode::Ok) ++ok;
        else if (st.code == sp_proto::SourceStateCode::Stale) ++stale;
        else ++unknown;
    }
    stats_.instruments_ok.set(ok); stats_.instruments_stale.set(stale); stats_.instruments_unknown.set(unknown);
}

sp_proto::SourceStateCode Gateway::state(uint32_t instrument_id) const noexcept {
    for (size_t i = 0; i < states_.size(); ++i) if (ins_.all()[i].id == instrument_id) return states_[i].code;
    return sp_proto::SourceStateCode::Unknown;
}

RepublishResult Gateway::republish(uint64_t instrument, uint16_t venue, uint64_t correlation) noexcept {
    if (venue != 0 && venue != pack_.venue_id) { ++stats_.republish_not_found; return RepublishResult::NOT_FOUND; }
    bool found = false;
    for (size_t i = 0; i < states_.size(); ++i) {
        if (instrument != 0 && ins_.all()[i].id != instrument) continue;
        found = true;
        InsState& st = states_[i];
        st.correlation = correlation;
        if (!st.queued) { st.queued = true; republish_.push_back(i); }
    }
    if (!found) { ++stats_.republish_not_found; return RepublishResult::NOT_FOUND; }
    ++stats_.republish_queued;
    return RepublishResult::QUEUED;
}

void Gateway::republish_one(size_t i, uint64_t now) noexcept {
    InsState& st = states_[i];
    st.queued = false;
    publish_definition(ins_.all()[i], now);
    for (BookSlot& b : books_) {
        if (b.idx != i || !b.strategy->synced()) continue;
        BookEmit e;
        e.exch_ts = now;
        if (b.strategy->full_depth()) { e.kind = BookEmit::FULL; emits_.push_back(e); }
        e.kind = BookEmit::SNAPSHOT;
        emits_.push_back(e);
        publish_emits(b, now);
    }
    ++stats_.republished;
    refresh_state(i, sp_proto::SourceReason::Republish, true, st.correlation);
}

void Gateway::on_connected(uint64_t now) {
    if (rec_) rec_->event(now, "connected");
    ++stats_.connects; stats_.connected.set(1);
    connected_ = true;
    publish_definitions(now);
    flush();
    // Instruments without a book are whole from here; books go Ok when they sync.
    for (size_t i = 0; i < states_.size(); ++i) refresh_state(i, sp_proto::SourceReason::None);
}

void Gateway::on_disconnected(uint64_t now) {
    if (rec_) rec_->event(now, "disconnected");
    ++stats_.disconnects; stats_.connected.set(0);
    connected_ = false;
    for (auto& b : books_) b.strategy->on_disconnect();
    flush();
    for (size_t i = 0; i < states_.size(); ++i) refresh_state(i, sp_proto::SourceReason::Disconnect);
}

void Gateway::tick(uint64_t now) {
    if (opts_.heartbeat_ms && now - last_heartbeat_ >= opts_.heartbeat_ms * 1'000'000ull) {
        publish_heartbeat(now);
        flush();
        // The beat: the whole source, so late subscribers learn SourceEpoch without asking.
        publish_state(0, connected_ ? sp_proto::SourceStateCode::Ok : sp_proto::SourceStateCode::Stale, sp_proto::SourceReason::None, 0);
    }
    // INTENT: a republish is a burst (a deep book is thousands of messages); one per
    // interval keeps a "republish all" from flooding subscribers and the gateway's own
    // live path. At most one per tick, so the tick rate caps it too.
    if (!republish_.empty() && opts_.republish_per_second && now >= next_republish_) {
        const size_t i = republish_.front();
        republish_.pop_front();
        republish_one(i, now);
        next_republish_ = now + 1'000'000'000ull / opts_.republish_per_second;
    }
    if (opts_.definitions_ms && last_definitions_ && now - last_definitions_ >= opts_.definitions_ms * 1'000'000ull) publish_definitions(now);
    for (auto& b : books_) {
        // INTENT: an overflow resyncs at once, but one that recurs within a minute means
        // max_levels is smaller than the exchange's book, and every resync would overflow
        // again: hold the next one off for a minute rather than hammer the REST endpoint
        // or the connection. The book stays Stale meanwhile.
        const uint64_t ov = b.strategy->book().overflows();
        if (ov != b.overflows_seen) {
            if (b.last_overflow && now - b.last_overflow < kOverflowHoldNs) b.hold_until = now + kOverflowHoldNs;
            b.last_overflow = now;
            b.overflows_seen = ov;
            log_error("book", b.ins->symbol + ": more than max_levels (" + std::to_string(b.strategy->book().max_levels()) + ") levels on a side; resyncing");
        }
        if (now < b.hold_until) continue;
        if (b.strategy->take_reconnect_request()) { reconnect_ = true; ++stats_.overflow_reconnects; }
        if (b.strategy->take_snapshot_request()) {
            requests_.push_back({b.ins->symbol, expand(b.sd->snapshot.url_tpl, b.ins->stream_symbol, b.ins->symbol, b.sd)});
            ++stats_.snapshot_requests;
        }
    }
    stats_.resyncs.set(resyncs());
    uint64_t ov = 0;
    for (const auto& b : books_) ov += b.strategy->book().overflows();
    stats_.book_overflows.set(ov);
    flush();
}

bool Gateway::next_snapshot_request(SnapshotRequest& out) {
    if (requests_.empty()) return false;
    out = std::move(requests_.front());
    requests_.pop_front();
    return true;
}

bool Gateway::prepare(std::string_view json) noexcept {
    buf_.assign(json.data(), json.size());
    buf_.reserve(buf_.size() + simdjson::SIMDJSON_PADDING);
    return true;
}

void Gateway::publish_book_update(const BookSlot& slot, Side side, Action action, uint8_t level, int64_t px, int64_t qty, uint64_t exch_ts, uint64_t recv_ts) noexcept {
    uint8_t buf[sp_canonical::BookUpdate::kSize];
    const CanonicalMessage* m = find_message("BookUpdate");
    begin_message(buf, *m, slot.ins->id, pack_.venue_id, 0, recv_ts);
    put_u64(buf, kPrefixExchangeTs, exch_ts);
    put_i64(buf, find_field(*m, "Price")->offset, px);
    put_i64(buf, find_field(*m, "Qty")->offset, qty);
    put(buf, find_field(*m, "Side")->offset, static_cast<uint8_t>(side));
    put(buf, find_field(*m, "Action")->offset, static_cast<uint8_t>(action));
    put(buf, find_field(*m, "Level")->offset, level);
    publish(buf, sizeof buf, stats_.book_updates, slot.ins);
}

void Gateway::publish_emits(BookSlot& slot, uint64_t recv_ts) noexcept {
    for (const BookEmit& e : emits_) {
        if (e.kind == BookEmit::UPDATE) {
            publish_book_update(slot, e.side, e.action, e.level, e.px, e.qty, e.exch_ts, recv_ts);
        } else if (e.kind == BookEmit::FULL) {
            // INTENT: Clear then every level is the only in-band statement of a whole
            // book; a top-ten BookSnapshot cannot rebuild depth. Without it, a reader
            // (and the snapshot service) cannot recover a book after a resync.
            publish_book_update(slot, Side::Unknown, Action::Clear, 255, 0, 0, e.exch_ts, recv_ts);
            const Book& bk = slot.strategy->book();
            for (Side side : {Side::Buy, Side::Sell}) {
                uint32_t level = 0;
                bk.for_each(side, [&](int64_t px, int64_t qty) {
                    publish_book_update(slot, side, Action::Add, level < 10 ? static_cast<uint8_t>(level) : 255, px, qty, e.exch_ts, recv_ts);
                    ++level;
                });
            }
        } else {
            uint8_t buf[sp_canonical::BookSnapshot::kSize];
            const CanonicalMessage* m = find_message("BookSnapshot");
            begin_message(buf, *m, slot.ins->id, pack_.venue_id, 0, recv_ts);
            put_u64(buf, kPrefixExchangeTs, e.exch_ts);
            if (e.resynced) put(buf, kPrefixFlags, static_cast<uint16_t>(1));
            const Book& bk = slot.strategy->book();
            const uint16_t bpx = find_field(*m, "BidPx")->offset, bq = find_field(*m, "BidQty")->offset;
            const uint16_t apx = find_field(*m, "AskPx")->offset, aq = find_field(*m, "AskQty")->offset;
            bk.top(Side::Buy, 10, top_);
            const size_t nb = top_.size();
            for (size_t i = 0; i < nb; ++i) { put_i64(buf, static_cast<uint16_t>(bpx + 8 * i), top_[i].px); put_i64(buf, static_cast<uint16_t>(bq + 8 * i), top_[i].qty); }
            bk.top(Side::Sell, 10, top_);
            const size_t na = top_.size();
            for (size_t i = 0; i < na; ++i) { put_i64(buf, static_cast<uint16_t>(apx + 8 * i), top_[i].px); put_i64(buf, static_cast<uint16_t>(aq + 8 * i), top_[i].qty); }
            put(buf, find_field(*m, "Depth")->offset, static_cast<uint8_t>(std::max(nb, na)));
            publish(buf, sizeof buf, stats_.book_snapshots, slot.ins);
        }
    }
    emits_.clear();
}

// INTENT: simdjson On Demand walks forward; once we iterate the `each` array the
// frame's other keys are behind us. Read every "$." field (and a frame-scope
// `kind` role) into text first, then iterate.
void Gateway::cache_frame_tokens(const StreamDef& sd, simdjson::ondemand::object& root) noexcept {
    frame_.reset(sd.fields.size());
    kind_ = {};
    for (size_t i = 0; i < sd.fields.size(); ++i) {
        const FieldMap& f = sd.fields[i];
        if (!f.frame_scope) continue;
        simdjson::ondemand::value v;
        std::string_view tok;
        if (!walk(root, f.path, v) || !read_token(v, tok)) continue;
        frame_.text[i] = tok;
        frame_.present[i] = true;
        if (f.target == "kind") kind_ = tok;
    }
}

void Gateway::handle_record(const StreamDef& sd, const Instrument* in, simdjson::ondemand::object& rec, std::string_view kind, uint64_t recv_ts) noexcept {
    ++stats_.records;
    if (!in) {
        // Route mode with a per-record symbol.
        simdjson::ondemand::value v;
        std::string_view sym;
        if (!walk(rec, pack_.connect.route_symbol, v) || !read_string(v, sym)) { ++stats_.unknown_symbols; return; }
        in = ins_.by_symbol(sym);
        if (!in) { ++stats_.unknown_symbols; return; }
    }
    std::string err;
    if (sd.msg) {
        uint8_t buf[512];
        begin_message(buf, *sd.msg, in->id, pack_.venue_id, 0, recv_ts);
        if (!Mapper::map_flat(sd, rec, sd.has_each ? &frame_ : nullptr, buf, err)) { ++stats_.map_errors; log_error(sd.message.c_str(), err); return; }
        OneWriter* counter = &stats_.other;
        if (sd.msg->template_id == sp_canonical::Trade::kTemplateId) counter = &stats_.trades;
        else if (sd.msg->template_id == sp_canonical::Quote::kTemplateId) counter = &stats_.quotes;
        publish(buf, sd.msg->size, *counter, in);
        return;
    }
    // The `kind` role (record scope) overrides the frame type for book strategies.
    if (const FieldMap* kr = Pack::role(sd.fields, "kind")) {
        if (kr->frame_scope) kind = kind_;
        else {
            simdjson::ondemand::value v;
            std::string_view tok;
            if (!walk(rec, kr->path, v) || !read_token(v, tok)) { ++stats_.book_errors; log_error("book", "kind: path '" + kr->path.text + "' not found"); return; }
            kind = tok;
        }
    }
    for (BookSlot& b : books_) {
        if (b.sd != &sd || b.ins != in) continue;
        const bool applied = b.strategy->on_event(rec, kind, recv_ts, emits_, err);
        if (!applied) {
            if (!err.empty()) { ++stats_.book_errors; log_error("book", err); }
            emits_.clear();
        }
        const bool whole = has_full(emits_);
        publish_emits(b, recv_ts);
        // A gap shows as synced() going false; a whole book is a new Ok even when synced() did not change (an in-band resync).
        refresh_state(b.idx, sp_proto::SourceReason::Gap, whole);
        return;
    }
}

void Gateway::on_frame(std::string_view json, uint64_t recv_ts) {
    const uint64_t t0 = mono_ns();
    const uint64_t before = stats_.published;
    ++stats_.frames;
    if (rec_) rec_->frame(recv_ts, json);
    process_frame(json, recv_ts);
    flush();
    if (stats_.published != before) {
        const uint64_t d = mono_ns() - t0; lat_.record(d); lat_.note_max(d);
        // The same clock and start point the agent compares against (RecvTs), so the two histograms subtract.
        const uint64_t rt = real_ns(); if (rt > recv_ts) lat_rt_.record(rt - recv_ts);
    }
}

void Gateway::process_frame(std::string_view json, uint64_t recv_ts) {
    const ConnectDef& c = pack_.connect;
    if (!c.ping_reply.empty() && trim(json) == c.ping_reply) { ++stats_.acks; return; }
    prepare(json);
    simdjson::ondemand::document doc;
    if (parser_.iterate(simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity())).get(doc) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
    simdjson::ondemand::object root;
    if (doc.get_object().get(root) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }

    const StreamDef* sd = nullptr;
    const Instrument* in = nullptr;
    std::string_view kind;     // a view into the frame or simdjson's string buffer; valid for this frame
    if (c.use_route) {
        simdjson::ondemand::value v;
        std::string_view type;
        if (!walk(root, c.route_type, v) || !read_token(v, type)) { ++stats_.acks; return; }   // subscribe ack, error, status
        kind = type;
        sd = pack_.find_by_kind(kind);
        if (!sd) { ++stats_.unmatched_streams; return; }
        if (c.route_symbol_frame || !sd->has_each) {
            std::string_view sym;
            if (!walk(root, c.route_symbol, v) || !read_string(v, sym)) { ++stats_.unknown_symbols; return; }
            in = ins_.by_symbol(sym);
            if (!in) { ++stats_.unknown_symbols; return; }
        }
    } else {
        std::string_view stream;
        if (root[c.wrapper_stream].get_string().get(stream) != simdjson::SUCCESS) { ++stats_.acks; return; } // subscribe ack or error
        std::string_view sym;
        sd = pack_.find_by_name(stream, sym);
        if (!sd) { ++stats_.unmatched_streams; return; }
        in = ins_.by_stream_symbol(sym);
        if (!in) { ++stats_.unknown_symbols; return; }
        if (!c.wrapper_type.empty()) {
            // Bitstamp: {"event": "bts:subscription_succeeded", "channel": "live_trades_btcusd", ...} shares the channel name.
            std::string_view type;
            if (root[c.wrapper_type].get_string().get(type) != simdjson::SUCCESS) { ++stats_.acks; return; }
            if (!sd->accepts(type)) { ++stats_.acks; return; }
            kind = type;
        }
    }

    if (sd->has_each) cache_frame_tokens(*sd, root);
    if (sd->has_each) {
        simdjson::ondemand::value v;
        simdjson::ondemand::array arr;
        if (!walk(root, sd->each, v) || v.get_array().get(arr) != simdjson::SUCCESS) { ++stats_.acks; return; }   // no records: an ack or a status frame
        for (auto el : arr) {
            simdjson::ondemand::object rec;
            if (el.get_object().get(rec) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
            handle_record(*sd, in, rec, kind, recv_ts);
        }
        return;
    }
    if (c.use_route) { handle_record(*sd, in, root, kind, recv_ts); return; }
    simdjson::ondemand::object data;
    if (root[c.wrapper_data].get_object().get(data) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
    handle_record(*sd, in, data, kind, recv_ts);
}

void Gateway::on_snapshot(std::string_view venue_symbol, std::string_view body, uint64_t recv_ts) {
    if (rec_) rec_->snapshot(recv_ts, venue_symbol, body);
    const Instrument* in = ins_.by_symbol(venue_symbol);
    if (!in) { ++stats_.unknown_symbols; return; }
    for (BookSlot& b : books_) {
        if (b.ins != in || !b.sd->has_snapshot) continue;
        prepare(body);
        simdjson::ondemand::document doc;
        simdjson::ondemand::object obj;
        if (parser_.iterate(simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity())).get(doc) != simdjson::SUCCESS ||
            doc.get_object().get(obj) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
        std::string err;
        const bool ok = b.strategy->on_snapshot(obj, recv_ts, emits_, err);
        if (!err.empty()) { ++stats_.book_errors; log_error("snapshot", err); }
        if (ok) ++stats_.snapshots_applied;
        const bool whole = has_full(emits_);
        publish_emits(b, recv_ts);
        refresh_state(b.idx, sp_proto::SourceReason::Gap, whole);
    }
    flush();
}

} // namespace feeds
