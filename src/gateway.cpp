#include "gateway.h"

#include "book/binance_diff.h"
#include "canonical_encode.h"
#include "mapper.h"

#include <algorithm>
#include <cstdio>
#include <cstring>

namespace feeds {

Gateway::Gateway(const Pack& pack, const Instruments& instruments, Publisher& pub, GatewayOptions opts, Recorder* rec)
    : pack_(pack), ins_(instruments), pub_(pub), opts_(opts), rec_(rec) {
    for (const StreamDef& sd : pack_.streams) {
        if (sd.book.empty()) continue;
        for (const Instrument& in : ins_.all()) {
            BookSlot slot;
            slot.strategy = std::make_unique<BinanceDiff>(pack_, sd, opts_.snapshot_every);
            slot.sd = &sd;
            slot.ins = &in;
            books_.push_back(std::move(slot));
        }
    }
    buf_.reserve(65536 + simdjson::SIMDJSON_PADDING);
    emits_.reserve(1024);
}

std::vector<std::string> Gateway::stream_names() const {
    std::vector<std::string> out;
    for (const StreamDef& sd : pack_.streams)
        for (const Instrument& in : ins_.all()) out.push_back(expand_symbol(sd.name_tpl, in.stream_symbol, in.symbol));
    return out;
}

std::string Gateway::subscribe_message() const {
    std::string arr = "[";
    bool first = true;
    for (const auto& n : stream_names()) { if (!first) arr += ','; first = false; arr += '"'; arr += n; arr += '"'; }
    arr += ']';
    std::string msg = pack_.connect.subscribe_json;
    const std::string quoted = "\"{streams}\"";
    size_t pos = msg.find(quoted);
    if (pos != std::string::npos) msg.replace(pos, quoted.size(), arr);
    else if ((pos = msg.find("{streams}")) != std::string::npos) msg.replace(pos, 9, arr);
    return msg;
}

uint64_t Gateway::resyncs() const noexcept {
    uint64_t n = 0;
    for (const auto& b : books_) n += b.strategy->resyncs();
    return n;
}

void Gateway::log_error(const char* what, const std::string& err) noexcept {
    if (!opts_.log_errors || errors_logged_ >= 20) return;
    ++errors_logged_;
    std::fprintf(stderr, "subplane-gateway: %s: %s%s\n", what, err.c_str(), errors_logged_ == 20 ? " (further errors not logged)" : "");
}

void Gateway::publish(uint8_t* msg, size_t len, uint64_t& counter) noexcept {
    const uint64_t seq = ++seq_;
    std::memcpy(msg + kPrefixSequence, &seq, 8);
    if (pub_.publish(msg, len)) { ++stats_.published; ++counter; }
    else ++stats_.dropped;
}

void Gateway::publish_heartbeat(uint64_t now) noexcept {
    uint8_t buf[sp_canonical::Heartbeat::kSize];
    const CanonicalMessage* m = find_message("Heartbeat");
    begin_message(buf, *m, 0, pack_.venue_id, 0, now);
    put_u64(buf, kPrefixExchangeTs, now);
    publish(buf, sizeof buf, stats_.heartbeats);
    last_heartbeat_ = now;
}

void Gateway::publish_definitions(uint64_t now) noexcept {
    const CanonicalMessage* m = find_message("InstrumentDefinition");
    for (const Instrument& in : ins_.all()) {
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
        publish(buf, sizeof buf, stats_.definitions);
    }
    last_definitions_ = now;
}

void Gateway::on_connected(uint64_t now) {
    if (rec_) rec_->event(now, "connected");
    publish_definitions(now);
}

void Gateway::on_disconnected(uint64_t now) {
    if (rec_) rec_->event(now, "disconnected");
    for (auto& b : books_) b.strategy->on_disconnect();
}

void Gateway::tick(uint64_t now) {
    if (opts_.heartbeat_ms && now - last_heartbeat_ >= opts_.heartbeat_ms * 1'000'000ull) publish_heartbeat(now);
    if (opts_.definitions_ms && last_definitions_ && now - last_definitions_ >= opts_.definitions_ms * 1'000'000ull) publish_definitions(now);
    for (auto& b : books_) {
        if (b.strategy->take_snapshot_request()) {
            requests_.push_back({b.ins->symbol, expand_symbol(b.sd->snapshot.url_tpl, b.ins->stream_symbol, b.ins->symbol)});
            ++stats_.snapshot_requests;
        }
    }
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

void Gateway::publish_emits(BookSlot& slot, uint64_t recv_ts) noexcept {
    for (const BookEmit& e : emits_) {
        if (e.kind == BookEmit::UPDATE) {
            uint8_t buf[sp_canonical::BookUpdate::kSize];
            const CanonicalMessage* m = find_message("BookUpdate");
            begin_message(buf, *m, slot.ins->id, pack_.venue_id, 0, recv_ts);
            put_u64(buf, kPrefixExchangeTs, e.exch_ts);
            put_i64(buf, find_field(*m, "Price")->offset, e.px);
            put_i64(buf, find_field(*m, "Qty")->offset, e.qty);
            put(buf, find_field(*m, "Side")->offset, static_cast<uint8_t>(e.side));
            put(buf, find_field(*m, "Action")->offset, static_cast<uint8_t>(e.action));
            put(buf, find_field(*m, "Level")->offset, e.level);
            publish(buf, sizeof buf, stats_.book_updates);
        } else {
            uint8_t buf[sp_canonical::BookSnapshot::kSize];
            const CanonicalMessage* m = find_message("BookSnapshot");
            begin_message(buf, *m, slot.ins->id, pack_.venue_id, 0, recv_ts);
            put_u64(buf, kPrefixExchangeTs, e.exch_ts);
            if (e.resynced) put(buf, kPrefixFlags, static_cast<uint16_t>(1));
            const Book& bk = slot.strategy->book();
            const uint16_t bpx = find_field(*m, "BidPx")->offset, bq = find_field(*m, "BidQty")->offset;
            const uint16_t apx = find_field(*m, "AskPx")->offset, aq = find_field(*m, "AskQty")->offset;
            const size_t nb = std::min<size_t>(10, bk.bids().size()), na = std::min<size_t>(10, bk.asks().size());
            for (size_t i = 0; i < nb; ++i) { put_i64(buf, static_cast<uint16_t>(bpx + 8 * i), bk.bids()[i].px); put_i64(buf, static_cast<uint16_t>(bq + 8 * i), bk.bids()[i].qty); }
            for (size_t i = 0; i < na; ++i) { put_i64(buf, static_cast<uint16_t>(apx + 8 * i), bk.asks()[i].px); put_i64(buf, static_cast<uint16_t>(aq + 8 * i), bk.asks()[i].qty); }
            put(buf, find_field(*m, "Depth")->offset, static_cast<uint8_t>(std::max(nb, na)));
            publish(buf, sizeof buf, stats_.book_snapshots);
        }
    }
    emits_.clear();
}

void Gateway::on_frame(std::string_view json, uint64_t recv_ts) {
    ++stats_.frames;
    if (rec_) rec_->frame(recv_ts, json);
    prepare(json);
    simdjson::ondemand::document doc;
    if (parser_.iterate(simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity())).get(doc) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
    simdjson::ondemand::object obj;
    if (doc.get_object().get(obj) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
    std::string_view stream;
    if (obj[pack_.connect.wrapper_stream].get_string().get(stream) != simdjson::SUCCESS) { ++stats_.acks; return; } // subscribe ack or error
    const size_t at = stream.find('@');
    if (at == std::string_view::npos) { ++stats_.unmatched_streams; return; }
    const StreamDef* sd = pack_.find_by_suffix(stream.substr(at));
    if (!sd) { ++stats_.unmatched_streams; return; }
    const Instrument* in = ins_.by_stream_symbol(stream.substr(0, at));
    if (!in) { ++stats_.unknown_symbols; return; }
    simdjson::ondemand::object data;
    if (obj[pack_.connect.wrapper_data].get_object().get(data) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }

    std::string err;
    if (sd->msg) {
        uint8_t buf[512];
        begin_message(buf, *sd->msg, in->id, pack_.venue_id, 0, recv_ts);
        if (!Mapper::map_flat(*sd, data, buf, err)) { ++stats_.map_errors; log_error(sd->message.c_str(), err); return; }
        uint64_t* counter = &stats_.other;
        if (sd->msg->template_id == sp_canonical::Trade::kTemplateId) counter = &stats_.trades;
        else if (sd->msg->template_id == sp_canonical::Quote::kTemplateId) counter = &stats_.quotes;
        publish(buf, sd->msg->size, *counter);
        return;
    }
    for (BookSlot& b : books_) {
        if (b.sd != sd || b.ins != in) continue;
        if (!b.strategy->on_event(data, recv_ts, emits_, err)) {
            if (!err.empty()) { ++stats_.book_errors; log_error("book", err); }
            emits_.clear();
            return;
        }
        publish_emits(b, recv_ts);
        return;
    }
}

void Gateway::on_snapshot(std::string_view venue_symbol, std::string_view body, uint64_t recv_ts) {
    if (rec_) rec_->snapshot(recv_ts, venue_symbol, body);
    const Instrument* in = ins_.by_symbol(venue_symbol);
    if (!in) { ++stats_.unknown_symbols; return; }
    for (BookSlot& b : books_) {
        if (b.ins != in) continue;
        prepare(body);
        simdjson::ondemand::document doc;
        simdjson::ondemand::object obj;
        if (parser_.iterate(simdjson::padded_string_view(buf_.data(), buf_.size(), buf_.capacity())).get(doc) != simdjson::SUCCESS ||
            doc.get_object().get(obj) != simdjson::SUCCESS) { ++stats_.parse_errors; return; }
        std::string err;
        const bool ok = b.strategy->on_snapshot(obj, recv_ts, emits_, err);
        if (!err.empty()) { ++stats_.book_errors; log_error("snapshot", err); }
        if (ok) ++stats_.snapshots_applied;
        publish_emits(b, recv_ts);
    }
}

} // namespace feeds
