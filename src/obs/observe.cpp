#include "obs/observe.h"

#include "obs/endpoint_key.h"

#include <algorithm>
#include <chrono>
#include <cstdio>

namespace feeds {

namespace {

uint64_t wall_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

// Every GatewayStats counter that is a plain count, in stats-line order.
template <typename F>
void each_counter(const GatewayStats& s, F&& f) {
    f("frames", "Text frames received from the exchange (or a recording)", s.frames);
    f("records", "Records found in frames (one frame may carry several)", s.records);
    f("acks", "Frames that carry no data: subscribe acks, status, pongs", s.acks);
    f("unmatched_streams", "Frames for a stream the pack does not define", s.unmatched_streams);
    f("unknown_symbols", "Records for a symbol not in the instrument list", s.unknown_symbols);
    f("parse_errors", "Frames that are not valid JSON objects", s.parse_errors);
    f("map_errors", "Records a field mapping failed on", s.map_errors);
    f("book_errors", "Book strategy errors (bad levels, missing kind)", s.book_errors);
    f("published", "Canonical messages delivered to the publisher", s.published);
    f("offers", "Publisher offers (one per batch)", s.offers);
    f("offer_bytes", "Bytes offered to the publisher", s.offer_bytes);
    f("trades", "Trade messages built", s.trades);
    f("quotes", "Quote messages built", s.quotes);
    f("book_updates", "BookUpdate messages built", s.book_updates);
    f("book_snapshots", "BookSnapshot messages built", s.book_snapshots);
    f("heartbeats", "Heartbeat messages built", s.heartbeats);
    f("definitions", "InstrumentDefinition messages built", s.definitions);
    f("other", "Messages of other canonical types built", s.other);
    f("snapshot_requests", "REST book snapshot requests issued", s.snapshot_requests);
    f("snapshots_applied", "REST book snapshots applied to a book", s.snapshots_applied);
    f("connects", "Connections to the exchange established", s.connects);
    f("disconnects", "Connections to the exchange lost or closed", s.disconnects);
    f("resyncs", "Book resynchronisations after a gap, summed over books", s.resyncs);
    f("book_overflows", "New levels refused because a book side already held max_levels (each a resync)", s.book_overflows);
    f("overflow_reconnects", "Reconnects to resync an overflowed in-band book", s.overflow_reconnects);
    f("state_messages", "SourceState messages offered on the state stream", s.state_messages);
    f("state_dropped", "SourceState messages the state stream did not take", s.state_dropped);
    f("republished", "Instruments republished in band after a Republish request", s.republished);
}

} // namespace

std::string json_str(std::string_view s) {
    std::string o = "\"";
    for (char c : s) {
        if (c == '"' || c == '\\') { o += '\\'; o += c; }
        else if (static_cast<unsigned char>(c) < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o += b; }
        else o += c;
    }
    return o + "\"";
}

bool Observer::valid_label_name(const std::string& k) {
    bool ok = !k.empty() && !(k[0] >= '0' && k[0] <= '9') && k.rfind("__", 0) != 0;
    for (char c : k) ok = ok && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_');
    return ok;
}

bool Observer::parse_labels(const std::string& csv, PromWriter::Labels& out, std::string& bad) {
    size_t at = 0;
    while (at <= csv.size()) {
        const size_t end = std::min(csv.find(',', at), csv.size());
        const std::string kv = csv.substr(at, end - at);
        at = end + 1;
        if (kv.empty()) continue;
        const size_t eq = kv.find('=');
        const std::string k = kv.substr(0, eq);
        if (eq == std::string::npos || !valid_label_name(k)) { bad = kv; return false; }
        out.emplace_back(k, kv.substr(eq + 1));
    }
    return true;
}

Observer::Observer(const Gateway& gw, const Pack& pack, const Instruments& ins, const Publisher& pub, Identity id,
                   std::string upstream_url, bool live, const ControlHandler* control, const ControlInput* control_in)
    : gw_(gw), pack_(pack), ins_(ins), pub_(pub), id_(std::move(id)), control_(control), live_(live) {
    if (control_in) { control_key_ = control_in->endpoint_key(id_.host); control_describe_ = control_in->describe(); }
    upstream_key_ = url_key(upstream_url);
    upstream_describe_ = (live ? "websocket " : "replay ") + upstream_url;
    if (live) {
        // INTENT: the REST snapshot endpoint is the exchange's recovery path for
        // books: losing it loses resyncs, not the live stream, hence role recap.
        for (const StreamDef& sd : pack.streams) {
            if (!sd.has_snapshot) continue;
            const std::string k = url_key(sd.snapshot.url_tpl);
            if (std::find(recap_keys_.begin(), recap_keys_.end(), k) == recap_keys_.end()) recap_keys_.push_back(k);
        }
    }
    downstream_key_ = pub.endpoint_key(id_.host);
    if (const Publisher* sp = pub.state_publisher()) state_key_ = sp->endpoint_key(id_.host);
}

std::string Observer::data_state() const {
    if (!live_) return "up";
    return gw_.stats().connected ? "up" : "down";
}

std::string Observer::publication_state() const { return pub_.connected() ? "up" : "down"; }

bool Observer::healthy(std::string& reason) const {
    if (live_ && !gw_.stats().connected) { reason = "not connected to " + upstream_key_; return false; }
    return true;
}

std::string Observer::metrics_text() const {
    const GatewayStats& s = gw_.stats();
    PromWriter w({{"service", id_.service}, {"instance", id_.instance}, {"host", id_.host}});
    PromWriter::Labels info = {{"version", id_.version}, {"epoch", std::to_string(id_.epoch)}, {"site", id_.site}, {"env", id_.env},
                               {"venue", pack_.exchange}};
    info.insert(info.end(), id_.labels.begin(), id_.labels.end());
    w.gauge("subplane_service_info", "Identity of this service; always 1", info, 1);
    const uint64_t now = wall_ns();
    w.gauge("subplane_uptime_seconds", "Seconds since the gateway started", {},
            now > id_.epoch ? static_cast<double>((now - id_.epoch) / 1'000'000'000ull) : 0.0);

    const PromWriter::Labels v = {{"venue", pack_.exchange}};
    each_counter(s, [&](const char* n, const char* help, uint64_t x) { w.counter("subplane_gateway_" + std::string(n) + "_total", help, v, x); });
    const char* drop_help = "Canonical messages not delivered, by reason";
    w.counter("subplane_gateway_dropped_total", drop_help, {{"venue", pack_.exchange}, {"reason", "back_pressured"}}, s.dropped_back_pressured);
    w.counter("subplane_gateway_dropped_total", drop_help, {{"venue", pack_.exchange}, {"reason", "not_connected"}}, s.dropped_not_connected);
    w.counter("subplane_gateway_dropped_total", drop_help, {{"venue", pack_.exchange}, {"reason", "failed"}}, s.dropped_failed);
    w.gauge("subplane_gateway_connected", "1 while connected to the exchange", v, s.connected ? 1 : 0);
    w.gauge("subplane_gateway_publication_connected", "1 while a subscriber is connected to the output", v, pub_.connected() ? 1 : 0);
    w.gauge("subplane_gateway_last_sequence", "Sequence of the last message offered", v, static_cast<double>(static_cast<uint64_t>(s.last_sequence)));
    w.gauge("subplane_gateway_instruments", "Instruments configured", v, static_cast<double>(ins_.all().size()));
    const char* state_help = "Instruments by SourceState: ok, stale (lost since it was ok), unknown (not yet ok)";
    w.gauge("subplane_gateway_instrument_state", state_help, {{"venue", pack_.exchange}, {"state", "ok"}}, static_cast<double>(static_cast<uint64_t>(s.instruments_ok)));
    w.gauge("subplane_gateway_instrument_state", state_help, {{"venue", pack_.exchange}, {"state", "stale"}}, static_cast<double>(static_cast<uint64_t>(s.instruments_stale)));
    w.gauge("subplane_gateway_instrument_state", state_help, {{"venue", pack_.exchange}, {"state", "unknown"}}, static_cast<double>(static_cast<uint64_t>(s.instruments_unknown)));
    const char* rp_help = "Republish requests by result";
    w.counter("subplane_gateway_republish_requests_total", rp_help, {{"venue", pack_.exchange}, {"result", "queued"}}, s.republish_queued);
    w.counter("subplane_gateway_republish_requests_total", rp_help, {{"venue", pack_.exchange}, {"result", "not_found"}}, s.republish_not_found);
    w.histogram_ns("subplane_gateway_frame_to_offer_seconds", "Frame entry to the end of its last offer, frames that published",
                   v, gw_.frame_latency().snapshot());
    w.histogram_ns("subplane_gateway_recv_to_offer_seconds", "Frame receive (RecvTs, CLOCK_REALTIME) to the end of its last offer",
                   v, gw_.recv_to_offer_rt().snapshot());

    const char* edge_help = "An upstream or downstream endpoint: 1 up, 0 not up";
    w.gauge("subplane_topology_edge", edge_help, {{"direction", "upstream"}, {"role", "data"}, {"key", upstream_key_}, {"venue", pack_.exchange}},
            data_state() == "up" ? 1 : 0);
    for (const auto& k : recap_keys_) {
        // A REST endpoint has no connection to watch: reported up, its failures show in snapshot counters.
        w.gauge("subplane_topology_edge", edge_help, {{"direction", "upstream"}, {"role", "recap"}, {"key", k}, {"venue", pack_.exchange}}, 1);
    }
    if (!downstream_key_.empty()) {
        w.gauge("subplane_topology_edge", edge_help, {{"direction", "downstream"}, {"role", "data"}, {"key", downstream_key_}, {"venue", pack_.exchange}},
                pub_.connected() ? 1 : 0);
    }
    if (control_) {
        const ControlStats& c = control_->stats();
        const auto cc = [&](const char* n, const char* help, uint64_t x) { w.counter("subplane_gateway_control_" + std::string(n) + "_total", help, v, x); };
        cc("frames", "SPP control frames received", c.frames);
        cc("malformed", "SPP control frames or messages that did not parse", c.malformed);
        cc("hellos", "SPP Hello messages received", c.hellos);
        cc("republish", "SPP Republish requests received", c.republish);
        cc("unsupported", "SPP messages answered Status{Unsupported}", c.unsupported);
        cc("replies", "SPP reply frames offered to a requester", c.replies);
        cc("reply_dropped", "SPP reply frames not delivered", c.reply_dropped);
        w.gauge("subplane_gateway_control_peers", "Requesters connected to the SPP control input", v, static_cast<double>(static_cast<uint64_t>(c.peers)));
        if (!control_key_.empty()) {
            w.gauge("subplane_topology_edge", edge_help, {{"direction", "downstream"}, {"role", "recap"}, {"key", control_key_}, {"venue", pack_.exchange}},
                    c.peers ? 1 : 0);
        }
    }
    if (!state_key_.empty()) {
        w.gauge("subplane_topology_edge", edge_help, {{"direction", "downstream"}, {"role", "recap"}, {"key", state_key_}, {"venue", pack_.exchange}},
                pub_.state_publisher()->connected() ? 1 : 0);
    }
    return w.str();
}

std::string Observer::metrics_json() const {
    const GatewayStats& s = gw_.stats();
    std::string o = "{\"venue\":" + json_str(pack_.exchange) + ",\"counters\":{";
    bool first = true;
    each_counter(s, [&](const char* n, const char*, uint64_t x) {
        o += first ? "" : ",";
        first = false;
        o += "\"" + std::string(n) + "\":" + std::to_string(x);
    });
    o += ",\"dropped\":" + std::to_string(static_cast<uint64_t>(s.dropped));
    o += ",\"dropped_back_pressured\":" + std::to_string(static_cast<uint64_t>(s.dropped_back_pressured));
    o += ",\"dropped_not_connected\":" + std::to_string(static_cast<uint64_t>(s.dropped_not_connected));
    o += ",\"dropped_failed\":" + std::to_string(static_cast<uint64_t>(s.dropped_failed));
    o += "},\"connected\":" + std::string(s.connected ? "true" : "false");
    o += ",\"publication_connected\":" + std::string(pub_.connected() ? "true" : "false");
    o += ",\"last_sequence\":" + std::to_string(static_cast<uint64_t>(s.last_sequence));
    o += ",\"frame_to_offer_ns\":" + to_json(gw_.frame_latency().snapshot(), "frame_to_offer_ns");
    o += ",\"recv_to_offer_ns\":" + to_json(gw_.recv_to_offer_rt().snapshot(), "recv_to_offer_ns");
    return o + "}";
}

std::string Observer::topology_json() const {
    std::string o = "{\"schema\":\"subplane.topology/1\",\"node\":{";
    o += "\"id\":" + json_str(id_.host + "/" + id_.service + "/" + id_.instance);
    o += ",\"service\":" + json_str(id_.service) + ",\"instance\":" + json_str(id_.instance) + ",\"host\":" + json_str(id_.host);
    o += ",\"pid\":" + std::to_string(id_.pid) + ",\"version\":" + json_str(id_.version) + ",\"epoch\":" + std::to_string(id_.epoch);
    o += ",\"site\":" + json_str(id_.site) + ",\"env\":" + json_str(id_.env) + "},\"upstream\":[";
    const std::string local = "{\"venue\":" + json_str(pack_.exchange) + ",\"venue_id\":" + std::to_string(pack_.venue_id) +
                              ",\"instruments\":" + std::to_string(ins_.all().size()) + "}";
    o += "{\"key\":" + json_str(upstream_key_) + ",\"role\":\"data\",\"state\":" + json_str(data_state()) + ",\"local\":" + local +
         ",\"describe\":" + json_str(upstream_describe_) + "}";
    for (const auto& k : recap_keys_) {
        o += ",{\"key\":" + json_str(k) + ",\"role\":\"recap\",\"state\":\"up\",\"local\":" + local + ",\"describe\":\"rest book snapshots\"}";
    }
    o += "],\"downstream\":[";
    if (!downstream_key_.empty()) {
        // INTENT: an Aeron publication knows only whether some subscriber is
        // connected, not who. Who reads it is the collector's join: the
        // agent's upstream edge with this same key.
        o += "{\"key\":" + json_str(downstream_key_) + ",\"role\":\"data\",\"state\":" + json_str(publication_state()) +
             ",\"local\":{\"venue\":" + json_str(pack_.exchange) + "},\"describe\":" + json_str(pub_.describe()) + "}";
    }
    if (!state_key_.empty()) {
        // The state stream: losing it loses recovery (SourceState), not data, hence role recap.
        const Publisher* sp = pub_.state_publisher();
        o += std::string(downstream_key_.empty() ? "" : ",") + "{\"key\":" + json_str(state_key_) + ",\"role\":\"recap\",\"state\":" +
             json_str(sp->connected() ? "up" : "down") + ",\"local\":{\"venue\":" + json_str(pack_.exchange) + "},\"describe\":" +
             json_str(sp->describe() + " (SourceState)") + "}";
    }
    if (!control_key_.empty()) {
        // INTENT: downstream, as the snap's recap channel is: a service lists what it
        // serves downstream and what it uses upstream, so the requester (spwsgw-snap,
        // upstream there) joins on this key. Republish requests come in here (SPP);
        // losing it loses recovery, not data.
        o += std::string(downstream_key_.empty() && state_key_.empty() ? "" : ",") + "{\"key\":" + json_str(control_key_) +
             ",\"role\":\"recap\",\"state\":" + json_str(control_ && control_->stats().peers ? "up" : "down") +
             ",\"local\":{\"venue\":" + json_str(pack_.exchange) + "},\"describe\":" + json_str(control_describe_ + " (SPP Republish)") + "}";
    }
    return o + "]}";
}

} // namespace feeds
