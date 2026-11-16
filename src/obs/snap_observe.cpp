#include "obs/snap_observe.h"

#include <chrono>

namespace feeds {

namespace {

uint64_t wall_ns() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

// Every SnapStats counter that is a plain count, in stats-line order.
template <typename F>
void each_counter(const SnapStats& s, F&& f) {
    f("data_frames", "Offers received on the gateway's data stream", s.data_frames);
    f("messages", "Canonical messages applied or checked", s.messages);
    f("gaps", "Sequence gaps on the data stream (every Ok instrument goes Stale)", s.gaps);
    f("restarts", "Gateway restarts seen (a new SourceEpoch or Sequence starting again)", s.restarts);
    f("malformed", "Data or state offers that did not parse", s.malformed);
    f("book_overflows", "Levels refused because a book side already held --book-levels (the instrument goes Stale)", s.book_overflows);
    f("state_messages", "SourceState messages received", s.state_messages);
    f("states_applied", "SourceState messages applied once the data reached their Sequence", s.states_applied);
    f("states_ignored", "SourceState{Ok} ignored for a book this copy did not see whole since its last gap", s.states_ignored);
    f("republish_asks", "Republish asks made to the gateway (one instrument, or all)", s.republish_asks);
}

template <typename F>
void each_recap(const ControlStats& c, F&& f) {
    f("frames", "SPP recap request frames received", c.frames);
    f("resolves", "SPP Resolve requests answered from the instruments file (--instruments)", c.resolves);
    f("not_here", "RecapRequests refused at once: the instruments file puts the instrument on another partition, or does not list it", c.not_here);
    f("malformed", "SPP recap frames or messages that did not parse", c.malformed);
    f("hellos", "SPP Hello messages received", c.hellos);
    f("unsupported", "SPP messages answered Status{Unsupported}", c.unsupported);
    f("replies", "SPP reply frames offered to a requester", c.replies);
    f("reply_dropped", "SPP reply frames not delivered", c.reply_dropped);
}

template <typename F>
void each_link(const LinkStats& l, F&& f) {
    f("hellos_sent", "SPP Hello requests sent to the gateway", l.hellos_sent);
    f("republish_sent", "SPP Republish requests sent to the gateway", l.republish_sent);
    f("offer_failed", "Requests the gateway's control channel did not take (retried)", l.offer_failed);
    f("hellos", "SPP Hello replies from the gateway", l.hellos);
    f("status_ok", "Status{Ok} replies from the gateway", l.status_ok);
    f("status_not_found", "Status{NotFound} replies from the gateway", l.status_not_found);
    f("status_other", "Other Status replies from the gateway", l.status_other);
    f("malformed", "Gateway replies that did not parse", l.malformed);
}

std::string up(bool b) { return b ? "up" : "down"; }

} // namespace

SnapObserver::SnapObserver(const SnapCore& core, const ControlStats& recap, const GatewayLink* link, const SnapLive& live, Identity id, SnapEdges edges)
    : core_(core), recap_(recap), link_(link), live_(live), id_(std::move(id)), e_(std::move(edges)) {}

bool SnapObserver::healthy(std::string& reason) const {
    if (!live_.data_images) { reason = "no data from " + e_.data_key; return false; }
    return true;
}

std::string SnapObserver::metrics_text() const {
    const SnapStats& s = core_.stats();
    PromWriter w({{"service", id_.service}, {"instance", id_.instance}, {"host", id_.host}});
    PromWriter::Labels info = {{"version", id_.version}, {"epoch", std::to_string(id_.epoch)}, {"site", id_.site}, {"env", id_.env}};
    info.insert(info.end(), id_.labels.begin(), id_.labels.end());
    w.gauge("subplane_service_info", "Identity of this service; always 1", info, 1);
    const uint64_t now = wall_ns();
    w.gauge("subplane_uptime_seconds", "Seconds since the snapshot service started", {},
            now > id_.epoch ? static_cast<double>((now - id_.epoch) / 1'000'000'000ull) : 0.0);

    each_counter(s, [&](const char* n, const char* help, uint64_t x) { w.counter("subplane_snap_" + std::string(n) + "_total", help, {}, x); });
    w.gauge("subplane_snap_last_sequence", "Sequence of the last data message applied (a recap's AsOf)", {}, static_cast<double>(static_cast<uint64_t>(s.last_sequence)));
    w.gauge("subplane_snap_data_connected", "1 while the gateway's data stream has an image", {}, live_.data_images ? 1 : 0);
    w.gauge("subplane_snap_state_connected", "1 while the gateway's state stream has an image", {}, live_.state_images ? 1 : 0);
    const char* state_help = "Instruments by state: ok (served), stale (lost since it was ok), unknown (not yet ok)";
    w.gauge("subplane_snap_instrument_state", state_help, {{"state", "ok"}}, static_cast<double>(static_cast<uint64_t>(s.instruments_ok)));
    w.gauge("subplane_snap_instrument_state", state_help, {{"state", "stale"}}, static_cast<double>(static_cast<uint64_t>(s.instruments_stale)));
    w.gauge("subplane_snap_instrument_state", state_help, {{"state", "unknown"}}, static_cast<double>(static_cast<uint64_t>(s.instruments_unknown)));

    const char* rq_help = "RecapRequests for one instrument by result: queued (its section is due after the gather window), "
                          "coalesced (already wanted), unknown_instrument (never seen here)";
    const uint64_t queued = s.requests - s.wants_coalesced - s.rejects_unknown;
    w.counter("subplane_snap_recap_requests_total", rq_help, {{"result", "queued"}}, queued);
    w.counter("subplane_snap_recap_requests_total", rq_help, {{"result", "coalesced"}}, s.wants_coalesced);
    w.counter("subplane_snap_recap_requests_total", rq_help, {{"result", "unknown_instrument"}}, s.rejects_unknown);
    w.counter("subplane_snap_recap_bulk_requests_total", "RecapRequests for every instrument (Bulk)", {}, s.bulk_requests);
    w.counter("subplane_snap_sections_total", "Sections built: one instrument's state, published once for every agent that wants it", {}, s.sections);
    w.counter("subplane_snap_section_bytes_total", "Bytes of the sections built", {}, s.section_bytes);
    w.gauge("subplane_snap_recap_wanted", "Instruments with recap interest whose section is not out yet", {}, static_cast<double>(static_cast<uint64_t>(s.wanted)));
    w.gauge("subplane_snap_recap_waiting", "Of those, instruments whose section is due but that are not Ok yet", {}, static_cast<double>(static_cast<uint64_t>(s.waiting)));
    w.counter("subplane_snap_sections_offered_total", "Sections the section stream took", {}, live_.sections_offered);
    w.counter("subplane_snap_section_bytes_offered_total", "Bytes of the sections the section stream took", {}, live_.section_bytes_offered);
    w.counter("subplane_snap_sections_back_pressured_total", "Section offers retried because a subscriber is behind", {}, live_.sections_back_pressured);
    w.counter("subplane_snap_sections_dropped_total", "Sections not delivered: nobody subscribed, or the publication failed", {}, live_.sections_dropped);
    w.counter("subplane_snap_sections_throttled_total", "Waits for the section rate budget (--section-rate)", {}, live_.sections_throttled);
    w.gauge("subplane_snap_sections_connected", "1 while an agent subscribes to the section stream", {}, live_.sections_connected ? 1 : 0);
    each_recap(recap_, [&](const char* n, const char* help, uint64_t x) { w.counter("subplane_snap_recap_" + std::string(n) + "_total", help, {}, x); });
    w.gauge("subplane_snap_recap_peers", "Requesters connected to the recap channel", {}, static_cast<double>(static_cast<uint64_t>(recap_.peers)));
    if (link_) {
        each_link(link_->stats(), [&](const char* n, const char* help, uint64_t x) { w.counter("subplane_snap_gateway_" + std::string(n) + "_total", help, {}, x); });
        w.gauge("subplane_snap_gateway_connected", "1 while the gateway's control channel takes requests", {}, live_.gateway_connected ? 1 : 0);
    }

    const char* edge_help = "An upstream or downstream endpoint: 1 up, 0 not up";
    w.gauge("subplane_topology_edge", edge_help, {{"direction", "upstream"}, {"role", "data"}, {"key", e_.data_key}}, live_.data_images ? 1 : 0);
    w.gauge("subplane_topology_edge", edge_help, {{"direction", "upstream"}, {"role", "recap"}, {"key", e_.state_key}}, live_.state_images ? 1 : 0);
    if (link_ && !e_.gateway_key.empty())
        w.gauge("subplane_topology_edge", edge_help, {{"direction", "upstream"}, {"role", "recap"}, {"key", e_.gateway_key}}, live_.gateway_connected ? 1 : 0);
    if (!e_.recap_key.empty())
        w.gauge("subplane_topology_edge", edge_help, {{"direction", "downstream"}, {"role", "recap"}, {"key", e_.recap_key}}, recap_.peers ? 1 : 0);
    return w.str();
}

std::string SnapObserver::metrics_json() const {
    const SnapStats& s = core_.stats();
    std::string o = "{\"counters\":{";
    bool first = true;
    each_counter(s, [&](const char* n, const char*, uint64_t x) {
        o += first ? "" : ",";
        first = false;
        o += "\"" + std::string(n) + "\":" + std::to_string(x);
    });
    const auto num = [](uint64_t v) { return std::to_string(v); };
    o += "},\"recap\":{\"requests\":" + num(s.requests) + ",\"coalesced\":" + num(s.wants_coalesced) +
         ",\"unknown_instrument\":" + num(s.rejects_unknown) + ",\"bulk_requests\":" + num(s.bulk_requests) +
         ",\"sections\":" + num(s.sections) + ",\"section_bytes\":" + num(s.section_bytes) + ",\"wanted\":" + num(s.wanted) +
         ",\"waiting\":" + num(s.waiting) + ",\"sections_offered\":" + num(live_.sections_offered) +
         ",\"sections_back_pressured\":" + num(live_.sections_back_pressured) + ",\"sections_dropped\":" + num(live_.sections_dropped) +
         ",\"sections_throttled\":" + num(live_.sections_throttled) + ",\"sections_connected\":" + std::string(live_.sections_connected ? "true" : "false");
    each_recap(recap_, [&](const char* n, const char*, uint64_t x) { o += ",\"" + std::string(n) + "\":" + std::to_string(x); });
    o += ",\"peers\":" + std::to_string(static_cast<uint64_t>(recap_.peers)) + "}";
    if (link_) {
        o += ",\"gateway\":{\"connected\":" + std::string(live_.gateway_connected ? "true" : "false");
        each_link(link_->stats(), [&](const char* n, const char*, uint64_t x) { o += ",\"" + std::string(n) + "\":" + std::to_string(x); });
        o += "}";
    }
    o += ",\"instruments\":{\"ok\":" + std::to_string(static_cast<uint64_t>(s.instruments_ok)) + ",\"stale\":" + std::to_string(static_cast<uint64_t>(s.instruments_stale)) +
         ",\"unknown\":" + std::to_string(static_cast<uint64_t>(s.instruments_unknown)) + "}";
    o += ",\"data_connected\":" + std::string(live_.data_images ? "true" : "false");
    o += ",\"state_connected\":" + std::string(live_.state_images ? "true" : "false");
    o += ",\"last_sequence\":" + std::to_string(static_cast<uint64_t>(s.last_sequence));
    return o + "}";
}

std::string SnapObserver::topology_json() const {
    const SnapStats& s = core_.stats();
    std::string o = "{\"schema\":\"subplane.topology/1\",\"node\":{";
    o += "\"id\":" + json_str(id_.host + "/" + id_.service + "/" + id_.instance);
    o += ",\"service\":" + json_str(id_.service) + ",\"instance\":" + json_str(id_.instance) + ",\"host\":" + json_str(id_.host);
    o += ",\"pid\":" + std::to_string(id_.pid) + ",\"version\":" + json_str(id_.version) + ",\"epoch\":" + std::to_string(id_.epoch);
    o += ",\"site\":" + json_str(id_.site) + ",\"env\":" + json_str(id_.env) + "},\"upstream\":[";
    const uint64_t n = s.instruments_ok + s.instruments_stale + s.instruments_unknown;
    const std::string local = "{\"instruments\":" + std::to_string(n) + "}";
    const auto edge = [&](const std::string& key, const char* role, const std::string& state, const std::string& describe) {
        return "{\"key\":" + json_str(key) + ",\"role\":\"" + role + "\",\"state\":" + json_str(state) + ",\"local\":" + local +
               ",\"describe\":" + json_str(describe) + "}";
    };
    o += edge(e_.data_key, "data", up(live_.data_images != 0), e_.data_describe);
    // The state stream and the gateway's Republish input: losing either loses recovery, not data.
    o += "," + edge(e_.state_key, "recap", up(live_.state_images != 0), e_.state_describe);
    if (link_ && !e_.gateway_key.empty()) o += "," + edge(e_.gateway_key, "recap", up(live_.gateway_connected != 0), e_.gateway_describe);
    o += "],\"downstream\":[";
    // INTENT: the recap channel is keyed as the agent keys its partition's recap
    // request channel (upstream there), so the collector joins agent to snap.
    if (!e_.recap_key.empty()) o += edge(e_.recap_key, "recap", up(recap_.peers != 0), e_.recap_describe);
    return o + "]}";
}

} // namespace feeds
