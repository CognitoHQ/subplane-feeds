// spwsgw-snap: a gateway's snapshot service (docs/snap.md). A silent, untethered
// subscriber of one gateway's canonical data stream and SPP state stream that
// rebuilds every instrument from the stream alone, takes the agents'
// RecapRequests as interest, and publishes each wanted instrument once, as a
// section, on its section stream, paced. Asks the gateway to Republish what it
// cannot trust.
//
//   spwsgw-snap --recap-channel aeron:udp?endpoint=H:P --recap-response H:P --section-channel C
//               [--section-stream 1103] [--section-term-length N] [--section-offer-bytes 65536]
//               [--section-rate 25000000] [--section-burst 1048576]
//               [--gather-ms 20] [--bulk-gather-ms 1000]
//               [--gateway-control aeron:udp?endpoint=H:P --gateway-response H:P]
//               [--data-channel aeron:ipc] [--data-stream 1001] [--state-stream 1002]
//               [--recap-stream 1101] [--recap-response-stream 1102]
//               [--gateway-control-stream 1003] [--gateway-response-stream 1004]
//               [--book-levels 5000] [--ask-every-ms 1000] [--name spwsgw-snap]
//               [--aeron-dir D] [--embedded-driver] [--seconds N] [--spin] [--quiet]
//               [--http 127.0.0.1:9102] [--site S] [--env E] [--labels k=v,...]

#include "aeron_io.h"
#include "obs/endpoint_key.h"
#include "obs/snap_observe.h"
#include "publisher.h"
#include "snap/gateway_link.h"
#include "snap/rate_budget.h"
#include "snap/recap_server.h"
#include "snap/snap.h"
#if FEEDS_WITH_NETWORK
#include "net.h"
#endif

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <thread>

#include <unistd.h>

namespace {

using namespace feeds;

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

void usage() {
    std::fputs("usage: spwsgw-snap --recap-channel aeron:udp?endpoint=H:P --recap-response H:P --section-channel C\n"
               "                   [--section-stream N] [--section-term-length N] [--section-offer-bytes N] [--section-rate BYTES_PER_S] [--section-burst BYTES]\n"
               "                   [--gather-ms N] [--bulk-gather-ms N]\n"
               "                   [--gateway-control aeron:udp?endpoint=H:P --gateway-response H:P]\n"
               "                   [--data-channel C] [--data-stream N] [--state-stream N] [--recap-stream N] [--recap-response-stream N]\n"
               "                   [--gateway-control-stream N] [--gateway-response-stream N] [--recap-term-length N] [--recap-max-pending N]\n"
               "                   [--book-levels N] [--ask-every-ms N] [--name S] [--aeron-dir D] [--embedded-driver] [--seconds N] [--spin] [--quiet]\n"
               "                   [--http HOST:PORT] [--site S] [--env E] [--labels k=v,...]\n", stderr);
}

// INTENT: tether=false: a slow or stalled snapshot service must never hold back
// the gateway's publication (and so the agents on it); it falls behind, loses
// its image, and recovers by Republish like any other gap.
std::string untethered(const std::string& ch) {
    if (ch.find("tether=") != std::string::npos) return ch;
    return ch + (ch.find('?') == std::string::npos ? "?" : "|") + "tether=false";
}

void print_stats(const SnapCore& core, const GatewayLink* link, const RecapServer& server, FILE* f) {
    const SnapStats& s = core.stats();
    std::fprintf(f, "spwsgw-snap: messages=%llu gaps=%llu restarts=%llu malformed=%llu states=%llu applied=%llu ignored=%llu overflows=%llu "
                    "ok=%llu stale=%llu unknown=%llu requests=%llu bulk=%llu coalesced=%llu unknown_instrument=%llu sections=%llu waiting=%llu asks=%llu seq=%llu",
                 (unsigned long long)s.messages, (unsigned long long)s.gaps, (unsigned long long)s.restarts, (unsigned long long)s.malformed,
                 (unsigned long long)s.state_messages, (unsigned long long)s.states_applied, (unsigned long long)s.states_ignored, (unsigned long long)s.book_overflows,
                 (unsigned long long)s.instruments_ok, (unsigned long long)s.instruments_stale, (unsigned long long)s.instruments_unknown,
                 (unsigned long long)s.requests, (unsigned long long)s.bulk_requests, (unsigned long long)s.wants_coalesced, (unsigned long long)s.rejects_unknown,
                 (unsigned long long)s.sections, (unsigned long long)s.waiting, (unsigned long long)s.republish_asks, (unsigned long long)core.last_sequence());
    if (link) std::fprintf(f, " republish_sent=%llu gateway_ok=%llu", (unsigned long long)link->stats().republish_sent, (unsigned long long)link->stats().status_ok);
    std::fprintf(f, " reply_dropped=%llu\n", (unsigned long long)server.stats().reply_dropped);
}

} // namespace

int main(int argc, char** argv) {
    std::map<std::string, std::string> a;
    std::set<std::string> flags;
    const std::set<std::string> boolean = {"embedded-driver", "spin", "quiet", "help"};
    const std::set<std::string> valued = {"recap-channel", "recap-response", "recap-stream", "recap-response-stream", "recap-term-length", "recap-max-pending",
                                          "section-channel", "section-stream", "section-term-length", "section-offer-bytes", "section-rate", "section-burst", "gather-ms", "bulk-gather-ms",
                                          "gateway-control", "gateway-response", "gateway-control-stream", "gateway-response-stream",
                                          "data-channel", "data-stream", "state-stream", "book-levels", "ask-every-ms", "name", "aeron-dir", "seconds",
                                          "http", "site", "env", "labels"};
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        if (k.rfind("--", 0) != 0) { usage(); return 2; }
        k = k.substr(2);
        if (boolean.count(k)) { flags.insert(k); continue; }
        if (!valued.count(k) || i + 1 >= argc) { usage(); return 2; }
        a[k] = argv[++i];
    }
    if (flags.count("help")) { usage(); return 0; }
    auto get = [&](const char* k, const char* def) { auto it = a.find(k); return it == a.end() ? std::string(def) : it->second; };
    auto getu = [&](const char* k, uint64_t def) { auto it = a.find(k); return it == a.end() ? def : std::strtoull(it->second.c_str(), nullptr, 10); };
    const bool quiet = flags.count("quiet") != 0;
    if (!a.count("recap-channel") || !a.count("recap-response") || !a.count("section-channel")) { usage(); return 2; }
    if (a.count("gateway-control") != a.count("gateway-response")) { std::fputs("spwsgw-snap: --gateway-control and --gateway-response go together\n", stderr); return 2; }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    const uint64_t epoch = now_ns();
    const std::string name = get("name", "spwsgw-snap");
    std::string err;

    AeronClient client;
    AeronClient::Config cc;
    cc.aeron_dir = get("aeron-dir", "");
    cc.embedded_driver = flags.count("embedded-driver") != 0;
    cc.client_name = name;
    if (!client.open(cc, err)) { std::fprintf(stderr, "spwsgw-snap: %s\n", err.c_str()); return 1; }

    const std::string data_channel = untethered(get("data-channel", "aeron:ipc"));
    AeronSubscriber data, state;
    if (!data.open(client.get(), data_channel, static_cast<int32_t>(getu("data-stream", 1001)), cc.driver_timeout_ms, err) ||
        !state.open(client.get(), data_channel, static_cast<int32_t>(getu("state-stream", 1002)), cc.driver_timeout_ms, err)) {
        std::fprintf(stderr, "spwsgw-snap: %s\n", err.c_str());
        return 1;
    }

    SnapCore core(static_cast<uint32_t>(getu("book-levels", Book::kDefaultMaxLevels)));
    core.set_gather({getu("gather-ms", 20) * 1'000'000ull, getu("bulk-gather-ms", 1000) * 1'000'000ull});
    RecapServer server(core, name, epoch);

    // INTENT: the section stream is shared by every agent on the partition.
    // A section (a whole book: about 100 bytes a level) goes out in offers of
    // at most --section-offer-bytes, cut at message boundaries: Aeron's largest
    // message is an eighth of a term, and smaller offers pace evenly. One
    // section is finished before the next starts, so sections never
    // interleave and an agent assembles each across offers. Flow control stays
    // the channel's (fc=max by default on multicast: a slow agent loses a
    // section and asks again, it never slows the others); the pace is
    // --section-rate.
    std::string section_channel = a["section-channel"];
    if (section_channel.find("term-length=") == std::string::npos)
        section_channel += (section_channel.find('?') == std::string::npos ? "?" : "|") + std::string("term-length=") + std::to_string(getu("section-term-length", 4u << 20));
    const size_t offer_bytes = static_cast<size_t>(getu("section-offer-bytes", 64u << 10));
    AeronPublication sections;
    if (!sections.open(client.get(), section_channel, static_cast<int32_t>(getu("section-stream", 1103)), cc.driver_timeout_ms, err)) {
        std::fprintf(stderr, "spwsgw-snap: section stream: %s\n", err.c_str());
        return 1;
    }
    TokenBucket budget(getu("section-rate", 25'000'000), getu("section-burst", 1u << 20));
    AeronControlConfig rc;
    rc.channel = a["recap-channel"];
    rc.stream_id = static_cast<int32_t>(getu("recap-stream", 1101));
    rc.response_control = a["recap-response"];
    rc.response_stream_id = static_cast<int32_t>(getu("recap-response-stream", 1102));
    // Answers on the response channel are small (Hello, rejects): the books go
    // out as sections, on the section stream.
    rc.term_length = static_cast<uint32_t>(getu("recap-term-length", 64u << 10));
    rc.max_pending = static_cast<size_t>(getu("recap-max-pending", 64u << 10));
    auto recap = make_aeron_control(client.get(), client.dir(), rc, err);
    if (!recap) { std::fprintf(stderr, "spwsgw-snap: %s\n", err.c_str()); return 1; }

    AeronRequester gateway;
    std::unique_ptr<GatewayLink> link;
    if (a.count("gateway-control")) {
        AeronRequester::Config gc;
        gc.request_channel = a["gateway-control"];
        gc.request_stream_id = static_cast<int32_t>(getu("gateway-control-stream", 1003));
        gc.response_control = a["gateway-response"];
        gc.response_stream_id = static_cast<int32_t>(getu("gateway-response-stream", 1004));
        if (!gateway.open(client.get(), gc, err)) { std::fprintf(stderr, "spwsgw-snap: gateway control: %s\n", err.c_str()); return 1; }
        link = std::make_unique<GatewayLink>(name, epoch);
    }
    if (!quiet) std::fprintf(stderr, "spwsgw-snap: data %s streams %d/%d, recap %s, sections %s stream %d; %s\n", data_channel.c_str(), data.stream_id(), state.stream_id(),
                             recap->describe().c_str(), section_channel.c_str(), sections.stream_id(),
                             link ? ("gateway control " + gateway.config().request_channel).c_str() : "no gateway control (no Republish asks)");

    // Observability (docs/observability.md): the HTTP thread reads only OneWriter values.
    SnapLive live;
    Identity id;
    id.service = "spwsgw-snap";
    {
        char host[256] = {};
        id.host = ::gethostname(host, sizeof host - 1) == 0 ? host : "unknown";
    }
    id.instance = name;
    id.version = FEEDS_VERSION;
    id.site = get("site", "");
    id.env = get("env", "");
    id.epoch = epoch;
    id.pid = static_cast<int32_t>(::getpid());
    if (std::string bad; !Observer::parse_labels(get("labels", ""), id.labels, bad)) {
        std::fprintf(stderr, "spwsgw-snap: --labels: invalid label '%s' (want name=value)\n", bad.c_str());
        return 2;
    }
    SnapEdges edges;
    edges.data_key = aeron_key(data_channel, data.stream_id(), client.dir(), id.host);
    edges.data_describe = "aeron " + data_channel + " stream " + std::to_string(data.stream_id());
    edges.state_key = aeron_key(data_channel, state.stream_id(), client.dir(), id.host);
    edges.state_describe = "aeron " + data_channel + " stream " + std::to_string(state.stream_id()) + " (SourceState)";
    if (link) {
        edges.gateway_key = aeron_key(gateway.config().request_channel, gateway.config().request_stream_id, client.dir(), id.host);
        edges.gateway_describe = "aeron " + gateway.config().request_channel + " stream " + std::to_string(gateway.config().request_stream_id) + " (SPP Republish)";
    }
    edges.recap_key = recap->endpoint_key(id.host);
    edges.recap_describe = recap->describe() + " (SPP recap)";
    const SnapObserver observer(core, server.stats(), link.get(), live, id, edges);
#if FEEDS_WITH_NETWORK
    HttpServer http;
    if (a.count("http")) {
        auto handler = [&observer](const std::string& target) {
            HttpReply r;
            const std::string path = target.substr(0, target.find('?'));
            const std::string query = target.find('?') == std::string::npos ? "" : target.substr(target.find('?') + 1);
            if (path == "/metrics" && query.find("format=json") != std::string::npos) {
                r.content_type = "application/json"; r.body = observer.metrics_json();
            } else if (path == "/metrics") {
                r.content_type = "text/plain; version=0.0.4; charset=utf-8"; r.body = observer.metrics_text();
            } else if (path == "/topology") {
                r.content_type = "application/json"; r.body = observer.topology_json();
            } else if (path == "/health") {
                std::string why;
                if (observer.healthy(why)) r.body = "ok\n";
                else { r.status = 503; r.body = why + "\n"; }
            } else {
                r.status = 404; r.body = "routes: /metrics /metrics?format=json /topology /health\n";
            }
            return r;
        };
        if (!http.start(a["http"], handler, err)) { std::fprintf(stderr, "spwsgw-snap: %s\n", err.c_str()); return 2; }
        if (!quiet) std::fprintf(stderr, "spwsgw-snap: http on port %u\n", unsigned(http.port()));
    }
#else
    if (a.count("http")) { std::fputs("spwsgw-snap: built without the network client; --http is unavailable\n", stderr); return 2; }
#endif

    const uint64_t ask_every_ns = getu("ask-every-ms", 1000) * 1'000'000ull;
    const uint64_t stop_at = a.count("seconds") ? now_ns() + getu("seconds", 0) * 1'000'000'000ull : 0;
    const bool spin = flags.count("spin") != 0;
    uint64_t lost_seen = 0, last_report = now_ns();
    std::vector<uint8_t> frame, section;
    size_t section_at = 0;         // how much of `section` is out
    bool section_waited = false;   // counted once per section, however long it waits
    while (!g_stop) {
        int work = data.poll([](void* c, const uint8_t* b, size_t n) { static_cast<SnapCore*>(c)->on_data(b, n); }, &core, 64);
        if (const uint64_t lost = data.images_lost(); lost != lost_seen) { lost_seen = lost; core.on_data_lost(); }
        live.data_images.set(static_cast<uint64_t>(data.images()));
        // INTENT: after the data poll, so a state is usually seen with its data already applied.
        work += state.poll([](void* c, const uint8_t* b, size_t n) { static_cast<SnapCore*>(c)->on_state(b, n); }, &core, 16);
        live.state_images.set(static_cast<uint64_t>(state.images()));
        server.set_time(now_ns());
        work += recap->poll(server, 16);
        const uint64_t now = now_ns();
        // One section at a time: the next one is chosen (single interest first)
        // only when this one is out, so a request that arrives meanwhile still
        // goes ahead of the rest of a bulk.
        if (section.empty() && core.next_section(now, section)) { section_at = 0; section_waited = false; }
        if (!section.empty()) {
            // The next offer: whole SPP messages (u32 length + message) up to offer_bytes, at least one.
            size_t end = section_at;
            while (end < section.size()) {
                uint32_t n = 0;
                std::memcpy(&n, section.data() + end, 4);
                if (end != section_at && end + 4 + n - section_at > offer_bytes) break;
                end += 4 + n;
            }
            const size_t len = end - section_at;
            if (!budget.try_take(len, now)) {
                if (!section_waited) { ++live.sections_throttled; section_waited = true; }
            } else {
                switch (sections.offer(section.data() + section_at, len)) {
                case AeronPublication::Result::OK:
                    live.section_bytes_offered += len;
                    section_at = end;
                    if (section_at == section.size()) { ++live.sections_offered; section.clear(); }
                    ++work;
                    break;
                case AeronPublication::Result::BACK_PRESSURED:
                    budget.give_back(len);
                    if (!section_waited) { ++live.sections_back_pressured; section_waited = true; }
                    break;
                default:   // nobody subscribes, or the publication is gone: the agents ask again on their timeout
                    budget.give_back(len);
                    ++live.sections_dropped;
                    section.clear();
                    break;
                }
            }
        }
        live.sections_connected.set(sections.connected() ? 1 : 0);
        if (link) {
            work += gateway.poll([](void* c, const uint8_t* b, size_t n) { static_cast<GatewayLink*>(c)->on_reply(b, n); }, link.get(), 16);
            live.gateway_connected.set(gateway.connected() ? 1 : 0);
            if (!gateway.connected()) link->disconnected();
            else if (link->next(core, now, ask_every_ns, frame)) {
                if (gateway.offer(frame.data(), frame.size())) link->sent();
                else link->not_sent();
            }
        }
        if (!quiet && now - last_report >= 60'000'000'000ull) { print_stats(core, link.get(), server, stderr); last_report = now; }
        if (stop_at && now >= stop_at) break;
        if (work == 0 && !spin) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    print_stats(core, link.get(), server, stderr);
#if FEEDS_WITH_NETWORK
    http.stop();   // INTENT: before the closes: the handler reads stats the closes do not outlive
#endif
    recap->close();
    sections.close();
    gateway.close();
    state.close();
    data.close();
    client.close();
    return 0;
}
