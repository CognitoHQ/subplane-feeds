// spwsgw: one exchange connection per box, canonical SBE out over Aeron.
//
//   spwsgw --pack packs/binance-spot.yaml (--instruments site.yaml | --symbols BTCUSDT,ETHUSDT)
//                    [--registry packs/registry.yaml] [--out aeron|stdout|null] [--channel aeron:ipc] [--stream-id 1001] [--topic T --topic-registry F [--partition N]]
//                    [--aeron-dir D] [--embedded-driver] [--state-stream 1002] [--record frames.jsonl] [--replay frames.jsonl]
//                    [--republish-per-second 20] [--source-epoch N]
//                    [--control-channel aeron:udp?endpoint=H:P --control-response H:P [--control-stream 1003] [--control-response-stream 1004]]
//                    [--snapshot-every 100] [--batch-bytes 1376] [--heartbeat-ms 1000] [--seconds N] [--subscribe] [--quiet]
//                    [--http 127.0.0.1:9101] [--instance NAME] [--site S] [--env E] [--labels k=v,...]
//   spwsgw models        # the canonical messages and fields a pack can target

#include "gateway.h"
#include "topic_registry.h"
#include "obs/observe.h"
#include "replayer.h"
#if FEEDS_WITH_NETWORK
#include "net.h"
#include <boost/asio.hpp>
#endif

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <thread>
#include <unistd.h>
#include <sstream>
#include <string>
#include <vector>

namespace {

using namespace feeds;

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

[[maybe_unused]] uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

void usage() {
    std::fputs("usage: spwsgw --pack P (--instruments F | --symbols A,B) [--registry R] [--out aeron|stdout|null] [--channel C]\n"
               "                        [--topic T --topic-registry site/topics.yaml [--partition N]] [--stream-id N] [--aeron-dir D] [--embedded-driver] [--state-stream N] [--record F] [--replay F] [--snapshot-every N] [--batch-bytes N]\n"
               "                        [--republish-per-second N] [--source-epoch N]\n"
               "                        [--control-channel C --control-response HOST:PORT [--control-stream N] [--control-response-stream N]]\n"
               "                        [--heartbeat-ms N] [--definitions-ms N] [--seconds N] [--subscribe] [--spin] [--quiet]\n"
               "                        [--http HOST:PORT] [--instance NAME] [--site S] [--env E] [--labels k=v,...]\n"
               "       spwsgw models\n", stderr);
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string part;
    while (std::getline(ss, part, sep)) if (!part.empty()) out.push_back(part);
    return out;
}

const char* kind_name(FieldKind k) {
    switch (k) { case FieldKind::I64: return "decimal (int64 at 1e-9)"; case FieldKind::U64: return "uint64"; case FieldKind::U32: return "uint32";
                 case FieldKind::U16: return "uint16"; case FieldKind::U8: return "uint8"; case FieldKind::CHARS: return "chars"; case FieldKind::I64_ARRAY: return "decimal[]"; }
    return "?";
}

// `models`: what a pack can target. Prefix fields are the gateway's; the rest take a path (and a conversion when the wire needs one).
void print_models() {
    std::printf("prefix (set by the gateway): InstrumentId VenueId Sequence RecvTs; ExchangeTs and Flags are mappable on every message\n");
    for (const CanonicalMessage& m : kCanonicalMessages) {
        std::printf("%s (template %u, %u bytes)\n", std::string(m.name).c_str(), m.template_id, m.size);
        for (size_t i = 0; i < m.nfields; ++i) {
            const CanonicalField& f = m.fields[i];
            std::printf("  %-12s %s%s\n", std::string(f.name).c_str(), kind_name(f.kind), f.kind == FieldKind::CHARS ? (" [" + std::to_string(f.count) + "]").c_str() : f.kind == FieldKind::I64_ARRAY ? " (strategy only)" : "");
        }
    }
    std::printf("conversions: path@<time: s ms us ns iso8601 | map name | level layout>; see packs/registry.yaml\n");
}

void print_stats(const Gateway& gw, FILE* f) {
    const GatewayStats& s = gw.stats();
    std::fprintf(f, "frames=%llu published=%llu offers=%llu offer_bytes=%llu dropped=%llu trades=%llu quotes=%llu book_updates=%llu snapshots=%llu heartbeats=%llu definitions=%llu "
                    "map_errors=%llu book_errors=%llu parse_errors=%llu unmatched=%llu unknown_symbols=%llu acks=%llu resyncs=%llu seq=%llu\n",
                 (unsigned long long)s.frames, (unsigned long long)s.published, (unsigned long long)s.offers, (unsigned long long)s.offer_bytes, (unsigned long long)s.dropped, (unsigned long long)s.trades,
                 (unsigned long long)s.quotes, (unsigned long long)s.book_updates, (unsigned long long)s.book_snapshots, (unsigned long long)s.heartbeats,
                 (unsigned long long)s.definitions, (unsigned long long)s.map_errors, (unsigned long long)s.book_errors, (unsigned long long)s.parse_errors,
                 (unsigned long long)s.unmatched_streams, (unsigned long long)s.unknown_symbols, (unsigned long long)s.acks,
                 (unsigned long long)gw.resyncs(), (unsigned long long)gw.sequence());
    const LatencyHistogram& h = gw.frame_latency();
    std::fprintf(f, "frame_to_offer_ns: n=%llu p50=%llu p99=%llu p999=%llu max=%llu mean=%llu\n", (unsigned long long)h.count(), (unsigned long long)h.quantile(0.5),
                 (unsigned long long)h.quantile(0.99), (unsigned long long)h.quantile(0.999), (unsigned long long)h.max(), (unsigned long long)h.mean());
    const LatencyHistogram& r = gw.recv_to_offer_rt();
    std::fprintf(f, "recv_to_offer_rt_ns: n=%llu p50=%llu p99=%llu p999=%llu max=%llu\n", (unsigned long long)r.count(), (unsigned long long)r.quantile(0.5),
                 (unsigned long long)r.quantile(0.99), (unsigned long long)r.quantile(0.999), (unsigned long long)r.max());
}

} // namespace

int main(int argc, char** argv) {
    if (argc >= 2 && std::strcmp(argv[1], "models") == 0) { print_models(); return 0; }
    std::map<std::string, std::string> a;
    std::set<std::string> flags;
    const std::set<std::string> valued = {"pack", "registry", "instruments", "symbols", "out", "channel", "stream-id", "aeron-dir", "record", "replay",
                                          "snapshot-every", "batch-bytes", "heartbeat-ms", "definitions-ms", "seconds",
                                          "state-stream", "republish-per-second", "source-epoch",
                                          "control-channel", "control-stream", "control-response", "control-response-stream",
                                          "topic", "topic-registry", "partition",
                                          "http", "instance", "site", "env", "labels"};
    const std::set<std::string> boolean = {"embedded-driver", "quiet", "help", "subscribe", "spin"};
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        if (k.rfind("--", 0) != 0) { std::fprintf(stderr, "spwsgw: unexpected argument %s\n", argv[i]); usage(); return 2; }
        k = k.substr(2);
        if (boolean.count(k)) { flags.insert(k); continue; }
        if (!valued.count(k)) { std::fprintf(stderr, "spwsgw: unknown option --%s\n", k.c_str()); usage(); return 2; }
        if (i + 1 >= argc) { std::fprintf(stderr, "spwsgw: --%s needs a value\n", k.c_str()); return 2; }
        a[k] = argv[++i];
    }
    if (flags.count("help")) { usage(); return 0; }
    auto get = [&](const char* k, const std::string& def) { auto it = a.find(k); return it == a.end() ? def : it->second; };
    auto getu = [&](const char* k, uint64_t def) { auto it = a.find(k); return it == a.end() ? def : std::strtoull(it->second.c_str(), nullptr, 10); };
    const bool quiet = flags.count("quiet") != 0;

    std::string err;
    Pack pack;
    if (!a.count("pack")) { std::fputs("spwsgw: --pack is required\n", stderr); usage(); return 2; }
    if (!Pack::load_with_registry(a["pack"], get("registry", ""), pack, err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 2; }

    Instruments instruments;
    if (a.count("instruments")) {
        if (!instruments.load_file(a["instruments"], pack.connect.symbol_lower, err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 2; }
    } else if (a.count("symbols")) {
        if (!instruments.load_symbols(split(a["symbols"], ','), pack.connect.symbol_lower, err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 2; }
        if (!quiet) std::fputs("spwsgw: --symbols assigns ids 1..n in order; use --instruments for stable ids\n", stderr);
    } else { std::fputs("spwsgw: --instruments or --symbols is required\n", stderr); usage(); return 2; }

    GatewayOptions opts;
    opts.heartbeat_ms = getu("heartbeat-ms", 1000);
    opts.definitions_ms = getu("definitions-ms", 60000);
    opts.snapshot_every = static_cast<uint32_t>(getu("snapshot-every", 100));
    opts.batch_bytes = static_cast<uint32_t>(getu("batch-bytes", 1376));   // one Aeron MTU (1408) minus its 32-byte frame header
    opts.log_errors = !quiet;
    opts.republish_per_second = static_cast<uint32_t>(getu("republish-per-second", 20));
    const bool replaying = a.count("replay") != 0;
    const uint64_t started = now_ns();
    // A replay is deterministic: a fixed epoch unless given, so recordings diff exactly.
    opts.source_epoch = getu("source-epoch", replaying ? 1 : started);

    // --topic names the topic this gateway publishes (the site's registry gives
    // its id): the stream is topic_id << 16 | partition_num, stamped on every
    // heartbeat, and the Aeron stream id unless --stream-id says otherwise.
    int32_t topic_stream = 0;
    if (a.count("topic")) {
        if (!a.count("topic-registry")) { std::fputs("spwsgw: --topic needs --topic-registry (the site's topics.yaml)\n", stderr); return 2; }
        feeds::TopicRegistry topics;
        if (!topics.load_file(a["topic-registry"], err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 2; }
        const feeds::TopicEntry* te = topics.find(a["topic"]);
        if (!te) { std::fprintf(stderr, "spwsgw: topic %s is not in %s\n", a["topic"].c_str(), a["topic-registry"].c_str()); return 2; }
        const uint64_t part = getu("partition", 1);
        if (part < 1 || part > 65535) { std::fputs("spwsgw: --partition must be 1..65535\n", stderr); return 2; }
        topic_stream = feeds::stream_id_of(te->id, static_cast<uint16_t>(part));
        opts.stream_stamp = topic_stream;
        // INTENT: one publisher per partition: this gateway publishes only the
        // instruments the instruments file puts on its partition (default 1).
        const size_t before = instruments.all().size();
        if (instruments.keep_partition(static_cast<uint16_t>(part)) == 0) {
            std::fprintf(stderr, "spwsgw: no instrument is on partition %llu (instruments set `partition:`; default 1)\n", static_cast<unsigned long long>(part));
            return 2;
        }
        if (!quiet && instruments.all().size() != before)
            std::fprintf(stderr, "spwsgw: partition %llu publishes %zu of %zu instruments\n", static_cast<unsigned long long>(part), instruments.all().size(), before);
        if (!quiet) std::fprintf(stderr, "spwsgw: topic %s (id %u) partition %llu: stream_id %d\n", te->name.c_str(), te->id,
                                 static_cast<unsigned long long>(part), topic_stream);
    } else if (a.count("topic-registry") || a.count("partition")) {
        std::fputs("spwsgw: --topic-registry and --partition go with --topic\n", stderr); return 2;
    }

    std::unique_ptr<Publisher> owned;
    Publisher* pub = nullptr;
    Publisher* state_pub = nullptr;
    StdoutPublisher stdout_pub(std::cout);
    StdoutStatePublisher stdout_state(std::cout);
    NullPublisher null_pub;
    const std::string out = get("out", "aeron");
    if (out == "stdout") {
        pub = &stdout_pub;
        if (getu("state-stream", 1002)) state_pub = &stdout_state;   // interleaved with the data, in publish order
    } else if (out == "null") {
        pub = &null_pub;
    } else if (out == "aeron") {
#if FEEDS_WITH_AERON
        AeronPublisherConfig ac;
        ac.channel = get("channel", "aeron:ipc");
        ac.stream_id = static_cast<int32_t>(getu("stream-id", topic_stream != 0 ? static_cast<uint64_t>(topic_stream) : 1001));
        ac.aeron_dir = get("aeron-dir", "");
        ac.embedded_driver = flags.count("embedded-driver") != 0;
        ac.state_stream_id = static_cast<int32_t>(getu("state-stream", 1002));
        if (ac.state_stream_id == ac.stream_id) { std::fputs("spwsgw: --state-stream must differ from --stream-id\n", stderr); return 2; }
        owned = make_aeron_publisher(ac, err);
        if (!owned) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 1; }
        pub = owned.get();
        state_pub = pub->state_publisher();
#else
        std::fputs("spwsgw: built without Aeron; use --out stdout\n", stderr); return 2;
#endif
    } else { std::fprintf(stderr, "spwsgw: --out must be aeron, stdout or null\n"); return 2; }

    Recorder recorder;
    Recorder* rec = nullptr;
    if (a.count("record")) {
        if (!recorder.open(a["record"], err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 1; }
        rec = &recorder;
    }

    Gateway gw(pack, instruments, *pub, opts, rec, state_pub);
    // SPP control (Hello, Republish), polled from the gateway's timer.
    ControlHandler control_handler(gw, "spwsgw", opts.source_epoch);
    std::unique_ptr<ControlInput> control;
    if (a.count("control-channel")) {
#if FEEDS_WITH_AERON
        AeronControlConfig cc;
        cc.channel = a["control-channel"];
        cc.stream_id = static_cast<int32_t>(getu("control-stream", 1003));
        cc.response_control = get("control-response", "");
        cc.response_stream_id = static_cast<int32_t>(getu("control-response-stream", 1004));
        if (!owned) { std::fputs("spwsgw: --control-channel needs --out aeron\n", stderr); return 2; }
        control = make_aeron_control(*owned, cc, err);
        if (!control) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 2; }
        if (!quiet) std::fprintf(stderr, "spwsgw: control on %s\n", control->describe().c_str());
#else
        std::fputs("spwsgw: built without Aeron; --control-channel is unavailable\n", stderr); return 2;
#endif
    }
    if (flags.count("subscribe")) { for (const auto& m : gw.subscribe_messages()) std::printf("%s\n", m.c_str()); return 0; }
    if (!quiet) std::fprintf(stderr, "spwsgw: %s, %zu instruments, %zu streams -> %s\n", pack.exchange.c_str(), instruments.all().size(), gw.stream_names().size(), pub->describe().c_str());

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    Identity id;
    {
        char host[256] = {};
        id.host = ::gethostname(host, sizeof host - 1) == 0 ? host : "unknown";
    }
    id.instance = get("instance", pack.exchange);
    id.version = FEEDS_VERSION;
    id.site = get("site", "");
    id.env = get("env", "");
    id.epoch = started;
    id.pid = static_cast<int32_t>(::getpid());
    if (std::string bad; !Observer::parse_labels(get("labels", ""), id.labels, bad)) {
        std::fprintf(stderr, "spwsgw: --labels: invalid label '%s' (want name=value)\n", bad.c_str());
        return 2;
    }
    const std::string upstream = replaying ? "file://" + std::filesystem::absolute(a["replay"]).lexically_normal().string() : pack.connect.url;
    const Observer observer(gw, pack, instruments, *pub, id, upstream, !replaying, &control_handler, control.get());
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
        if (!http.start(a["http"], handler, err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 2; }
        if (!quiet) std::fprintf(stderr, "spwsgw: http on port %u\n", unsigned(http.port()));
    }
#else
    if (a.count("http")) { std::fputs("spwsgw: built without the network client; --http is unavailable\n", stderr); return 2; }
#endif

    if (replaying) {
        ReplayStats rs;
        if (!replay_file(a["replay"], gw, rs, err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return 1; }
        if (!quiet) std::fprintf(stderr, "replayed frames=%llu snapshots=%llu events=%llu bad_lines=%llu\n", (unsigned long long)rs.frames, (unsigned long long)rs.snapshots, (unsigned long long)rs.events, (unsigned long long)rs.bad_lines);
        print_stats(gw, stderr);
        if (a.count("http") || control) {
            // Keep serving the final numbers, for tests and for inspecting a replay, until --seconds or a signal.
            // With a control input the replayed books stay live: heartbeats, Republish and its rate limit run on.
            const uint64_t until = now_ns() + getu("seconds", 0) * 1'000'000'000ull;
            while (!g_stop && (!a.count("seconds") || now_ns() < until)) {
                if (control) { control->poll(control_handler, 16); gw.tick(now_ns()); }
                std::this_thread::sleep_for(std::chrono::milliseconds(control ? 1 : 20));
            }
            if (control) print_stats(gw, stderr);
#if FEEDS_WITH_NETWORK
            http.stop();
#endif
        }
        if (control) control->close();   // INTENT: before pub->close(): it shares the publisher's Aeron client
        pub->close();
        return rs.bad_lines ? 1 : 0;
    }

#if FEEDS_WITH_NETWORK
    boost::asio::io_context io;
    WsOptions wo;
    wo.url = pack.connect.url;
    wo.subscribe = gw.subscribe_messages();
    wo.ping_text = pack.connect.ping_text;
    wo.ping_every_s = pack.connect.ping_every_s;
    wo.reconnect_before_s = pack.connect.reconnect_before_s;
    struct Ctx { Gateway* gw; bool quiet; } ctx{&gw, quiet};
    WsCallbacks cbs;
    cbs.ctx = &ctx;
    cbs.on_frame = [](void* c, std::string_view f, uint64_t ts) { static_cast<Ctx*>(c)->gw->on_frame(f, ts); };
    cbs.on_connected = [](void* c, uint64_t ts) { Ctx* x = static_cast<Ctx*>(c); if (!x->quiet) std::fputs("spwsgw: connected and subscribed\n", stderr); x->gw->on_connected(ts); };
    cbs.on_disconnected = [](void* c, uint64_t ts, const std::string&) { static_cast<Ctx*>(c)->gw->on_disconnected(ts); };
    WsClient client(io, wo, cbs);
    client.start();

    const uint64_t stop_at = a.count("seconds") ? now_ns() + getu("seconds", 0) * 1'000'000'000ull : 0;
    uint64_t last_report = now_ns();
    boost::asio::steady_timer ticker(io);
    std::function<void()> arm = [&]() {
        ticker.expires_after(std::chrono::milliseconds(10));
        ticker.async_wait([&](boost::system::error_code ec) {
            if (ec) return;
            const uint64_t now = now_ns();
            if (control) control->poll(control_handler, 16);
            gw.tick(now);
            if (gw.take_reconnect_request()) client.reconnect("a book overflowed max_levels; reconnecting for a new snapshot");
            SnapshotRequest rq;
            while (gw.next_snapshot_request(rq)) {
                const std::string sym = rq.venue_symbol;
                https_get(io, rq.url, [&, sym](int status, std::string body) {
                    if (status == 200) gw.on_snapshot(sym, body, now_ns());
                    else std::fprintf(stderr, "spwsgw: snapshot %s failed: %d %s\n", sym.c_str(), status, status == 0 ? body.c_str() : "");
                });
            }
            if (!quiet && now - last_report >= 60'000'000'000ull) { print_stats(gw, stderr); last_report = now; }
            if (g_stop || (stop_at && now >= stop_at)) { client.stop(); io.stop(); return; }
            arm();
        });
    };
    arm();
    if (flags.count("spin")) {
        // INTENT: io.run() sleeps in epoll between frames, so the core drops into a
        // C-state and the next frame is processed cold and slow (measured 2026-10-13:
        // depth frames took 2.5x longer live than on replay). Polling keeps the core hot
        // at the price of owning it; pin it with taskset.
        while (!io.stopped()) { if (io.poll() == 0) { for (int i = 0; i < 64; ++i) __builtin_ia32_pause(); } }
    } else {
        io.run();
    }
    print_stats(gw, stderr);
    http.stop();   // INTENT: before close(): the handler reads the publication's connected state
    if (control) control->close();
    pub->close();
    return 0;
#else
    std::fputs("spwsgw: built without the network client (FEEDS_WITH_NETWORK=OFF); only --replay works\n", stderr);
    return 2;
#endif
}
