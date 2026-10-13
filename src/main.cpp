// subplane-gateway: one exchange connection per box, canonical SBE out over Aeron.
//
//   subplane-gateway --pack packs/binance-spot.yaml (--instruments site.yaml | --symbols BTCUSDT,ETHUSDT)
//                    [--out aeron|stdout] [--channel aeron:ipc] [--stream-id 1001] [--aeron-dir D] [--embedded-driver]
//                    [--record frames.jsonl] [--replay frames.jsonl] [--snapshot-every 100] [--heartbeat-ms 1000]
//                    [--seconds N] [--quiet]

#include "gateway.h"
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
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace {

using namespace feeds;

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

[[maybe_unused]] uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

void usage() {
    std::fputs("usage: subplane-gateway --pack P (--instruments F | --symbols A,B) [--out aeron|stdout] [--channel C] [--stream-id N]\n"
               "                        [--aeron-dir D] [--embedded-driver] [--record F] [--replay F] [--snapshot-every N]\n"
               "                        [--heartbeat-ms N] [--definitions-ms N] [--seconds N] [--quiet]\n", stderr);
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string part;
    while (std::getline(ss, part, sep)) if (!part.empty()) out.push_back(part);
    return out;
}

void print_stats(const Gateway& gw, FILE* f) {
    const GatewayStats& s = gw.stats();
    std::fprintf(f, "frames=%llu published=%llu dropped=%llu trades=%llu quotes=%llu book_updates=%llu snapshots=%llu heartbeats=%llu definitions=%llu "
                    "map_errors=%llu book_errors=%llu parse_errors=%llu unmatched=%llu unknown_symbols=%llu acks=%llu resyncs=%llu seq=%llu\n",
                 (unsigned long long)s.frames, (unsigned long long)s.published, (unsigned long long)s.dropped, (unsigned long long)s.trades,
                 (unsigned long long)s.quotes, (unsigned long long)s.book_updates, (unsigned long long)s.book_snapshots, (unsigned long long)s.heartbeats,
                 (unsigned long long)s.definitions, (unsigned long long)s.map_errors, (unsigned long long)s.book_errors, (unsigned long long)s.parse_errors,
                 (unsigned long long)s.unmatched_streams, (unsigned long long)s.unknown_symbols, (unsigned long long)s.acks,
                 (unsigned long long)gw.resyncs(), (unsigned long long)gw.sequence());
}

} // namespace

int main(int argc, char** argv) {
    std::map<std::string, std::string> a;
    std::set<std::string> flags;
    const std::set<std::string> valued = {"pack", "instruments", "symbols", "out", "channel", "stream-id", "aeron-dir", "record", "replay",
                                          "snapshot-every", "heartbeat-ms", "definitions-ms", "seconds"};
    const std::set<std::string> boolean = {"embedded-driver", "quiet", "help"};
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        if (k.rfind("--", 0) != 0) { std::fprintf(stderr, "subplane-gateway: unexpected argument %s\n", argv[i]); usage(); return 2; }
        k = k.substr(2);
        if (boolean.count(k)) { flags.insert(k); continue; }
        if (!valued.count(k)) { std::fprintf(stderr, "subplane-gateway: unknown option --%s\n", k.c_str()); usage(); return 2; }
        if (i + 1 >= argc) { std::fprintf(stderr, "subplane-gateway: --%s needs a value\n", k.c_str()); return 2; }
        a[k] = argv[++i];
    }
    if (flags.count("help")) { usage(); return 0; }
    auto get = [&](const char* k, const std::string& def) { auto it = a.find(k); return it == a.end() ? def : it->second; };
    auto getu = [&](const char* k, uint64_t def) { auto it = a.find(k); return it == a.end() ? def : std::strtoull(it->second.c_str(), nullptr, 10); };
    const bool quiet = flags.count("quiet") != 0;

    std::string err;
    Pack pack;
    if (!a.count("pack")) { std::fputs("subplane-gateway: --pack is required\n", stderr); usage(); return 2; }
    if (!Pack::load(a["pack"], pack, err)) { std::fprintf(stderr, "subplane-gateway: %s\n", err.c_str()); return 2; }

    Instruments instruments;
    if (a.count("instruments")) {
        if (!instruments.load_file(a["instruments"], pack.connect.symbol_lower, err)) { std::fprintf(stderr, "subplane-gateway: %s\n", err.c_str()); return 2; }
    } else if (a.count("symbols")) {
        if (!instruments.load_symbols(split(a["symbols"], ','), pack.connect.symbol_lower, err)) { std::fprintf(stderr, "subplane-gateway: %s\n", err.c_str()); return 2; }
        if (!quiet) std::fputs("subplane-gateway: --symbols assigns ids 1..n in order; use --instruments for stable ids\n", stderr);
    } else { std::fputs("subplane-gateway: --instruments or --symbols is required\n", stderr); usage(); return 2; }

    GatewayOptions opts;
    opts.heartbeat_ms = getu("heartbeat-ms", 1000);
    opts.definitions_ms = getu("definitions-ms", 60000);
    opts.snapshot_every = static_cast<uint32_t>(getu("snapshot-every", 100));
    opts.log_errors = !quiet;

    std::unique_ptr<Publisher> owned;
    Publisher* pub = nullptr;
    StdoutPublisher stdout_pub(std::cout);
    const std::string out = get("out", "aeron");
    if (out == "stdout") {
        pub = &stdout_pub;
    } else if (out == "aeron") {
#if FEEDS_WITH_AERON
        AeronPublisherConfig ac;
        ac.channel = get("channel", "aeron:ipc");
        ac.stream_id = static_cast<int32_t>(getu("stream-id", 1001));
        ac.aeron_dir = get("aeron-dir", "");
        ac.embedded_driver = flags.count("embedded-driver") != 0;
        owned = make_aeron_publisher(ac, err);
        if (!owned) { std::fprintf(stderr, "subplane-gateway: %s\n", err.c_str()); return 1; }
        pub = owned.get();
#else
        std::fputs("subplane-gateway: built without Aeron; use --out stdout\n", stderr); return 2;
#endif
    } else { std::fprintf(stderr, "subplane-gateway: --out must be aeron or stdout\n"); return 2; }

    Recorder recorder;
    Recorder* rec = nullptr;
    if (a.count("record")) {
        if (!recorder.open(a["record"], err)) { std::fprintf(stderr, "subplane-gateway: %s\n", err.c_str()); return 1; }
        rec = &recorder;
    }

    Gateway gw(pack, instruments, *pub, opts, rec);
    if (!quiet) std::fprintf(stderr, "subplane-gateway: %s, %zu instruments, %zu streams -> %s\n", pack.exchange.c_str(), instruments.all().size(), gw.stream_names().size(), pub->describe().c_str());

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    if (a.count("replay")) {
        ReplayStats rs;
        if (!replay_file(a["replay"], gw, rs, err)) { std::fprintf(stderr, "subplane-gateway: %s\n", err.c_str()); return 1; }
        if (!quiet) std::fprintf(stderr, "replayed frames=%llu snapshots=%llu events=%llu bad_lines=%llu\n", (unsigned long long)rs.frames, (unsigned long long)rs.snapshots, (unsigned long long)rs.events, (unsigned long long)rs.bad_lines);
        print_stats(gw, stderr);
        pub->close();
        return rs.bad_lines ? 1 : 0;
    }

#if FEEDS_WITH_NETWORK
    boost::asio::io_context io;
    WsOptions wo;
    wo.url = pack.connect.url;
    wo.subscribe = gw.subscribe_message();
    wo.reconnect_before_s = pack.connect.reconnect_before_s;
    WsCallbacks cbs;
    cbs.on_frame = [&](std::string_view f, uint64_t ts) { gw.on_frame(f, ts); };
    cbs.on_connected = [&](uint64_t ts) { if (!quiet) std::fputs("subplane-gateway: connected and subscribed\n", stderr); gw.on_connected(ts); };
    cbs.on_disconnected = [&](uint64_t ts, const std::string&) { gw.on_disconnected(ts); };
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
            gw.tick(now);
            SnapshotRequest rq;
            while (gw.next_snapshot_request(rq)) {
                const std::string sym = rq.venue_symbol;
                https_get(io, rq.url, [&, sym](int status, std::string body) {
                    if (status == 200) gw.on_snapshot(sym, body, now_ns());
                    else std::fprintf(stderr, "subplane-gateway: snapshot %s failed: %d %s\n", sym.c_str(), status, status == 0 ? body.c_str() : "");
                });
            }
            if (!quiet && now - last_report >= 60'000'000'000ull) { print_stats(gw, stderr); last_report = now; }
            if (g_stop || (stop_at && now >= stop_at)) { client.stop(); io.stop(); return; }
            arm();
        });
    };
    arm();
    io.run();
    print_stats(gw, stderr);
    pub->close();
    return 0;
#else
    std::fputs("subplane-gateway: built without the network client (FEEDS_WITH_NETWORK=OFF); only --replay works\n", stderr);
    return 2;
#endif
}
