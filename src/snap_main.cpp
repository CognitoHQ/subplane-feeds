// spwsgw-snap: a gateway's snapshot service (docs/snap.md). A silent, untethered
// subscriber of one gateway's canonical data stream and SPP state stream that
// rebuilds every instrument from the stream alone and answers the agent's
// RecapRequests. Asks the gateway to Republish what it cannot trust.
//
//   spwsgw-snap --recap-channel aeron:udp?endpoint=H:P --recap-response H:P
//               [--gateway-control aeron:udp?endpoint=H:P --gateway-response H:P]
//               [--data-channel aeron:ipc] [--data-stream 1001] [--state-stream 1002]
//               [--recap-stream 1101] [--recap-response-stream 1102]
//               [--gateway-control-stream 1003] [--gateway-response-stream 1004]
//               [--book-levels 5000] [--ask-every-ms 1000] [--name spwsgw-snap]
//               [--aeron-dir D] [--embedded-driver] [--seconds N] [--spin] [--quiet]

#include "aeron_io.h"
#include "publisher.h"
#include "snap/gateway_link.h"
#include "snap/recap_server.h"
#include "snap/snap.h"

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <set>
#include <string>
#include <thread>

namespace {

using namespace feeds;

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

void usage() {
    std::fputs("usage: spwsgw-snap --recap-channel aeron:udp?endpoint=H:P --recap-response H:P\n"
               "                   [--gateway-control aeron:udp?endpoint=H:P --gateway-response H:P]\n"
               "                   [--data-channel C] [--data-stream N] [--state-stream N] [--recap-stream N] [--recap-response-stream N]\n"
               "                   [--gateway-control-stream N] [--gateway-response-stream N] [--recap-term-length N] [--recap-max-pending N]\n"
               "                   [--book-levels N] [--ask-every-ms N] [--name S] [--aeron-dir D] [--embedded-driver] [--seconds N] [--spin] [--quiet]\n", stderr);
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
                    "ok=%llu stale=%llu unknown=%llu requests=%llu replies=%llu not_ready=%llu unknown_instrument=%llu asks=%llu seq=%llu",
                 (unsigned long long)s.messages, (unsigned long long)s.gaps, (unsigned long long)s.restarts, (unsigned long long)s.malformed,
                 (unsigned long long)s.state_messages, (unsigned long long)s.states_applied, (unsigned long long)s.states_ignored, (unsigned long long)s.book_overflows,
                 (unsigned long long)s.instruments_ok, (unsigned long long)s.instruments_stale, (unsigned long long)s.instruments_unknown,
                 (unsigned long long)s.requests, (unsigned long long)s.replies, (unsigned long long)s.rejects_not_ready, (unsigned long long)s.rejects_unknown,
                 (unsigned long long)s.republish_asks, (unsigned long long)core.last_sequence());
    if (link) std::fprintf(f, " republish_sent=%llu gateway_ok=%llu", (unsigned long long)link->stats().republish_sent, (unsigned long long)link->stats().status_ok);
    std::fprintf(f, " reply_dropped=%llu\n", (unsigned long long)server.stats().reply_dropped);
}

} // namespace

int main(int argc, char** argv) {
    std::map<std::string, std::string> a;
    std::set<std::string> flags;
    const std::set<std::string> boolean = {"embedded-driver", "spin", "quiet", "help"};
    const std::set<std::string> valued = {"recap-channel", "recap-response", "recap-stream", "recap-response-stream", "recap-term-length", "recap-max-pending",
                                          "gateway-control", "gateway-response", "gateway-control-stream", "gateway-response-stream",
                                          "data-channel", "data-stream", "state-stream", "book-levels", "ask-every-ms", "name", "aeron-dir", "seconds"};
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
    if (!a.count("recap-channel") || !a.count("recap-response")) { usage(); return 2; }
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
    RecapServer server(core, name, epoch);
    AeronControlConfig rc;
    rc.channel = a["recap-channel"];
    rc.stream_id = static_cast<int32_t>(getu("recap-stream", 1101));
    rc.response_control = a["recap-response"];
    rc.response_stream_id = static_cast<int32_t>(getu("recap-response-stream", 1102));
    // A reply is a whole book: about 100 bytes a level. The term must hold the
    // largest reply, and max_pending lets it wait there for a slow requester.
    rc.term_length = static_cast<uint32_t>(getu("recap-term-length", 16u << 20));
    rc.max_pending = static_cast<size_t>(getu("recap-max-pending", 16u << 20));
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
    if (!quiet) std::fprintf(stderr, "spwsgw-snap: data %s streams %d/%d, recap %s; %s\n", data_channel.c_str(), data.stream_id(), state.stream_id(),
                             recap->describe().c_str(), link ? ("gateway control " + gateway.config().request_channel).c_str() : "no gateway control (no Republish asks)");

    const uint64_t ask_every_ns = getu("ask-every-ms", 1000) * 1'000'000ull;
    const uint64_t stop_at = a.count("seconds") ? now_ns() + getu("seconds", 0) * 1'000'000'000ull : 0;
    const bool spin = flags.count("spin") != 0;
    uint64_t lost_seen = 0, last_report = now_ns();
    std::vector<uint8_t> frame;
    while (!g_stop) {
        int work = data.poll([](void* c, const uint8_t* b, size_t n) { static_cast<SnapCore*>(c)->on_data(b, n); }, &core, 64);
        if (const uint64_t lost = data.images_lost(); lost != lost_seen) { lost_seen = lost; core.on_data_lost(); }
        // INTENT: after the data poll, so a state is usually seen with its data already applied.
        work += state.poll([](void* c, const uint8_t* b, size_t n) { static_cast<SnapCore*>(c)->on_state(b, n); }, &core, 16);
        work += recap->poll(server, 16);
        const uint64_t now = now_ns();
        if (link) {
            work += gateway.poll([](void* c, const uint8_t* b, size_t n) { static_cast<GatewayLink*>(c)->on_reply(b, n); }, link.get(), 16);
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
    recap->close();
    gateway.close();
    state.close();
    data.close();
    client.close();
    return 0;
}
