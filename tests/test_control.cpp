// The SPP control input: the network-free handler, and (with Aeron) a requester
// talking to the gateway over a real response channel.
#include "control.h"
#include "gateway.h"

#include "doctest.h"

#include <cstring>
#include <sstream>

#if FEEDS_WITH_AERON
#include <aeronc.h>
#include <chrono>
#include <filesystem>
#include <random>
#include <sched.h>
#include <thread>
#include <unistd.h>
#endif

using namespace feeds;

namespace {

struct Rig {
    Pack pack; Instruments ins; std::ostringstream out; StdoutPublisher pub{out}; StdoutStatePublisher state{out};
    Rig() {
        std::string err;
        REQUIRE_MESSAGE(Pack::load_with_registry(std::string(FEEDS_SOURCE_DIR) + "/packs/okx.yaml", "", pack, err), err);
        REQUIRE(ins.load_symbols({"BTC-USDT", "ETH-USDT"}, pack.connect.symbol_lower, err));
    }
    static GatewayOptions opts() { GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false; o.republish_per_second = 1000; return o; }
};

template <class Enc, class F> void add(std::vector<uint8_t>& frame, F fill) {
    uint8_t buf[256];
    Enc e(buf, sizeof buf);
    fill(e);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    REQUIRE(n > 0);
    const size_t at = frame.size();
    frame.resize(at + 4 + n);
    std::memcpy(frame.data() + at, &n, 4);
    std::memcpy(frame.data() + at + 4, buf, n);
}

// The messages of a reply frame, as (template, CorrelationId, a code or role).
struct Msg { uint16_t tid; uint64_t corr; unsigned what; };
std::vector<Msg> parse(const std::vector<uint8_t>& f) {
    std::vector<Msg> v;
    size_t at = 0;
    while (at + 4 <= f.size()) {
        uint32_t n; std::memcpy(&n, f.data() + at, 4); at += 4;
        REQUIRE(at + n <= f.size());
        sp_proto::Header h; REQUIRE(sp_proto::read_header(f.data() + at, n, h));
        if (h.template_id == sp_proto::HelloDecoder::kTemplateId) {
            sp_proto::HelloDecoder d; REQUIRE(d.wrap(f.data() + at, n));
            v.push_back({h.template_id, d.CorrelationId(), static_cast<unsigned>(d.Role())});
        } else if (h.template_id == sp_proto::StatusDecoder::kTemplateId) {
            sp_proto::StatusDecoder d; REQUIRE(d.wrap(f.data() + at, n));
            v.push_back({h.template_id, d.CorrelationId(), static_cast<unsigned>(d.Code())});
        } else {
            v.push_back({h.template_id, 0, 0});
        }
        at += n;
    }
    return v;
}

constexpr unsigned kOk = static_cast<unsigned>(sp_proto::StatusCode::Ok);
constexpr unsigned kUnsupported = static_cast<unsigned>(sp_proto::StatusCode::Unsupported);
constexpr unsigned kNotFound = static_cast<unsigned>(sp_proto::StatusCode::NotFound);

} // namespace

TEST_CASE("control: Hello is answered with the gateway's Hello; Republish with Status; replies in request order") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.pub, Rig::opts(), nullptr, &r.state);
    ControlHandler h(gw, "spwsgw", 99);
    std::vector<uint8_t> in, reply;
    add<sp_proto::HelloEncoder>(in, [](auto& e) { e.CorrelationId(1).PeerEpoch(5).ProtoVersion(1).MinProtoVersion(1).Role(sp_proto::Role::Snap).ServiceName("okx-snap"); });
    add<sp_proto::RepublishEncoder>(in, [](auto& e) { e.CorrelationId(2).Instrument(0); });
    add<sp_proto::RepublishEncoder>(in, [](auto& e) { e.CorrelationId(3).Instrument(77); });
    h.on_frame(in.data(), in.size(), reply);
    auto m = parse(reply);
    REQUIRE(m.size() == 3);
    CHECK(m[0].tid == sp_proto::HelloDecoder::kTemplateId); CHECK(m[0].corr == 1); CHECK(m[0].what == static_cast<unsigned>(sp_proto::Role::Gateway));
    CHECK(m[1].tid == sp_proto::StatusDecoder::kTemplateId); CHECK(m[1].corr == 2); CHECK(m[1].what == kOk);
    CHECK(m[2].corr == 3); CHECK(m[2].what == kNotFound);
    sp_proto::HelloDecoder d;
    REQUIRE(d.wrap(reply.data() + 4, reply.size() - 4));
    CHECK(d.PeerEpoch() == 99); CHECK(d.ProtoVersion() == 1);
    CHECK(h.stats().hellos == 1); CHECK(h.stats().republish == 2);
    // The queued republish runs from tick(): every instrument, state with the correlation.
    gw.on_connected(1);
    gw.tick(1'000'000'000); gw.tick(1'001'000'001);
    const std::string out = r.out.str();
    CHECK(out.find("SourceState iid=1 venue=40 seq=3 state=Ok reason=Republish corr=2") != std::string::npos);
    CHECK(out.find("SourceState iid=2 venue=40 seq=4 state=Ok reason=Republish corr=2") != std::string::npos);
}

TEST_CASE("control: unknown and unsupported messages get Status{Unsupported}; nothing unknown is fatal") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.pub, Rig::opts());
    ControlHandler h(gw, "spwsgw", 1);
    std::vector<uint8_t> in, reply;
    add<sp_proto::HelloEncoder>(in, [](auto& e) { e.CorrelationId(4).ProtoVersion(3).MinProtoVersion(2); });          // only speaks 2+
    add<sp_proto::RecapRequestEncoder>(in, [](auto& e) { e.CorrelationId(5).Instrument(1); });                       // not a snapshot service
    add<sp_proto::ExtensionEncoder>(in, [](auto& e) { e.CorrelationId(6).ExtensionId(0x8001).Flags(1); });          // must-understand
    add<sp_proto::ExtensionEncoder>(in, [](auto& e) { e.CorrelationId(7).ExtensionId(0x8001).Flags(0); });          // ignorable
    add<sp_proto::StatusEncoder>(in, [](auto& e) { e.CorrelationId(8).Code(sp_proto::StatusCode::Ok); });           // a stray answer
    h.on_frame(in.data(), in.size(), reply);
    auto m = parse(reply);
    REQUIRE(m.size() == 3);
    CHECK(m[0].corr == 4); CHECK(m[0].what == kUnsupported);
    CHECK(m[1].corr == 5); CHECK(m[1].what == kUnsupported);
    CHECK(m[2].corr == 6); CHECK(m[2].what == kUnsupported);
    CHECK(h.stats().unsupported == 3); CHECK(h.stats().status == 1);

    // A newer sender's longer Republish block is read as far as known.
    std::vector<uint8_t> neu;
    add<sp_proto::RepublishEncoder>(neu, [](auto& e) { e.CorrelationId(9).Instrument(2); });
    uint32_t n; std::memcpy(&n, neu.data(), 4);
    const uint16_t bl = sp_proto::RepublishEncoder::kBlockLength + 8, v2 = 2;
    std::memcpy(neu.data() + 4, &bl, 2); std::memcpy(neu.data() + 10, &v2, 2);
    neu.insert(neu.end(), 8, 0); n += 8; std::memcpy(neu.data(), &n, 4);
    reply.clear();
    h.on_frame(neu.data(), neu.size(), reply);
    m = parse(reply);
    REQUIRE(m.size() == 1); CHECK(m[0].corr == 9); CHECK(m[0].what == kOk);

    // Truncated frames and foreign schemas are counted and skipped.
    reply.clear();
    const uint8_t junk[] = {200, 0, 0, 0, 1, 2, 3};
    h.on_frame(junk, sizeof junk, reply);
    CHECK(reply.empty()); CHECK(h.stats().malformed == 1);
}

#if FEEDS_WITH_AERON
namespace {
int free_udp_port() {
    static std::mt19937 rng(static_cast<unsigned>(::getpid()) ^ static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));
    return 20000 + static_cast<int>(rng() % 30000);
}
struct Requester {
    aeron_context_t* ctx = nullptr; aeron_t* client = nullptr;
    aeron_subscription_t* sub = nullptr; aeron_exclusive_publication_t* pub = nullptr;
    aeron_subscription_t* state = nullptr;
    std::vector<std::vector<uint8_t>> frames, states;
    static void on_frame(void* cd, const uint8_t* b, size_t n, aeron_header_t*) { static_cast<Requester*>(cd)->frames.emplace_back(b, b + n); }
    static void on_state(void* cd, const uint8_t* b, size_t n, aeron_header_t*) { static_cast<Requester*>(cd)->states.emplace_back(b, b + n); }
    template <class T, class A, class P> static bool wait(T*& out, A* async, P poll) {
        // INTENT: by time, not by iterations: a loaded CI runner's driver takes longer
        // than a fixed number of yields (the test failed there at about 100 ms).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!out && std::chrono::steady_clock::now() < deadline) {
            if (poll(&out, async) < 0) { MESSAGE("aeron: " << aeron_errmsg()); return false; }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!out) MESSAGE("aeron: async add timed out");
        return out != nullptr;
    }
    bool start(const std::string& dir, const std::string& request, const std::string& response) {
        if (aeron_context_init(&ctx) < 0 || aeron_context_set_dir(ctx, dir.c_str()) < 0 || aeron_init(&client, ctx) < 0 || aeron_start(client) < 0) return false;
        aeron_async_add_subscription_t* as = nullptr;
        if (aeron_async_add_subscription(&as, client, response.c_str(), 1004, nullptr, nullptr, nullptr, nullptr) < 0 || !wait(sub, as, aeron_async_add_subscription_poll)) return false;
        aeron_subscription_constants_t sc{};
        aeron_subscription_constants(sub, &sc);
        const std::string ch = request + "|response-correlation-id=" + std::to_string(sc.registration_id);
        aeron_async_add_exclusive_publication_t* ap = nullptr;
        if (aeron_async_add_exclusive_publication(&ap, client, ch.c_str(), 1003) < 0 || !wait(pub, ap, aeron_async_add_exclusive_publication_poll)) return false;
        aeron_async_add_subscription_t* ss = nullptr;
        return aeron_async_add_subscription(&ss, client, "aeron:ipc", 1002, nullptr, nullptr, nullptr, nullptr) == 0 && wait(state, ss, aeron_async_add_subscription_poll);
    }
    ~Requester() {
        if (state) aeron_subscription_close(state, nullptr, nullptr);
        if (pub) aeron_exclusive_publication_close(pub, nullptr, nullptr);
        if (sub) aeron_subscription_close(sub, nullptr, nullptr);
        if (client) aeron_close(client);
        if (ctx) aeron_context_close(ctx);
    }
};
} // namespace

TEST_CASE("control over Aeron: Hello and Republish on a response channel; the republished state arrives on the state stream") {
    Rig r;
    const std::string dir = (std::filesystem::temp_directory_path() / ("feeds-ctl-" + std::to_string(::getpid()))).string();
    AeronPublisherConfig pc;
    pc.channel = "aeron:ipc"; pc.stream_id = 1001; pc.state_stream_id = 1002; pc.aeron_dir = dir; pc.embedded_driver = true;
    std::string err;
    auto pub = make_aeron_publisher(pc, err);
    REQUIRE_MESSAGE(pub, err);
    const int p1 = free_udp_port(), p2 = p1 + 1;
    AeronControlConfig cc;
    cc.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p1);
    cc.response_control = "127.0.0.1:" + std::to_string(p2);
    auto control = make_aeron_control(*pub, cc, err);
    REQUIRE_MESSAGE(control, err);
    CHECK(control->endpoint_key("box1") == "aeron-udp://box1:" + std::to_string(p1) + "#1003");   // loopback names the host (endpoint key rules)

    Gateway gw(r.pack, r.ins, *pub, Rig::opts(), nullptr, pub->state_publisher());
    ControlHandler h(gw, "spwsgw", 42);
    gw.on_connected(1);

    Requester q;
    REQUIRE(q.start(dir, "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p1) + "|term-length=65536",
                    "aeron:udp?control-mode=response|control=127.0.0.1:" + std::to_string(p2)));
    std::vector<uint8_t> req;
    add<sp_proto::HelloEncoder>(req, [](auto& e) { e.CorrelationId(1).ProtoVersion(1).MinProtoVersion(1).Role(sp_proto::Role::Snap); });
    add<sp_proto::RepublishEncoder>(req, [](auto& e) { e.CorrelationId(31).Instrument(2); });
    bool sent = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline && (q.frames.empty() || q.states.empty())) {
        if (!sent) sent = aeron_exclusive_publication_offer(q.pub, req.data(), req.size(), nullptr, nullptr) >= 0;
        control->poll(h, 16);
        gw.tick(static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
        aeron_subscription_poll(q.sub, &Requester::on_frame, &q, 10);
        aeron_subscription_poll(q.state, &Requester::on_state, &q, 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    REQUIRE(q.frames.size() == 1);   // one request frame, one reply frame
    auto m = parse(q.frames[0]);
    REQUIRE(m.size() == 2);
    CHECK(m[0].tid == sp_proto::HelloDecoder::kTemplateId); CHECK(m[0].what == static_cast<unsigned>(sp_proto::Role::Gateway));
    CHECK(m[1].corr == 31); CHECK(m[1].what == kOk);
    // The state stream carried the Ok states of the connect, then the republished one with the correlation.
    bool republished = false;
    for (const auto& f : q.states) {
        sp_proto::SourceStateDecoder d;
        REQUIRE(f.size() > 4);
        REQUIRE(d.wrap(f.data() + 4, f.size() - 4));
        if (d.Instrument() == 2 && d.CorrelationId() == 31) { republished = true; CHECK(d.Reason() == sp_proto::SourceReason::Republish); }
    }
    CHECK(republished);
    CHECK(h.stats().peers == 1); CHECK(h.stats().replies == 1); CHECK(h.stats().reply_dropped == 0);
    control->close();
    pub->close();
    std::filesystem::remove_all(dir);
}
#endif
