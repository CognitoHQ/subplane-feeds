// The gateway's observability surface: endpoint keys against the shared vectors,
// drop reasons, and the /metrics and /topology documents.
#include "obs/endpoint_key.h"
#include "obs/observe.h"

#include "doctest.h"

#include <yaml-cpp/yaml.h>

#include <string>

using namespace feeds;

TEST_CASE("endpoint keys: every vector in contract/topology-keys.json (the agent's docs/topology-keys.json)") {
    const YAML::Node v = YAML::LoadFile(std::string(FEEDS_SOURCE_DIR) + "/contract/topology-keys.json"); // JSON is YAML
    REQUIRE(v["schema"].as<std::string>() == "subplane.topology-keys/1");
    int n = 0;
    for (const auto& t : v["aeron"]) {
        INFO(t["channel"].as<std::string>());
        CHECK(aeron_key(t["channel"].as<std::string>(), t["stream"].as<int32_t>(), t["aeron_dir"].as<std::string>(),
                        t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["udp"]) {
        CHECK(udp_key(t["group"].as<std::string>(), t["address"].as<std::string>(), t["port"].as<uint16_t>(),
                      t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["tcp"]) {
        CHECK(tcp_key(t["address"].as<std::string>(), t["port"].as<uint16_t>(), t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["shm_ring"]) {
        CHECK(shm_ring_key(t["path"].as<std::string>(), t["host"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    for (const auto& t : v["url"]) {
        INFO(t["url"].as<std::string>());
        CHECK(url_key(t["url"].as<std::string>()) == t["key"].as<std::string>());
        ++n;
    }
    CHECK(n == 20);
}

namespace {
// Answers each offer with a scripted result, and pretends to be an Aeron stream.
struct ScriptedPublisher final : Publisher {
    Offer next = Offer::OK;
    bool sub = false;
    Offer publish(const uint8_t*, size_t) noexcept override { return next; }
    std::string describe() const override { return "scripted"; }
    bool connected() const noexcept override { return sub; }
    std::string endpoint_key(std::string_view host) const override { return aeron_key("aeron:ipc", 1001, "/dev/shm/aeron-test", host); }
};
struct Rig {
    Pack pack; Instruments ins; ScriptedPublisher pub;
    Rig() {
        std::string err;
        REQUIRE_MESSAGE(Pack::load_with_registry(std::string(FEEDS_SOURCE_DIR) + "/packs/binance-spot.yaml", "", pack, err), err);
        REQUIRE(ins.load_symbols({"BTCUSDT", "ETHUSDT"}, pack.connect.symbol_lower, err));
    }
    static GatewayOptions opts() { GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false; return o; }
    Identity id() const { Identity i; i.instance = "bn1"; i.host = "Box1"; i.version = "0.1.0"; i.epoch = 7; i.pid = 42; i.site = "ny4"; i.labels = {{"team", "md"}}; return i; }
};
const char* kTrade = R"({"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1.0","q":"1.0","T":1,"m":true,"M":true}})";
bool has(const std::string& doc, const std::string& needle) { return doc.find(needle) != std::string::npos; }
} // namespace

TEST_CASE("drops are counted by the publisher's reason, and dropped is their sum") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.pub, Rig::opts());
    gw.on_frame(kTrade, 1);
    r.pub.next = Offer::NOT_CONNECTED; gw.on_frame(kTrade, 2); gw.on_frame(kTrade, 3);
    r.pub.next = Offer::BACK_PRESSURED; gw.on_frame(kTrade, 4);
    r.pub.next = Offer::FAILED; gw.on_frame(kTrade, 5);
    const GatewayStats& s = gw.stats();
    CHECK(s.published == 1);
    CHECK(s.dropped_not_connected == 2); CHECK(s.dropped_back_pressured == 1); CHECK(s.dropped_failed == 1);
    CHECK(s.dropped == 4);
    CHECK(s.last_sequence == 5);
}

TEST_CASE("metrics: identity, counters by venue, drop reasons, edges, histograms in seconds") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.pub, Rig::opts());
    gw.on_connected(1);
    gw.on_frame(kTrade, 2);
    r.pub.next = Offer::NOT_CONNECTED; gw.on_frame(kTrade, 3);
    const Observer o(gw, r.pack, r.ins, r.pub, r.id(), r.pack.connect.url, true);
    const std::string m = o.metrics_text();
    const std::string c = R"(service="spwsgw",instance="bn1",host="Box1")";
    CHECK(has(m, "# TYPE subplane_service_info gauge\nsubplane_service_info{" + c + R"(,version="0.1.0",epoch="7",site="ny4",env="",venue="binance-spot",team="md"} 1)"));
    CHECK(has(m, "subplane_gateway_frames_total{" + c + R"(,venue="binance-spot"} 2)"));
    CHECK(has(m, "subplane_gateway_trades_total{" + c + R"(,venue="binance-spot"} 2)"));
    CHECK(has(m, "subplane_gateway_dropped_total{" + c + R"(,venue="binance-spot",reason="not_connected"} 1)"));
    CHECK(has(m, "subplane_gateway_dropped_total{" + c + R"(,venue="binance-spot",reason="back_pressured"} 0)"));
    CHECK(has(m, "subplane_gateway_connected{" + c + R"(,venue="binance-spot"} 1)"));
    CHECK(has(m, "subplane_gateway_publication_connected{" + c + R"(,venue="binance-spot"} 0)"));
    CHECK(has(m, "# TYPE subplane_gateway_frame_to_offer_seconds histogram"));
    CHECK(has(m, "subplane_gateway_recv_to_offer_seconds_bucket{" + c + R"(,venue="binance-spot",le="+Inf"})"));
    const std::string up = R"(subplane_topology_edge{)" + c + R"(,direction="upstream",role="data",key=")" + url_key(r.pack.connect.url) + R"(",venue="binance-spot"} 1)";
    CHECK(has(m, up));
    CHECK(has(m, R"(direction="upstream",role="recap",key="https://data-api.binance.vision/api/v3/depth",venue="binance-spot"} 1)"));
    CHECK(has(m, R"(direction="downstream",role="data",key="aeron-ipc://box1/dev/shm/aeron-test#1001",venue="binance-spot"} 0)"));
    // Each family once: HELP and TYPE are not repeated for the per-reason series.
    size_t n = 0;
    for (size_t p = m.find("# TYPE subplane_gateway_dropped_total"); p != std::string::npos; p = m.find("# TYPE subplane_gateway_dropped_total", p + 1)) ++n;
    CHECK(n == 1);
}

TEST_CASE("topology: node identity, the exchange and its snapshot endpoint upstream, the Aeron stream downstream") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.pub, Rig::opts());
    const Observer o(gw, r.pack, r.ins, r.pub, r.id(), r.pack.connect.url, true);
    std::string t = o.topology_json();
    CHECK(has(t, R"({"schema":"subplane.topology/1","node":{"id":"Box1/spwsgw/bn1","service":"spwsgw","instance":"bn1","host":"Box1","pid":42,"version":"0.1.0","epoch":7,"site":"ny4","env":""})"));
    CHECK(has(t, R"("key":")" + url_key(r.pack.connect.url) + R"(","role":"data","state":"down")"));
    CHECK(has(t, R"("key":"https://data-api.binance.vision/api/v3/depth","role":"recap","state":"up")"));
    CHECK(has(t, R"("downstream":[{"key":"aeron-ipc://box1/dev/shm/aeron-test#1001","role":"data","state":"down")"));
    std::string why;
    CHECK_FALSE(o.healthy(why));
    CHECK(has(why, "not connected"));
    gw.on_connected(1);
    r.pub.sub = true;
    t = o.topology_json();
    CHECK(has(t, R"("role":"data","state":"up","local":{"venue":"binance-spot","venue_id":10,"instruments":2})"));
    CHECK(has(t, R"("downstream":[{"key":"aeron-ipc://box1/dev/shm/aeron-test#1001","role":"data","state":"up")"));
    CHECK(o.healthy(why));
    YAML::Node doc = YAML::Load(t);   // well-formed JSON
    CHECK(doc["upstream"].size() == 2);
}

TEST_CASE("topology on replay: the recording is upstream, no REST endpoints, always healthy") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.pub, Rig::opts());
    const Observer o(gw, r.pack, r.ins, r.pub, r.id(), "file:///data/frames.jsonl", false);
    const std::string t = o.topology_json();
    CHECK(has(t, R"("key":"file:///data/frames.jsonl","role":"data","state":"up")"));
    CHECK_FALSE(has(t, R"("role":"recap")"));
    std::string why;
    CHECK(o.healthy(why));
}

TEST_CASE("identity label names follow the Prometheus rules") {
    CHECK(Observer::valid_label_name("team"));
    CHECK(Observer::valid_label_name("_x9"));
    CHECK_FALSE(Observer::valid_label_name(""));
    CHECK_FALSE(Observer::valid_label_name("9a"));
    CHECK_FALSE(Observer::valid_label_name("__x"));
    CHECK_FALSE(Observer::valid_label_name("a-b"));
}
