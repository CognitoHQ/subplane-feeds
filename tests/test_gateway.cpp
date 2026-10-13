#include "gateway.h"

#include "doctest.h"

#include <sstream>

using namespace feeds;

namespace {
struct Fixture {
    Pack pack; Instruments ins; std::ostringstream out; StdoutPublisher pub{out};
    Fixture() {
        std::string err;
        REQUIRE_MESSAGE(Pack::load(std::string(FEEDS_SOURCE_DIR) + "/packs/binance-spot.yaml", pack, err), err);
        REQUIRE(ins.load_symbols({"BTCUSDT", "ETHUSDT"}, true, err));
    }
    std::vector<std::string> lines() { std::vector<std::string> v; std::string l; std::istringstream s(out.str()); while (std::getline(s, l)) v.push_back(l); return v; }
};
}

TEST_CASE("a Binance trade frame becomes a canonical Trade with scaled decimals") {
    Fixture f;
    GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false;
    Gateway gw(f.pack, f.ins, f.pub, o);
    gw.on_frame(R"({"stream":"btcusdt@trade","data":{"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,"p":"65432.10000000","q":"0.00100000","T":1672515782130,"m":true,"M":true}})", 42);
    gw.on_frame(R"({"stream":"ethusdt@bookTicker","data":{"u":400900217,"s":"ETHUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000","A":"40.66000000"}})", 43);
    auto l = f.lines();
    REQUIRE(l.size() == 2);
    CHECK(l[0] == "Trade iid=1 venue=10 seq=1 ets=1672515782130000000 flags=0 px=65432100000000 qty=1000000 id=12345 aggr=2");
    CHECK(l[1] == "Quote iid=2 venue=10 seq=2 ets=0 flags=0 bid=25351900000 bidqty=31210000000 ask=25365200000 askqty=40660000000");
    CHECK(gw.stats().trades == 1); CHECK(gw.stats().quotes == 1); CHECK(gw.sequence() == 2);
}

TEST_CASE("unknown streams, symbols and acks are counted, not published") {
    Fixture f;
    GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false;
    Gateway gw(f.pack, f.ins, f.pub, o);
    gw.on_frame(R"({"result":null,"id":1})", 1);
    gw.on_frame(R"({"stream":"btcusdt@kline_1m","data":{}})", 2);
    gw.on_frame(R"({"stream":"dogeusdt@trade","data":{}})", 3);
    gw.on_frame(R"({"stream":"btcusdt@trade","data":{"p":"not a number","q":"1","T":1,"t":1,"m":false}})", 4);
    gw.on_frame("not json", 5);
    CHECK(gw.stats().acks == 1); CHECK(gw.stats().unmatched_streams == 1); CHECK(gw.stats().unknown_symbols == 1);
    CHECK(gw.stats().map_errors == 1); CHECK(gw.stats().parse_errors == 1);
    CHECK(gw.stats().published == 0);
}

TEST_CASE("heartbeats, definitions and the subscribe message") {
    Fixture f;
    GatewayOptions o; o.heartbeat_ms = 1000; o.definitions_ms = 60000; o.log_errors = false;
    Gateway gw(f.pack, f.ins, f.pub, o);
    gw.on_connected(1'000'000'000);
    gw.tick(1'000'000'000);
    gw.tick(1'500'000'000);
    gw.tick(2'000'000'000);
    auto l = f.lines();
    REQUIRE(l.size() == 4);
    CHECK(l[0] == "InstrumentDefinition iid=1 venue=10 seq=1 ets=1000000000 flags=0 symbol=BTCUSDT exchange=binance-spot base= quote=");
    CHECK(l[2].rfind("Heartbeat iid=0 venue=10 seq=3", 0) == 0);
    CHECK(l[3].rfind("Heartbeat iid=0 venue=10 seq=4", 0) == 0);
    CHECK(gw.subscribe_message() == R"({"method": "SUBSCRIBE", "params": ["btcusdt@trade","ethusdt@trade","btcusdt@bookTicker","ethusdt@bookTicker","btcusdt@depth@100ms","ethusdt@depth@100ms"], "id": "1"})");
}

TEST_CASE("book stream: snapshot request, snapshot, diffs") {
    Fixture f;
    GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false; o.snapshot_every = 0;
    Gateway gw(f.pack, f.ins, f.pub, o);
    gw.tick(1);
    SnapshotRequest rq;
    REQUIRE(gw.next_snapshot_request(rq));
    CHECK(rq.venue_symbol == "BTCUSDT"); CHECK(rq.url == "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000");
    REQUIRE(gw.next_snapshot_request(rq)); CHECK(rq.venue_symbol == "ETHUSDT");
    CHECK_FALSE(gw.next_snapshot_request(rq));
    gw.on_frame(R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1672515782136,"s":"BTCUSDT","U":157,"u":160,"b":[["0.0024","10"]],"a":[["0.0026","100"]]}})", 10);
    gw.on_snapshot("BTCUSDT", R"({"lastUpdateId":158,"bids":[["0.0024","5"],["0.0023","1"]],"asks":[["0.0026","7"]]})", 11);
    gw.on_frame(R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1672515782236,"s":"BTCUSDT","U":161,"u":161,"b":[["0.0023","0"]],"a":[]}})", 12);
    auto l = f.lines();
    REQUIRE(l.size() == 2);
    CHECK(l[0] == "BookSnapshot iid=1 venue=10 seq=1 ets=1672515782136000000 flags=0 depth=2 b0=2400000x10000000000 b1=2300000x1000000000 a0=2600000x100000000000 a1=0x0");
    CHECK(l[1] == "BookUpdate iid=1 venue=10 seq=2 ets=1672515782236000000 flags=0 side=1 action=3 level=1 px=2300000 qty=0");
    CHECK(gw.stats().snapshots_applied == 1); CHECK(gw.resyncs() == 0);
}
