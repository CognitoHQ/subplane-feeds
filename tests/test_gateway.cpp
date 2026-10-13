#include "gateway.h"

#include "doctest.h"

#include <sstream>

using namespace feeds;

namespace {
struct Fixture {
    Pack pack; Instruments ins; std::ostringstream out; StdoutPublisher pub{out};
    explicit Fixture(const char* pack_name = "binance-spot", std::vector<std::string> symbols = {"BTCUSDT", "ETHUSDT"}) {
        std::string err;
        REQUIRE_MESSAGE(Pack::load_with_registry(std::string(FEEDS_SOURCE_DIR) + "/packs/" + pack_name + ".yaml", "", pack, err), err);
        REQUIRE(ins.load_symbols(symbols, pack.connect.symbol_lower, err));
    }
    std::vector<std::string> lines() { std::vector<std::string> v; std::string l; std::istringstream s(out.str()); while (std::getline(s, l)) v.push_back(l); return v; }
};
GatewayOptions quiet() { GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false; return o; }
}

TEST_CASE("a Binance trade frame becomes a canonical Trade with scaled decimals") {
    Fixture f;
    Gateway gw(f.pack, f.ins, f.pub, quiet());
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
    Gateway gw(f.pack, f.ins, f.pub, quiet());
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
    REQUIRE(gw.subscribe_messages().size() == 1);
    CHECK(gw.subscribe_messages()[0] == R"({"method": "SUBSCRIBE", "params": ["btcusdt@trade","ethusdt@trade","btcusdt@bookTicker","ethusdt@bookTicker","btcusdt@depth@100ms","ethusdt@depth@100ms"], "id": 1})");
}

TEST_CASE("book stream: snapshot request, snapshot, diffs") {
    Fixture f;
    GatewayOptions o = quiet(); o.snapshot_every = 0;
    Gateway gw(f.pack, f.ins, f.pub, o);
    gw.tick(1);
    SnapshotRequest rq;
    REQUIRE(gw.next_snapshot_request(rq));
    CHECK(rq.venue_symbol == "BTCUSDT"); CHECK(rq.url == "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000");
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

TEST_CASE("Coinbase: routing by type and product_id, iso8601, in-band level2") {
    Fixture f("coinbase-exchange", {"BTC-USD"});
    GatewayOptions o = quiet(); o.snapshot_every = 0;
    Gateway gw(f.pack, f.ins, f.pub, o);
    REQUIRE(gw.subscribe_messages().size() == 1);
    CHECK(gw.subscribe_messages()[0] == R"({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches", "ticker", "level2_batch"]})");
    gw.on_frame(R"({"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]})", 1);
    gw.on_frame(R"({"type":"match","trade_id":10,"maker_order_id":"a","taker_order_id":"b","side":"sell","size":"0.5","price":"100.25","product_id":"BTC-USD","sequence":50,"time":"2024-01-02T03:04:05.123456Z"})", 2);
    gw.on_frame(R"({"type":"ticker","sequence":51,"product_id":"BTC-USD","price":"100.25","best_bid":"100.2","best_bid_size":"1","best_ask":"100.3","best_ask_size":"2","time":"2024-01-02T03:04:05.5Z"})", 3);
    gw.on_frame(R"({"type":"match","product_id":"ETH-USD","trade_id":1,"side":"buy","size":"1","price":"1","time":"2024-01-02T03:04:05Z"})", 4);
    gw.on_frame(R"({"type":"l2update","product_id":"BTC-USD","changes":[["buy","100.2","3"]],"time":"2024-01-02T03:04:06Z"})", 5);   // before the snapshot: dropped
    gw.on_frame(R"({"type":"snapshot","product_id":"BTC-USD","bids":[["100.2","1"],["100.1","2"]],"asks":[["100.3","2"]]})", 6);
    gw.on_frame(R"({"type":"l2update","product_id":"BTC-USD","changes":[["buy","100.2","3"],["sell","100.3","0"]],"time":"2024-01-02T03:04:07Z"})", 7);
    auto l = f.lines();
    REQUIRE(l.size() == 5);
    CHECK(l[0] == "Trade iid=1 venue=20 seq=1 ets=1704164645123456000 flags=0 px=100250000000 qty=500000000 id=10 aggr=1");
    CHECK(l[1] == "Quote iid=1 venue=20 seq=2 ets=1704164645500000000 flags=0 bid=100200000000 bidqty=1000000000 ask=100300000000 askqty=2000000000");
    CHECK(l[2] == "BookSnapshot iid=1 venue=20 seq=3 ets=0 flags=0 depth=2 b0=100200000000x1000000000 b1=100100000000x2000000000 a0=100300000000x2000000000 a1=0x0");
    CHECK(l[3] == "BookUpdate iid=1 venue=20 seq=4 ets=1704164647000000000 flags=0 side=1 action=2 level=0 px=100200000000 qty=3000000000");
    CHECK(l[4] == "BookUpdate iid=1 venue=20 seq=5 ets=1704164647000000000 flags=0 side=2 action=3 level=0 px=100300000000 qty=0");
    CHECK(gw.stats().unmatched_streams == 1); CHECK(gw.stats().unknown_symbols == 1); CHECK(gw.stats().book_errors == 0);
}

TEST_CASE("Kraken: several records per frame, symbol per record, kind on the frame, object levels") {
    Fixture f("kraken", {"BTC/USD", "ETH/USD"});
    GatewayOptions o = quiet(); o.snapshot_every = 0;
    Gateway gw(f.pack, f.ins, f.pub, o);
    CHECK(gw.subscribe_messages().size() == 3);
    CHECK(gw.subscribe_messages()[0] == R"({"method": "subscribe", "params": {"channel": "trade", "symbol": ["BTC/USD","ETH/USD"]}})");
    gw.on_frame(R"({"method":"subscribe","result":{"channel":"trade","symbol":"BTC/USD"},"success":true,"time_in":"2024-01-01T00:00:00.000000Z"})", 1);
    gw.on_frame(R"({"channel":"heartbeat"})", 1);
    gw.on_frame(R"({"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":65000.1,"qty":0.001,"ord_type":"market","trade_id":7,"timestamp":"2024-01-02T03:04:05.000001Z"},{"symbol":"ETH/USD","side":"sell","price":3000,"qty":2,"ord_type":"limit","trade_id":8,"timestamp":"2024-01-02T03:04:05.000002Z"}]})", 2);
    gw.on_frame(R"({"channel":"ticker","type":"snapshot","data":[{"symbol":"ETH/USD","bid":2999.5,"bid_qty":1.5,"ask":3000.5,"ask_qty":2.5,"last":3000}]})", 3);
    gw.on_frame(R"({"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":65000,"qty":1},{"price":64999,"qty":2}],"asks":[{"price":65001,"qty":3}],"checksum":1}]})", 4);
    gw.on_frame(R"({"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":64999,"qty":0}],"asks":[],"checksum":2,"timestamp":"2024-01-02T03:04:06Z"}]})", 5);
    auto l = f.lines();
    REQUIRE(l.size() == 5);
    CHECK(l[0] == "Trade iid=1 venue=30 seq=1 ets=1704164645000001000 flags=0 px=65000100000000 qty=1000000 id=7 aggr=1");
    CHECK(l[1] == "Trade iid=2 venue=30 seq=2 ets=1704164645000002000 flags=0 px=3000000000000 qty=2000000000 id=8 aggr=2");
    CHECK(l[2] == "Quote iid=2 venue=30 seq=3 ets=0 flags=0 bid=2999500000000 bidqty=1500000000 ask=3000500000000 askqty=2500000000");
    CHECK(l[3] == "BookSnapshot iid=1 venue=30 seq=4 ets=0 flags=0 depth=2 b0=65000000000000x1000000000 b1=64999000000000x2000000000 a0=65001000000000x3000000000 a1=0x0");
    CHECK(l[4] == "BookUpdate iid=1 venue=30 seq=5 ets=1704164646000000000 flags=0 side=1 action=3 level=1 px=64999000000000 qty=0");
    CHECK(gw.stats().acks == 1); CHECK(gw.stats().unmatched_streams == 1); CHECK(gw.stats().records == 5);
}

TEST_CASE("OKX: frame-scope symbol under each, stream objects, text pong") {
    Fixture f("okx", {"BTC-USDT"});
    Gateway gw(f.pack, f.ins, f.pub, quiet());
    REQUIRE(gw.subscribe_messages().size() == 1);
    CHECK(gw.subscribe_messages()[0] == R"({"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT"},{"channel": "tickers", "instId": "BTC-USDT"}]})");
    gw.on_frame("pong", 1);
    gw.on_frame(R"({"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"},"connId":"x"})", 2);
    gw.on_frame(R"({"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"130","px":"42.1","sz":"0.5","side":"buy","ts":"1629386781174"}]})", 3);
    gw.on_frame(R"({"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","bidPx":"42","bidSz":"1","askPx":"42.2","askSz":"2","ts":"1629386781175"}]})", 4);
    auto l = f.lines();
    REQUIRE(l.size() == 2);
    CHECK(l[0] == "Trade iid=1 venue=40 seq=1 ets=1629386781174000000 flags=0 px=42100000000 qty=500000000 id=130 aggr=1");
    CHECK(l[1] == "Quote iid=1 venue=40 seq=2 ets=1629386781175000000 flags=0 bid=42000000000 bidqty=1000000000 ask=42200000000 askqty=2000000000");
    CHECK(gw.stats().acks == 2);
}

TEST_CASE("Bitstamp: symbol at the end of the channel, event as the wrapper type, snapshot-only book") {
    Fixture f("bitstamp", {"btcusd"});
    Gateway gw(f.pack, f.ins, f.pub, quiet());
    REQUIRE(gw.subscribe_messages().size() == 2);
    CHECK(gw.subscribe_messages()[0] == R"({"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}})");
    CHECK(gw.subscribe_messages()[1] == R"({"event": "bts:subscribe", "data": {"channel": "order_book_btcusd"}})");
    gw.on_frame(R"({"event":"bts:subscription_succeeded","channel":"live_trades_btcusd","data":{}})", 1);
    gw.on_frame(R"({"event":"trade","channel":"live_trades_btcusd","data":{"id":9,"timestamp":"1700000000","amount":0.1,"amount_str":"0.10000000","price":30000,"price_str":"30000.00","type":1,"microtimestamp":"1700000000123456","buy_order_id":1,"sell_order_id":2}})", 2);
    gw.on_frame(R"({"event":"data","channel":"order_book_btcusd","data":{"timestamp":"1700000001","microtimestamp":"1700000001000000","bids":[["29999.00","1.0"]],"asks":[["30001.00","2.0"],["30002.00","3.0"]]}})", 3);
    auto l = f.lines();
    REQUIRE(l.size() == 2);
    CHECK(l[0] == "Trade iid=1 venue=50 seq=1 ets=1700000000123456000 flags=0 px=30000000000000 qty=100000000 id=9 aggr=2");
    CHECK(l[1] == "BookSnapshot iid=1 venue=50 seq=2 ets=1700000001000000000 flags=0 depth=2 b0=29999000000000x1000000000 b1=0x0 a0=30001000000000x2000000000 a1=30002000000000x3000000000");
    CHECK(gw.stats().acks == 1);
}

TEST_CASE("batching: the messages of one frame share one offer, flushed at the end of the frame") {
    Fixture f("kraken", {"BTC/USD", "ETH/USD"});
    NullPublisher null;
    GatewayOptions o = quiet(); o.batch_bytes = 1376;
    Gateway gw(f.pack, f.ins, null, o);
    gw.on_frame(R"({"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2024-01-02T03:04:05Z"},{"symbol":"ETH/USD","side":"sell","price":2,"qty":2,"trade_id":2,"timestamp":"2024-01-02T03:04:05Z"},{"symbol":"BTC/USD","side":"buy","price":3,"qty":3,"trade_id":3,"timestamp":"2024-01-02T03:04:05Z"}]})", 1);
    CHECK(gw.stats().published == 3); CHECK(gw.stats().offers == 1); CHECK(null.bytes() == 3 * sp_canonical::Trade::kSize);
    gw.on_frame(R"({"channel":"ticker","type":"snapshot","data":[{"symbol":"ETH/USD","bid":1,"bid_qty":1,"ask":2,"ask_qty":2}]})", 2);
    CHECK(gw.stats().offers == 2);   // a new frame is a new offer: nothing waited
    // A frame bigger than the batch limit is offered in pieces, in order, still within the frame.
    GatewayOptions small = quiet(); small.batch_bytes = 2 * sp_canonical::Trade::kSize;
    NullPublisher null2;
    Gateway gw2(f.pack, f.ins, null2, small);
    gw2.on_frame(R"({"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2024-01-02T03:04:05Z"},{"symbol":"ETH/USD","side":"sell","price":2,"qty":2,"trade_id":2,"timestamp":"2024-01-02T03:04:05Z"},{"symbol":"BTC/USD","side":"buy","price":3,"qty":3,"trade_id":3,"timestamp":"2024-01-02T03:04:05Z"}]})", 1);
    CHECK(gw2.stats().published == 3); CHECK(gw2.stats().offers == 2);
    // The stdout publisher walks a batch message by message, so the fixture format is unchanged.
    std::ostringstream out; StdoutPublisher sp(out);
    Gateway gw3(f.pack, f.ins, sp, o);
    gw3.on_frame(R"({"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2024-01-02T03:04:05Z"},{"symbol":"ETH/USD","side":"sell","price":2,"qty":2,"trade_id":2,"timestamp":"2024-01-02T03:04:05Z"}]})", 1);
    std::string l; int n = 0; std::istringstream in(out.str()); while (std::getline(in, l)) { ++n; CHECK(l.rfind("Trade iid=", 0) == 0); }
    CHECK(n == 2);
}
