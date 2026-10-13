#include "pack.h"

#include "doctest.h"

using namespace feeds;

namespace {
const std::string kPacks = std::string(FEEDS_SOURCE_DIR) + "/packs/";
Registry shipped_registry() { Registry r; std::string err; REQUIRE_MESSAGE(r.load_file(kPacks + "registry.yaml", err), err); return r; }

const char* kGood = R"(
exchange: binance-spot
venue_id: 10
connect:
  url: wss://data-stream.binance.vision/stream
  wrapper: { stream: stream, data: data }
  subscribe: { method: SUBSCRIBE, params: "{streams}", id: 1 }
streams:
  - name: "{symbol}@trade"
    message: Trade
    fields:
      ExchangeTs: T@ms
      Price: { path: p, type: decimal }
      Aggressor: { path: m, map: { "true": Sell, "false": Buy } }
  - name: "{symbol}@depth@100ms"
    book: binance_diff
    fields: { first_id: U, last_id: u, bids: b, asks: a, ExchangeTs: { path: E, unit: ms } }
    snapshot:
      url: "https://data-api.binance.vision/api/v3/depth?symbol={SYMBOL}&limit=1000"
      fields: { last_id: lastUpdateId, bids: bids, asks: asks }
)";
}

TEST_CASE("every shipped pack loads against the shipped registry") {
    const Registry reg = shipped_registry();
    for (const char* name : {"binance-spot", "coinbase-exchange", "kraken", "bitstamp", "okx"}) {
        Pack p; std::string err;
        CHECK_MESSAGE(Pack::load(kPacks + name + ".yaml", reg, p, err), err);
        CHECK(p.venue_id != 0);
    }
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load_with_registry(kPacks + "binance-spot.yaml", "", p, err), err);   // finds registry.yaml next to the pack
    CHECK(p.exchange == "binance-spot");
    CHECK(p.streams.size() == 3);
    std::string_view sym;
    REQUIRE(p.find_by_name("btcusdt@trade", sym) != nullptr); CHECK(sym == "btcusdt");
    CHECK(p.find_by_name("btcusdt@depth@100ms", sym) != nullptr);
    CHECK(p.find_by_name("btcusdt@depth", sym) == nullptr);
    CHECK(p.connect.subscribe.size() == 1);
    CHECK(p.connect.subscribe[0] == R"({"method": "SUBSCRIBE", "params": "{streams}", "id": 1})");
    CHECK(p.streams[0].fields[4].map == p.registry->map("taker_from_maker_bool"));
}

TEST_CASE("pack fields resolve to canonical fields with their conversions") {
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load_text(kGood, Registry{}, p, err), err);
    const StreamDef& t = p.streams[0];
    REQUIRE(t.msg != nullptr);
    CHECK(t.fields[0].field->name == "ExchangeTs"); CHECK(t.fields[0].unit == TimeUnit::MS);
    CHECK(t.fields[1].type == ValueType::DECIMAL);
    REQUIRE(t.fields[2].map != nullptr); CHECK(t.fields[2].map->at("true") == "Sell");
    CHECK(p.streams[1].book == "binance_diff");
    CHECK(p.streams[1].has_snapshot);
    CHECK(Pack::role(p.streams[1].fields, "bids")->levels->name == "pairs");
    CHECK(expand_symbol(p.streams[1].snapshot.url_tpl, "btcusdt", "BTCUSDT") == "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000");
    CHECK(expand_symbol(t.name_tpl, "btcusdt", "BTCUSDT") == "btcusdt@trade");
    Pack copy = p;   // inline maps live in the shared registry, so copies keep working
    CHECK(copy.streams[0].fields[2].map->at("false") == "Buy");
}

TEST_CASE("route mode, each, frame paths, prefix and suffix names, ping, channels") {
    const Registry reg = shipped_registry();
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load(kPacks + "okx.yaml", reg, p, err), err);
    CHECK(p.connect.use_route);
    CHECK(p.connect.route_type.text == "arg.channel");
    CHECK(p.connect.route_symbol.text == "arg.instId"); CHECK(p.connect.route_symbol_frame);
    CHECK(p.connect.ping_text == "ping"); CHECK(p.connect.ping_every_s == 20); CHECK(p.connect.ping_reply == "pong");
    CHECK(p.connect.stream_object == R"({"channel": "{channel}", "instId": "{SYMBOL}"})");
    REQUIRE(p.find_by_kind("trades") != nullptr); CHECK(p.find_by_kind("trades")->channel == "trades");
    CHECK(p.find_by_kind("trades")->has_each); CHECK(p.find_by_kind("trades")->each.text == "data");
    CHECK(p.find_by_kind("books") == nullptr);

    REQUIRE_MESSAGE(Pack::load(kPacks + "kraken.yaml", reg, p, err), err);
    CHECK_FALSE(p.connect.route_symbol_frame);
    const StreamDef* book = p.find_by_kind("book");
    REQUIRE(book != nullptr);
    CHECK(Pack::role(book->fields, "kind")->frame_scope);
    CHECK(Pack::role(book->fields, "bids")->levels->object);
    CHECK(Pack::role(book->fields, "ExchangeTs")->unit == TimeUnit::ISO8601);
    CHECK(p.connect.subscribe.size() == 3);
    CHECK(p.connect.subscribe[2] == R"({"method": "subscribe", "params": {"channel": "book", "symbol": "{symbols}", "depth": 25}})");

    REQUIRE_MESSAGE(Pack::load(kPacks + "bitstamp.yaml", reg, p, err), err);
    std::string_view sym;
    REQUIRE(p.find_by_name("live_trades_btcusd", sym) != nullptr); CHECK(sym == "btcusd");
    REQUIRE(p.find_by_name("order_book_ethusd", sym) != nullptr); CHECK(sym == "ethusd");
    CHECK(p.find_by_name("order_book_", sym) == nullptr);
    CHECK(p.streams[0].channel == "live_trades");
    CHECK(p.connect.wrapper_type == "event");
    CHECK(p.streams[0].accepts("trade")); CHECK_FALSE(p.streams[0].accepts("data"));

    REQUIRE_MESSAGE(Pack::load(kPacks + "coinbase-exchange.yaml", reg, p, err), err);
    const StreamDef* l2 = p.find_by_kind("l2update");
    REQUIRE(l2 != nullptr); CHECK(l2 == p.find_by_kind("snapshot"));
    CHECK(Pack::role(l2->fields, "changes")->levels->has_side);
    CHECK(Pack::role(l2->fields, "changes")->levels->side_map == p.registry->map("side"));
}

TEST_CASE("pack errors name the problem") {
    Pack p; std::string err;
    auto bad = [&](const std::string& find, std::string yaml) {
        CHECK_FALSE(Pack::load_text(yaml, Registry{}, p, err));
        CHECK_MESSAGE(err.find(find) != std::string::npos, err);
    };
    std::string g = kGood;
    bad("unknown key 'colour'", g + "colour: red\n");
    bad("not a field of Trade", std::string(g).replace(g.find("Price:"), 6, "Prize:"));
    bad("prefix fields are set by the gateway", std::string(g).replace(g.find("Price:"), 6, "Sequence:"));
    bad("unknown canonical message", std::string(g).replace(g.find("message: Trade"), 14, "message: Tirade"));
    bad("unknown book strategy", std::string(g).replace(g.find("binance_diff"), 12, "magic_diff"));
    bad("role 'asks' is required", std::string(g).replace(g.find("asks: a, "), 9, ""));
    bad("must contain {symbol}", std::string(g).replace(g.find("{symbol}@trade"), 14, "btc@trade"));
    bad("map value 'Sideways'", std::string(g).replace(g.find("Sell"), 4, "Sideways"));
    bad("neither a registered time format nor a map", std::string(g).replace(g.find("T@ms"), 4, "T@min"));
    bad("not a registered level layout", std::string(g).replace(g.find("bids: b,"), 8, "bids: b@cubes,"));
    bad("exactly one of 'wrapper'", std::string(g).replace(g.find("wrapper: { stream: stream, data: data }"), 39, ""));
    bad("'match' is required when connect.route", std::string(g).replace(g.find("wrapper: { stream: stream, data: data }"), 39, "route: { type: t, symbol: s }"));
    bad("venue_id", "exchange: x\nconnect: {url: wss://a/b, wrapper: {}, subscribe: {p: \"{streams}\"}}\nstreams: [{name: \"{symbol}@t\", message: Trade, fields: {}}]\n");
    bad("must use {streams}", std::string(g).replace(g.find("{streams}"), 9, "all"));
    bad("only the 'kind' role may use a frame path", std::string(g).replace(g.find("bids: b,"), 8, "bids: $.b,"));
}

TEST_CASE("a pack may extend the registry locally") {
    std::string y = kGood;
    y.replace(y.find("T@ms"), 4, "T@binance_ts");
    y += "registry:\n  times: { binance_ts: ms }\n";
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load_text(y, Registry{}, p, err), err);
    CHECK(p.streams[0].fields[0].unit == TimeUnit::MS);
    CHECK(p.registry->time("binance_ts") != nullptr);
}
