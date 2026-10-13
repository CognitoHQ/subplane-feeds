#include "pack.h"

#include "doctest.h"

using namespace feeds;

namespace {
const char* kGood = R"(
exchange: binance-spot
venue_id: 10
connect:
  url: wss://data-stream.binance.vision/stream
  subscribe: { method: SUBSCRIBE, params: "{streams}", id: 1 }
streams:
  - name: "{symbol}@trade"
    message: Trade
    fields:
      ExchangeTs: { path: T, unit: ms }
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

TEST_CASE("the shipped Binance pack loads") {
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load(std::string(FEEDS_SOURCE_DIR) + "/packs/binance-spot.yaml", p, err), err);
    CHECK(p.exchange == "binance-spot");
    CHECK(p.venue_id == 10);
    CHECK(p.streams.size() == 3);
    CHECK(p.find_by_suffix("@trade") != nullptr);
    CHECK(p.find_by_suffix("@depth@100ms") != nullptr);
    CHECK(p.find_by_suffix("@depth") == nullptr);
    CHECK(p.connect.subscribe_json.find("{streams}") != std::string::npos);
}

TEST_CASE("pack fields resolve to canonical fields with their conversions") {
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load_text(kGood, p, err), err);
    const StreamDef& t = p.streams[0];
    REQUIRE(t.msg != nullptr);
    CHECK(t.fields[0].field->name == "ExchangeTs"); CHECK(t.fields[0].unit == TimeUnit::MS);
    CHECK(t.fields[1].type == ValueType::DECIMAL);
    CHECK(t.fields[2].map.at("true") == "Sell");
    CHECK(p.streams[1].book == "binance_diff");
    CHECK(p.streams[1].has_snapshot);
    CHECK(expand_symbol(p.streams[1].snapshot.url_tpl, "btcusdt", "BTCUSDT") == "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000");
    CHECK(expand_symbol(t.name_tpl, "btcusdt", "BTCUSDT") == "btcusdt@trade");
}

TEST_CASE("pack errors name the problem") {
    Pack p; std::string err;
    auto bad = [&](const std::string& find, std::string yaml) {
        CHECK_FALSE(Pack::load_text(yaml, p, err));
        CHECK_MESSAGE(err.find(find) != std::string::npos, err);
    };
    std::string g = kGood;
    bad("unknown key 'colour'", g + "colour: red\n");
    bad("not a field of Trade", std::string(g).replace(g.find("Price:"), 6, "Prize:"));
    bad("prefix fields are set by the gateway", std::string(g).replace(g.find("Price:"), 6, "Sequence:"));
    bad("unknown canonical message", std::string(g).replace(g.find("message: Trade"), 14, "message: Tirade"));
    bad("unknown book strategy", std::string(g).replace(g.find("binance_diff"), 12, "magic_diff"));
    bad("needs role 'asks'", std::string(g).replace(g.find("asks: a, "), 9, ""));
    bad("must contain {symbol}", std::string(g).replace(g.find("{symbol}@trade"), 14, "btc@trade"));
    bad("map value 'Sideways'", std::string(g).replace(g.find("Sell"), 4, "Sideways"));
    bad("unit must be", std::string(g).replace(g.find("unit: ms"), 8, "unit: min"));
    bad("venue_id", "exchange: x\nconnect: {url: wss://a/b, subscribe: {p: \"{streams}\"}}\nstreams: [{name: \"{symbol}@t\", message: Trade, fields: {}}]\n");
}
