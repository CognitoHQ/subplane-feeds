#include "json_path.h"

#include "doctest.h"

using namespace feeds;

namespace {
struct Doc {
    simdjson::ondemand::parser parser;
    simdjson::padded_string json;
    simdjson::ondemand::document doc;
    simdjson::ondemand::object obj;
    explicit Doc(const char* text) : json(std::string(text)) {
        REQUIRE(parser.iterate(json).get(doc) == simdjson::SUCCESS);
        REQUIRE(doc.get_object().get(obj) == simdjson::SUCCESS);
    }
};
}

TEST_CASE("path parsing") {
    JsonPath p; std::string err;
    CHECK(JsonPath::parse("data.k.p", p, err)); CHECK(p.steps.size() == 3); CHECK(p.steps[1].key == "k");
    CHECK(JsonPath::parse("b.0.1", p, err)); CHECK(p.steps[1].is_index); CHECK(p.steps[2].index == 1);
    CHECK_FALSE(JsonPath::parse("", p, err));
    CHECK_FALSE(JsonPath::parse("a..b", p, err));
    CHECK_FALSE(JsonPath::parse("a.b.c.d.e", p, err)); CHECK(err.find("deeper") != std::string::npos);
}

TEST_CASE("walk resolves keys and indexes in any order") {
    Doc d(R"({"e":"trade","p":"0.001","q":100,"m":true,"b":[["0.0024","10"],["0.0023","5"]],"n":{"x":{"y":7}}})");
    JsonPath p; std::string err; simdjson::ondemand::value v;
    int64_t dec; uint64_t u; bool b; std::string_view s;
    REQUIRE(JsonPath::parse("m", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_bool(v, b)); CHECK(b);
    REQUIRE(JsonPath::parse("p", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_decimal(v, dec)); CHECK(dec == 1'000'000);
    REQUIRE(JsonPath::parse("q", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_decimal(v, dec)); CHECK(dec == 100'000'000'000); // JSON number as decimal
    REQUIRE(JsonPath::parse("q", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_uint64(v, u)); CHECK(u == 100);
    REQUIRE(JsonPath::parse("e", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_string(v, s)); CHECK(s == "trade");
    REQUIRE(JsonPath::parse("b.1.0", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_decimal(v, dec)); CHECK(dec == 2'300'000);
    REQUIRE(JsonPath::parse("n.x.y", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_uint64(v, u)); CHECK(u == 7);
    REQUIRE(JsonPath::parse("missing", p, err)); CHECK_FALSE(walk(d.obj, p, v));
    REQUIRE(JsonPath::parse("b.5", p, err)); CHECK_FALSE(walk(d.obj, p, v));
    REQUIRE(JsonPath::parse("m", p, err)); REQUIRE(walk(d.obj, p, v)); CHECK(read_token(v, s)); CHECK(s == "true");
}
