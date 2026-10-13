#include "decimal.h"

#include <string>
#include "doctest.h"

using namespace feeds;

TEST_CASE("decimal strings become exact int64 at 1e-9") {
    int64_t v = 0;
    CHECK(parse_decimal("25.35190000", v)); CHECK(v == 25'351'900'000);
    CHECK(parse_decimal("0.001", v)); CHECK(v == 1'000'000);
    CHECK(parse_decimal("100", v)); CHECK(v == 100'000'000'000);
    CHECK(parse_decimal("-1.5", v)); CHECK(v == -1'500'000'000);
    CHECK(parse_decimal("0", v)); CHECK(v == 0);
    CHECK(parse_decimal("9223372036.854775807", v)); CHECK(v == INT64_MAX);
    CHECK(parse_decimal("65432.10000000", v)); CHECK(v == 65'432'100'000'000);
}

TEST_CASE("decimal rejects garbage and overflow, truncates beyond nine fractional digits") {
    int64_t v = 0;
    bool trunc = false;
    CHECK_FALSE(parse_decimal("", v));
    CHECK_FALSE(parse_decimal("abc", v));
    CHECK_FALSE(parse_decimal("1.2.3", v));
    CHECK_FALSE(parse_decimal("1e5", v));
    CHECK_FALSE(parse_decimal("9223372037", v));           // > int64 at 1e-9
    CHECK_FALSE(parse_decimal(" 1", v));
    CHECK(parse_decimal("1.1234567891", v, &trunc)); CHECK(trunc); CHECK(v == 1'123'456'789);
    CHECK(parse_decimal("1.123456789", v, &trunc)); CHECK_FALSE(trunc);
}

TEST_CASE("integers and units") {
    uint64_t u = 0; int64_t i = 0;
    CHECK(parse_uint64("18446744073709551615", u)); CHECK(u == UINT64_MAX);
    CHECK_FALSE(parse_uint64("18446744073709551616", u));
    CHECK_FALSE(parse_uint64("-1", u));
    CHECK(parse_int64("-9223372036854775808", i)); CHECK(i == INT64_MIN);
    CHECK_FALSE(parse_int64("9223372036854775808", i));
    TimeUnit tu;
    CHECK(parse_time_unit("ms", tu)); CHECK(to_ns(1672515782136ull, tu) == 1672515782136000000ull);
    CHECK(parse_time_unit("s", tu)); CHECK(to_ns(5, tu) == 5'000'000'000ull);
    CHECK(parse_time_unit("us", tu)); CHECK(to_ns(5, tu) == 5'000ull);
    CHECK_FALSE(parse_time_unit("minutes", tu));
    char buf[40];
    format_decimal(25'351'900'000, buf, sizeof buf); CHECK(std::string(buf) == "25.351900000");
    format_decimal(-1'500'000'000, buf, sizeof buf); CHECK(std::string(buf) == "-1.500000000");
}

TEST_CASE("iso8601 timestamps parse exactly to nanoseconds") {
    uint64_t ns = 0;
    CHECK(parse_iso8601("1970-01-01T00:00:00Z", ns)); CHECK(ns == 0);
    CHECK(parse_iso8601("2024-01-02T03:04:05.123456Z", ns)); CHECK(ns == 1704164645123456000ull);
    CHECK(parse_iso8601("2024-01-02T03:04:05Z", ns)); CHECK(ns == 1704164645000000000ull);
    CHECK(parse_iso8601("2024-01-02T03:04:05.5Z", ns)); CHECK(ns == 1704164645500000000ull);
    CHECK(parse_iso8601("2024-01-02T03:04:05.123456789Z", ns)); CHECK(ns == 1704164645123456789ull);
    CHECK(parse_iso8601("2024-02-29T00:00:00Z", ns)); CHECK(ns == 1709164800000000000ull);   // leap day
    CHECK(parse_iso8601("2014-11-07T08:19:27.028459Z", ns)); CHECK(ns == 1415348367028459000ull);
    CHECK_FALSE(parse_iso8601("2024-01-02T03:04:05", ns));              // no Z
    CHECK_FALSE(parse_iso8601("2024-01-02T03:04:05+01:00", ns));        // offsets are not accepted
    CHECK_FALSE(parse_iso8601("2024-01-02T03:04:05.1234567890Z", ns));  // ten fraction digits
    CHECK_FALSE(parse_iso8601("2023-02-29T00:00:00Z", ns));             // not a leap year
    CHECK_FALSE(parse_iso8601("2024-13-01T00:00:00Z", ns));
    CHECK_FALSE(parse_iso8601("2024-01-02 03:04:05Z", ns));
    CHECK_FALSE(parse_iso8601("", ns));
    CHECK(parse_time("1672515782136", TimeUnit::MS, ns)); CHECK(ns == 1672515782136000000ull);
    CHECK(parse_time("2024-01-02T03:04:05Z", TimeUnit::ISO8601, ns)); CHECK(ns == 1704164645000000000ull);
    CHECK_FALSE(parse_time("2024-01-02T03:04:05Z", TimeUnit::MS, ns));
}
