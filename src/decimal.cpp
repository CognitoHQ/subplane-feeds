#include "decimal.h"

#include <cstdio>

namespace feeds {

bool parse_decimal(std::string_view s, int64_t& out, bool* truncated) noexcept {
    if (truncated) *truncated = false;
    size_t i = 0;
    bool neg = false;
    if (i < s.size() && (s[i] == '-' || s[i] == '+')) { neg = s[i] == '-'; ++i; }
    if (i >= s.size()) return false;
    constexpr uint64_t kMaxMag = 9223372036854775807ull;
    uint64_t mag = 0;
    bool any = false;
    for (; i < s.size() && s[i] >= '0' && s[i] <= '9'; ++i) {
        any = true;
        const uint64_t d = static_cast<uint64_t>(s[i] - '0');
        if (mag > (kMaxMag - d) / 10) return false;
        mag = mag * 10 + d;
    }
    int frac = 0;
    if (i < s.size() && s[i] == '.') {
        ++i;
        for (; i < s.size() && s[i] >= '0' && s[i] <= '9'; ++i) {
            any = true;
            if (frac == 9) { if (truncated) *truncated = true; continue; }
            const uint64_t d = static_cast<uint64_t>(s[i] - '0');
            if (mag > (kMaxMag - d) / 10) return false;
            mag = mag * 10 + d;
            ++frac;
        }
    }
    if (!any || i != s.size()) return false;
    for (; frac < 9; ++frac) {
        if (mag > kMaxMag / 10) return false;
        mag *= 10;
    }
    out = neg ? -static_cast<int64_t>(mag) : static_cast<int64_t>(mag);
    return true;
}

bool parse_uint64(std::string_view s, uint64_t& out) noexcept {
    if (s.empty()) return false;
    uint64_t v = 0;
    for (const char c : s) {
        if (c < '0' || c > '9') return false;
        const uint64_t d = static_cast<uint64_t>(c - '0');
        if (v > (18446744073709551615ull - d) / 10) return false;
        v = v * 10 + d;
    }
    out = v;
    return true;
}

bool parse_int64(std::string_view s, int64_t& out) noexcept {
    bool neg = false;
    if (!s.empty() && (s[0] == '-' || s[0] == '+')) { neg = s[0] == '-'; s.remove_prefix(1); }
    uint64_t mag = 0;
    if (!parse_uint64(s, mag)) return false;
    if (mag > 9223372036854775807ull + (neg ? 1u : 0u)) return false;
    out = neg ? static_cast<int64_t>(0 - mag) : static_cast<int64_t>(mag);
    return true;
}

bool parse_time_unit(std::string_view s, TimeUnit& out) noexcept {
    if (s == "s") out = TimeUnit::S;
    else if (s == "ms") out = TimeUnit::MS;
    else if (s == "us") out = TimeUnit::US;
    else if (s == "ns") out = TimeUnit::NS;
    else if (s == "iso8601") out = TimeUnit::ISO8601;
    else return false;
    return true;
}

uint64_t to_ns(uint64_t v, TimeUnit u) noexcept {
    switch (u) {
        case TimeUnit::S: return v * 1'000'000'000ull;
        case TimeUnit::MS: return v * 1'000'000ull;
        case TimeUnit::US: return v * 1'000ull;
        default: return v;
    }
}

namespace {
bool digits(std::string_view s, size_t pos, size_t n, uint64_t& out) noexcept {
    if (pos + n > s.size()) return false;
    uint64_t v = 0;
    for (size_t i = 0; i < n; ++i) {
        const char c = s[pos + i];
        if (c < '0' || c > '9') return false;
        v = v * 10 + static_cast<uint64_t>(c - '0');
    }
    out = v;
    return true;
}
// Howard Hinnant's days_from_civil.
int64_t days_from_civil(int64_t y, unsigned m, unsigned d) noexcept {
    y -= m <= 2;
    const int64_t era = (y >= 0 ? y : y - 399) / 400;
    const uint64_t yoe = static_cast<uint64_t>(y - era * 400);
    const uint64_t doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const uint64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int64_t>(doe) - 719468;
}
} // namespace

bool parse_iso8601(std::string_view s, uint64_t& out_ns) noexcept {
    uint64_t y, mo, d, h, mi, sec;
    if (s.size() < 20) return false;
    if (!digits(s, 0, 4, y) || s[4] != '-' || !digits(s, 5, 2, mo) || s[7] != '-' || !digits(s, 8, 2, d) || s[10] != 'T' ||
        !digits(s, 11, 2, h) || s[13] != ':' || !digits(s, 14, 2, mi) || s[16] != ':' || !digits(s, 17, 2, sec)) return false;
    size_t i = 19;
    uint64_t frac = 0;
    if (i < s.size() && s[i] == '.') {
        ++i;
        int n = 0;
        for (; i < s.size() && s[i] >= '0' && s[i] <= '9'; ++i) {
            if (n == 9) return false;
            frac = frac * 10 + static_cast<uint64_t>(s[i] - '0');
            ++n;
        }
        if (n == 0) return false;
        for (; n < 9; ++n) frac *= 10;
    }
    if (i + 1 != s.size() || s[i] != 'Z') return false;
    if (y < 1970 || mo < 1 || mo > 12 || d < 1 || d > 31 || h > 23 || mi > 59 || sec > 60) return false;
    static constexpr unsigned kDays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    if (d > kDays[mo - 1] + ((mo == 2 && leap) ? 1u : 0u)) return false;
    const int64_t days = days_from_civil(static_cast<int64_t>(y), static_cast<unsigned>(mo), static_cast<unsigned>(d));
    const uint64_t secs = static_cast<uint64_t>(days) * 86400ull + h * 3600 + mi * 60 + sec;
    out_ns = secs * 1'000'000'000ull + frac;
    return true;
}

void format_decimal(int64_t v, char* buf, size_t cap) noexcept {
    const bool neg = v < 0;
    const uint64_t mag = neg ? 0 - static_cast<uint64_t>(v) : static_cast<uint64_t>(v);
    std::snprintf(buf, cap, "%s%llu.%09llu", neg ? "-" : "",
                  static_cast<unsigned long long>(mag / 1'000'000'000ull),
                  static_cast<unsigned long long>(mag % 1'000'000'000ull));
}

} // namespace feeds
