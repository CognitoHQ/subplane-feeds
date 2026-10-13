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

void format_decimal(int64_t v, char* buf, size_t cap) noexcept {
    const bool neg = v < 0;
    const uint64_t mag = neg ? 0 - static_cast<uint64_t>(v) : static_cast<uint64_t>(v);
    std::snprintf(buf, cap, "%s%llu.%09llu", neg ? "-" : "",
                  static_cast<unsigned long long>(mag / 1'000'000'000ull),
                  static_cast<unsigned long long>(mag % 1'000'000'000ull));
}

} // namespace feeds
