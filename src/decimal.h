#pragma once

// Exact conversions for the canonical schema: decimal strings to int64 at 1e-9
// (the schema's fixed PriceExponent), integers, and timestamps to nanoseconds.
// No floating point anywhere: "25.35190000" must become exactly 25351900000.

#include <cstdint>
#include <string_view>

namespace feeds {

inline constexpr int kDecimalExponent = -9;
inline constexpr int64_t kDecimalScale = 1'000'000'000;

// Parses [-]digits[.digits] with up to 9 fractional digits (more are truncated,
// which is noted via `truncated`). Rejects anything else and values beyond int64.
bool parse_decimal(std::string_view s, int64_t& out, bool* truncated = nullptr) noexcept;

bool parse_int64(std::string_view s, int64_t& out) noexcept;
bool parse_uint64(std::string_view s, uint64_t& out) noexcept;

enum class TimeUnit : uint8_t { NONE, S, MS, US, NS, ISO8601 };
bool parse_time_unit(std::string_view s, TimeUnit& out) noexcept;   // s | ms | us | ns | iso8601
uint64_t to_ns(uint64_t v, TimeUnit u) noexcept;                     // integer units only

// "YYYY-MM-DDTHH:MM:SS[.fffffffff]Z" -> nanoseconds since the Unix epoch, exactly.
// Rejects offsets other than Z, missing Z, out-of-range fields, more than 9 fraction digits.
bool parse_iso8601(std::string_view s, uint64_t& out_ns) noexcept;

// A timestamp token in `u`: digits for the integer units, the ISO-8601 text otherwise.
inline bool parse_time(std::string_view tok, TimeUnit u, uint64_t& out_ns) noexcept {
    if (u == TimeUnit::ISO8601) return parse_iso8601(tok, out_ns);
    uint64_t v = 0;
    if (!parse_uint64(tok, v)) return false;
    out_ns = to_ns(v, u);
    return true;
}

// int64 at 1e-9 -> "123.456789000" (always 9 fractional digits; exact, diffable).
void format_decimal(int64_t v, char* buf, size_t cap) noexcept;

} // namespace feeds
