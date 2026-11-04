#pragma once

// A dot path into a JSON document ("data.k.p", "b.0.1"), at most 4 steps, each
// an object key or a non-negative array index, resolved against a simdjson
// On Demand value. Lookups are unordered (simdjson rewinds within the object),
// so a pack may list fields in any order.

#include <simdjson.h>

#include <cstdint>
#include <inplace_vector>
#include <string>
#include <string_view>

namespace feeds {

struct PathStep {
    std::string key;     // object key, or empty when index is used
    uint32_t    index = 0;
    bool        is_index = false;
};

struct JsonPath {
    static constexpr size_t kMaxDepth = 4;
    std::inplace_vector<PathStep, kMaxDepth> steps;   // in place: walk() runs per field per frame
    std::string text;

    static bool parse(std::string_view s, JsonPath& out, std::string& err);
};

// Resolves `path` from `root`. Returns false when any step is missing.
bool walk(simdjson::ondemand::object& root, const JsonPath& path, simdjson::ondemand::value& out) noexcept;

// Scalar readers over an On Demand value. Numbers given as JSON strings or
// as JSON numbers are both accepted; the raw token is parsed exactly.
bool read_string(simdjson::ondemand::value& v, std::string_view& out) noexcept;
bool read_int64(simdjson::ondemand::value& v, int64_t& out) noexcept;
bool read_uint64(simdjson::ondemand::value& v, uint64_t& out) noexcept;
bool read_bool(simdjson::ondemand::value& v, bool& out) noexcept;
bool read_decimal(simdjson::ondemand::value& v, int64_t& out) noexcept;   // -> int64 at 1e-9
// The scalar as text, whatever its JSON type (for enum maps): "true", "123", "Buy".
bool read_token(simdjson::ondemand::value& v, std::string_view& out) noexcept;

inline std::string_view trim(std::string_view s) noexcept {
    while (!s.empty() && (s.back() == ' ' || s.back() == '\n' || s.back() == '\r' || s.back() == '\t')) s.remove_suffix(1);
    while (!s.empty() && (s.front() == ' ' || s.front() == '\n' || s.front() == '\r' || s.front() == '\t')) s.remove_prefix(1);
    return s;
}

} // namespace feeds
