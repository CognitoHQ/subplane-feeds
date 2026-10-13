#pragma once

// JSON -> canonical fields for flat streams, driven by a StreamDef. The message
// buffer is already begun (header + prefix); this writes only the pack's fields.
// Frame-scope fields ("$." paths under `each`) arrive as tokens the gateway read
// before it started iterating the records: simdjson On Demand is forward-only.

#include "pack.h"

#include <simdjson.h>

#include <string>
#include <vector>

namespace feeds {

// Tokens of the frame-scope fields of one stream, indexed like StreamDef::fields.
// Views into the frame buffer or simdjson's string buffer, both of which live until the next frame.
struct FrameTokens {
    static constexpr size_t kMax = 32;
    std::string_view text[kMax];
    bool present[kMax] = {};
    void reset(size_t n) noexcept { for (size_t i = 0; i < n && i < kMax; ++i) { text[i] = {}; present[i] = false; } }
};

class Mapper {
public:
    // Writes one mapped field from its scalar token (the text of a JSON string, or the raw number/bool).
    static bool write_token(const FieldMap& f, std::string_view tok, uint8_t* msg, std::string& err) noexcept;
    static bool write_field(const FieldMap& f, simdjson::ondemand::value& v, uint8_t* msg, std::string& err) noexcept;
    // Writes every field of `sd` from `rec` (and `frame` for "$." fields; null = resolve them against `rec` too).
    // False on the first failure (the message is then not published).
    static bool map_flat(const StreamDef& sd, simdjson::ondemand::object& rec, const FrameTokens* frame, uint8_t* msg, std::string& err) noexcept;
};

} // namespace feeds
