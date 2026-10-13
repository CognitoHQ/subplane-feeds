#pragma once

// JSON -> canonical fields for flat streams, driven by a StreamDef. The message
// buffer is already begun (header + prefix); this writes only the pack's fields.

#include "pack.h"

#include <simdjson.h>

#include <string>

namespace feeds {

class Mapper {
public:
    // Writes one mapped field. False with err when the value is missing or malformed.
    static bool write_field(const FieldMap& f, simdjson::ondemand::value& v, uint8_t* msg, std::string& err) noexcept;
    // Writes every field of `sd` from `data`. False on the first failure (the message is then not published).
    static bool map_flat(const StreamDef& sd, simdjson::ondemand::object& data, uint8_t* msg, std::string& err) noexcept;
};

} // namespace feeds
