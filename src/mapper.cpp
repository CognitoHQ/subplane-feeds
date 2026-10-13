#include "mapper.h"

#include "canonical_encode.h"

#include <cstring>

namespace feeds {
namespace {

bool apply_scale(int64_t& v, int64_t scale, std::string& err) noexcept {
    if (scale == 0 || scale == 1) return true;
    int64_t r = 0;
    if (__builtin_mul_overflow(v, scale, &r)) { err = "scaled value overflows int64"; return false; }
    v = r;
    return true;
}

bool mapped(const FieldMap& f, std::string_view tok, int64_t& out, std::string& err) noexcept {
    auto it = f.map->find(std::string(tok));
    if (it == f.map->end()) { err = "value '" + std::string(tok) + "' not in map"; return false; }
    uint8_t e;
    if (enum_value(it->second, e)) { out = e; return true; }
    return parse_int64(it->second, out);
}

bool token_bool(std::string_view tok, bool& out) noexcept {
    if (tok == "true" || tok == "1") { out = true; return true; }
    if (tok == "false" || tok == "0") { out = false; return true; }
    return false;
}

} // namespace

bool Mapper::write_token(const FieldMap& f, std::string_view tok, uint8_t* msg, std::string& err) noexcept {
    const CanonicalField& cf = *f.field;
    const ValueType t = f.type;
    switch (cf.kind) {
        case FieldKind::I64: {
            int64_t x = 0;
            if (f.map) { if (!mapped(f, tok, x, err)) return false; }
            else if (t == ValueType::INT) { if (!parse_int64(tok, x)) { err = "not an integer"; return false; } }
            else if (t == ValueType::BOOL) { bool b; if (!token_bool(tok, b)) { err = "not a bool"; return false; } x = b; }
            else if (!parse_decimal(tok, x)) { err = "not a decimal"; return false; }   // AUTO and DECIMAL
            if (!apply_scale(x, f.scale, err)) return false;
            put_i64(msg, cf.offset, x);
            return true;
        }
        case FieldKind::U64: case FieldKind::U32: case FieldKind::U16: case FieldKind::U8: {
            uint64_t x = 0;
            if (f.map) { int64_t m; if (!mapped(f, tok, m, err)) return false; if (m < 0) { err = "negative map value"; return false; } x = static_cast<uint64_t>(m); }
            else if (f.unit != TimeUnit::NONE) { if (!parse_time(tok, f.unit, x)) { err = "not a timestamp in the given format"; return false; } }
            else if (t == ValueType::BOOL) { bool b; if (!token_bool(tok, b)) { err = "not a bool"; return false; } x = b; }
            else if (t == ValueType::DECIMAL) { int64_t d; if (!parse_decimal(tok, d) || d < 0) { err = "not a non-negative decimal"; return false; } x = static_cast<uint64_t>(d); }
            else if (!parse_uint64(tok, x)) { err = "not an unsigned integer"; return false; }
            if (f.scale > 1) { uint64_t r = 0; if (__builtin_mul_overflow(x, static_cast<uint64_t>(f.scale), &r)) { err = "scaled value overflows"; return false; } x = r; }
            const uint64_t max = cf.size == 8 ? UINT64_MAX : (1ull << (cf.size * 8)) - 1;
            if (x > max) { err = "value " + std::to_string(x) + " exceeds " + std::to_string(cf.size) + "-byte field"; return false; }
            if (cf.size == 8) put(msg, cf.offset, x);
            else if (cf.size == 4) put(msg, cf.offset, static_cast<uint32_t>(x));
            else if (cf.size == 2) put(msg, cf.offset, static_cast<uint16_t>(x));
            else put(msg, cf.offset, static_cast<uint8_t>(x));
            return true;
        }
        case FieldKind::CHARS: {
            if (tok.size() > cf.size) { err = "string longer than " + std::to_string(cf.size); return false; }
            std::memset(msg + cf.offset, 0, cf.size);
            std::memcpy(msg + cf.offset, tok.data(), tok.size());
            return true;
        }
        case FieldKind::I64_ARRAY:
            err = "array fields are filled by a strategy, not a mapping";
            return false;
    }
    return false;
}

bool Mapper::write_field(const FieldMap& f, simdjson::ondemand::value& v, uint8_t* msg, std::string& err) noexcept {
    std::string_view tok;
    if (!read_token(v, tok)) { err = "not a scalar"; return false; }
    return write_token(f, tok, msg, err);
}

bool Mapper::map_flat(const StreamDef& sd, simdjson::ondemand::object& rec, const FrameTokens* frame, uint8_t* msg, std::string& err) noexcept {
    for (size_t i = 0; i < sd.fields.size(); ++i) {
        const FieldMap& f = sd.fields[i];
        std::string e;
        if (f.frame_scope && frame) {
            if (!frame->present[i]) { err = f.target + ": frame path '$." + f.path.text + "' not found"; return false; }
            if (!write_token(f, frame->text[i], msg, e)) { err = f.target + ": " + e; return false; }
            continue;
        }
        simdjson::ondemand::value v;
        if (!walk(rec, f.path, v)) { err = f.target + ": path '" + f.path.text + "' not found"; return false; }
        if (!write_field(f, v, msg, e)) { err = f.target + ": " + e; return false; }
    }
    return true;
}

} // namespace feeds
