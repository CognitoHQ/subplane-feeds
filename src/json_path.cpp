#include "json_path.h"

#include "decimal.h"

namespace feeds {

bool JsonPath::parse(std::string_view s, JsonPath& out, std::string& err) {
    out = JsonPath{};
    out.text = std::string(s);
    if (s.empty()) { err = "path is empty"; return false; }
    size_t start = 0;
    while (start <= s.size()) {
        size_t dot = s.find('.', start);
        if (dot == std::string_view::npos) dot = s.size();
        const std::string_view part = s.substr(start, dot - start);
        if (part.empty()) { err = "path '" + out.text + "' has an empty step"; return false; }
        if (out.steps.size() == kMaxDepth) { err = "path '" + out.text + "' deeper than " + std::to_string(kMaxDepth); return false; }
        PathStep st;
        uint64_t idx = 0;
        if (parse_uint64(part, idx)) {
            if (idx > 1u << 20) { err = "path '" + out.text + "': index too large"; return false; }
            st.is_index = true; st.index = static_cast<uint32_t>(idx);
        } else {
            st.key = std::string(part);
        }
        out.steps.unchecked_push_back(std::move(st));   // depth checked above
        start = dot + 1;
        if (dot == s.size()) break;
    }
    return true;
}

bool walk(simdjson::ondemand::object& root, const JsonPath& path, simdjson::ondemand::value& out) noexcept {
    if (path.steps.empty()) return false;
    const PathStep& first = path.steps[0];
    if (first.is_index) return false;
    simdjson::ondemand::value cur;
    if (root[first.key].get(cur) != simdjson::SUCCESS) return false;
    for (size_t i = 1; i < path.steps.size(); ++i) {
        const PathStep& st = path.steps[i];
        simdjson::ondemand::value next;
        if (st.is_index) {
            simdjson::ondemand::array arr;
            if (cur.get_array().get(arr) != simdjson::SUCCESS) return false;
            if (arr.at(st.index).get(next) != simdjson::SUCCESS) return false;
        } else {
            simdjson::ondemand::object obj;
            if (cur.get_object().get(obj) != simdjson::SUCCESS) return false;
            if (obj[st.key].get(next) != simdjson::SUCCESS) return false;
        }
        cur = next;
    }
    out = cur;
    return true;
}

bool read_string(simdjson::ondemand::value& v, std::string_view& out) noexcept {
    return v.get_string().get(out) == simdjson::SUCCESS;
}

bool read_token(simdjson::ondemand::value& v, std::string_view& out) noexcept {
    simdjson::ondemand::json_type t;
    if (v.type().get(t) != simdjson::SUCCESS) return false;
    if (t == simdjson::ondemand::json_type::string) return read_string(v, out);
    out = trim(v.raw_json_token());
    return true;
}

bool read_int64(simdjson::ondemand::value& v, int64_t& out) noexcept {
    std::string_view tok;
    if (!read_token(v, tok)) return false;
    return parse_int64(tok, out);
}

bool read_uint64(simdjson::ondemand::value& v, uint64_t& out) noexcept {
    std::string_view tok;
    if (!read_token(v, tok)) return false;
    return parse_uint64(tok, out);
}

bool read_bool(simdjson::ondemand::value& v, bool& out) noexcept {
    std::string_view tok;
    if (!read_token(v, tok)) return false;
    if (tok == "true" || tok == "1") { out = true; return true; }
    if (tok == "false" || tok == "0") { out = false; return true; }
    return false;
}

bool read_decimal(simdjson::ondemand::value& v, int64_t& out) noexcept {
    std::string_view tok;
    if (!read_token(v, tok)) return false;
    return parse_decimal(tok, out);
}

} // namespace feeds
