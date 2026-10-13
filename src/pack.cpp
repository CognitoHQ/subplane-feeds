#include "pack.h"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <set>

namespace feeds {
namespace {

const std::set<std::string> kFieldKeys = {"path", "type", "unit", "scale", "map"};
const std::set<std::string> kStreamKeys = {"name", "message", "book", "fields", "snapshot"};
const std::set<std::string> kConnectKeys = {"url", "wrapper", "subscribe", "symbol_case", "reconnect_before_s"};
const std::set<std::string> kTopKeys = {"exchange", "venue_id", "connect", "streams"};
// Strategy roles a book stream may bind (checked so a typo fails at load, not at runtime).
const std::set<std::string> kBookRoles = {"first_id", "last_id", "bids", "asks", "ExchangeTs"};
const std::set<std::string> kSnapshotRoles = {"last_id", "bids", "asks"};

bool check_keys(const YAML::Node& n, const std::set<std::string>& allowed, const std::string& where, std::string& err) {
    for (const auto& kv : n) {
        const std::string k = kv.first.as<std::string>();
        if (!allowed.count(k)) { err = where + ": unknown key '" + k + "'"; return false; }
    }
    return true;
}

bool parse_field(const std::string& target, const YAML::Node& n, FieldMap& out, const std::string& where, std::string& err) {
    out = FieldMap{};
    out.target = target;
    std::string path_text;
    if (n.IsScalar()) {
        path_text = n.as<std::string>();
    } else if (n.IsMap()) {
        if (!check_keys(n, kFieldKeys, where, err)) return false;
        if (!n["path"]) { err = where + ": 'path' is required"; return false; }
        path_text = n["path"].as<std::string>();
        if (n["type"]) {
            const std::string t = n["type"].as<std::string>();
            if (t == "decimal") out.type = ValueType::DECIMAL;
            else if (t == "int") out.type = ValueType::INT;
            else if (t == "uint") out.type = ValueType::UINT;
            else if (t == "bool") out.type = ValueType::BOOL;
            else if (t == "string") out.type = ValueType::STRING;
            else { err = where + ": type must be decimal|int|uint|bool|string, got '" + t + "'"; return false; }
        }
        if (n["unit"]) {
            if (!parse_time_unit(n["unit"].as<std::string>(), out.unit)) { err = where + ": unit must be s|ms|us|ns"; return false; }
        }
        if (n["scale"]) {
            out.scale = n["scale"].as<int64_t>();
            if (out.scale == 0) { err = where + ": scale must be non-zero"; return false; }
        }
        if (n["map"]) {
            if (!n["map"].IsMap()) { err = where + ": map must be a mapping"; return false; }
            for (const auto& kv : n["map"]) out.map[kv.first.as<std::string>()] = kv.second.as<std::string>();
        }
    } else {
        err = where + ": expected a path string or a mapping";
        return false;
    }
    return JsonPath::parse(path_text, out.path, err) || (err = where + ": " + err, false);
}

bool parse_fields(const YAML::Node& n, std::vector<FieldMap>& out, const std::string& where, std::string& err) {
    if (!n || !n.IsMap()) { err = where + ": 'fields' must be a mapping"; return false; }
    for (const auto& kv : n) {
        FieldMap f;
        const std::string target = kv.first.as<std::string>();
        if (!parse_field(target, kv.second, f, where + "." + target, err)) return false;
        out.push_back(std::move(f));
    }
    return true;
}

// Resolves map values: enum names or integers, validated once at load.
bool check_map_values(const FieldMap& f, const std::string& where, std::string& err) {
    for (const auto& kv : f.map) {
        uint8_t e; int64_t i;
        if (!enum_value(kv.second, e) && !parse_int64(kv.second, i)) {
            err = where + ": map value '" + kv.second + "' is neither an enum name nor an integer"; return false;
        }
    }
    return true;
}

std::string yaml_to_json(const YAML::Node& n) {
    YAML::Emitter e;
    e << YAML::Flow << YAML::DoubleQuoted << n;
    return e.c_str();
}

} // namespace

std::string expand_symbol(std::string_view tpl, std::string_view stream_symbol, std::string_view venue_symbol) {
    std::string out;
    out.reserve(tpl.size() + 16);
    for (size_t i = 0; i < tpl.size();) {
        if (tpl.compare(i, 8, "{symbol}") == 0) { out += stream_symbol; i += 8; }
        else if (tpl.compare(i, 8, "{SYMBOL}") == 0) { out += venue_symbol; i += 8; }
        else out += tpl[i++];
    }
    return out;
}

const StreamDef* Pack::find_by_suffix(std::string_view suffix) const noexcept {
    for (const auto& s : streams) if (s.suffix == suffix) return &s;
    return nullptr;
}

const FieldMap* Pack::role(const std::vector<FieldMap>& v, std::string_view name) const noexcept {
    for (const auto& f : v) if (f.target == name) return &f;
    return nullptr;
}

bool Pack::load(const std::string& path, Pack& out, std::string& err) {
    YAML::Node root;
    try { root = YAML::LoadFile(path); } catch (const YAML::Exception& e) { err = "pack " + path + ": " + e.what(); return false; }
    try {
        std::string text = YAML::Dump(root);
        return load_text(text, out, err);
    } catch (const YAML::Exception& e) { err = "pack " + path + ": " + e.what(); return false; }
}

bool Pack::load_text(const std::string& yaml, Pack& out, std::string& err) {
    out = Pack{};
    YAML::Node root;
    try { root = YAML::Load(yaml); } catch (const YAML::Exception& e) { err = std::string("pack: ") + e.what(); return false; }
    try {
        if (!root.IsMap()) { err = "pack: expected a mapping"; return false; }
        if (!check_keys(root, kTopKeys, "pack", err)) return false;
        out.exchange = root["exchange"].as<std::string>("");
        if (out.exchange.empty()) { err = "pack: 'exchange' is required"; return false; }
        const int venue = root["venue_id"].as<int>(0);
        if (venue <= 0 || venue > 65535) { err = "pack: 'venue_id' must be 1..65535"; return false; }
        out.venue_id = static_cast<uint16_t>(venue);

        const YAML::Node c = root["connect"];
        if (!c || !c.IsMap()) { err = "pack: 'connect' is required"; return false; }
        if (!check_keys(c, kConnectKeys, "connect", err)) return false;
        out.connect.url = c["url"].as<std::string>("");
        if (out.connect.url.rfind("wss://", 0) != 0 && out.connect.url.rfind("ws://", 0) != 0) { err = "connect.url must start with wss:// or ws://"; return false; }
        if (c["wrapper"]) {
            out.connect.wrapper_stream = c["wrapper"]["stream"].as<std::string>("stream");
            out.connect.wrapper_data = c["wrapper"]["data"].as<std::string>("data");
        }
        if (!c["subscribe"]) { err = "connect.subscribe is required"; return false; }
        out.connect.subscribe_json = yaml_to_json(c["subscribe"]);
        if (out.connect.subscribe_json.find("{streams}") == std::string::npos) { err = "connect.subscribe must contain the {streams} placeholder"; return false; }
        const std::string sc = c["symbol_case"].as<std::string>("lower");
        if (sc != "lower" && sc != "upper") { err = "connect.symbol_case must be lower or upper"; return false; }
        out.connect.symbol_lower = sc == "lower";
        out.connect.reconnect_before_s = c["reconnect_before_s"].as<uint64_t>(0);

        const YAML::Node ss = root["streams"];
        if (!ss || !ss.IsSequence() || ss.size() == 0) { err = "pack: 'streams' must be a non-empty list"; return false; }
        for (size_t i = 0; i < ss.size(); ++i) {
            const YAML::Node n = ss[i];
            const std::string where = "streams[" + std::to_string(i) + "]";
            if (!n.IsMap() || !check_keys(n, kStreamKeys, where, err)) return false;
            StreamDef sd;
            sd.name_tpl = n["name"].as<std::string>("");
            const size_t pos = sd.name_tpl.find("{symbol}");
            if (sd.name_tpl.empty() || pos == std::string::npos) { err = where + ": 'name' must contain {symbol}"; return false; }
            sd.suffix = sd.name_tpl.substr(0, pos) + sd.name_tpl.substr(pos + 8);
            sd.message = n["message"].as<std::string>("");
            sd.book = n["book"].as<std::string>("");
            if (sd.message.empty() == sd.book.empty()) { err = where + ": exactly one of 'message' or 'book' is required"; return false; }
            if (!parse_fields(n["fields"], sd.fields, where + ".fields", err)) return false;
            if (!sd.message.empty()) {
                sd.msg = find_message(sd.message);
                if (!sd.msg) { err = where + ": unknown canonical message '" + sd.message + "'"; return false; }
                for (auto& f : sd.fields) {
                    f.field = find_field(*sd.msg, f.target);
                    if (!f.field) { err = where + ".fields." + f.target + ": not a field of " + sd.message + " (prefix fields are set by the gateway)"; return false; }
                    if (!check_map_values(f, where + ".fields." + f.target, err)) return false;
                }
                if (n["snapshot"]) { err = where + ": 'snapshot' only applies to book streams"; return false; }
            } else {
                if (sd.book != "binance_diff") { err = where + ": unknown book strategy '" + sd.book + "' (known: binance_diff)"; return false; }
                for (const auto& f : sd.fields)
                    if (!kBookRoles.count(f.target)) { err = where + ".fields." + f.target + ": not a role of strategy " + sd.book; return false; }
                for (const char* r : {"first_id", "last_id", "bids", "asks"})
                    if (!out.role(sd.fields, r)) { err = where + ": strategy " + sd.book + " needs role '" + r + "'"; return false; }
                if (const YAML::Node sn = n["snapshot"]) {
                    if (!sn.IsMap() || !check_keys(sn, {"url", "fields"}, where + ".snapshot", err)) return false;
                    sd.snapshot.url_tpl = sn["url"].as<std::string>("");
                    if (sd.snapshot.url_tpl.rfind("https://", 0) != 0) { err = where + ".snapshot.url must start with https://"; return false; }
                    if (!parse_fields(sn["fields"], sd.snapshot.fields, where + ".snapshot.fields", err)) return false;
                    for (const auto& f : sd.snapshot.fields)
                        if (!kSnapshotRoles.count(f.target)) { err = where + ".snapshot.fields." + f.target + ": unknown role"; return false; }
                    for (const char* r : {"last_id", "bids", "asks"})
                        if (!out.role(sd.snapshot.fields, r)) { err = where + ".snapshot needs role '" + r + "'"; return false; }
                    sd.has_snapshot = true;
                } else {
                    err = where + ": strategy binance_diff needs a 'snapshot' block"; return false;
                }
            }
            for (const auto& other : out.streams)
                if (other.suffix == sd.suffix) { err = where + ": duplicate stream name " + sd.name_tpl; return false; }
            out.streams.push_back(std::move(sd));
        }
        return true;
    } catch (const YAML::Exception& e) {
        err = std::string("pack: ") + e.what();
        return false;
    }
}

} // namespace feeds
