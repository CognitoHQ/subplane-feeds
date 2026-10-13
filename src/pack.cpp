#include "pack.h"

#include <yaml-cpp/yaml.h>

#include <cctype>
#include <fstream>
#include <set>
#include <sstream>

namespace feeds {
namespace {

const std::set<std::string> kFieldKeys = {"path", "type", "unit", "scale", "map", "levels"};
const std::set<std::string> kStreamKeys = {"name", "channel", "match", "message", "book", "each", "snapshot_kind", "fields", "snapshot"};
const std::set<std::string> kConnectKeys = {"url", "wrapper", "route", "subscribe", "stream_object", "symbol_case", "reconnect_before_s", "ping"};
const std::set<std::string> kTopKeys = {"exchange", "venue_id", "registry", "connect", "streams"};
const std::set<std::string> kStrategies = {"binance_diff", "l2_inband", "snapshot_only"};
// Roles per strategy; `levels` roles take a level layout, `time` roles a time format, `path` roles are plain.
struct Role { const char* name; enum { PATH, LEVELS, TIME } kind; bool required; };
const std::vector<Role> kBinanceDiffRoles = {{"first_id", Role::PATH, true}, {"last_id", Role::PATH, true}, {"bids", Role::LEVELS, true}, {"asks", Role::LEVELS, true}, {"ExchangeTs", Role::TIME, false}};
const std::vector<Role> kBinanceSnapshotRoles = {{"last_id", Role::PATH, true}, {"bids", Role::LEVELS, true}, {"asks", Role::LEVELS, true}};
const std::vector<Role> kL2InbandRoles = {{"bids", Role::LEVELS, false}, {"asks", Role::LEVELS, false}, {"changes", Role::LEVELS, false}, {"kind", Role::PATH, false}, {"ExchangeTs", Role::TIME, false}};
const std::vector<Role> kSnapshotOnlyRoles = {{"bids", Role::LEVELS, true}, {"asks", Role::LEVELS, true}, {"ExchangeTs", Role::TIME, false}};

bool check_keys(const YAML::Node& n, const std::set<std::string>& allowed, const std::string& where, std::string& err) {
    for (const auto& kv : n) {
        const std::string k = kv.first.as<std::string>();
        if (!allowed.count(k)) { err = where + ": unknown key '" + k + "'"; return false; }
    }
    return true;
}

bool parse_path(std::string text, FieldMap& out, const std::string& where, std::string& err) {
    if (text.rfind("$.", 0) == 0) { out.frame_scope = true; text = text.substr(2); }
    if (!JsonPath::parse(text, out.path, err)) { err = where + ": " + err; return false; }
    return true;
}

bool check_map_values(const EnumMap& m, const std::string& where, std::string& err) {
    for (const auto& kv : m) {
        uint8_t e; int64_t i;
        if (!enum_value(kv.second, e) && !parse_int64(kv.second, i)) { err = where + ": map value '" + kv.second + "' is neither an enum name nor an integer"; return false; }
    }
    return true;
}

// `path@name` or a mapping. `name` is resolved later against the target's kind.
bool parse_field(const std::string& target, const YAML::Node& n, FieldMap& out, std::string& conv, Registry& reg, const std::string& where, std::string& err) {
    out = FieldMap{};
    out.target = target;
    conv.clear();
    if (n.IsScalar()) {
        std::string text = n.as<std::string>();
        const size_t at = text.rfind('@');
        if (at != std::string::npos) { conv = text.substr(at + 1); text = text.substr(0, at); }
        return parse_path(text, out, where, err);
    }
    if (!n.IsMap()) { err = where + ": expected a path string or a mapping"; return false; }
    if (!check_keys(n, kFieldKeys, where, err)) return false;
    if (!n["path"]) { err = where + ": 'path' is required"; return false; }
    if (!parse_path(n["path"].as<std::string>(), out, where, err)) return false;
    if (n["type"]) {
        const std::string t = n["type"].as<std::string>();
        if (t == "decimal") out.type = ValueType::DECIMAL;
        else if (t == "int") out.type = ValueType::INT;
        else if (t == "uint") out.type = ValueType::UINT;
        else if (t == "bool") out.type = ValueType::BOOL;
        else if (t == "string") out.type = ValueType::STRING;
        else if (t == "iso8601") out.unit = TimeUnit::ISO8601;
        else { err = where + ": type must be decimal|int|uint|bool|string|iso8601, got '" + t + "'"; return false; }
    }
    if (n["unit"]) conv = n["unit"].as<std::string>();           // resolved against registry.times
    if (n["levels"]) conv = n["levels"].as<std::string>();       // resolved against registry.levels
    if (n["scale"]) {
        out.scale = n["scale"].as<int64_t>();
        if (out.scale == 0) { err = where + ": scale must be non-zero"; return false; }
    }
    if (n["map"]) {
        if (n["map"].IsScalar()) conv = n["map"].as<std::string>(); // registry name
        else if (n["map"].IsMap()) {
            // INTENT: inline maps live in the pack's own registry copy so FieldMap holds
            // only a pointer; the registry is shared by every copy of the Pack.
            EnumMap m;
            for (const auto& kv : n["map"]) m[kv.first.as<std::string>()] = kv.second.as<std::string>();
            if (!check_map_values(m, where, err)) return false;
            const std::string name = "inline:" + where;
            reg.maps[name] = std::move(m);
            out.map = reg.map(name);
        } else { err = where + ": map must be a name or a mapping"; return false; }
    }
    return true;
}

// Resolves `conv` for a canonical message field by the field's kind.
bool resolve_message_conv(FieldMap& f, const std::string& conv, const Registry& reg, const std::string& where, std::string& err) {
    if (conv.empty()) return true;
    if (const EnumMap* m = reg.map(conv)) { f.map = m; return true; }
    if (const TimeUnit* t = reg.time(conv)) {
        if (f.field->kind == FieldKind::I64 || f.field->kind == FieldKind::CHARS) { err = where + ": time format '" + conv + "' on a non-integer field"; return false; }
        f.unit = *t; return true;
    }
    err = where + ": '@" + conv + "' is neither a registered time format nor a map";
    return false;
}

bool resolve_role_conv(FieldMap& f, const std::string& conv, const Role& role, const Registry& reg, const std::string& where, std::string& err) {
    switch (role.kind) {
        case Role::LEVELS:
            f.levels = reg.level(conv.empty() ? "pairs" : conv);
            if (!f.levels) { err = where + ": '@" + conv + "' is not a registered level layout"; return false; }
            return true;
        case Role::TIME:
            if (conv.empty() && f.unit == TimeUnit::NONE) { err = where + ": a time format is required (e.g. ts@ms)"; return false; }
            if (!conv.empty()) { const TimeUnit* t = reg.time(conv); if (!t) { err = where + ": '@" + conv + "' is not a registered time format"; return false; } f.unit = *t; }
            return true;
        case Role::PATH:
            if (!conv.empty()) { err = where + ": role '" + role.name + "' takes no conversion"; return false; }
            return true;
    }
    return false;
}

bool parse_roles(const YAML::Node& n, const std::vector<Role>& roles, Registry& reg, std::vector<FieldMap>& out, const std::string& where, std::string& err) {
    if (!n || !n.IsMap()) { err = where + ": must be a mapping of roles"; return false; }
    for (const auto& kv : n) {
        const std::string target = kv.first.as<std::string>();
        const Role* role = nullptr;
        for (const auto& r : roles) if (target == r.name) role = &r;
        if (!role) { err = where + "." + target + ": not a role of this strategy"; return false; }
        FieldMap f; std::string conv;
        if (!parse_field(target, kv.second, f, conv, reg, where + "." + target, err)) return false;
        if (f.frame_scope && target != "kind") { err = where + "." + target + ": only the 'kind' role may use a frame path ($.)"; return false; }
        if (!resolve_role_conv(f, conv, *role, reg, where + "." + target, err)) return false;
        out.push_back(std::move(f));
    }
    for (const auto& r : roles)
        if (r.required && !Pack::role(out, r.name)) { err = where + ": role '" + std::string(r.name) + "' is required"; return false; }
    return true;
}

// YAML -> JSON, keeping scalar types: quoted YAML scalars stay strings, bare numbers/bools/null stay bare.
void yaml_to_json(const YAML::Node& n, std::string& out) {
    auto quote = [&](const std::string& s) {
        out += '"';
        for (char c : s) { if (c == '"' || c == '\\') out += '\\'; out += c; }
        out += '"';
    };
    switch (n.Type()) {
        case YAML::NodeType::Map: {
            out += '{'; bool first = true;
            for (const auto& kv : n) { if (!first) out += ", "; first = false; quote(kv.first.as<std::string>()); out += ": "; yaml_to_json(kv.second, out); }
            out += '}'; break;
        }
        case YAML::NodeType::Sequence: {
            out += '['; bool first = true;
            for (const auto& v : n) { if (!first) out += ", "; first = false; yaml_to_json(v, out); }
            out += ']'; break;
        }
        case YAML::NodeType::Scalar: {
            const std::string s = n.Scalar();
            const bool quoted = n.Tag() == "!";
            int64_t i; 
            if (!quoted && (s == "true" || s == "false" || s == "null" || parse_int64(s, i))) out += s;
            else if (!quoted && !s.empty() && s.find_first_not_of("0123456789.-") == std::string::npos && s.find('.') != std::string::npos) out += s;
            else quote(s);
            break;
        }
        default: out += "null";
    }
}

} // namespace

bool StreamDef::accepts(std::string_view kind) const noexcept {
    if (match.empty()) return true;
    for (const auto& m : match) if (m == kind) return true;
    return false;
}

std::string expand_symbol(std::string_view tpl, std::string_view stream_symbol, std::string_view venue_symbol, std::string_view channel) {
    std::string out;
    out.reserve(tpl.size() + 16);
    for (size_t i = 0; i < tpl.size();) {
        if (tpl.compare(i, 8, "{symbol}") == 0) { out += stream_symbol; i += 8; }
        else if (tpl.compare(i, 8, "{SYMBOL}") == 0) { out += venue_symbol; i += 8; }
        else if (tpl.compare(i, 9, "{channel}") == 0) { out += channel; i += 9; }
        else out += tpl[i++];
    }
    return out;
}

std::string default_registry_path(const std::string& pack_path) {
    const size_t slash = pack_path.rfind('/');
    return (slash == std::string::npos ? std::string() : pack_path.substr(0, slash + 1)) + "registry.yaml";
}

const StreamDef* Pack::find_by_name(std::string_view name, std::string_view& symbol_out) const noexcept {
    for (const auto& s : streams) {
        if (name.size() <= s.prefix.size() + s.suffix.size()) continue;
        if (name.compare(0, s.prefix.size(), s.prefix) != 0) continue;
        if (name.compare(name.size() - s.suffix.size(), s.suffix.size(), s.suffix) != 0) continue;
        symbol_out = name.substr(s.prefix.size(), name.size() - s.prefix.size() - s.suffix.size());
        return &s;
    }
    return nullptr;
}

const StreamDef* Pack::find_by_kind(std::string_view kind) const noexcept {
    for (const auto& s : streams) for (const auto& m : s.match) if (m == kind) return &s;
    return nullptr;
}

const FieldMap* Pack::role(const std::vector<FieldMap>& v, std::string_view name) noexcept {
    for (const auto& f : v) if (f.target == name) return &f;
    return nullptr;
}

bool Pack::load(const std::string& path, const Registry& base, Pack& out, std::string& err) {
    std::ifstream in(path);
    if (!in) { err = "pack " + path + ": cannot open"; return false; }
    std::stringstream ss; ss << in.rdbuf();
    if (!load_text(ss.str(), base, out, err)) { err = "pack " + path + ": " + err; return false; }
    return true;
}

bool Pack::load_with_registry(const std::string& path, const std::string& registry_path, Pack& out, std::string& err) {
    Registry base;
    std::string rp = registry_path;
    if (rp.empty()) { rp = default_registry_path(path); if (!std::ifstream(rp)) rp.clear(); }
    if (!rp.empty() && !base.load_file(rp, err)) return false;
    return load(path, base, out, err);
}

bool Pack::load_text(const std::string& yaml, const Registry& base, Pack& out, std::string& err) {
    out = Pack{};
    out.registry = std::make_shared<Registry>(base);
    Registry& reg = *out.registry;
    YAML::Node root;
    try { root = YAML::Load(yaml); } catch (const YAML::Exception& e) { err = e.what(); return false; }
    try {
        if (!root.IsMap()) { err = "expected a mapping"; return false; }
        if (!check_keys(root, kTopKeys, "pack", err)) return false;
        out.exchange = root["exchange"].as<std::string>("");
        if (out.exchange.empty() || out.exchange.size() > 16) { err = "'exchange' is required (1..16 chars)"; return false; }
        const int venue = root["venue_id"].as<int>(0);
        if (venue <= 0 || venue > 65535) { err = "'venue_id' must be 1..65535"; return false; }
        out.venue_id = static_cast<uint16_t>(venue);
        if (root["registry"] && !reg.load_node(root["registry"], err)) return false;

        const YAML::Node c = root["connect"];
        ConnectDef& cd = out.connect;
        if (!c || !c.IsMap()) { err = "'connect' is required"; return false; }
        if (!check_keys(c, kConnectKeys, "connect", err)) return false;
        cd.url = c["url"].as<std::string>("");
        if (cd.url.rfind("wss://", 0) != 0 && cd.url.rfind("ws://", 0) != 0) { err = "connect.url must start with wss:// or ws://"; return false; }
        if (static_cast<bool>(c["wrapper"]) == static_cast<bool>(c["route"])) { err = "connect: exactly one of 'wrapper' (stream-name frames) or 'route' (type/symbol fields) is required"; return false; }
        if (c["wrapper"]) {
            if (!check_keys(c["wrapper"], {"stream", "data", "type"}, "connect.wrapper", err)) return false;
            cd.wrapper_stream = c["wrapper"]["stream"].as<std::string>("stream");
            cd.wrapper_data = c["wrapper"]["data"].as<std::string>("data");
            cd.wrapper_type = c["wrapper"]["type"].as<std::string>("");
        } else {
            cd.use_route = true;
            if (!check_keys(c["route"], {"type", "symbol"}, "connect.route", err)) return false;
            FieldMap t, s;
            if (!parse_path(c["route"]["type"].as<std::string>(""), t, "connect.route.type", err)) return false;
            std::string sym = c["route"]["symbol"].as<std::string>("");
            if (!parse_path(sym, s, "connect.route.symbol", err)) return false;
            cd.route_type = t.path; cd.route_symbol = s.path; cd.route_symbol_frame = s.frame_scope;
        }
        if (!c["subscribe"]) { err = "connect.subscribe is required"; return false; }
        if (c["subscribe"].IsSequence()) { for (const auto& t : c["subscribe"]) { std::string j; yaml_to_json(t, j); cd.subscribe.push_back(j); } }
        else { std::string j; yaml_to_json(c["subscribe"], j); cd.subscribe.push_back(j); }
        for (const auto& t : cd.subscribe) {
            if (t.find("{stream}") == std::string::npos && t.find("{streams}") == std::string::npos && t.find("{symbols}") == std::string::npos && t.find("{channel}") == std::string::npos) {
                err = "connect.subscribe: a template must use {streams}, {symbols}, {stream} or {channel}"; return false;
            }
        }
        if (c["stream_object"]) { yaml_to_json(c["stream_object"], cd.stream_object); }
        const std::string sc = c["symbol_case"].as<std::string>("lower");
        if (sc != "lower" && sc != "upper") { err = "connect.symbol_case must be lower or upper"; return false; }
        cd.symbol_lower = sc == "lower";
        cd.reconnect_before_s = c["reconnect_before_s"].as<uint64_t>(0);
        if (const YAML::Node p = c["ping"]) {
            if (!p.IsMap() || !check_keys(p, {"text", "every_s", "reply"}, "connect.ping", err)) return false;
            cd.ping_text = p["text"].as<std::string>("");
            cd.ping_every_s = p["every_s"].as<uint64_t>(0);
            cd.ping_reply = p["reply"].as<std::string>("");
            if (cd.ping_text.empty() || cd.ping_every_s == 0) { err = "connect.ping needs 'text' and 'every_s'"; return false; }
        }

        const YAML::Node ss = root["streams"];
        if (!ss || !ss.IsSequence() || ss.size() == 0) { err = "'streams' must be a non-empty list"; return false; }
        for (size_t i = 0; i < ss.size(); ++i) {
            const YAML::Node n = ss[i];
            const std::string where = "streams[" + std::to_string(i) + "]";
            if (!n.IsMap() || !check_keys(n, kStreamKeys, where, err)) return false;
            StreamDef sd;
            sd.name_tpl = n["name"].as<std::string>("");
            const size_t pos = sd.name_tpl.find("{symbol}");
            if (sd.name_tpl.empty() || pos == std::string::npos) { err = where + ": 'name' must contain {symbol}"; return false; }
            sd.prefix = sd.name_tpl.substr(0, pos);
            sd.suffix = sd.name_tpl.substr(pos + 8);
            sd.channel = n["channel"].as<std::string>("");
            if (sd.channel.empty()) {
                std::string ch = sd.prefix + sd.suffix;
                while (!ch.empty() && (ch.front() == '@' || ch.front() == '_' || ch.front() == '.' || ch.front() == '-')) ch.erase(0, 1);
                while (!ch.empty() && (ch.back() == '@' || ch.back() == '_' || ch.back() == '.' || ch.back() == '-')) ch.pop_back();
                sd.channel = ch;
            }
            if (n["match"]) {
                if (n["match"].IsSequence()) for (const auto& m : n["match"]) sd.match.push_back(m.as<std::string>());
                else sd.match.push_back(n["match"].as<std::string>());
            }
            if (cd.use_route && sd.match.empty()) { err = where + ": 'match' is required when connect.route is used"; return false; }
            if (n["each"]) { FieldMap e; if (!parse_path(n["each"].as<std::string>(), e, where + ".each", err)) return false; sd.each = e.path; sd.has_each = true; }
            sd.snapshot_kind = n["snapshot_kind"].as<std::string>("snapshot");
            sd.message = n["message"].as<std::string>("");
            sd.book = n["book"].as<std::string>("");
            if (sd.message.empty() == sd.book.empty()) { err = where + ": exactly one of 'message' or 'book' is required"; return false; }
            if (n["fields"] && n["fields"].IsMap() && n["fields"].size() > 32) { err = where + ": at most 32 fields per stream"; return false; }
            if (!sd.message.empty()) {
                sd.msg = find_message(sd.message);
                if (!sd.msg) { err = where + ": unknown canonical message '" + sd.message + "'"; return false; }
                const YAML::Node fn = n["fields"];
                if (!fn || !fn.IsMap()) { err = where + ": 'fields' must be a mapping"; return false; }
                for (const auto& kv : fn) {
                    const std::string target = kv.first.as<std::string>();
                    FieldMap f; std::string conv;
                    if (!parse_field(target, kv.second, f, conv, reg, where + ".fields." + target, err)) return false;
                    f.field = find_field(*sd.msg, target);
                    if (!f.field) { err = where + ".fields." + target + ": not a field of " + sd.message + " (prefix fields are set by the gateway)"; return false; }
                    if (!resolve_message_conv(f, conv, reg, where + ".fields." + target, err)) return false;
                    sd.fields.push_back(std::move(f));
                }
                if (n["snapshot"]) { err = where + ": 'snapshot' only applies to book streams"; return false; }
            } else {
                if (!kStrategies.count(sd.book)) { err = where + ": unknown book strategy '" + sd.book + "' (known: binance_diff, l2_inband, snapshot_only)"; return false; }
                const std::vector<Role>& roles = sd.book == "binance_diff" ? kBinanceDiffRoles : sd.book == "l2_inband" ? kL2InbandRoles : kSnapshotOnlyRoles;
                if (!parse_roles(n["fields"], roles, reg, sd.fields, where + ".fields", err)) return false;
                if (sd.book == "binance_diff") {
                    const YAML::Node sn = n["snapshot"];
                    if (!sn || !sn.IsMap() || !check_keys(sn, {"url", "fields"}, where + ".snapshot", err)) { if (err.empty()) err = where + ": strategy binance_diff needs a 'snapshot' block"; return false; }
                    sd.snapshot.url_tpl = sn["url"].as<std::string>("");
                    if (sd.snapshot.url_tpl.rfind("https://", 0) != 0) { err = where + ".snapshot.url must start with https://"; return false; }
                    if (!parse_roles(sn["fields"], kBinanceSnapshotRoles, reg, sd.snapshot.fields, where + ".snapshot.fields", err)) return false;
                    sd.has_snapshot = true;
                } else if (n["snapshot"]) { err = where + ": strategy " + sd.book + " takes no 'snapshot' block"; return false; }
                if (sd.book == "l2_inband" && !role(sd.fields, "bids") && !role(sd.fields, "changes")) { err = where + ": l2_inband needs 'bids'/'asks' or 'changes'"; return false; }
            }
            for (const auto& other : out.streams)
                if (other.prefix == sd.prefix && other.suffix == sd.suffix) { err = where + ": duplicate stream name " + sd.name_tpl; return false; }
            out.streams.push_back(std::move(sd));
        }
        return true;
    } catch (const YAML::Exception& e) {
        err = e.what();
        return false;
    }
}

} // namespace feeds
