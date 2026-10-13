#include "registry.h"

#include "canonical_fields.h"

namespace feeds {

Registry::Registry() {
    times = {{"s", TimeUnit::S}, {"ms", TimeUnit::MS}, {"us", TimeUnit::US}, {"ns", TimeUnit::NS}, {"iso8601", TimeUnit::ISO8601}};
    LevelLayout pairs; pairs.name = "pairs";
    levels["pairs"] = pairs;
}

void Registry::rebind() noexcept {
    for (auto& kv : levels) kv.second.side_map = kv.second.has_side ? map(kv.second.side_map_name) : nullptr;
}

const TimeUnit* Registry::time(const std::string& n) const noexcept { auto it = times.find(n); return it == times.end() ? nullptr : &it->second; }
const EnumMap* Registry::map(const std::string& n) const noexcept { auto it = maps.find(n); return it == maps.end() ? nullptr : &it->second; }
const LevelLayout* Registry::level(const std::string& n) const noexcept { auto it = levels.find(n); return it == levels.end() ? nullptr : &it->second; }

bool Registry::load_file(const std::string& path, std::string& err) {
    try {
        return load_node(YAML::LoadFile(path), err) || (err = "registry " + path + ": " + err, false);
    } catch (const YAML::Exception& e) { err = "registry " + path + ": " + e.what(); return false; }
}

bool Registry::load_node(const YAML::Node& node, std::string& err) {
    try {
        if (!node || node.IsNull()) return true;
        if (!node.IsMap()) { err = "registry must be a mapping"; return false; }
        for (const auto& kv : node) {
            const std::string section = kv.first.as<std::string>();
            if (section != "times" && section != "maps" && section != "levels") { err = "registry: unknown section '" + section + "'"; return false; }
        }
        if (const YAML::Node t = node["times"]) {
            for (const auto& kv : t) {
                const std::string name = kv.first.as<std::string>(), target = kv.second.as<std::string>();
                const TimeUnit* u = time(target);
                if (!u) { err = "registry.times." + name + ": unknown time format '" + target + "'"; return false; }
                times[name] = *u;
            }
        }
        if (const YAML::Node m = node["maps"]) {
            for (const auto& kv : m) {
                const std::string name = kv.first.as<std::string>();
                if (!kv.second.IsMap()) { err = "registry.maps." + name + ": must be a mapping"; return false; }
                EnumMap em;
                for (const auto& e : kv.second) {
                    const std::string v = e.second.as<std::string>();
                    uint8_t ev; int64_t iv;
                    if (!enum_value(v, ev) && !parse_int64(v, iv)) { err = "registry.maps." + name + ": value '" + v + "' is neither an enum name nor an integer"; return false; }
                    em[e.first.as<std::string>()] = v;
                }
                maps[name] = std::move(em);
            }
        }
        if (const YAML::Node l = node["levels"]) {
            for (const auto& kv : l) {
                const std::string name = kv.first.as<std::string>();
                const YAML::Node n = kv.second;
                if (!n.IsMap()) { err = "registry.levels." + name + ": must be a mapping"; return false; }
                LevelLayout lay; lay.name = name;
                const std::string kind = n["kind"].as<std::string>("array");
                if (kind == "object") {
                    lay.object = true;
                    lay.price_key = n["price"].as<std::string>("price");
                    lay.qty_key = n["qty"].as<std::string>("qty");
                    if (n["side"]) { lay.has_side = true; lay.side_key = n["side"].as<std::string>(); }
                } else if (kind == "array") {
                    lay.price_idx = n["price"].as<uint32_t>(0);
                    lay.qty_idx = n["qty"].as<uint32_t>(1);
                    if (n["side"]) { lay.has_side = true; lay.side_idx = n["side"].as<uint32_t>(); }
                } else { err = "registry.levels." + name + ": kind must be array or object"; return false; }
                if (lay.has_side) {
                    lay.side_map_name = n["side_map"].as<std::string>("side");
                    if (!map(lay.side_map_name)) { err = "registry.levels." + name + ": side_map '" + lay.side_map_name + "' is not a registered map"; return false; }
                }
                levels[name] = lay;
            }
        }
        rebind();
        return true;
    } catch (const YAML::Exception& e) { err = std::string("registry: ") + e.what(); return false; }
}

} // namespace feeds
