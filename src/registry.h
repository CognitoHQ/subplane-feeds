#pragma once

// The registry: what packs share so they do not repeat it. Models are the
// canonical schema itself (canonical_fields.h); the registry holds named time
// formats, enum maps and level layouts, referenced from packs as `path@name`.
// packs/registry.yaml ships the defaults; a pack may add entries under `registry:`.

#include "decimal.h"

#include <yaml-cpp/yaml.h>

#include <cstdint>
#include <map>
#include <string>

namespace feeds {

using EnumMap = std::map<std::string, std::string>;

// How one price level is laid out in a JSON array of levels.
struct LevelLayout {
    std::string name;
    bool        object = false;            // {price: .., qty: ..} vs [price, qty, ...]
    uint32_t    price_idx = 0, qty_idx = 1, side_idx = 0;
    std::string price_key = "price", qty_key = "qty", side_key;
    bool        has_side = false;          // Coinbase changes: [side, price, size]
    std::string side_map_name;             // registry map name
    const EnumMap* side_map = nullptr;     // token -> Buy | Sell; rebind() after copying a Registry
};

struct Registry {
    std::map<std::string, TimeUnit>    times;
    std::map<std::string, EnumMap>     maps;
    std::map<std::string, LevelLayout> levels;

    Registry();                                           // the built-in names: s, ms, us, ns, iso8601, pairs
    Registry(const Registry& o) : times(o.times), maps(o.maps), levels(o.levels) { rebind(); }
    Registry& operator=(const Registry& o) { times = o.times; maps = o.maps; levels = o.levels; rebind(); return *this; }
    void rebind() noexcept;                               // level side_map pointers -> this registry's maps
    bool load_file(const std::string& path, std::string& err);
    bool load_node(const YAML::Node& node, std::string& err);  // merges; later entries win

    const TimeUnit*    time(const std::string& name) const noexcept;
    const EnumMap*     map(const std::string& name) const noexcept;
    const LevelLayout* level(const std::string& name) const noexcept;
};

} // namespace feeds
