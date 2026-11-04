#pragma once

// An exchange pack: the YAML that makes one gateway binary speak one exchange.
// Connection, subscribe templates, frame routing, and per-stream JSON -> canonical
// mapping with a deliberately closed vocabulary (path, type, unit, scale, map,
// levels, referenced from the registry as `path@name`). Anything the vocabulary
// cannot say is a compiled-in strategy named in the pack (`book:`).

#include "canonical_fields.h"
#include "decimal.h"
#include "json_path.h"
#include "registry.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace feeds {

enum class ValueType : uint8_t { AUTO, DECIMAL, INT, UINT, BOOL, STRING };

struct FieldMap {
    std::string target;                 // canonical field name, or a strategy role (first_id, bids, kind, ...)
    JsonPath    path;
    bool        frame_scope = false;    // path began with "$.": resolve against the frame, not the record
    ValueType   type = ValueType::AUTO;
    TimeUnit    unit = TimeUnit::NONE;
    int64_t     scale = 0;              // 0 = none
    const EnumMap* map = nullptr;       // token -> enum name or integer
    const LevelLayout* levels = nullptr; // for book roles that are arrays of levels
    const CanonicalField* field = nullptr;  // resolved for message fields; null for roles
};

struct SnapshotDef {
    std::string url_tpl;                // {SYMBOL} / {symbol} substituted
    std::vector<FieldMap> fields;       // roles: last_id, bids, asks
};

struct StreamDef {
    std::string name_tpl;               // e.g. "{symbol}@trade" or "live_trades_{symbol}"
    std::string prefix, suffix;         // name_tpl around {symbol}, for routing wrapper frames
    std::string channel;                // for the {channel} subscribe placeholder (defaults to the name without {symbol} and separators)
    std::vector<std::string> match;     // frame type values this stream accepts (route mode, or wrapper.type)
    std::string message;                // canonical message for flat streams
    const CanonicalMessage* msg = nullptr;
    std::string book;                   // strategy name for book streams; empty = flat
    std::string snapshot_kind = "snapshot";
    uint32_t    max_levels = 5000;      // book streams: levels a side the book holds; more is an overflow and a resync
    JsonPath    each;                   // records per frame live in this array (frame scope)
    bool        has_each = false;
    static constexpr size_t kMaxFields = 32;   // FrameTokens holds one token per field
    std::vector<FieldMap> fields;
    SnapshotDef snapshot;
    bool has_snapshot = false;

    bool accepts(std::string_view kind) const noexcept;
};

struct ConnectDef {
    std::string url;
    bool        use_route = false;      // route by fields (type/symbol paths) instead of a stream-name wrapper
    std::string wrapper_stream = "stream", wrapper_data = "data", wrapper_type;
    JsonPath    route_type, route_symbol;
    bool        route_symbol_frame = true;  // "$." or no `each`: frame scope; else per record
    std::vector<std::string> subscribe;     // JSON templates with placeholders
    std::string stream_object;              // JSON template for {streams} entries ({channel}, {SYMBOL}, {symbol})
    bool        symbol_lower = true;
    uint64_t    reconnect_before_s = 0;
    std::string ping_text, ping_reply;      // application-level ping (OKX); empty = WebSocket ping frames only
    uint64_t    ping_every_s = 0;
};

struct Pack {
    std::string exchange;
    uint16_t    venue_id = 0;
    ConnectDef  connect;
    std::vector<StreamDef> streams;
    std::shared_ptr<Registry> registry; // the registry this pack resolved against (shared defaults + local entries)

    static bool load(const std::string& path, const Registry& base, Pack& out, std::string& err);
    static bool load_text(const std::string& yaml, const Registry& base, Pack& out, std::string& err);
    // Loads the registry (`registry_path`, or packs/registry.yaml next to the pack when empty and present) then the pack.
    static bool load_with_registry(const std::string& path, const std::string& registry_path, Pack& out, std::string& err);

    // Wrapper mode: the stream named `name` -> definition and the symbol part. Null when no stream matches.
    const StreamDef* find_by_name(std::string_view name, std::string_view& symbol_out) const noexcept;
    // Route mode: the stream whose `match` contains `kind`.
    const StreamDef* find_by_kind(std::string_view kind) const noexcept;
    static const FieldMap* role(const std::vector<FieldMap>& v, std::string_view name) noexcept;
};

// Expands "{symbol}" (stream-name case), "{SYMBOL}" (venue case) and "{channel}".
std::string expand_symbol(std::string_view tpl, std::string_view stream_symbol, std::string_view venue_symbol, std::string_view channel = "");
// Loads packs/registry.yaml next to the pack when no explicit registry is given.
std::string default_registry_path(const std::string& pack_path);

} // namespace feeds
