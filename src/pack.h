#pragma once

// An exchange pack: the YAML that makes one gateway binary speak one exchange.
// Connection + subscribe template + per-stream JSON -> canonical mapping with a
// deliberately closed vocabulary (path, type, unit, scale, map). Anything the
// vocabulary cannot say is a compiled-in strategy named in the pack (`book:`).

#include "canonical_fields.h"
#include "decimal.h"
#include "json_path.h"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace feeds {

enum class ValueType : uint8_t { AUTO, DECIMAL, INT, UINT, BOOL, STRING };

struct FieldMap {
    std::string target;                 // canonical field name, or a strategy role (first_id, bids, ...)
    JsonPath    path;
    ValueType   type = ValueType::AUTO;
    TimeUnit    unit = TimeUnit::NONE;
    int64_t     scale = 0;              // 0 = none
    std::map<std::string, std::string> map; // token -> enum name or integer
    const CanonicalField* field = nullptr;  // resolved for message fields; null for roles
};

struct SnapshotDef {
    std::string url_tpl;                // {SYMBOL} / {symbol} substituted
    std::vector<FieldMap> fields;       // roles: last_id, bids, asks
};

struct StreamDef {
    std::string name_tpl;               // e.g. "{symbol}@trade"
    std::string suffix;                 // name_tpl with "{symbol}" removed, used to route combined frames
    std::string message;                // canonical message for flat streams
    const CanonicalMessage* msg = nullptr;
    std::string book;                   // strategy name for book streams; empty = flat
    std::vector<FieldMap> fields;
    SnapshotDef snapshot;
    bool has_snapshot = false;
};

struct ConnectDef {
    std::string url;
    std::string wrapper_stream = "stream", wrapper_data = "data";
    std::string subscribe_json;         // with "{streams}" placeholder, already JSON
    bool subscribe_streams_as_array = true;
    bool symbol_lower = true;
    uint64_t reconnect_before_s = 0;    // 0 = never proactively reconnect
};

struct Pack {
    std::string exchange;
    uint16_t    venue_id = 0;
    ConnectDef  connect;
    std::vector<StreamDef> streams;

    static bool load(const std::string& path, Pack& out, std::string& err);
    static bool load_text(const std::string& yaml, Pack& out, std::string& err);

    const StreamDef* find_by_suffix(std::string_view suffix) const noexcept;
    const FieldMap*  role(const std::vector<FieldMap>& v, std::string_view name) const noexcept;
};

// Expands "{symbol}" (stream-name case) and "{SYMBOL}" (venue case).
std::string expand_symbol(std::string_view tpl, std::string_view stream_symbol, std::string_view venue_symbol);

} // namespace feeds
