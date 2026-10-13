#include "replayer.h"

#include "gateway.h"
#include "json_path.h"

#include <fstream>

namespace feeds {

bool replay_file(const std::string& path, Gateway& gw, ReplayStats& stats, std::string& err) {
    std::ifstream in(path);
    if (!in) { err = "cannot open " + path; return false; }
    simdjson::ondemand::parser parser;
    std::string line;
    std::string padded;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        padded.assign(line);
        padded.reserve(padded.size() + simdjson::SIMDJSON_PADDING);
        simdjson::ondemand::document doc;
        if (parser.iterate(simdjson::padded_string_view(padded.data(), padded.size(), padded.capacity())).get(doc) != simdjson::SUCCESS) { ++stats.bad_lines; continue; }
        simdjson::ondemand::object obj;
        if (doc.get_object().get(obj) != simdjson::SUCCESS) { ++stats.bad_lines; continue; }
        uint64_t ts = 0;
        if (obj["ts"].get_uint64().get(ts) != simdjson::SUCCESS) { ++stats.bad_lines; continue; }
        simdjson::ondemand::value v;
        if (obj["frame"].get(v) == simdjson::SUCCESS) {
            std::string_view raw;
            if (v.raw_json().get(raw) != simdjson::SUCCESS) { ++stats.bad_lines; continue; }
            gw.on_frame(trim(raw), ts);
            ++stats.frames;
        } else if (obj["snapshot"].get(v) == simdjson::SUCCESS) {
            std::string_view sym, raw;
            if (!read_string(v, sym)) { ++stats.bad_lines; continue; }
            simdjson::ondemand::value body;
            if (obj["body"].get(body) != simdjson::SUCCESS || body.raw_json().get(raw) != simdjson::SUCCESS) { ++stats.bad_lines; continue; }
            gw.on_snapshot(sym, trim(raw), ts);
            ++stats.snapshots;
        } else if (obj["event"].get(v) == simdjson::SUCCESS) {
            std::string_view name;
            if (!read_string(v, name)) { ++stats.bad_lines; continue; }
            if (name == "connected") gw.on_connected(ts);
            else if (name == "disconnected") gw.on_disconnected(ts);
            ++stats.events;
        } else {
            ++stats.bad_lines;
        }
        gw.tick(ts);
    }
    return true;
}

} // namespace feeds
