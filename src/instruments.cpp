#include "instruments.h"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <cctype>

namespace feeds {
namespace {
std::string lower(std::string s) { for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c))); return s; }
std::string upper(std::string s) { for (auto& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c))); return s; }
}

bool Instruments::add(Instrument in, std::string& err) {
    if (in.symbol.empty() || in.symbol.size() > 32) { err = "instrument symbol must be 1..32 chars"; return false; }
    if (in.id == 0) { err = "instrument " + in.symbol + ": id must be >= 1"; return false; }
    if (by_symbol_.count(in.symbol)) { err = "duplicate instrument " + in.symbol; return false; }
    for (const auto& o : list_) if (o.id == in.id) { err = "instrument " + in.symbol + ": id " + std::to_string(in.id) + " already used by " + o.symbol; return false; }
    by_symbol_[in.symbol] = list_.size();
    by_stream_[in.stream_symbol] = list_.size();
    list_.push_back(std::move(in));
    return true;
}

bool Instruments::load_file(const std::string& path, bool lower_stream_names, std::string& err) {
    try {
        const YAML::Node root = YAML::LoadFile(path);
        const YAML::Node ins = root["instruments"];
        if (!ins || !ins.IsMap() || ins.size() == 0) { err = path + ": 'instruments' must be a non-empty mapping"; return false; }
        for (const auto& kv : ins) {
            Instrument in;
            in.symbol = kv.first.as<std::string>();
            in.stream_symbol = lower_stream_names ? lower(in.symbol) : upper(in.symbol);
            if (kv.second.IsScalar()) {
                in.id = kv.second.as<uint32_t>();
            } else if (kv.second.IsMap()) {
                in.id = kv.second["id"].as<uint32_t>(0);
                in.base = kv.second["base"].as<std::string>("");
                in.quote = kv.second["quote"].as<std::string>("");
                const uint32_t part = kv.second["partition"].as<uint32_t>(1);
                if (part < 1 || part > 65535) { err = path + ": instrument " + in.symbol + ": partition must be 1..65535"; return false; }
                in.partition = static_cast<uint16_t>(part);
            } else { err = path + ": instrument " + in.symbol + " must be an id or a mapping"; return false; }
            if (!add(std::move(in), err)) return false;
        }
        return true;
    } catch (const YAML::Exception& e) {
        err = path + ": " + e.what();
        return false;
    }
}

bool Instruments::load_symbols(const std::vector<std::string>& symbols, bool lower_stream_names, std::string& err) {
    uint32_t next = 1;
    for (const auto& s : symbols) {
        Instrument in;
        in.symbol = s;
        in.stream_symbol = lower_stream_names ? lower(s) : upper(s);
        in.id = next++;
        if (!add(std::move(in), err)) return false;
    }
    if (list_.empty()) { err = "no instruments"; return false; }
    return true;
}

size_t Instruments::keep_partition(uint16_t n) {
    std::vector<Instrument> all = std::move(list_);
    list_.clear();
    by_symbol_.clear();
    by_stream_.clear();
    std::string err;
    for (auto& in : all) if (in.partition == n) add(std::move(in), err);   // already validated once
    return list_.size();
}

const Instrument* Instruments::by_symbol(std::string_view s) const noexcept {
    auto it = by_symbol_.find(std::string(s));
    return it == by_symbol_.end() ? nullptr : &list_[it->second];
}
const Instrument* Instruments::by_stream_symbol(std::string_view s) const noexcept {
    auto it = by_stream_.find(std::string(s));
    return it == by_stream_.end() ? nullptr : &list_[it->second];
}

} // namespace feeds
