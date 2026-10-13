#pragma once

// Venue symbol -> InstrumentId. Ids must be stable across restarts because apps
// filter by id, so they come from a site file; `--symbols` alone auto-numbers
// from 1 for quick runs (the gateway warns).
//
//   instruments:
//     BTCUSDT: { id: 1, base: BTC, quote: USDT }
//     ETHUSDT: 2

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace feeds {

struct Instrument {
    uint32_t    id = 0;
    std::string symbol;          // venue symbol as the exchange spells it (BTCUSDT)
    std::string stream_symbol;   // as used in stream names (btcusdt)
    std::string base, quote;
};

class Instruments {
public:
    bool load_file(const std::string& path, bool lower_stream_names, std::string& err);
    bool load_symbols(const std::vector<std::string>& symbols, bool lower_stream_names, std::string& err); // auto ids 1..n
    const Instrument* by_symbol(std::string_view venue_symbol) const noexcept;
    const Instrument* by_stream_symbol(std::string_view s) const noexcept;
    const std::vector<Instrument>& all() const noexcept { return list_; }
    bool empty() const noexcept { return list_.empty(); }

private:
    bool add(Instrument in, std::string& err);
    std::vector<Instrument> list_;
    std::unordered_map<std::string, size_t> by_symbol_, by_stream_;
};

} // namespace feeds
