#pragma once

// Records raw frames and snapshot bodies as JSON lines so a session can be
// replayed through the same pack without the exchange (fixtures, CI, debugging).
//   {"ts":<recv ns>,"frame":<frame json>}
//   {"ts":<recv ns>,"snapshot":"BTCUSDT","body":<snapshot json>}
//   {"ts":<recv ns>,"event":"connected"|"disconnected"}

#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>

namespace feeds {

class Recorder {
public:
    ~Recorder() { close(); }
    bool open(const std::string& path, std::string& err);
    void frame(uint64_t ts, std::string_view json) noexcept;
    void snapshot(uint64_t ts, std::string_view symbol, std::string_view body) noexcept;
    void event(uint64_t ts, std::string_view name) noexcept;
    void close() noexcept;
    uint64_t lines() const noexcept { return lines_; }
private:
    FILE* f_ = nullptr;
    uint64_t lines_ = 0;
};

} // namespace feeds
