#pragma once

// Frame-to-offer latency: the agent's log-linear histogram (latency_hist.h, a copy)
// behind the small interface the gateway uses, plus a monotonic clock read.

#include "latency_hist.h"

#include <chrono>
#include <cstdint>

namespace feeds {

class LatencyHistogram {
public:
    void record(uint64_t ns) noexcept { h_.record(ns); }
    void note_max(uint64_t) noexcept {}
    uint64_t count() const noexcept { return h_.count.load(std::memory_order_relaxed); }
    uint64_t mean() const noexcept { const auto s = h_.snapshot(); return s.count ? s.sum / s.count : 0; }
    uint64_t max() const noexcept { return h_.max.load(std::memory_order_relaxed); }
    uint64_t quantile(double q) const noexcept { return percentile(h_.snapshot(), q); }
    LogHistogram::Snapshot snapshot() const noexcept { return h_.snapshot(); }
private:
    LogHistogram h_;
};

inline uint64_t real_ns() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

inline uint64_t mono_ns() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

} // namespace feeds
