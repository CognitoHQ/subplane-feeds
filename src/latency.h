#pragma once

// Frame-to-offer latency: a log2 histogram with 8 linear sub-buckets per power of
// two, from 1 ns to 2^40 ns. One writer, no locks, no allocation; quantiles are
// read from the bucket counts (upper edge of the bucket, so slightly pessimistic).

#include <chrono>
#include <cstdint>

namespace feeds {

class LatencyHistogram {
public:
    static constexpr int kBits = 40, kSub = 8;
    void record(uint64_t ns) noexcept {
        if (ns == 0) ns = 1;
        const int lg = 63 - __builtin_clzll(ns);
        if (lg >= kBits) { ++overflow_; return; }
        const int sub = lg < 3 ? 0 : static_cast<int>((ns >> (lg - 3)) & 7);
        ++buckets_[lg * kSub + sub];
        ++count_;
        sum_ += ns;
    }
    uint64_t count() const noexcept { return count_; }
    uint64_t mean() const noexcept { return count_ ? sum_ / count_ : 0; }
    uint64_t max() const noexcept { return max_; }
    uint64_t quantile(double q) const noexcept {
        if (!count_) return 0;
        const uint64_t target = static_cast<uint64_t>(static_cast<double>(count_) * q);
        uint64_t seen = 0;
        for (int i = 0; i < kBits * kSub; ++i) {
            seen += buckets_[i];
            if (seen > target) {
                const int lg = i / kSub, sub = i % kSub;
                return lg < 3 ? (uint64_t{1} << lg) : ((uint64_t{9} + static_cast<uint64_t>(sub)) << (lg - 3)) - 1;
            }
        }
        return 1ull << kBits;
    }
    void note_max(uint64_t ns) noexcept { if (ns > max_) max_ = ns; }
private:
    uint64_t buckets_[kBits * kSub] = {};
    uint64_t count_ = 0, sum_ = 0, max_ = 0, overflow_ = 0;
};

inline uint64_t mono_ns() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

} // namespace feeds
