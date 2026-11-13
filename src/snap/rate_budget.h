#pragma once

// How fast spwsgw-snap may publish sections (docs/snap.md, "Pacing"). The
// interface is what a shared, host-wide budget (the planned rate tracker:
// per interface, per topic) implements too, so the snap's section path does
// not change when it arrives. TokenBucket is the local one.

#include <cstdint>

namespace feeds {

class RateBudget {
public:
    virtual ~RateBudget() = default;
    // Takes `bytes` if the budget allows them now; false: wait and ask again.
    virtual bool try_take(uint64_t bytes, uint64_t now_ns) noexcept = 0;
    // Returns what a take did not use (the offer was back pressured).
    virtual void give_back(uint64_t bytes) noexcept = 0;
};

// `rate` bytes per second, up to `burst` saved up; rate 0 is unlimited.
class TokenBucket final : public RateBudget {
public:
    TokenBucket(uint64_t rate_bytes_per_s, uint64_t burst_bytes) noexcept
        : rate_(rate_bytes_per_s), burst_(burst_bytes > 0 ? burst_bytes : 1), tokens_(static_cast<int64_t>(burst_)) {}

    bool try_take(uint64_t bytes, uint64_t now_ns) noexcept override {
        if (rate_ == 0) return true;
        refill(now_ns);
        // INTENT: a section larger than the burst goes once the bucket is full
        // and leaves it in debt, which the refill then pays off; requiring the
        // whole size up front would block it forever.
        const int64_t need = static_cast<int64_t>(bytes < burst_ ? bytes : burst_);
        if (tokens_ < need) return false;
        tokens_ -= static_cast<int64_t>(bytes);
        return true;
    }
    void give_back(uint64_t bytes) noexcept override {
        if (rate_ == 0) return;
        tokens_ += static_cast<int64_t>(bytes);
        if (tokens_ > static_cast<int64_t>(burst_)) tokens_ = static_cast<int64_t>(burst_);
    }
    int64_t tokens() const noexcept { return tokens_; }

private:
    void refill(uint64_t now_ns) noexcept {
        if (last_ns_ == 0 || now_ns <= last_ns_) { if (last_ns_ == 0) last_ns_ = now_ns; return; }
        const uint64_t dt = now_ns - last_ns_;
        constexpr uint64_t kNsPerS = 1'000'000'000u;
        const uint64_t add = dt / kNsPerS * rate_ + dt % kNsPerS * rate_ / kNsPerS;   // no overflow below ~18 GB/s
        if (add == 0) return;   // keep last_ns_: the fraction accrues
        last_ns_ = now_ns;
        const int64_t t = tokens_ + static_cast<int64_t>(add);
        tokens_ = t > static_cast<int64_t>(burst_) ? static_cast<int64_t>(burst_) : t;
    }

    uint64_t rate_, burst_;
    int64_t  tokens_;
    uint64_t last_ns_ = 0;
};

} // namespace feeds
