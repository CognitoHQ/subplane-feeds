#pragma once

// A 64-bit counter or gauge with one writer (the gateway thread) and any number of
// readers (the HTTP thread's /metrics and /topology).

#include <atomic>
#include <cstdint>

namespace feeds {

// INTENT: one writer, so ++ and += are a relaxed load and store, not a
// lock-prefixed read-modify-write: the hot path pays a plain add, as with a
// uint64_t, and a reader on another thread sees a whole value, never a torn
// one. A second writer would lose counts silently; give it its own OneWriter.
// Reads convert implicitly, so code and tests use it as a uint64_t.
class OneWriter {
public:
    OneWriter& operator++() noexcept { v_.store(v_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed); return *this; }
    OneWriter& operator+=(uint64_t n) noexcept { v_.store(v_.load(std::memory_order_relaxed) + n, std::memory_order_relaxed); return *this; }
    void set(uint64_t n) noexcept { v_.store(n, std::memory_order_relaxed); }
    operator uint64_t() const noexcept { return v_.load(std::memory_order_relaxed); }
private:
    std::atomic<uint64_t> v_{0};
};

} // namespace feeds
