#pragma once

// Where canonical messages go: Aeron (production) or a text stream (tests, dry runs).

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

namespace feeds {

// The outcome of one offer. Anything but OK is a drop, counted by the gateway per reason.
enum class Offer : uint8_t {
    OK,
    BACK_PRESSURED,   // a subscriber is behind and the retry spin ran out
    NOT_CONNECTED,    // nobody subscribes yet: normal before the agent starts
    FAILED,           // closed, or the stream's position limit reached
};

class Publisher {
public:
    virtual ~Publisher() = default;
    // One or more whole canonical messages back to back, each self-framed by its SBE header
    // (8 bytes + blockLength). One offer on Aeron.
    virtual Offer publish(const uint8_t* msg, size_t len) noexcept = 0;
    virtual std::string describe() const = 0;
    virtual void close() noexcept {}
    // Whether a subscriber is connected. Safe from any thread while the publisher is open.
    virtual bool connected() const noexcept { return true; }
    // The downstream endpoint key for /topology (docs/observability.md), empty when the
    // output is not a stream another service can subscribe to (stdout, null).
    virtual std::string endpoint_key(std::string_view /*host*/) const { return {}; }
};

// One line per message: "<Message> iid=<n> seq=<n> ets=<ns> <fields...>", integers only (exact, diffable).
// Walks a batch by each message's SBE header (all canonical messages are flat: 8 + blockLength).
class StdoutPublisher final : public Publisher {
public:
    explicit StdoutPublisher(std::ostream& out) : out_(out) {}
    Offer publish(const uint8_t* msg, size_t len) noexcept override;
    std::string describe() const override { return "stdout"; }
private:
    bool publish_one(const uint8_t* msg, size_t len) noexcept;
    std::ostream& out_;
};

// Counts and discards: for measuring the gateway without a publisher's cost.
class NullPublisher final : public Publisher {
public:
    Offer publish(const uint8_t*, size_t len) noexcept override { ++n_; bytes_ += len; return Offer::OK; }
    std::string describe() const override { return "null"; }
    uint64_t offers() const noexcept { return n_; }
    uint64_t bytes() const noexcept { return bytes_; }
private:
    uint64_t n_ = 0, bytes_ = 0;
};

#if FEEDS_WITH_AERON
struct AeronPublisherConfig {
    std::string channel = "aeron:ipc";
    int32_t     stream_id = 1001;
    std::string aeron_dir;              // empty = Aeron default
    bool        embedded_driver = false;
    uint64_t    driver_timeout_ms = 10000;
    uint64_t    offer_retry_us = 2000;  // spin on back pressure for this long, then drop
};
std::unique_ptr<Publisher> make_aeron_publisher(const AeronPublisherConfig& cfg, std::string& err);
#endif

} // namespace feeds
