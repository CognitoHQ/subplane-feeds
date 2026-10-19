#pragma once

// Where canonical messages go: Aeron (production) or a text stream (tests, dry runs).

#include "control.h"

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
    // The state stream that travels with this output (SPP SourceState), when it has one.
    virtual Publisher* state_publisher() const noexcept { return nullptr; }
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

// The state stream as text: one line per SPP message, "SourceState iid=<n> venue=<n>
// seq=<n> state=<Unknown|Stale|Ok> reason=<...> corr=<n> epoch=<n>". Walks u32-length frames.
class StdoutStatePublisher final : public Publisher {
public:
    explicit StdoutStatePublisher(std::ostream& out) : out_(out) {}
    Offer publish(const uint8_t* msg, size_t len) noexcept override;
    std::string describe() const override { return "stdout"; }
private:
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
    int32_t     state_stream_id = 0;    // the SourceState stream on the same channel; 0 = none
};
std::unique_ptr<Publisher> make_aeron_publisher(const AeronPublisherConfig& cfg, std::string& err);

// The SPP control input (control.h): a subscription for requests and, per
// requester, a response publication (an Aeron response channel). UDP only: Aeron
// response channels need it. On the Aeron publisher's client (`aeron_pub` must
// come from make_aeron_publisher).
struct AeronControlConfig {
    std::string channel;                 // requests, e.g. aeron:udp?endpoint=127.0.0.1:40600
    int32_t     stream_id = 1003;
    std::string response_control;        // host:port requesters' response subscriptions use as control=
    int32_t     response_stream_id = 1004;
    uint64_t    timeout_ms = 10000;
    // Replies: the response publication's term length, the largest offer (replies are cut
    // at message boundaries; a reply may span offers, never interleaving with another
    // requester's, which has its own publication), and the most held per requester.
    uint32_t    term_length = 65536;
    size_t      max_offer = 8192;
    size_t      max_pending = 64 * 1024;
};
std::unique_ptr<ControlInput> make_aeron_control(Publisher& aeron_pub, const AeronControlConfig& cfg, std::string& err);
#endif

} // namespace feeds
