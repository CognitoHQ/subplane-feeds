#pragma once

// The gateway's SPP control input (docs/gateway.md, "Control"): Hello and
// Republish from snapshot services and operators, answered with Hello and
// Status. ControlHandler is network-free (frames in, a reply frame out);
// ControlInput is the transport that feeds it (Aeron in publisher_aeron.cpp,
// a fake in tests). Both run on the gateway thread, from its timer, never on
// the data path.

#include "one_writer.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace feeds {

class Gateway;

struct ControlStats {
    OneWriter frames, malformed;                      // frames received; frames or messages that do not parse
    OneWriter hellos, republish, status, unsupported; // messages by kind; unsupported = answered Status{Unsupported}
    OneWriter replies, reply_dropped;                 // reply frames offered, and those not delivered
    OneWriter peers;                                  // gauge: requesters seen (Aeron images)
};

// Anything that answers SPP request frames: the gateway's control (below), a
// snapshot service's recap server.
class SppHandler {
public:
    virtual ~SppHandler() = default;
    // One received frame: u32-length-prefixed SPP messages. Appends the replies, framed
    // the same way, to `reply` (one frame per request frame, so replies never interleave).
    virtual void on_frame(const uint8_t* buf, size_t len, std::vector<uint8_t>& reply) noexcept = 0;
    virtual ControlStats& stats() noexcept = 0;
};

class ControlHandler final : public SppHandler {
public:
    ControlHandler(Gateway& gw, std::string service_name, uint64_t epoch) : gw_(gw), name_(std::move(service_name)), epoch_(epoch) {}
    void on_frame(const uint8_t* buf, size_t len, std::vector<uint8_t>& reply) noexcept override;
    ControlStats& stats() noexcept override { return stats_; }
    const ControlStats& stats() const noexcept { return stats_; }

private:
    void on_message(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept;
    void hello(uint64_t correlation, std::vector<uint8_t>& reply) noexcept;
    void status(uint64_t correlation, uint16_t code, std::vector<uint8_t>& reply) noexcept;

    Gateway& gw_;
    std::string name_;
    uint64_t epoch_;
    ControlStats stats_;
};

class ControlInput {
public:
    virtual ~ControlInput() = default;
    // Reads at most `limit` fragments, hands each to `h`, sends the replies. Never blocks.
    virtual int poll(SppHandler& h, int limit) noexcept = 0;
    virtual std::string describe() const = 0;
    // The request stream's endpoint key for /topology, empty when it has none.
    virtual std::string endpoint_key(std::string_view /*host*/) const { return {}; }
    virtual void close() noexcept {}
};

} // namespace feeds
