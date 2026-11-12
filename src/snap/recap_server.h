#pragma once

// spwsgw-snap's SPP side: answers the agent's Hello, and turns RecapRequests
// into SnapCore interest (the sections go out on the section stream, not here;
// only an unknown instrument is answered, with RecapReject). Network-free; the
// Aeron binding is the gateway's AeronControl.

#include "control.h"
#include "snap/snap.h"

#include <string>

namespace feeds {

class RecapServer final : public SppHandler {
public:
    RecapServer(SnapCore& core, std::string service_name, uint64_t epoch) : core_(core), name_(std::move(service_name)), epoch_(epoch) {}
    void on_frame(const uint8_t* buf, size_t len, std::vector<uint8_t>& reply) noexcept override;
    ControlStats& stats() noexcept override { return stats_; }
    const ControlStats& stats() const noexcept { return stats_; }
    // The clock interest is registered at (gather windows); set before each poll.
    void set_time(uint64_t now) noexcept { now_ = now; }

private:
    void on_message(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept;
    SnapCore& core_;
    std::string name_;
    uint64_t epoch_;
    uint64_t now_ = 0;
    ControlStats stats_;
};

} // namespace feeds
