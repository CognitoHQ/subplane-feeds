#pragma once

// spwsgw-snap's SPP side: answers the agent's Hello, turns RecapRequests into
// SnapCore interest (the sections go out on the section stream, not here; only
// an unknown instrument is answered, with RecapReject), and answers Resolve
// from the gateway's instruments file. Network-free; the Aeron binding is the
// gateway's AeronControl.

#include "control.h"
#include "instruments.h"
#include "snap/snap.h"

#include <string>
#include <unordered_map>

namespace feeds {

class RecapServer final : public SppHandler {
public:
    RecapServer(SnapCore& core, std::string service_name, uint64_t epoch) : core_(core), name_(std::move(service_name)), epoch_(epoch) {}
    void on_frame(const uint8_t* buf, size_t len, std::vector<uint8_t>& reply) noexcept override;
    ControlStats& stats() noexcept override { return stats_; }
    const ControlStats& stats() const noexcept { return stats_; }
    // The clock interest is registered at (gather windows); set before each poll.
    void set_time(uint64_t now) noexcept { now_ = now; }

    // The topic as the gateway's instruments file describes it (every
    // partition): answers SPP Resolve, and refuses at once a RecapRequest for
    // an instrument the file puts on another partition. Without it, Resolve is
    // Unsupported and interest is taken for any instrument.
    struct Directory {
        const Instruments* list = nullptr;
        uint16_t topic_id = 0;
        uint16_t partitions = 0;   // the highest partition the file names
        uint16_t partition = 1;    // this snap's
        uint16_t venue = 0;        // VenueId as the gateway stamps it (its pack's venue_id)
    };
    void set_directory(Directory d);

private:
    void on_message(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept;
    void resolve(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept;
    const Instrument* by_id(uint64_t id) const noexcept;
    SnapCore& core_;
    Directory dir_;
    std::unordered_map<uint64_t, size_t> by_id_;   // into dir_.list->all()
    std::string name_;
    uint64_t epoch_;
    uint64_t now_ = 0;
    ControlStats stats_;
};

} // namespace feeds
