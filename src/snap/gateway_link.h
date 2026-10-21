#pragma once

// spwsgw-snap's side of the gateway's control channel (docs/snap.md): Hello,
// then Republish asks from SnapCore::next_ask, and the gateway's Hello and
// Status replies. Network-free: it builds request frames and reads reply
// frames; snap_main moves them over an AeronRequester.

#include "one_writer.h"
#include "snap/snap.h"

#include <cstdint>
#include <string>
#include <vector>

namespace feeds {

struct LinkStats {
    OneWriter hellos_sent, republish_sent, offer_failed;   // requests offered; offers the channel did not take (retried)
    OneWriter hellos, status_ok, status_not_found, status_other, malformed;   // replies
};

class GatewayLink {
public:
    GatewayLink(std::string service_name, uint64_t epoch) : name_(std::move(service_name)), epoch_(epoch) {}

    // The next request frame to offer, if any: Hello once per connection, then the
    // pending ask, else a new one from `core`. Stays pending until sent() is called.
    bool next(SnapCore& core, uint64_t now, uint64_t ask_every_ns, std::vector<uint8_t>& frame);
    void sent() noexcept;                 // the frame from next() was taken
    void not_sent() noexcept { ++stats_.offer_failed; }
    void disconnected() noexcept { hello_sent_ = false; }   // say Hello again on the next connection

    void on_reply(const uint8_t* buf, size_t len) noexcept;

    uint64_t gateway_epoch() const noexcept { return gateway_epoch_; }   // from its Hello; 0 until then
    const LinkStats& stats() const noexcept { return stats_; }

private:
    enum class Kind : uint8_t { NONE, HELLO, REPUBLISH };
    std::string name_;
    uint64_t epoch_;
    uint64_t next_correlation_ = 1, gateway_epoch_ = 0;
    bool hello_sent_ = false;
    Kind pending_ = Kind::NONE;
    SnapCore::Ask ask_;
    LinkStats stats_;
};

} // namespace feeds
