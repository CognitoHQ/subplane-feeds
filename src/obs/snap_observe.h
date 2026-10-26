#pragma once

// spwsgw-snap's observability surface (docs/observability.md, "The snapshot
// service's metrics"): /metrics in Prometheus text or JSON, /topology, /health.
// Cold; called on the HTTP thread.
//
// INTENT: everything read here is a OneWriter (SnapStats, ControlStats,
// LinkStats, SnapLive) or immutable after startup (the identity and the edge
// keys). SnapCore's instruments, books and sequence belong to the snap thread;
// reading them from here would be a data race, which is why the sequence is
// mirrored into SnapStats::last_sequence.

#include "obs/observe.h"
#include "snap/gateway_link.h"
#include "snap/snap.h"

#include <string>

namespace feeds {

// What the snap thread knows about its Aeron connections, set every loop.
struct SnapLive {
    OneWriter data_images, state_images;   // images on the gateway's data and state streams
    OneWriter gateway_connected;           // 1 while the gateway's control channel takes requests
};

// The endpoints, keyed by the endpoint-key rules, and how each is described.
struct SnapEdges {
    std::string data_key, data_describe;         // the gateway's canonical stream
    std::string state_key, state_describe;       // the gateway's SourceState stream
    std::string gateway_key, gateway_describe;   // the gateway's SPP control channel (Republish); empty without one
    std::string recap_key, recap_describe;       // the recap request channel the agents ask on
};

class SnapObserver {
public:
    // `link` is null without --gateway-control.
    SnapObserver(const SnapCore& core, const ControlStats& recap, const GatewayLink* link, const SnapLive& live, Identity id, SnapEdges edges);

    std::string metrics_text() const;
    std::string metrics_json() const;
    std::string topology_json() const;
    // False with a reason while no gateway data stream is connected.
    bool healthy(std::string& reason) const;

private:
    const SnapCore& core_;
    const ControlStats& recap_;
    const GatewayLink* link_;
    const SnapLive& live_;
    Identity id_;
    SnapEdges e_;
};

} // namespace feeds
