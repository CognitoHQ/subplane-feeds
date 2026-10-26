#pragma once

// The gateway's observability surface (docs/observability.md): /metrics in
// Prometheus text or JSON, /topology, /health. Cold; called on the HTTP thread.
//
// INTENT: everything read here is safe while the gateway thread runs: the
// stats are one-writer OneWriter values, the histograms are relaxed atomics, the pack
// and instruments are immutable after startup, and Publisher::connected() and
// endpoint_key() are documented thread-safe. Reading anything else of the
// Gateway (its books, its sequence) from here would be a data race.

#include "control.h"
#include "gateway.h"
#include "obs/prom_writer.h"

#include <cstdint>
#include <string>

namespace feeds {

struct Identity {
    std::string service = "spwsgw";
    std::string instance;          // --instance, default the pack's exchange
    std::string host;              // gethostname()
    std::string version;
    std::string site, env;         // --site, --env
    PromWriter::Labels labels;     // --label k=v, on subplane_service_info only
    uint64_t epoch = 0;            // wall ns at start: changes on every restart
    int32_t pid = 0;
};

class Observer {
public:
    // `upstream_url`: the exchange's WebSocket URL, or file://<recording> on replay.
    // `live`: connected to an exchange (REST snapshot endpoints are upstream edges).
    // `control`, `control_in`: the SPP control input, when there is one.
    Observer(const Gateway& gw, const Pack& pack, const Instruments& ins, const Publisher& pub, Identity id,
             std::string upstream_url, bool live, const ControlHandler* control = nullptr, const ControlInput* control_in = nullptr);

    std::string metrics_text() const;
    std::string metrics_json() const;
    std::string topology_json() const;
    // False with a reason when the gateway cannot do its job: live and not connected to the exchange.
    bool healthy(std::string& reason) const;

    const Identity& identity() const noexcept { return id_; }
    // Identity validation: a label name matches [a-zA-Z_][a-zA-Z0-9_]* and does not start with "__".
    static bool valid_label_name(const std::string& k);
    // --labels "k=v,...": false with `bad` (the offending item) if a name is invalid or has no '='.
    static bool parse_labels(const std::string& csv, PromWriter::Labels& out, std::string& bad);

private:
    struct Edge { std::string direction, role, key, describe; };
    std::string data_state() const;
    std::string publication_state() const;

    const Gateway& gw_;
    const Pack& pack_;
    const Instruments& ins_;
    const Publisher& pub_;
    Identity id_;
    std::string upstream_key_, upstream_describe_;
    std::vector<std::string> recap_keys_;   // distinct REST snapshot endpoints
    std::string downstream_key_;
    std::string state_key_;                 // the SourceState stream, when the output has one
    const ControlHandler* control_;
    std::string control_key_, control_describe_;   // the SPP request stream, when there is a control input
    bool live_;
};

std::string json_str(std::string_view s);

} // namespace feeds
