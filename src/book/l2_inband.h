#pragma once

// In-band level-2 protocol (Coinbase level2, Kraken book): the socket delivers a
// full snapshot first, then absolute-size changes (0 deletes). No REST. The
// `kind` of each event tells snapshot from update: a frame type the stream
// matched (Coinbase: snapshot | l2update) or the `kind` role (Kraken: $.type).
// Roles: bids/asks (snapshot and, for Kraken, updates), changes (rows with a
// side column), optional ExchangeTs. Checksums are not verified (see docs).

#include "book.h"

namespace feeds {

class L2Inband final : public BookBase {
public:
    L2Inband(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every);
    bool on_event(simdjson::ondemand::object& rec, std::string_view kind, uint64_t recv_ts, std::vector<BookEmit>& out, std::string& err) override;
    bool on_snapshot(simdjson::ondemand::object&, uint64_t, std::vector<BookEmit>&, std::string& err) override { err = "l2_inband takes no REST snapshot"; return false; }
    bool take_snapshot_request() noexcept override { return false; }
    void on_disconnect() noexcept override { synced_ = false; book_.clear(); }
    bool synced() const noexcept override { return synced_; }
    uint64_t dropped_before_sync() const noexcept { return dropped_; }
private:
    const FieldMap *r_bids_, *r_asks_, *r_changes_, *r_ts_;
    std::string snapshot_kind_;
    bool synced_ = false, had_snapshot_ = false;
    uint64_t dropped_ = 0;
    std::vector<LevelRow> rows_;
};

} // namespace feeds
