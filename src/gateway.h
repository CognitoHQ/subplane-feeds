#pragma once

// The network-independent core: raw frames in, canonical messages out. Routes a
// frame to its StreamDef and Instrument (by wrapper stream name or by fields),
// iterates `each` records, maps flat streams, drives book strategies, assigns the
// gap-free Sequence, and publishes heartbeats and instrument definitions on a
// timer. One thread calls everything.
//
// Recovery (docs/gateway.md, "Recovery"): every instrument has a SourceState
// (Unknown, Stale, Ok) published on a state stream of its own, never in the
// canonical data. A book that (re)syncs publishes itself whole in band (Clear,
// then every level), then the state goes Ok at that Sequence. republish() asks
// for the same again, rate-limited, for one instrument or all.

#include "book/book.h"
#include "one_writer.h"
#include "instruments.h"
#include "latency.h"
#include "mapper.h"
#include "pack.h"
#include "publisher.h"
#include "recorder.h"
#include "subplane_proto_codec.h"

#include <simdjson.h>

#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace feeds {

struct GatewayOptions {
    uint64_t heartbeat_ms = 1000;
    uint64_t definitions_ms = 60000;
    uint32_t snapshot_every = 100;   // BookSnapshot every N book updates per instrument (0 = only on sync)
    uint32_t batch_bytes = 1376;     // messages of one input frame share one offer up to this many bytes (0 = one message per offer)
    bool     log_errors = true;      // first few mapping errors to stderr
    uint64_t source_epoch = 0;       // SourceState.SourceEpoch: this process's start, ns; a new value tells subscribers Sequence restarted
    uint32_t republish_per_second = 20;   // instruments republished per second at most (one per tick() at the most)
};

// Written by the gateway thread only; readable from any thread (one_writer.h), which
// is what lets /metrics run on its own thread.
struct GatewayStats {
    OneWriter frames, acks, unmatched_streams, unknown_symbols, parse_errors, map_errors, book_errors;
    OneWriter records, published, offers, offer_bytes;
    // Messages not delivered, by the publisher's reason (Offer); `dropped` is their sum.
    OneWriter dropped, dropped_back_pressured, dropped_not_connected, dropped_failed;
    OneWriter trades, quotes, book_updates, book_snapshots, heartbeats, definitions, other;
    OneWriter snapshot_requests, snapshots_applied;
    OneWriter connects, disconnects;   // exchange connection (on_connected / on_disconnected)
    OneWriter connected;               // 1 between on_connected and on_disconnected
    OneWriter last_sequence;           // the last Sequence offered
    OneWriter resyncs;                 // sum over book strategies, refreshed by tick()
    OneWriter state_messages, state_dropped;              // SourceState offers, and those not delivered
    OneWriter republish_queued, republish_not_found;      // republish() requests by outcome
    OneWriter republished;                                // instruments republished
    OneWriter instruments_ok, instruments_stale, instruments_unknown;   // gauges: instruments by SourceState
};

enum class RepublishResult : uint8_t { QUEUED, NOT_FOUND };

class Gateway {
public:
    // `state`: where SourceState goes (length-prefixed SPP frames); null publishes none.
    Gateway(const Pack& pack, const Instruments& instruments, Publisher& pub, GatewayOptions opts, Recorder* rec = nullptr, Publisher* state = nullptr);

    // A raw text frame from the exchange (or a recording). Records it when a recorder is attached.
    void on_frame(std::string_view json, uint64_t recv_ts);
    // A snapshot body for a venue symbol (REST response, or a recorded one).
    void on_snapshot(std::string_view venue_symbol, std::string_view body, uint64_t recv_ts);
    void on_connected(uint64_t now);
    void on_disconnected(uint64_t now);
    // Timers: heartbeat and definitions. Call often with the current (or recorded) time.
    void tick(uint64_t now);
    // Pops one pending snapshot request (strategies that need a snapshot). False when none.
    bool next_snapshot_request(SnapshotRequest& out);
    // SPP Republish: publish `instrument` (0 = every one) again, in band, from tick(): its
    // definition, its books whole when synced, then its SourceState with `correlation`.
    // `venue` 0 matches any. A request for an instrument already queued joins it.
    RepublishResult republish(uint64_t instrument, uint16_t venue, uint64_t correlation) noexcept;
    sp_proto::SourceStateCode state(uint32_t instrument_id) const noexcept;

    // Every (stream, instrument) name, e.g. "btcusdt@trade"; the order is stream-major.
    std::vector<std::string> stream_names() const;
    // The subscribe messages in send order, with the pack's placeholders expanded.
    std::vector<std::string> subscribe_messages() const;
    const GatewayStats& stats() const noexcept { return stats_; }
    // Wall time from entering on_frame to the last offer of that frame (frames that published something).
    const LatencyHistogram& frame_latency() const noexcept { return lat_; }
    // RecvTs (CLOCK_REALTIME at frame receive) to the end of the offer, the same clock the agent's ingest_ts uses.
    const LatencyHistogram& recv_to_offer_rt() const noexcept { return lat_rt_; }
    uint64_t sequence() const noexcept { return seq_; }
    uint64_t resyncs() const noexcept;   // gateway thread; other threads read stats().resyncs

private:
    struct BookSlot { std::unique_ptr<BookStrategy> strategy; const StreamDef* sd; const Instrument* ins; size_t idx; };
    struct InsState {
        sp_proto::SourceStateCode code = sp_proto::SourceStateCode::Unknown;
        sp_proto::SourceReason reason = sp_proto::SourceReason::None;
        bool ever_ok = false, queued = false;
        uint64_t correlation = 0;      // of the Republish that queued it
    };

    void publish(uint8_t* msg, size_t len, OneWriter& counter) noexcept;   // appends to the current batch
    void offer(const uint8_t* msg, size_t len, uint64_t messages) noexcept;
    void flush() noexcept;                                                   // one offer for everything appended since the last flush
    void publish_heartbeat(uint64_t now) noexcept;
    void publish_definitions(uint64_t now) noexcept;
    void publish_emits(BookSlot& slot, uint64_t recv_ts) noexcept;
    void publish_book_update(const BookSlot& slot, Side side, Action action, uint8_t level, int64_t px, int64_t qty, uint64_t exch_ts, uint64_t recv_ts) noexcept;
    void publish_definition(const Instrument& in, uint64_t now) noexcept;
    void publish_state(uint64_t instrument, sp_proto::SourceStateCode code, sp_proto::SourceReason reason, uint64_t correlation) noexcept;
    // Publishes instrument `i`'s SourceState when it changed (or always, with `force`).
    // `why` is the reason when it is not Ok; Ok says Startup, Resync or Republish itself.
    void refresh_state(size_t i, sp_proto::SourceReason why, bool force = false, uint64_t correlation = 0) noexcept;
    bool instrument_ok(size_t i) const noexcept;
    void republish_one(size_t i, uint64_t now) noexcept;
    void count_states() noexcept;
    void process_frame(std::string_view json, uint64_t recv_ts);
    void handle_record(const StreamDef& sd, const Instrument* in, simdjson::ondemand::object& rec, std::string_view kind, uint64_t recv_ts) noexcept;
    void cache_frame_tokens(const StreamDef& sd, simdjson::ondemand::object& root) noexcept;
    bool prepare(std::string_view json) noexcept; // copies into the padded buffer
    void log_error(const char* what, const std::string& err) noexcept;
    std::string expand(std::string_view tpl, std::string_view stream_symbol, std::string_view venue_symbol, const StreamDef* sd) const;

    const Pack& pack_;
    const Instruments& ins_;
    Publisher& pub_;
    GatewayOptions opts_;
    Recorder* rec_;
    Publisher* state_;
    simdjson::ondemand::parser parser_;
    std::string buf_;                  // frame copy with simdjson padding
    std::vector<BookSlot> books_;      // (book stream, instrument) pairs
    std::vector<BookEmit> emits_;
    std::vector<Level> top_;           // scratch for snapshots, reserved once
    std::vector<uint8_t> batch_;       // messages of the current frame, back to back, each self-framed by its SBE header
    uint64_t pending_ = 0;             // messages in batch_
    std::deque<SnapshotRequest> requests_;
    std::vector<InsState> states_;     // per instrument, in ins_.all() order
    std::deque<size_t> republish_;     // instrument indexes waiting for republish_one()
    uint64_t next_republish_ = 0;
    bool connected_ = false;
    FrameTokens frame_;                // "$." field tokens of the current frame
    std::string_view kind_;            // the `kind` role value of the current frame, when frame-scope
    GatewayStats stats_;
    LatencyHistogram lat_, lat_rt_;
    uint64_t seq_ = 0, last_heartbeat_ = 0, last_definitions_ = 0;
    uint32_t errors_logged_ = 0;
};

} // namespace feeds
