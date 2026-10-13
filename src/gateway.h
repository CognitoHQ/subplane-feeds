#pragma once

// The network-independent core: raw frames in, canonical messages out. Routes a
// frame to its StreamDef and Instrument (by wrapper stream name or by fields),
// iterates `each` records, maps flat streams, drives book strategies, assigns the
// gap-free Sequence, and publishes heartbeats and instrument definitions on a
// timer. One thread calls everything.

#include "book/book.h"
#include "instruments.h"
#include "latency.h"
#include "mapper.h"
#include "pack.h"
#include "publisher.h"
#include "recorder.h"

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
    bool     log_errors = true;      // first few mapping errors to stderr
};

struct GatewayStats {
    uint64_t frames = 0, acks = 0, unmatched_streams = 0, unknown_symbols = 0, parse_errors = 0, map_errors = 0, book_errors = 0;
    uint64_t records = 0, published = 0, dropped = 0;
    uint64_t trades = 0, quotes = 0, book_updates = 0, book_snapshots = 0, heartbeats = 0, definitions = 0, other = 0;
    uint64_t snapshot_requests = 0, snapshots_applied = 0;
};

class Gateway {
public:
    Gateway(const Pack& pack, const Instruments& instruments, Publisher& pub, GatewayOptions opts, Recorder* rec = nullptr);

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

    // Every (stream, instrument) name, e.g. "btcusdt@trade"; the order is stream-major.
    std::vector<std::string> stream_names() const;
    // The subscribe messages in send order, with the pack's placeholders expanded.
    std::vector<std::string> subscribe_messages() const;
    const GatewayStats& stats() const noexcept { return stats_; }
    // Wall time from entering on_frame to the last offer of that frame (frames that published something).
    const LatencyHistogram& frame_latency() const noexcept { return lat_; }
    uint64_t sequence() const noexcept { return seq_; }
    uint64_t resyncs() const noexcept;

private:
    struct BookSlot { std::unique_ptr<BookStrategy> strategy; const StreamDef* sd; const Instrument* ins; };

    void publish(uint8_t* msg, size_t len, uint64_t& counter) noexcept;
    void publish_heartbeat(uint64_t now) noexcept;
    void publish_definitions(uint64_t now) noexcept;
    void publish_emits(BookSlot& slot, uint64_t recv_ts) noexcept;
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
    simdjson::ondemand::parser parser_;
    std::string buf_;                  // frame copy with simdjson padding
    std::vector<BookSlot> books_;      // (book stream, instrument) pairs
    std::vector<BookEmit> emits_;
    std::deque<SnapshotRequest> requests_;
    FrameTokens frame_;                // "$." field tokens of the current frame
    std::string_view kind_;            // the `kind` role value of the current frame, when frame-scope
    GatewayStats stats_;
    LatencyHistogram lat_;
    uint64_t seq_ = 0, last_heartbeat_ = 0, last_definitions_ = 0;
    uint32_t errors_logged_ = 0;
};

} // namespace feeds
