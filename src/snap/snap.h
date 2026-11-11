#pragma once

// spwsgw-snap's state (docs/snap.md): a silent subscriber of one gateway's
// canonical data stream and SPP state stream that keeps, per instrument, the
// whole book and the last self-contained values, and knows whether they can be
// trusted. Network-free: feed it data frames and state frames, ask it for a
// recap reply. One thread.
//
// It is the first consumer of the gateway's output that must rebuild books
// exactly from the stream alone; if it cannot, apps cannot either.

#include "book/book.h"
#include "one_writer.h"
#include "subplane_proto_codec.h"

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace feeds {

struct SnapStats {
    OneWriter data_frames, messages, gaps, restarts, malformed, book_overflows;
    OneWriter state_messages, states_applied, states_ignored;   // ignored: an Ok for a book this copy did not see whole
    OneWriter requests, replies, rejects_not_ready, rejects_unknown;
    OneWriter republish_asks;                       // instruments (or all) asked for again
    OneWriter instruments_ok, instruments_stale, instruments_unknown;   // gauges
    OneWriter last_sequence;                        // gauge: last_sequence(), for the HTTP thread
};

class SnapCore {
public:
    // `max_levels`: a side of each book, at least the gateway's (its pack's max_levels),
    // or a deep book overflows here and goes Stale until republished.
    explicit SnapCore(uint32_t max_levels = Book::kDefaultMaxLevels) : max_levels_(max_levels) {}

    // One data offer: canonical messages back to back, each 8 + blockLength.
    void on_data(const uint8_t* buf, size_t len) noexcept;
    // One state offer: u32-length-prefixed SPP messages (SourceState).
    void on_state(const uint8_t* buf, size_t len) noexcept;
    // The data stream lost its publisher (image gone): nothing is trustworthy until it is republished.
    void on_data_lost() noexcept;

    // A recap reply for `instrument` (`venue` 0 matches any): RecapBegin, RecapRecords, RecapEnd,
    // or a single RecapReject, appended to `out` as u32-length-prefixed SPP messages.
    void answer(uint64_t correlation, uint64_t instrument, uint16_t venue, std::vector<uint8_t>& out);

    // What to ask the gateway for: true once with `all` after start or a restart;
    // then instruments that are Stale, each at most once per `every_ns`.
    struct Ask { bool all = false; uint64_t instrument = 0; uint16_t venue = 0; };
    bool next_ask(uint64_t now, uint64_t every_ns, Ask& out) noexcept;

    uint64_t last_sequence() const noexcept { return last_seq_; }
    uint64_t source_epoch() const noexcept { return epoch_; }
    sp_proto::SourceStateCode state(uint64_t instrument, uint16_t venue) const noexcept;
    // The rebuilt book, for tests and inspection; null when unknown.
    const Book* book(uint64_t instrument, uint16_t venue) const noexcept;
    const SnapStats& stats() const noexcept { return stats_; }

private:
    struct Inst {
        explicit Inst(uint32_t max_levels) : book(max_levels) {}
        uint32_t id = 0;
        uint16_t venue = 0;
        sp_proto::SourceStateCode state = sp_proto::SourceStateCode::Unknown;
        bool full_depth = false;            // a Clear was seen: BookUpdates carry the whole book
        bool has_book = false;
        uint64_t whole_at = 0;              // Sequence of the last Clear (or whole BookSnapshot); 0 = not whole
        uint64_t iseq = 0;                  // InstrumentSeq of the last message seen; replies are as of it
        Book book;
        std::vector<uint8_t> definition, quote, trade, status, snapshot;   // last message of each, as published
        uint64_t last_ask = 0;
    };
    struct PendingState { uint64_t key; uint64_t sequence; sp_proto::SourceStateCode state; };

    static uint64_t key(uint64_t instrument, uint16_t venue) noexcept { return (static_cast<uint64_t>(venue) << 32) | (instrument & 0xFFFFFFFFull); }
    Inst& inst(uint32_t id, uint16_t venue);
    const Inst* find(uint64_t instrument, uint16_t venue) const noexcept;
    void on_message(const uint8_t* msg, size_t len) noexcept;
    void apply_due_states() noexcept;
    void set_state(Inst& in, sp_proto::SourceStateCode s) noexcept;
    void forget_all() noexcept;            // every instrument Unknown: a restart or a lost stream
    void stale_all() noexcept;             // every Ok instrument Stale: a gap on the data stream
    void count_states() noexcept;

    std::unordered_map<uint64_t, Inst> insts_;
    std::vector<PendingState> pending_;    // states whose Sequence the data stream has not reached
    uint32_t max_levels_;
    uint64_t last_seq_ = 0, epoch_ = 0;
    uint64_t gap_at_ = 0;                  // first Sequence after the last gap
    bool ask_all_ = true;
    bool restarted_ = false;               // the data stream restarted; the new epoch is expected, not news
    std::vector<Level> scratch_;
    SnapStats stats_;
};

} // namespace feeds
