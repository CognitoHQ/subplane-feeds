#pragma once

// spwsgw-snap's state (docs/snap.md): a silent subscriber of one gateway's
// canonical data stream and SPP state stream that keeps, per instrument, the
// whole book and the last self-contained values, and knows whether they can be
// trusted. Network-free: feed it data frames and state frames, register recap
// interest, take the sections that are due. One thread.
//
// Recap is interest, not request and reply: a RecapRequest says "publish this
// instrument" (or, Bulk, "every instrument"), and each wanted instrument is
// published once, as a section on the shared section stream, however many
// agents asked during its gather window. An instrument that is not Ok yet
// keeps its interest and is published when it is (a gateway restart costs one
// section per instrument, not a NotReady and a retry from every agent).
//
// It is the first consumer of the gateway's output that must rebuild books
// exactly from the stream alone; if it cannot, apps cannot either.

#include "book/book.h"
#include "one_writer.h"
#include "subplane_proto_codec.h"

#include <cstdint>
#include <deque>
#include <unordered_map>
#include <vector>

namespace feeds {

struct SnapStats {
    OneWriter data_frames, messages, gaps, restarts, malformed, book_overflows;
    OneWriter state_messages, states_applied, states_ignored;   // ignored: an Ok for a book this copy did not see whole
    OneWriter requests, bulk_requests;              // RecapRequests: for one instrument, for every instrument
    OneWriter rejects_unknown, wants_coalesced;     // of `requests`: an instrument never seen; one already wanted
    OneWriter sections, section_bytes;              // sections built
    OneWriter wanted, waiting;                      // gauges: instruments with interest, and those waiting to be Ok
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

    // Gather windows: how long the first interest in an instrument waits for
    // others to join before its section is due. A bulk (every instrument, a
    // partition-wide resync every agent sees at once) waits longer.
    struct Gather { uint64_t single_ns = 20'000'000; uint64_t bulk_ns = 1'000'000'000; };
    void set_gather(Gather g) noexcept { gather_ = g; }

    // Interest in one instrument (`venue` 0 matches any). UNKNOWN: this copy has
    // never seen it (not this partition's, or not published yet).
    enum class Want : uint8_t { QUEUED, COALESCED, UNKNOWN };
    Want want(uint64_t instrument, uint16_t venue, uint64_t now) noexcept;
    // Interest in every instrument this copy knows: one bulk window per partition.
    void want_all(uint64_t now) noexcept;
    // The next due section, appended to `out` as u32-length-prefixed SPP
    // messages (RecapBegin naming the instrument, RecapRecords, RecapEnd), its
    // interest cleared. Single interest goes before bulk. False when none is due.
    bool next_section(uint64_t now, std::vector<uint8_t>& out);

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
        uint64_t iseq = 0;                  // InstrumentSeq of the last message seen; sections are as of it
        uint64_t want_due = 0;              // interest: its section is due then; 0 = none
        bool want_single = false;           // the interest is (also) a single request: ahead of bulk
        bool want_parked = false;           // due, but not Ok: published when it is
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
    void build_section(const Inst& in, uint64_t section_id, std::vector<uint8_t>& out);
    Inst* pop_due(std::deque<uint64_t>& q, bool single, uint64_t now) noexcept;

    std::unordered_map<uint64_t, Inst> insts_;
    std::vector<PendingState> pending_;    // states whose Sequence the data stream has not reached
    uint32_t max_levels_;
    uint64_t last_seq_ = 0, epoch_ = 0;
    uint64_t gap_at_ = 0;                  // first Sequence after the last gap
    bool ask_all_ = true;
    bool restarted_ = false;               // the data stream restarted; the new epoch is expected, not news
    std::vector<Level> scratch_;
    std::deque<uint64_t> singles_, bulk_;   // keys with interest, in due order (stale entries skipped)
    Gather gather_;
    uint64_t bulk_window_ = 0;             // the open bulk window ends then
    uint64_t section_id_ = 0;
    SnapStats stats_;
};

} // namespace feeds
