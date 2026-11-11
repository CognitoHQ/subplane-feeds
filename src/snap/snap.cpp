#include "snap/snap.h"

#include "canonical_encode.h"
#include "canonical_fields.h"

#include <algorithm>
#include <cstring>

namespace feeds {
namespace {

using sp_proto::SourceStateCode;

uint32_t u32(const uint8_t* p, size_t off) noexcept { uint32_t v; std::memcpy(&v, p + off, 4); return v; }
uint16_t u16(const uint8_t* p, size_t off) noexcept { uint16_t v; std::memcpy(&v, p + off, 2); return v; }
uint64_t u64(const uint8_t* p, size_t off) noexcept { uint64_t v; std::memcpy(&v, p + off, 8); return v; }

template <class Enc, class F> void append(std::vector<uint8_t>& out, size_t cap, F fill) {
    const size_t at = out.size();
    out.resize(at + 4 + cap);
    Enc e(out.data() + at + 4, cap);
    fill(e);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    std::memcpy(out.data() + at, &n, 4);
    out.resize(at + 4 + n);
}

} // namespace

SnapCore::Inst& SnapCore::inst(uint32_t id, uint16_t venue) {
    // INTENT: a book's levels are allocated here, once per instrument (its first
    // message), so applying levels later never allocates.
    Inst& in = insts_.try_emplace(key(id, venue), max_levels_).first->second;
    if (in.id == 0) { in.id = id; in.venue = venue; count_states(); }
    return in;
}

const SnapCore::Inst* SnapCore::find(uint64_t instrument, uint16_t venue) const noexcept {
    if (venue != 0) {
        auto it = insts_.find(key(instrument, venue));
        return it == insts_.end() ? nullptr : &it->second;
    }
    // Venue 0: the instrument id alone, when it is unique across venues.
    const Inst* hit = nullptr;
    for (const auto& [k, in] : insts_) {
        if (in.id != instrument) continue;
        if (hit) return nullptr;
        hit = &in;
    }
    return hit;
}

void SnapCore::on_data(const uint8_t* buf, size_t len) noexcept {
    ++stats_.data_frames;
    size_t at = 0;
    while (at + sp_canonical::kHeaderSize <= len) {
        const size_t n = sp_canonical::kHeaderSize + u16(buf, at);
        if (n < kPrefixRecvTs + 8 || at + n > len) { ++stats_.malformed; return; }
        on_message(buf + at, n);
        at += n;
    }
    if (at != len) ++stats_.malformed;
    apply_due_states();
}

// INTENT: the data stream's Sequence is the only clock shared with the state
// stream and with the agent (RecapBegin.AsOf). A gap means messages this copy
// never saw: every book may be wrong, so all go Stale until republished. A
// sequence that starts again is a gateway restart even before its new epoch
// arrives on the state stream.
void SnapCore::on_message(const uint8_t* m, size_t n) noexcept {
    ++stats_.messages;
    const uint64_t seq = u64(m, kPrefixSequence);
    if (last_seq_ != 0 && seq <= last_seq_) {
        if (seq == 1 || seq + 1000 < last_seq_) { ++stats_.restarts; forget_all(); restarted_ = true; }
        else return;   // a duplicate: already applied
    } else if (last_seq_ != 0 && seq != last_seq_ + 1) {
        ++stats_.gaps;
        gap_at_ = seq;
        stale_all();
    } else if (last_seq_ == 0 && seq != 1) {
        gap_at_ = seq;   // joined mid-stream: everything before is as good as a gap
    }
    last_seq_ = seq;
    stats_.last_sequence.set(seq);
    const uint16_t tid = u16(m, 2);
    const uint32_t id = u32(m, kPrefixInstrumentId);
    const uint16_t venue = u16(m, kPrefixVenueId);
    if (tid == sp_canonical::Heartbeat::kTemplateId || id == 0) return;
    Inst& in = inst(id, venue);
    if (const uint64_t is = u64(m, kPrefixInstrumentSeq); is != 0) in.iseq = is;
    const uint32_t len = static_cast<uint32_t>(n);
    if (const auto* b = sp_canonical::view<sp_canonical::BookUpdate>(m, len)) {
        in.has_book = true;
        const auto action = static_cast<Action>(b->Action);
        if (action == Action::Clear) {
            // The gateway is publishing the book whole: until its Ok it is being rebuilt.
            in.book.clear();
            in.full_depth = true;
            in.whole_at = seq;
            if (in.state == SourceStateCode::Ok) set_state(in, SourceStateCode::Stale);
            return;
        }
        const Side side = static_cast<Side>(b->Side);
        if (side != Side::Buy && side != Side::Sell) { ++stats_.malformed; return; }
        if (in.full_depth && in.whole_at == 0) return;   // overflowed: nothing to apply to until the next Clear
        if (in.book.apply(side, b->Price, action == Action::Delete ? 0 : b->Qty).overflow) {
            // Deeper than this copy can hold: not the gateway's book any more. Stale
            // until the next whole book (the republish next_ask will ask for).
            ++stats_.book_overflows;
            in.book.clear();
            in.whole_at = 0;   // full_depth stays: its BookSnapshots are still only a top ten
            if (in.state == SourceStateCode::Ok) set_state(in, SourceStateCode::Stale);
        }
    } else if (const auto* s = sp_canonical::view<sp_canonical::BookSnapshot>(m, len)) {
        in.has_book = true;
        in.snapshot.assign(m, m + n);
        if (in.full_depth) return;   // the top ten of a book we hold whole
        // A book published only as snapshots (snapshot_only): each one is the book.
        in.book.clear();
        in.whole_at = seq;
        for (unsigned i = 0; i < s->Depth && i < 10; ++i) {
            if (s->BidQty[i]) in.book.apply(Side::Buy, s->BidPx[i], s->BidQty[i]);
            if (s->AskQty[i]) in.book.apply(Side::Sell, s->AskPx[i], s->AskQty[i]);
        }
    } else if (tid == sp_canonical::InstrumentDefinition::kTemplateId) {
        in.definition.assign(m, m + n);
    } else if (tid == sp_canonical::Quote::kTemplateId) {
        in.quote.assign(m, m + n);
    } else if (tid == sp_canonical::Trade::kTemplateId) {
        in.trade.assign(m, m + n);
    } else if (tid == sp_canonical::Status::kTemplateId) {
        in.status.assign(m, m + n);
    }
}

void SnapCore::on_state(const uint8_t* buf, size_t len) noexcept {
    size_t at = 0;
    while (at + 4 <= len) {
        const uint32_t n = u32(buf, at);
        at += 4;
        if (n > len - at) { ++stats_.malformed; return; }
        sp_proto::SourceStateDecoder d;
        if (d.wrap(buf + at, n)) {
            ++stats_.state_messages;
            // INTENT: a new epoch is a gateway restart. If the data stream already
            // showed it (Sequence back to 1) the new data is applied: forgetting again
            // would drop the new books and the Ok that follows them.
            if (epoch_ != 0 && d.SourceEpoch() != epoch_) {
                if (!restarted_) { ++stats_.restarts; forget_all(); }
                restarted_ = false;
            }
            epoch_ = d.SourceEpoch();
            if (d.Instrument() != 0)
                pending_.push_back({key(d.Instrument(), d.Venue()), d.Sequence(), d.State()});
        }
        at += n;
    }
    apply_due_states();
}

void SnapCore::apply_due_states() noexcept {
    // INTENT: a state speaks about the data up to its Sequence. Applying an Ok before
    // this copy has seen that data would serve a half-built book.
    size_t kept = 0;
    for (size_t i = 0; i < pending_.size(); ++i) {
        const PendingState& p = pending_[i];
        if (p.sequence > last_seq_) { pending_[kept++] = p; continue; }
        auto it = insts_.find(p.key);
        Inst& in = it != insts_.end() ? it->second : inst(static_cast<uint32_t>(p.key), static_cast<uint16_t>(p.key >> 32));
        // INTENT: the gateway's Ok means "the stream holds this book whole up to Sequence".
        // It holds for this copy only if it saw the whole of it: a Clear after its last
        // gap, and no overflow since. Otherwise it stays Stale and next_ask asks again.
        if (p.state == SourceStateCode::Ok && in.has_book && (in.whole_at == 0 || in.whole_at < gap_at_)) { ++stats_.states_ignored; continue; }
        set_state(in, p.state);
        ++stats_.states_applied;
    }
    pending_.resize(kept);
}

void SnapCore::on_data_lost() noexcept { stale_all(); }

void SnapCore::set_state(Inst& in, SourceStateCode s) noexcept {
    in.state = s;
    count_states();
}

void SnapCore::forget_all() noexcept {
    for (auto& [k, in] : insts_) { in.state = SourceStateCode::Unknown; in.book.clear(); in.full_depth = false; }
    for (auto& [k, in] : insts_) in.whole_at = 0;
    pending_.clear();
    last_seq_ = 0;
    stats_.last_sequence.set(0);
    gap_at_ = 0;
    ask_all_ = true;
    count_states();
}

void SnapCore::stale_all() noexcept {
    for (auto& [k, in] : insts_) if (in.state == SourceStateCode::Ok) in.state = SourceStateCode::Stale;
    count_states();
}

void SnapCore::count_states() noexcept {
    uint64_t ok = 0, stale = 0, unknown = 0;
    for (const auto& [k, in] : insts_) {
        if (in.state == SourceStateCode::Ok) ++ok;
        else if (in.state == SourceStateCode::Stale) ++stale;
        else ++unknown;
    }
    stats_.instruments_ok.set(ok); stats_.instruments_stale.set(stale); stats_.instruments_unknown.set(unknown);
}

bool SnapCore::next_ask(uint64_t now, uint64_t every_ns, Ask& out) noexcept {
    if (ask_all_) { ask_all_ = false; out = Ask{true, 0, 0}; ++stats_.republish_asks; return true; }
    for (auto& [k, in] : insts_) {
        if (in.state == SourceStateCode::Ok || (in.last_ask && now - in.last_ask < every_ns)) continue;
        if (!in.last_ask) { in.last_ask = now; continue; }   // the first wait: the gateway may be resyncing on its own
        in.last_ask = now;
        out = Ask{false, in.id, in.venue};
        ++stats_.republish_asks;
        return true;
    }
    return false;
}

sp_proto::SourceStateCode SnapCore::state(uint64_t instrument, uint16_t venue) const noexcept {
    const Inst* in = find(instrument, venue);
    return in ? in->state : SourceStateCode::Unknown;
}

const Book* SnapCore::book(uint64_t instrument, uint16_t venue) const noexcept {
    const Inst* in = find(instrument, venue);
    return in && in->has_book ? &in->book : nullptr;
}

// INTENT: the records are canonical messages, exactly what the gateway would
// publish to rebuild the instrument, so an app applies a recap with the same
// code as the live stream: definition, Clear, every level as Add, then the
// last quote, trade and status. Each record's Sequence is S, the live sequence
// the state is consistent with; the agent merges live messages after S.
void SnapCore::answer(uint64_t correlation, uint64_t instrument, uint16_t venue, std::vector<uint8_t>& out) {
    ++stats_.requests;
    const Inst* in = find(instrument, venue);
    if (!in || in->state != SourceStateCode::Ok) {
        const auto reason = in ? sp_proto::RejectReason::NotReady : sp_proto::RejectReason::UnknownInstrument;
        if (in) ++stats_.rejects_not_ready; else ++stats_.rejects_unknown;
        append<sp_proto::RecapRejectEncoder>(out, 64, [&](sp_proto::RecapRejectEncoder& e) { e.CorrelationId(correlation).Reason(reason); });
        return;
    }
    const uint64_t s = last_seq_;
    const size_t begin = out.size();
    append<sp_proto::RecapBeginEncoder>(out, 64, [&](sp_proto::RecapBeginEncoder& e) { e.CorrelationId(correlation).AsOf(s).SourceEpoch(epoch_); });
    uint32_t count = 0;
    auto record = [&](const uint8_t* msg, size_t n) {
        append<sp_proto::RecapRecordEncoder>(out, 8 + 8 + 4 + n, [&](sp_proto::RecapRecordEncoder& e) { e.CorrelationId(correlation).Payload(msg, n); });
        ++count;
    };
    auto stored = [&](const std::vector<uint8_t>& m) {
        if (m.empty()) return;
        uint8_t buf[512];
        const size_t n = std::min(m.size(), sizeof buf);
        std::memcpy(buf, m.data(), n);
        std::memcpy(buf + kPrefixSequence, &s, 8);
        // INTENT: every record of the reply says the instrument as of S, so an
        // agent orders the snapshot and the live messages after it by InstrumentSeq.
        std::memcpy(buf + kPrefixInstrumentSeq, &in->iseq, 8);
        record(buf, n);
    };
    stored(in->definition);
    if (in->has_book && in->full_depth) {
        const CanonicalMessage* bu = find_message("BookUpdate");
        const uint16_t o_px = find_field(*bu, "Price")->offset, o_qty = find_field(*bu, "Qty")->offset;
        const uint16_t o_side = find_field(*bu, "Side")->offset, o_act = find_field(*bu, "Action")->offset, o_lvl = find_field(*bu, "Level")->offset;
        uint8_t buf[sp_canonical::BookUpdate::kSize];
        auto level = [&](Side side, Action action, uint8_t lvl, int64_t px, int64_t qty) {
            begin_message(buf, *bu, in->id, in->venue, s, 0);
            put_u64(buf, kPrefixInstrumentSeq, in->iseq);
            put_i64(buf, o_px, px); put_i64(buf, o_qty, qty);
            put(buf, o_side, static_cast<uint8_t>(side)); put(buf, o_act, static_cast<uint8_t>(action)); put(buf, o_lvl, lvl);
            record(buf, sizeof buf);
        };
        level(Side::Unknown, Action::Clear, 255, 0, 0);
        for (Side side : {Side::Buy, Side::Sell}) {
            uint32_t i = 0;
            in->book.for_each(side, [&](int64_t px, int64_t qty) { level(side, Action::Add, i < 10 ? static_cast<uint8_t>(i) : 255, px, qty); ++i; });
        }
    }
    // A full-depth book is stated by Clear + levels; its last BookSnapshot may be older than S.
    if (!in->full_depth) stored(in->snapshot);
    stored(in->quote);
    stored(in->trade);
    stored(in->status);
    append<sp_proto::RecapEndEncoder>(out, 64, [&](sp_proto::RecapEndEncoder& e) { e.CorrelationId(correlation).RecordCount(count); });
    // RecordCount up front too, now that it is known (RecapBegin.RecordCount, root offset 24).
    std::memcpy(out.data() + begin + 4 + sp_proto::kHeaderSize + 24, &count, 4);
    ++stats_.replies;
}

} // namespace feeds
