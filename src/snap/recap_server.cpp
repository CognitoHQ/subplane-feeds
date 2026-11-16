#include "snap/recap_server.h"

#include <cstring>

namespace feeds {
namespace {

constexpr uint16_t kProtoVersion = 1;
constexpr uint16_t kRecapRequestBulk = 1u << 0;   // RecapRequest.Flags bit 0: every instrument
constexpr uint16_t kResolveReplyLast = 1u << 0;   // ResolveReply.Flags bit 0: the final reply
// INTENT: instruments per ResolveReply. 128 x 48 bytes plus headers is about
// 6 KiB, under Aeron's largest message on the 64 KiB response term (term/8).
constexpr uint16_t kResolvePage = 128;
constexpr size_t kResolvePageBytes = 8192;

template <class Enc, class F> void append(std::vector<uint8_t>& out, F fill) noexcept {
    uint8_t buf[128];
    Enc e(buf, sizeof buf);
    fill(e);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    if (n == 0) return;
    const size_t at = out.size();
    out.resize(at + 4 + n);
    std::memcpy(out.data() + at, &n, 4);
    std::memcpy(out.data() + at + 4, buf, n);
}

void status(uint64_t correlation, sp_proto::StatusCode code, std::vector<uint8_t>& reply) noexcept {
    append<sp_proto::StatusEncoder>(reply, [&](sp_proto::StatusEncoder& e) { e.CorrelationId(correlation).Code(code); });
}

void reject_unknown(uint64_t correlation, std::vector<uint8_t>& reply) noexcept {
    append<sp_proto::RecapRejectEncoder>(reply, [&](sp_proto::RecapRejectEncoder& e) {
        e.CorrelationId(correlation).Reason(sp_proto::RejectReason::UnknownInstrument);
    });
}

} // namespace

void RecapServer::set_directory(Directory d) {
    dir_ = d;
    by_id_.clear();
    if (dir_.list == nullptr) return;
    const auto& all = dir_.list->all();
    for (size_t i = 0; i < all.size(); ++i) by_id_.emplace(all[i].id, i);
}

const Instrument* RecapServer::by_id(uint64_t id) const noexcept {
    const auto it = by_id_.find(id);
    return it == by_id_.end() ? nullptr : &dir_.list->all()[it->second];
}

// One instrument (by id or by name) or the whole topic, in pages; an unknown
// one is refused as recap refuses it.
void RecapServer::resolve(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept {
    sp_proto::ResolveDecoder d;
    if (!d.wrap(msg, len)) { ++stats_.malformed; return; }
    ++stats_.resolves;
    std::vector<const Instrument*> pick;
    if (d.Venue() != 0 && d.Venue() != dir_.venue) {
        // another venue's instrument: not on this topic
    } else if (d.Instrument() != 0) {
        if (const Instrument* in = by_id(d.Instrument())) pick.push_back(in);
    } else if (!d.Name().empty()) {
        if (const Instrument* in = dir_.list->by_symbol(d.Name())) pick.push_back(in);
    } else {
        for (const Instrument& in : dir_.list->all()) pick.push_back(&in);
    }
    if (pick.empty() && (d.Instrument() != 0 || !d.Name().empty() || d.Venue() != 0)) { reject_unknown(d.CorrelationId(), reply); return; }
    size_t at = 0;
    do {
        const auto n = static_cast<uint16_t>(pick.size() - at < kResolvePage ? pick.size() - at : kResolvePage);
        uint8_t buf[kResolvePageBytes];
        sp_proto::ResolveReplyEncoder e(buf, sizeof buf);
        e.CorrelationId(d.CorrelationId()).TopicId(dir_.topic_id).Partitions(dir_.partitions)
         .Flags(at + n == pick.size() ? kResolveReplyLast : 0);
        e.begin_Instruments(n);
        for (uint16_t i = 0; i < n; ++i) {
            const Instrument& in = *pick[at + i];
            e.Instruments(i).Instrument(in.id).Venue(dir_.venue).Partition(in.partition).Symbol(in.symbol);
        }
        const auto bytes = static_cast<uint32_t>(e.finish());
        if (bytes == 0) { ++stats_.malformed; return; }   // cannot happen: a page fits the buffer
        const size_t pos = reply.size();
        reply.resize(pos + 4 + bytes);
        std::memcpy(reply.data() + pos, &bytes, 4);
        std::memcpy(reply.data() + pos + 4, buf, bytes);
        at += n;
    } while (at < pick.size());
}

void RecapServer::on_frame(const uint8_t* buf, size_t len, std::vector<uint8_t>& reply) noexcept {
    ++stats_.frames;
    size_t at = 0;
    while (at + 4 <= len) {
        uint32_t n = 0;
        std::memcpy(&n, buf + at, 4);
        at += 4;
        if (n > len - at) { ++stats_.malformed; return; }
        on_message(buf + at, n, reply);
        at += n;
    }
    if (at != len) ++stats_.malformed;
}

// The same evolution rules as the gateway's control input (control.cpp).
void RecapServer::on_message(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept {
    sp_proto::Header h;
    if (!sp_proto::read_header(msg, len, h) || h.schema_id != sp_proto::kSchemaId || sp_proto::kHeaderSize + h.block_length > len) {
        ++stats_.malformed;
        return;
    }
    uint64_t correlation = 0;
    if (h.block_length >= 8) std::memcpy(&correlation, msg + sp_proto::kHeaderSize, 8);
    switch (h.template_id) {
    case sp_proto::HelloDecoder::kTemplateId: {
        sp_proto::HelloDecoder d;
        if (!d.wrap(msg, len)) { ++stats_.malformed; return; }
        ++stats_.hellos;
        if (d.MinProtoVersion() > kProtoVersion) { ++stats_.unsupported; status(correlation, sp_proto::StatusCode::Unsupported, reply); return; }
        append<sp_proto::HelloEncoder>(reply, [&](sp_proto::HelloEncoder& e) {
            e.CorrelationId(correlation).PeerEpoch(epoch_).ProtoVersion(kProtoVersion).MinProtoVersion(kProtoVersion)
                .Role(sp_proto::Role::Snap).ServiceName(name_);
        });
        return;
    }
    case sp_proto::RecapRequestDecoder::kTemplateId: {
        sp_proto::RecapRequestDecoder d;
        if (!d.wrap(msg, len)) { ++stats_.malformed; return; }
        if (d.Flags() & kRecapRequestBulk) { core_.want_all(now_); return; }
        if (dir_.list != nullptr) {
            // INTENT: the file says which partition publishes each instrument, so
            // one this partition will never carry is refused now, not held as
            // interest that waits for data that cannot come.
            const Instrument* in = by_id(d.Instrument());
            if (in == nullptr || in->partition != dir_.partition) { ++stats_.not_here; reject_unknown(d.CorrelationId(), reply); return; }
        }
        if (core_.want(d.Instrument(), d.Venue(), now_) == SnapCore::Want::UNKNOWN) reject_unknown(d.CorrelationId(), reply);
        return;
    }
    case sp_proto::ResolveDecoder::kTemplateId:
        if (dir_.list == nullptr) break;   // Unsupported
        resolve(msg, len, reply);
        return;
    case sp_proto::StatusDecoder::kTemplateId:
        ++stats_.status;
        return;
    case sp_proto::ExtensionDecoder::kTemplateId: {
        sp_proto::ExtensionDecoder d;
        if (!d.wrap(msg, len)) { ++stats_.malformed; return; }
        if ((d.Flags() & 1u) == 0) return;
        break;
    }
    default:
        break;
    }
    ++stats_.unsupported;
    if (correlation != 0) status(correlation, sp_proto::StatusCode::Unsupported, reply);
}

} // namespace feeds
