#include "snap/recap_server.h"

#include <cstring>

namespace feeds {
namespace {

constexpr uint16_t kProtoVersion = 1;
constexpr uint16_t kRecapRequestBulk = 1u << 0;   // RecapRequest.Flags bit 0: every instrument

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

} // namespace

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
        if (core_.want(d.Instrument(), d.Venue(), now_) == SnapCore::Want::UNKNOWN) {
            append<sp_proto::RecapRejectEncoder>(reply, [&](sp_proto::RecapRejectEncoder& e) {
                e.CorrelationId(d.CorrelationId()).Reason(sp_proto::RejectReason::UnknownInstrument);
            });
        }
        return;
    }
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
