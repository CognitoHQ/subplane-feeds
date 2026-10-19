#include "control.h"

#include "gateway.h"
#include "subplane_proto_codec.h"

#include <cstring>

namespace feeds {
namespace {

constexpr uint16_t kProtoVersion = 1;   // the SPP version this gateway speaks, highest and lowest

// Appends one length-prefixed message encoded by `fill` into a scratch buffer.
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

} // namespace

void ControlHandler::on_frame(const uint8_t* buf, size_t len, std::vector<uint8_t>& reply) noexcept {
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

void ControlHandler::hello(uint64_t correlation, std::vector<uint8_t>& reply) noexcept {
    append<sp_proto::HelloEncoder>(reply, [&](sp_proto::HelloEncoder& e) {
        e.CorrelationId(correlation).PeerEpoch(epoch_).ProtoVersion(kProtoVersion).MinProtoVersion(kProtoVersion)
            .Role(sp_proto::Role::Gateway).ServiceName(name_);
    });
}

void ControlHandler::status(uint64_t correlation, uint16_t code, std::vector<uint8_t>& reply) noexcept {
    append<sp_proto::StatusEncoder>(reply, [&](sp_proto::StatusEncoder& e) {
        e.CorrelationId(correlation).Code(static_cast<sp_proto::StatusCode>(code));
    });
}

// INTENT: the evolution rules (subplane-protocol.md, "Evolution") are what let
// agents, snapshot services and gateways upgrade on their own schedules: newer
// versions and longer blocks are read as far as known, unknown core messages get
// Status{Unsupported} with their CorrelationId, and nothing unknown is fatal.
void ControlHandler::on_message(const uint8_t* msg, size_t len, std::vector<uint8_t>& reply) noexcept {
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
        if (d.MinProtoVersion() > kProtoVersion) { ++stats_.unsupported; status(correlation, static_cast<uint16_t>(sp_proto::StatusCode::Unsupported), reply); return; }
        hello(correlation, reply);
        return;
    }
    case sp_proto::RepublishDecoder::kTemplateId: {
        sp_proto::RepublishDecoder d;
        if (!d.wrap(msg, len)) { ++stats_.malformed; return; }
        ++stats_.republish;
        const RepublishResult r = gw_.republish(d.Instrument(), d.Venue(), d.CorrelationId());
        status(correlation, static_cast<uint16_t>(r == RepublishResult::QUEUED ? sp_proto::StatusCode::Ok : sp_proto::StatusCode::NotFound), reply);
        return;
    }
    case sp_proto::StatusDecoder::kTemplateId:
        ++stats_.status;   // a peer's answer to something we never ask: nothing to do
        return;
    case sp_proto::ExtensionDecoder::kTemplateId: {
        sp_proto::ExtensionDecoder d;
        if (!d.wrap(msg, len)) { ++stats_.malformed; return; }
        if ((d.Flags() & 1u) == 0) return;   // not must-understand: ignored
        break;
    }
    default:
        break;
    }
    ++stats_.unsupported;
    if (correlation != 0) status(correlation, static_cast<uint16_t>(sp_proto::StatusCode::Unsupported), reply);
}

} // namespace feeds
