#include "snap/gateway_link.h"

#include "subplane_proto_codec.h"

#include <cstring>

namespace feeds {
namespace {

constexpr uint16_t kProtoVersion = 1;

template <class Enc, class F> void frame_of(std::vector<uint8_t>& out, F fill) {
    uint8_t buf[256];
    Enc e(buf, sizeof buf);
    fill(e);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    out.resize(4 + n);
    std::memcpy(out.data(), &n, 4);
    std::memcpy(out.data() + 4, buf, n);
}

} // namespace

bool GatewayLink::next(SnapCore& core, uint64_t now, uint64_t ask_every_ns, std::vector<uint8_t>& frame) {
    if (pending_ == Kind::NONE) {
        if (!hello_sent_) pending_ = Kind::HELLO;
        else if (core.next_ask(now, ask_every_ns, ask_)) pending_ = Kind::REPUBLISH;
        else return false;
    }
    const uint64_t corr = next_correlation_;
    if (pending_ == Kind::HELLO) {
        frame_of<sp_proto::HelloEncoder>(frame, [&](sp_proto::HelloEncoder& e) {
            e.CorrelationId(corr).PeerEpoch(epoch_).ProtoVersion(kProtoVersion).MinProtoVersion(kProtoVersion).Role(sp_proto::Role::Snap).ServiceName(name_);
        });
    } else {
        // INTENT: Instrument 0 is "all": the gateway queues every instrument and drains
        // them at its own republish rate, so one ask after a start costs one message.
        frame_of<sp_proto::RepublishEncoder>(frame, [&](sp_proto::RepublishEncoder& e) {
            e.CorrelationId(corr).Instrument(ask_.all ? 0 : ask_.instrument).Venue(ask_.all ? 0 : ask_.venue);
        });
    }
    return true;
}

void GatewayLink::sent() noexcept {
    if (pending_ == Kind::HELLO) { hello_sent_ = true; ++stats_.hellos_sent; }
    else if (pending_ == Kind::REPUBLISH) ++stats_.republish_sent;
    pending_ = Kind::NONE;
    ++next_correlation_;
}

void GatewayLink::on_reply(const uint8_t* buf, size_t len) noexcept {
    size_t at = 0;
    while (at + 4 <= len) {
        uint32_t n = 0;
        std::memcpy(&n, buf + at, 4);
        at += 4;
        if (n > len - at) { ++stats_.malformed; return; }
        const uint8_t* m = buf + at;
        at += n;
        sp_proto::Header h;
        if (!sp_proto::read_header(m, n, h) || h.schema_id != sp_proto::kSchemaId) { ++stats_.malformed; continue; }
        if (h.template_id == sp_proto::HelloDecoder::kTemplateId) {
            sp_proto::HelloDecoder d;
            if (!d.wrap(m, n)) { ++stats_.malformed; continue; }
            ++stats_.hellos;
            gateway_epoch_ = d.PeerEpoch();
        } else if (h.template_id == sp_proto::StatusDecoder::kTemplateId) {
            sp_proto::StatusDecoder d;
            if (!d.wrap(m, n)) { ++stats_.malformed; continue; }
            if (d.Code() == sp_proto::StatusCode::Ok) ++stats_.status_ok;
            else if (d.Code() == sp_proto::StatusCode::NotFound) ++stats_.status_not_found;
            else ++stats_.status_other;
        }
        // Anything else from a gateway is newer than this snap: ignored (evolution rules).
    }
    if (at != len) ++stats_.malformed;
}

} // namespace feeds
