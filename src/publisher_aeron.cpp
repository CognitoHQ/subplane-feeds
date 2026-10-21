#include "publisher.h"

#include "aeron_io.h"

#include "obs/endpoint_key.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <cstring>
#include <map>
#include <memory>
#include <sched.h>
#include <string>

namespace feeds {
namespace {

// INTENT: one Aeron client per process; the state stream is a second exclusive
// publication on it, owned and closed by the data publisher below.
std::string resolved_dir(const std::string& configured) { return aeron_resolved_dir(configured); }

class AeronStatePublication final : public Publisher {
public:
    AeronStatePublication(aeron_exclusive_publication_t* pub, const AeronPublisherConfig& cfg)
        : pub_(pub), cfg_(cfg), what_("aeron " + cfg.channel + " stream " + std::to_string(cfg.state_stream_id)) {}
    Offer publish(const uint8_t* msg, size_t len) noexcept override {
        // State changes are rare and small: one try, then the caller counts a drop;
        // a subscriber that missed one asks for Republish.
        const int64_t r = aeron_exclusive_publication_offer(pub_, msg, len, nullptr, nullptr);
        if (r >= 0) return Offer::OK;
        if (r == AERON_PUBLICATION_NOT_CONNECTED) return Offer::NOT_CONNECTED;
        if (r == AERON_PUBLICATION_BACK_PRESSURED || r == AERON_PUBLICATION_ADMIN_ACTION) return Offer::BACK_PRESSURED;
        return Offer::FAILED;
    }
    bool connected() const noexcept override { return pub_ && aeron_exclusive_publication_is_connected(pub_); }
    std::string describe() const override { return what_; }
    std::string endpoint_key(std::string_view host) const override { return aeron_key(cfg_.channel, cfg_.state_stream_id, resolved_dir(cfg_.aeron_dir), host); }
    aeron_exclusive_publication_t* pub_;
    AeronPublisherConfig cfg_;
    std::string what_;
};

class AeronPublisher final : public Publisher {
public:
    explicit AeronPublisher(AeronPublisherConfig cfg) : cfg_(std::move(cfg)) {}
    ~AeronPublisher() override { close(); }

    bool open(std::string& err) {
        AeronClient::Config cc;
        cc.aeron_dir = cfg_.aeron_dir; cc.embedded_driver = cfg_.embedded_driver; cc.driver_timeout_ms = cfg_.driver_timeout_ms; cc.client_name = "spwsgw";
        if (!client_.open(cc, err)) return false;
        if (!add_publication(cfg_.stream_id, pub_, err)) return false;
        if (cfg_.state_stream_id) {
            aeron_exclusive_publication_t* sp = nullptr;
            if (!add_publication(cfg_.state_stream_id, sp, err)) return false;
            state_ = std::make_unique<AeronStatePublication>(sp, cfg_);
        }
        return true;
    }

    Publisher* state_publisher() const noexcept override { return state_.get(); }
    aeron_t* client() const noexcept { return client_.get(); }
    std::string dir() const { return resolved_dir(cfg_.aeron_dir); }

    // INTENT: a gateway never blocks on a slow or absent subscriber. Back pressure
    // and admin actions get a short spin; NOT_CONNECTED (nobody listening yet) and
    // a spin that runs out are drops, counted by the caller, never a stall.
    Offer publish(const uint8_t* msg, size_t len) noexcept override {
        const auto start = std::chrono::steady_clock::now();
        for (;;) {
            const int64_t r = aeron_exclusive_publication_offer(pub_, msg, len, nullptr, nullptr);
            if (r >= 0) return Offer::OK;
            if (r == AERON_PUBLICATION_NOT_CONNECTED) return Offer::NOT_CONNECTED;
            if (r != AERON_PUBLICATION_BACK_PRESSURED && r != AERON_PUBLICATION_ADMIN_ACTION) return Offer::FAILED;
            if (std::chrono::steady_clock::now() - start > std::chrono::microseconds(cfg_.offer_retry_us)) return Offer::BACK_PRESSURED;
            sched_yield();
        }
    }

    // INTENT: is_connected reads the publication's log metadata in shared memory
    // with an acquire load, so the HTTP thread may call it while the gateway
    // thread offers. The publication must stay open meanwhile: main stops the
    // HTTP server before close().
    bool connected() const noexcept override { return pub_ && aeron_exclusive_publication_is_connected(pub_); }

    // The same driver directory the agent resolves (its aeron_util::resolved_dir),
    // so this key equals the agent's upstream key for the same stream.
    std::string endpoint_key(std::string_view host) const override { return aeron_key(cfg_.channel, cfg_.stream_id, resolved_dir(cfg_.aeron_dir), host); }

    std::string describe() const override {
        return "aeron " + cfg_.channel + " stream " + std::to_string(cfg_.stream_id) + (cfg_.embedded_driver ? " (embedded driver)" : "");
    }

    void close() noexcept override {
        if (state_) { aeron_exclusive_publication_close(state_->pub_, nullptr, nullptr); state_.reset(); }
        if (pub_) { aeron_exclusive_publication_close(pub_, nullptr, nullptr); pub_ = nullptr; }
        client_.close();
    }

private:
    bool add_publication(int32_t stream_id, aeron_exclusive_publication_t*& out, std::string& err) {
        aeron_async_add_exclusive_publication_t* async = nullptr;
        if (aeron_async_add_exclusive_publication(&async, client_.get(), cfg_.channel.c_str(), stream_id) < 0) return fail("add publication", err);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.driver_timeout_ms);
        while (!out) {
            if (aeron_async_add_exclusive_publication_poll(&out, async) < 0) return fail("publication", err);
            if (std::chrono::steady_clock::now() > deadline) { err = "aeron: timed out adding the publication"; return false; }
            sched_yield();
        }
        return true;
    }
    static bool fail(const char* what, std::string& err) { err = std::string("aeron ") + what + ": " + aeron_errmsg(); return false; }

    AeronPublisherConfig cfg_;
    AeronClient client_;
    aeron_exclusive_publication_t* pub_ = nullptr;
    std::unique_ptr<AeronStatePublication> state_;
};

// INTENT: polled from the gateway's timer, on the gateway thread, so the handler
// may call into the Gateway without locks. Images are polled one by one (not the
// subscription) so each fragment's requester is known and its replies go to that
// requester's own response publication; nothing here runs on the data path.
class AeronControl final : public ControlInput {
public:
    AeronControl(aeron_t* client, AeronControlConfig cfg, std::string dir) : client_(client), cfg_(std::move(cfg)), dir_(std::move(dir)) {}
    ~AeronControl() override { close(); }

    bool open(std::string& err) {
        if (cfg_.channel.find("aeron:udp") != 0) { err = "--control-channel must be aeron:udp (Aeron response channels need UDP)"; return false; }
        if (cfg_.response_control.empty()) { err = "--control-response (host:port) is required with --control-channel"; return false; }
        aeron_async_add_subscription_t* async = nullptr;
        if (aeron_async_add_subscription(&async, client_, cfg_.channel.c_str(), cfg_.stream_id, nullptr, nullptr, nullptr, nullptr) < 0) {
            err = std::string("aeron control subscription: ") + aeron_errmsg(); return false;
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(cfg_.timeout_ms);
        while (!sub_) {
            if (aeron_async_add_subscription_poll(&sub_, async) < 0) { err = std::string("aeron control subscription: ") + aeron_errmsg(); return false; }
            if (std::chrono::steady_clock::now() > deadline) { err = "aeron: timed out adding the control subscription"; return false; }
            sched_yield();
        }
        return true;
    }

    int poll(SppHandler& h, int limit) noexcept override {
        if (!sub_) return 0;
        h_ = &h; limit_ = limit; fragments_ = 0; ++generation_;
        aeron_subscription_for_each_image(sub_, &AeronControl::on_image, this);
        for (auto it = peers_.begin(); it != peers_.end();) {
            Peer& p = *it->second;
            if (p.generation != generation_) { drop(p); it = peers_.erase(it); continue; }   // the requester went away
            send(p);
            ++it;
        }
        h.stats().peers.set(peers_.size());
        h_ = nullptr;
        return fragments_;
    }

    std::string describe() const override { return "aeron control " + cfg_.channel + " stream " + std::to_string(cfg_.stream_id); }
    std::string endpoint_key(std::string_view host) const override { return aeron_key(cfg_.channel, cfg_.stream_id, dir_, host); }

    void close() noexcept override {
        for (auto& [id, p] : peers_) drop(*p);
        peers_.clear();
        if (sub_) { aeron_subscription_close(sub_, nullptr, nullptr); sub_ = nullptr; }
    }

private:
    struct Peer {
        AeronControl* owner = nullptr;
        int64_t correlation = 0;            // the image's: the response publication's response-correlation-id
        uint64_t generation = 0;
        aeron_image_fragment_assembler_t* assembler = nullptr;
        aeron_async_add_exclusive_publication_t* async = nullptr;
        aeron_exclusive_publication_t* pub = nullptr;
        std::vector<uint8_t> pending;       // replies not yet offered (the publication connects after the first request)
        size_t sent = 0;                    // bytes of `pending` already offered
    };

    static void on_image(aeron_image_t* image, void* clientd) {
        auto* self = static_cast<AeronControl*>(clientd);
        aeron_image_constants_t c{};
        if (aeron_image_constants(image, &c) < 0) return;
        auto& slot = self->peers_[c.correlation_id];
        if (!slot) {
            slot = std::make_unique<Peer>();
            slot->owner = self;
            slot->correlation = c.correlation_id;
            aeron_image_fragment_assembler_create(&slot->assembler, &AeronControl::on_fragment, slot.get());
        }
        slot->generation = self->generation_;
        if (self->fragments_ < self->limit_ && slot->assembler) {
            const int n = aeron_image_poll(image, aeron_image_fragment_assembler_handler, slot->assembler, static_cast<size_t>(self->limit_ - self->fragments_));
            if (n > 0) self->fragments_ += n;
        }
    }

    static void on_fragment(void* clientd, const uint8_t* buf, size_t len, aeron_header_t*) {
        auto* p = static_cast<Peer*>(clientd);
        SppHandler& h = *p->owner->h_;
        const size_t before = p->pending.size();
        h.on_frame(buf, len, p->pending);
        if (p->pending.size() - p->sent > p->owner->cfg_.max_pending) { p->pending.resize(before); ++h.stats().reply_dropped; }
    }

    void send(Peer& p) noexcept {
        if (!p.pub && !p.async) {
            const std::string ch = "aeron:udp?control-mode=response|control=" + cfg_.response_control + "|term-length=" + std::to_string(cfg_.term_length) +
                                   "|response-correlation-id=" + std::to_string(p.correlation);
            if (aeron_async_add_exclusive_publication(&p.async, client_, ch.c_str(), cfg_.response_stream_id) < 0) p.async = nullptr;
        }
        if (p.async) {
            const int rc = aeron_async_add_exclusive_publication_poll(&p.pub, p.async);
            if (rc != 0) p.async = nullptr;   // done, or failed (retried on the next poll)
        }
        while (p.pub && p.sent < p.pending.size()) {
            // Whole messages up to max_offer (a single larger message goes alone).
            size_t end = p.sent;
            while (end + 4 <= p.pending.size()) {
                uint32_t n = 0;
                std::memcpy(&n, p.pending.data() + end, 4);
                if (end > p.sent && end + 4 + n - p.sent > cfg_.max_offer) break;
                end += 4 + n;
            }
            const int64_t r = aeron_exclusive_publication_offer(p.pub, p.pending.data() + p.sent, end - p.sent, nullptr, nullptr);
            if (r >= 0) { ++h_->stats().replies; p.sent = end; continue; }
            if (r == AERON_PUBLICATION_NOT_CONNECTED || r == AERON_PUBLICATION_BACK_PRESSURED || r == AERON_PUBLICATION_ADMIN_ACTION) return;   // next poll
            ++h_->stats().reply_dropped;
            p.sent = p.pending.size();
        }
        if (p.sent == p.pending.size()) { p.pending.clear(); p.sent = 0; }
    }

    void drop(Peer& p) noexcept {
        if (p.pub) aeron_exclusive_publication_close(p.pub, nullptr, nullptr);
        if (p.assembler) aeron_image_fragment_assembler_delete(p.assembler);
        p.pub = nullptr; p.assembler = nullptr;
    }

    aeron_t* client_;
    AeronControlConfig cfg_;
    std::string dir_;
    aeron_subscription_t* sub_ = nullptr;
    std::map<int64_t, std::unique_ptr<Peer>> peers_;
    SppHandler* h_ = nullptr;
    int limit_ = 0, fragments_ = 0;
    uint64_t generation_ = 0;
};

} // namespace

std::unique_ptr<ControlInput> make_aeron_control(aeron_t* client, const std::string& dir, const AeronControlConfig& cfg, std::string& err) {
    auto c = std::make_unique<AeronControl>(client, cfg, dir);
    if (!c->open(err)) return nullptr;
    return c;
}

std::unique_ptr<ControlInput> make_aeron_control(Publisher& aeron_pub, const AeronControlConfig& cfg, std::string& err) {
    auto* p = dynamic_cast<AeronPublisher*>(&aeron_pub);
    if (!p) { err = "the control input needs the Aeron output (--out aeron)"; return nullptr; }
    return make_aeron_control(p->client(), p->dir(), cfg, err);
}

std::unique_ptr<Publisher> make_aeron_publisher(const AeronPublisherConfig& cfg, std::string& err) {
    auto p = std::make_unique<AeronPublisher>(cfg);
    if (!p->open(err)) return nullptr;
    return p;
}

} // namespace feeds
