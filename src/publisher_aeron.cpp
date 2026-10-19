#include "publisher.h"

#include "obs/endpoint_key.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <chrono>
#include <memory>
#include <sched.h>
#include <string>

namespace feeds {
namespace {

// INTENT: one Aeron client per process; the state stream is a second exclusive
// publication on it, owned and closed by the data publisher below.
std::string resolved_dir(const std::string& configured) {
    if (!configured.empty()) return configured;
    char buf[512] = {};
    return aeron_default_path(buf, sizeof buf) >= 0 ? std::string(buf) : std::string();
}

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
        if (cfg_.embedded_driver) {
            if (aeron_driver_context_init(&dctx_) < 0) return fail("driver context", err);
            if (!cfg_.aeron_dir.empty() && aeron_driver_context_set_dir(dctx_, cfg_.aeron_dir.c_str()) < 0) return fail("driver dir", err);
            aeron_driver_context_set_threading_mode(dctx_, AERON_THREADING_MODE_SHARED);
            aeron_driver_context_set_dir_delete_on_start(dctx_, true);
            aeron_driver_context_set_dir_delete_on_shutdown(dctx_, true);
            if (aeron_driver_init(&driver_, dctx_) < 0) return fail("driver init", err);
            if (aeron_driver_start(driver_, false) < 0) return fail("driver start", err);
        }
        if (aeron_context_init(&ctx_) < 0) return fail("context", err);
        if (!cfg_.aeron_dir.empty() && aeron_context_set_dir(ctx_, cfg_.aeron_dir.c_str()) < 0) return fail("dir", err);
        aeron_context_set_driver_timeout_ms(ctx_, cfg_.driver_timeout_ms);
        aeron_context_set_client_name(ctx_, "spwsgw");
        if (aeron_init(&client_, ctx_) < 0) return fail("init (is the media driver running?)", err);
        if (aeron_start(client_) < 0) return fail("start", err);
        if (!add_publication(cfg_.stream_id, pub_, err)) return false;
        if (cfg_.state_stream_id) {
            aeron_exclusive_publication_t* sp = nullptr;
            if (!add_publication(cfg_.state_stream_id, sp, err)) return false;
            state_ = std::make_unique<AeronStatePublication>(sp, cfg_);
        }
        return true;
    }

    Publisher* state_publisher() const noexcept override { return state_.get(); }

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
        if (client_) { aeron_close(client_); client_ = nullptr; }
        if (ctx_) { aeron_context_close(ctx_); ctx_ = nullptr; }
        if (driver_) { aeron_driver_close(driver_); driver_ = nullptr; }
        if (dctx_) { aeron_driver_context_close(dctx_); dctx_ = nullptr; }
    }

private:
    bool add_publication(int32_t stream_id, aeron_exclusive_publication_t*& out, std::string& err) {
        aeron_async_add_exclusive_publication_t* async = nullptr;
        if (aeron_async_add_exclusive_publication(&async, client_, cfg_.channel.c_str(), stream_id) < 0) return fail("add publication", err);
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
    aeron_driver_context_t* dctx_ = nullptr;
    aeron_driver_t* driver_ = nullptr;
    aeron_context_t* ctx_ = nullptr;
    aeron_t* client_ = nullptr;
    aeron_exclusive_publication_t* pub_ = nullptr;
    std::unique_ptr<AeronStatePublication> state_;
};

} // namespace

std::unique_ptr<Publisher> make_aeron_publisher(const AeronPublisherConfig& cfg, std::string& err) {
    auto p = std::make_unique<AeronPublisher>(cfg);
    if (!p->open(err)) return nullptr;
    return p;
}

} // namespace feeds
