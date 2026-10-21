#include "aeron_io.h"

#include <chrono>
#include <sched.h>

namespace feeds {
namespace {

bool fail(const char* what, std::string& err) { err = std::string("aeron ") + what + ": " + aeron_errmsg(); return false; }

template <class T, class A, class P> bool await(T*& out, A* async, P poll, uint64_t timeout_ms, const char* what, std::string& err) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!out) {
        if (poll(&out, async) < 0) return fail(what, err);
        if (std::chrono::steady_clock::now() > deadline) { err = std::string("aeron: timed out adding the ") + what; return false; }
        sched_yield();
    }
    return true;
}

} // namespace

std::string aeron_resolved_dir(const std::string& configured) {
    if (!configured.empty()) return configured;
    char buf[512] = {};
    return aeron_default_path(buf, sizeof buf) >= 0 ? std::string(buf) : std::string();
}

bool AeronClient::open(const Config& cfg, std::string& err) {
    cfg_ = cfg;
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
    aeron_context_set_client_name(ctx_, cfg_.client_name.c_str());
    if (aeron_init(&client_, ctx_) < 0) return fail("init (is the media driver running?)", err);
    if (aeron_start(client_) < 0) return fail("start", err);
    return true;
}

void AeronClient::close() noexcept {
    if (client_) { aeron_close(client_); client_ = nullptr; }
    if (ctx_) { aeron_context_close(ctx_); ctx_ = nullptr; }
    if (driver_) { aeron_driver_close(driver_); driver_ = nullptr; }
    if (dctx_) { aeron_driver_context_close(dctx_); dctx_ = nullptr; }
}

bool AeronSubscriber::open(aeron_t* client, std::string channel, int32_t stream_id, uint64_t timeout_ms, std::string& err) {
    channel_ = std::move(channel);
    stream_id_ = stream_id;
    if (aeron_fragment_assembler_create(&assembler_, &AeronSubscriber::on_fragment, this) < 0) return fail("fragment assembler", err);
    aeron_async_add_subscription_t* async = nullptr;
    if (aeron_async_add_subscription(&async, client, channel_.c_str(), stream_id_, nullptr, nullptr, &AeronSubscriber::on_unavailable, this) < 0)
        return fail("add subscription", err);
    return await(sub_, async, aeron_async_add_subscription_poll, timeout_ms, "subscription", err);
}

int AeronSubscriber::poll(FrameHandler h, void* ctx, int limit) noexcept {
    if (!sub_) return 0;
    h_ = h; ctx_ = ctx;
    const int n = aeron_subscription_poll(sub_, aeron_fragment_assembler_handler, assembler_, static_cast<size_t>(limit));
    return n < 0 ? 0 : n;
}

void AeronSubscriber::on_fragment(void* clientd, const uint8_t* buf, size_t len, aeron_header_t*) {
    auto* self = static_cast<AeronSubscriber*>(clientd);
    if (self->h_) self->h_(self->ctx_, buf, len);
}

void AeronSubscriber::on_unavailable(void* clientd, aeron_subscription_t*, aeron_image_t*) {
    static_cast<AeronSubscriber*>(clientd)->lost_.fetch_add(1, std::memory_order_acq_rel);
}

void AeronSubscriber::close() noexcept {
    if (sub_) { aeron_subscription_close(sub_, nullptr, nullptr); sub_ = nullptr; }
    if (assembler_) { aeron_fragment_assembler_delete(assembler_); assembler_ = nullptr; }
}

bool AeronRequester::open(aeron_t* client, const Config& cfg, std::string& err) {
    cfg_ = cfg;
    if (cfg_.request_channel.find("aeron:udp") != 0) { err = "the request channel must be aeron:udp (Aeron response channels need UDP)"; return false; }
    if (cfg_.response_control.empty()) { err = "the response control address (host:port) is required"; return false; }
    if (!replies_.open(client, "aeron:udp?control-mode=response|control=" + cfg_.response_control, cfg_.response_stream_id, cfg_.timeout_ms, err)) return false;
    aeron_subscription_constants_t sc{};
    if (aeron_subscription_constants(replies_.handle(), &sc) < 0) return fail("subscription constants", err);
    const std::string ch = cfg_.request_channel + "|response-correlation-id=" + std::to_string(sc.registration_id);
    aeron_async_add_exclusive_publication_t* async = nullptr;
    if (aeron_async_add_exclusive_publication(&async, client, ch.c_str(), cfg_.request_stream_id) < 0) return fail("add publication", err);
    return await(pub_, async, aeron_async_add_exclusive_publication_poll, cfg_.timeout_ms, "publication", err);
}

bool AeronRequester::offer(const uint8_t* buf, size_t len) noexcept {
    return pub_ && aeron_exclusive_publication_offer(pub_, buf, len, nullptr, nullptr) >= 0;
}

void AeronRequester::close() noexcept {
    if (pub_) { aeron_exclusive_publication_close(pub_, nullptr, nullptr); pub_ = nullptr; }
    replies_.close();
}

} // namespace feeds
