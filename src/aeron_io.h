#pragma once

// Aeron pieces shared by spwsgw and spwsgw-snap: the client (with an optional
// embedded media driver), a subscription that hands over whole frames, and the
// requester side of an SPP response channel. The gateway's publisher and
// control input (publisher_aeron.cpp) are built on the same client.

#include "control.h"

#include <aeronc.h>
#include <aeronmd.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace feeds {

// The driver directory as the agent resolves it (empty = Aeron's default), so
// endpoint keys match across processes.
std::string aeron_resolved_dir(const std::string& configured);

class AeronClient {
public:
    struct Config {
        std::string aeron_dir;           // empty = Aeron default
        bool        embedded_driver = false;
        uint64_t    driver_timeout_ms = 10000;
        std::string client_name = "spwsgw";
    };
    AeronClient() = default;
    AeronClient(const AeronClient&) = delete;
    AeronClient& operator=(const AeronClient&) = delete;
    ~AeronClient() { close(); }

    bool open(const Config& cfg, std::string& err);
    aeron_t* get() const noexcept { return client_; }
    std::string dir() const { return aeron_resolved_dir(cfg_.aeron_dir); }
    const Config& config() const noexcept { return cfg_; }
    void close() noexcept;   // after every publication and subscription on it is closed

private:
    Config cfg_;
    aeron_driver_context_t* dctx_ = nullptr;
    aeron_driver_t* driver_ = nullptr;
    aeron_context_t* ctx_ = nullptr;
    aeron_t* client_ = nullptr;
};

using FrameHandler = void (*)(void* ctx, const uint8_t* buf, size_t len);

// A subscription whose handler sees whole offers (fragments reassembled).
class AeronSubscriber {
public:
    AeronSubscriber() = default;
    AeronSubscriber(const AeronSubscriber&) = delete;
    AeronSubscriber& operator=(const AeronSubscriber&) = delete;
    ~AeronSubscriber() { close(); }

    bool open(aeron_t* client, std::string channel, int32_t stream_id, uint64_t timeout_ms, std::string& err);
    // Hands at most `limit` fragments' worth of frames to `h`. Never blocks.
    int poll(FrameHandler h, void* ctx, int limit) noexcept;
    size_t images() const noexcept { const int n = sub_ ? aeron_subscription_image_count(sub_) : 0; return n > 0 ? static_cast<size_t>(n) : 0; }
    // Images that went away (publisher gone or timed out). Bumped on Aeron's conductor thread.
    uint64_t images_lost() const noexcept { return lost_.load(std::memory_order_acquire); }
    const std::string& channel() const noexcept { return channel_; }
    int32_t stream_id() const noexcept { return stream_id_; }
    aeron_subscription_t* handle() const noexcept { return sub_; }
    void close() noexcept;

private:
    static void on_fragment(void* clientd, const uint8_t* buf, size_t len, aeron_header_t*);
    static void on_unavailable(void* clientd, aeron_subscription_t*, aeron_image_t*);

    std::string channel_;
    int32_t stream_id_ = 0;
    aeron_subscription_t* sub_ = nullptr;
    aeron_fragment_assembler_t* assembler_ = nullptr;
    FrameHandler h_ = nullptr;
    void* ctx_ = nullptr;
    std::atomic<uint64_t> lost_{0};
};

// The requester side of an SPP exchange over an Aeron response channel: a
// response subscription (control-mode=response) and a request publication that
// names it by response-correlation-id, so the peer's replies come back to this
// process alone. UDP only.
class AeronRequester {
public:
    struct Config {
        std::string request_channel;     // the peer's request channel, e.g. aeron:udp?endpoint=10.0.0.5:40600
        int32_t     request_stream_id = 1003;
        std::string response_control;    // host:port: the peer's --control-response
        int32_t     response_stream_id = 1004;
        uint64_t    timeout_ms = 10000;
    };
    AeronRequester() = default;
    AeronRequester(const AeronRequester&) = delete;
    AeronRequester& operator=(const AeronRequester&) = delete;
    ~AeronRequester() { close(); }

    bool open(aeron_t* client, const Config& cfg, std::string& err);
    bool connected() const noexcept { return pub_ && aeron_exclusive_publication_is_connected(pub_); }
    // One try; false when not connected or back pressured (the caller retries later).
    bool offer(const uint8_t* buf, size_t len) noexcept;
    int poll(FrameHandler h, void* ctx, int limit) noexcept { return replies_.poll(h, ctx, limit); }
    const Config& config() const noexcept { return cfg_; }
    void close() noexcept;

private:
    Config cfg_;
    AeronSubscriber replies_;
    aeron_exclusive_publication_t* pub_ = nullptr;
};

// A plain exclusive publication on an existing client: spwsgw-snap's section
// stream. One offer per call; the caller decides what to do when it is not taken.
class AeronPublication {
public:
    enum class Result : uint8_t { OK, BACK_PRESSURED, NOT_CONNECTED, FAILED };
    AeronPublication() = default;
    AeronPublication(const AeronPublication&) = delete;
    AeronPublication& operator=(const AeronPublication&) = delete;
    ~AeronPublication() { close(); }

    bool open(aeron_t* client, std::string channel, int32_t stream_id, uint64_t timeout_ms, std::string& err);
    Result offer(const uint8_t* buf, size_t len) noexcept;
    bool connected() const noexcept { return pub_ && aeron_exclusive_publication_is_connected(pub_); }
    const std::string& channel() const noexcept { return channel_; }
    int32_t stream_id() const noexcept { return stream_id_; }
    void close() noexcept;

private:
    std::string channel_;
    int32_t stream_id_ = 0;
    aeron_exclusive_publication_t* pub_ = nullptr;
};

// The SPP control input (control.h) on any Aeron client: the gateway's control
// channel, and spwsgw-snap's recap channel. `dir` is the client's driver directory.
struct AeronControlConfig;
std::unique_ptr<ControlInput> make_aeron_control(aeron_t* client, const std::string& dir, const AeronControlConfig& cfg, std::string& err);

} // namespace feeds
