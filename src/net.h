#pragma once

// The network side, built only with FEEDS_WITH_NETWORK (Boost.Beast + OpenSSL):
// a reconnecting WebSocket client and a one-shot HTTPS GET, both on one io_context.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace boost::asio { class io_context; }

namespace feeds {

struct WsCallbacks {
    std::function<void(std::string_view frame, uint64_t recv_ts)> on_frame;
    std::function<void(uint64_t now)> on_connected;      // after the subscribe message was sent
    std::function<void(uint64_t now, const std::string& why)> on_disconnected;
};

struct WsOptions {
    std::string url;               // wss://host[:port]/path
    std::string subscribe;         // sent right after the handshake (empty = nothing)
    uint64_t    reconnect_before_s = 0;
    uint64_t    backoff_ms = 1000, max_backoff_ms = 30000;
};

class WsClient {
public:
    WsClient(boost::asio::io_context& io, WsOptions opts, WsCallbacks cbs);
    ~WsClient();
    void start();
    void stop();
    bool connected() const noexcept;
    uint64_t reconnects() const noexcept;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

// HTTPS GET; `done(status, body)` on the io_context. status 0 = transport error, body = message.
void https_get(boost::asio::io_context& io, const std::string& url, std::function<void(int status, std::string body)> done);

} // namespace feeds
