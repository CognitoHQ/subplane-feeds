#pragma once

// The network side, built only with FEEDS_WITH_NETWORK (Boost.Beast + OpenSSL):
// a reconnecting WebSocket client and a one-shot HTTPS GET, both on one io_context.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace boost::asio { class io_context; }

namespace feeds {

// Plain function pointers with a context: one indirect call per frame, nothing type-erased on the path.
struct WsCallbacks {
    void* ctx = nullptr;
    void (*on_frame)(void* ctx, std::string_view frame, uint64_t recv_ts) = nullptr;
    void (*on_connected)(void* ctx, uint64_t now) = nullptr;      // after the subscribe messages were sent
    void (*on_disconnected)(void* ctx, uint64_t now, const std::string& why) = nullptr;
};

struct WsOptions {
    std::string url;               // wss://host[:port]/path
    std::vector<std::string> subscribe;   // sent in order right after the handshake
    std::string ping_text;         // application-level ping text (OKX "ping"); empty = WebSocket ping frames only
    uint64_t    ping_every_s = 0;
    uint64_t    reconnect_before_s = 0;
    uint64_t    backoff_ms = 1000, max_backoff_ms = 30000;
};

class WsClient {
public:
    WsClient(boost::asio::io_context& io, WsOptions opts, WsCallbacks cbs);
    ~WsClient();
    void start();
    void stop();
    // Drops the connection and reconnects after the usual backoff (on_disconnected, then on_connected). io thread.
    void reconnect(const char* why);
    bool connected() const noexcept;
    uint64_t reconnects() const noexcept;
private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

// The observability HTTP server (docs/observability.md): its own thread and
// io_context, never the gateway's, so --spin and the data path are untouched.
// One request per connection (Connection: close), GET only, 5 s to send it.
struct HttpReply { int status = 200; std::string content_type = "text/plain; charset=utf-8"; std::string body; };
class HttpServer {
public:
    // `target` is the request target ("/metrics?format=json"). Called on the server thread.
    using Handler = std::function<HttpReply(const std::string& target)>;
    HttpServer();
    ~HttpServer();
    // "host:port" (port 0 picks one); false with `err` if it cannot bind.
    bool start(const std::string& listen, Handler handler, std::string& err);
    void stop();                        // joins the thread; no handler runs after it returns
    uint16_t port() const noexcept;     // the bound port
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// HTTPS GET; `done(status, body)` on the io_context. status 0 = transport error, body = message.
void https_get(boost::asio::io_context& io, const std::string& url, std::function<void(int status, std::string body)> done);

} // namespace feeds
