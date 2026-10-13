#include "net.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <chrono>
#include <cstdio>

namespace feeds {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace ws = beast::websocket;
using tcp = net::ip::tcp;

namespace {

uint64_t now_ns() { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count()); }

struct Url { std::string host, port, target; bool tls = true; };

bool parse_url(const std::string& url, Url& out, std::string& err) {
    std::string rest;
    if (url.rfind("wss://", 0) == 0) { out.tls = true; rest = url.substr(6); }
    else if (url.rfind("ws://", 0) == 0) { out.tls = false; rest = url.substr(5); }
    else if (url.rfind("https://", 0) == 0) { out.tls = true; rest = url.substr(8); }
    else { err = "unsupported url scheme: " + url; return false; }
    const size_t slash = rest.find('/');
    const std::string hostport = rest.substr(0, slash);
    out.target = slash == std::string::npos ? "/" : rest.substr(slash);
    const size_t colon = hostport.find(':');
    out.host = hostport.substr(0, colon);
    out.port = colon == std::string::npos ? (out.tls ? "443" : "80") : hostport.substr(colon + 1);
    if (out.host.empty()) { err = "url has no host: " + url; return false; }
    return true;
}

} // namespace

struct WsClient::Impl : std::enable_shared_from_this<WsClient::Impl> {
    net::io_context& io;
    net::ssl::context ssl{net::ssl::context::tls_client};
    tcp::resolver resolver;
    std::unique_ptr<ws::stream<beast::ssl_stream<beast::tcp_stream>>> stream;
    beast::flat_buffer buffer;
    net::steady_timer timer, ping_timer;
    WsOptions opts;
    WsCallbacks cbs;
    Url url;
    bool connected_ = false, stopping = false;
    uint64_t backoff_ms, reconnects = 0, connected_at = 0;

    Impl(net::io_context& io_, WsOptions o, WsCallbacks c) : io(io_), resolver(io_), timer(io_), ping_timer(io_), opts(std::move(o)), cbs(std::move(c)), backoff_ms(opts.backoff_ms) {
        ssl.set_default_verify_paths();
        ssl.set_verify_mode(net::ssl::verify_peer);
    }

    void start() {
        std::string err;
        if (!parse_url(opts.url, url, err)) { std::fprintf(stderr, "spwsgw: %s\n", err.c_str()); return; }
        connect();
    }

    void connect() {
        if (stopping) return;
        stream = std::make_unique<ws::stream<beast::ssl_stream<beast::tcp_stream>>>(io, ssl);
        if (!SSL_set_tlsext_host_name(stream->next_layer().native_handle(), url.host.c_str())) { fail("sni", "cannot set SNI host"); return; }
        auto self = shared_from_this();
        resolver.async_resolve(url.host, url.port, [self](beast::error_code ec, tcp::resolver::results_type results) {
            if (ec) { self->fail("resolve", ec.message()); return; }
            beast::get_lowest_layer(*self->stream).expires_after(std::chrono::seconds(10));
            beast::get_lowest_layer(*self->stream).async_connect(results, [self](beast::error_code ec2, tcp::resolver::results_type::endpoint_type) {
                if (ec2) { self->fail("connect", ec2.message()); return; }
                self->stream->next_layer().async_handshake(net::ssl::stream_base::client, [self](beast::error_code ec3) {
                    if (ec3) { self->fail("tls handshake", ec3.message()); return; }
                    beast::get_lowest_layer(*self->stream).expires_never();
                    // INTENT: no idle timeout and Beast answers pings itself; Binance pings every
                    // three minutes and closes the connection if no pong arrives.
                    self->stream->set_option(ws::stream_base::timeout::suggested(beast::role_type::client));
                    self->stream->set_option(ws::stream_base::decorator([](ws::request_type& req) { req.set(beast::http::field::user_agent, "spwsgw"); }));
                    self->stream->async_handshake(self->url.host, self->url.target, [self](beast::error_code ec4) {
                        if (ec4) { self->fail("websocket handshake", ec4.message()); return; }
                        self->on_open();
                    });
                });
            });
        });
    }

    void on_open() {
        connected_ = true;
        connected_at = now_ns();
        backoff_ms = opts.backoff_ms;
        send_subscribe(0);
        if (opts.reconnect_before_s) {
            auto self = shared_from_this();
            timer.expires_after(std::chrono::seconds(opts.reconnect_before_s));
            timer.async_wait([self](beast::error_code ec) { if (!ec && self->connected_) self->fail("lifetime", "planned reconnect before the server's connection limit"); });
        }
    }

    // Subscribes go out one after another (one write at a time on a Beast stream), then reads start.
    void send_subscribe(size_t i) {
        auto self = shared_from_this();
        if (i < opts.subscribe.size()) {
            stream->async_write(net::buffer(opts.subscribe[i]), [self, i](beast::error_code ec, size_t) {
                if (ec) { self->fail("subscribe", ec.message()); return; }
                self->send_subscribe(i + 1);
            });
            return;
        }
        if (cbs.on_connected) cbs.on_connected(cbs.ctx, now_ns());
        read();
        if (!opts.ping_text.empty() && opts.ping_every_s) arm_ping();
    }

    void arm_ping() {
        auto self = shared_from_this();
        ping_timer.expires_after(std::chrono::seconds(opts.ping_every_s));
        ping_timer.async_wait([self](beast::error_code ec) {
            if (ec || !self->connected_ || !self->stream) return;
            self->stream->async_write(net::buffer(self->opts.ping_text), [self](beast::error_code ec2, size_t) {
                if (ec2) { self->fail("ping", ec2.message()); return; }
                self->arm_ping();
            });
        });
    }

    void read() {
        auto self = shared_from_this();
        stream->async_read(buffer, [self](beast::error_code ec, size_t) {
            if (ec) { self->fail("read", ec.message()); return; }
            const uint64_t ts = now_ns();
            if (self->cbs.on_frame) self->cbs.on_frame(self->cbs.ctx, std::string_view(static_cast<const char*>(self->buffer.cdata().data()), self->buffer.size()), ts);
            self->buffer.consume(self->buffer.size());
            self->read();
        });
    }

    void fail(const char* where, const std::string& why) {
        const bool was = connected_;
        connected_ = false;
        timer.cancel();
        ping_timer.cancel();
        if (stream) { beast::error_code ec; beast::get_lowest_layer(*stream).socket().close(ec); }
        stream.reset();
        if (was && cbs.on_disconnected) cbs.on_disconnected(cbs.ctx, now_ns(), std::string(where) + ": " + why);
        if (stopping) return;
        std::fprintf(stderr, "spwsgw: %s: %s; reconnecting in %llu ms\n", where, why.c_str(), static_cast<unsigned long long>(backoff_ms));
        ++reconnects;
        auto self = shared_from_this();
        timer.expires_after(std::chrono::milliseconds(backoff_ms));
        backoff_ms = std::min(backoff_ms * 2, opts.max_backoff_ms);
        timer.async_wait([self](beast::error_code ec) { if (!ec) self->connect(); });
    }

    void stop() {
        stopping = true;
        timer.cancel();
        ping_timer.cancel();
        if (stream) { beast::error_code ec; beast::get_lowest_layer(*stream).socket().close(ec); }
    }
};

WsClient::WsClient(net::io_context& io, WsOptions opts, WsCallbacks cbs) : impl_(std::make_shared<Impl>(io, std::move(opts), std::move(cbs))) {}
WsClient::~WsClient() { impl_->stop(); }
void WsClient::start() { impl_->start(); }
void WsClient::stop() { impl_->stop(); }
bool WsClient::connected() const noexcept { return impl_->connected_; }
uint64_t WsClient::reconnects() const noexcept { return impl_->reconnects; }

} // namespace feeds
