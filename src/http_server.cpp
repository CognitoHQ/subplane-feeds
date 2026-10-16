#include "net.h"

#include <boost/asio.hpp>
#include <boost/beast.hpp>

#include <chrono>
#include <thread>

namespace feeds {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

namespace {

struct Conn : std::enable_shared_from_this<Conn> {
    Conn(tcp::socket s, const HttpServer::Handler& h) : stream(std::move(s)), handler(h) {}
    beast::tcp_stream stream;
    const HttpServer::Handler& handler;
    beast::flat_buffer buf;
    http::request<http::string_body> req;
    http::response<http::string_body> res;

    void run() {
        stream.expires_after(std::chrono::seconds(5));
        http::async_read(stream, buf, req, [self = shared_from_this()](beast::error_code ec, size_t) { self->on_read(ec); });
    }
    void on_read(beast::error_code ec) {
        if (ec) return;
        res.version(req.version());
        res.keep_alive(false);
        if (req.method() != http::verb::get) {
            res.result(http::status::method_not_allowed);
            res.set(http::field::content_type, "text/plain; charset=utf-8");
            res.body() = "GET only\n";
        } else {
            HttpReply r = handler(std::string(req.target()));
            res.result(static_cast<unsigned>(r.status));
            res.set(http::field::content_type, r.content_type);
            res.body() = std::move(r.body);
        }
        res.prepare_payload();
        http::async_write(stream, res, [self = shared_from_this()](beast::error_code, size_t) {
            beast::error_code ignored;
            self->stream.socket().shutdown(tcp::socket::shutdown_send, ignored);
        });
    }
};

} // namespace

struct HttpServer::Impl {
    net::io_context io{1};
    tcp::acceptor acceptor{io};
    Handler handler;
    std::thread thread;

    void accept() {
        acceptor.async_accept([this](beast::error_code ec, tcp::socket s) {
            if (ec) return;   // closed by stop()
            std::make_shared<Conn>(std::move(s), handler)->run();
            accept();
        });
    }
};

HttpServer::HttpServer() : impl_(std::make_unique<Impl>()) {}
HttpServer::~HttpServer() { stop(); }

bool HttpServer::start(const std::string& listen, Handler handler, std::string& err) {
    const size_t colon = listen.rfind(':');
    if (colon == std::string::npos) { err = "--http wants host:port, got " + listen; return false; }
    std::string host = listen.substr(0, colon);
    if (host.size() > 1 && host.front() == '[' && host.back() == ']') host = host.substr(1, host.size() - 2);
    beast::error_code ec;
    const auto addr = net::ip::make_address(host.empty() ? "0.0.0.0" : host, ec);
    if (ec) { err = "--http: bad address '" + host + "': " + ec.message(); return false; }
    const unsigned long port = std::strtoul(listen.c_str() + colon + 1, nullptr, 10);
    if (port > 65535) { err = "--http: bad port in " + listen; return false; }
    const tcp::endpoint ep(addr, static_cast<uint16_t>(port));
    impl_->acceptor.open(ep.protocol(), ec);
    if (!ec) impl_->acceptor.set_option(net::socket_base::reuse_address(true), ec);
    if (!ec) impl_->acceptor.bind(ep, ec);
    if (!ec) impl_->acceptor.listen(net::socket_base::max_listen_connections, ec);
    if (ec) { err = "--http " + listen + ": " + ec.message(); return false; }
    impl_->handler = std::move(handler);
    impl_->accept();
    impl_->thread = std::thread([this] { impl_->io.run(); });
    return true;
}

void HttpServer::stop() {
    if (!impl_ || !impl_->thread.joinable()) return;
    net::post(impl_->io, [this] { beast::error_code ec; impl_->acceptor.close(ec); });
    impl_->io.stop();
    impl_->thread.join();
}

uint16_t HttpServer::port() const noexcept {
    beast::error_code ec;
    const auto ep = impl_->acceptor.local_endpoint(ec);
    return ec ? 0 : ep.port();
}

} // namespace feeds
