#include "net.h"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include <memory>

namespace feeds {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

namespace {

struct Get : std::enable_shared_from_this<Get> {
    net::ssl::context ssl{net::ssl::context::tls_client};
    tcp::resolver resolver;
    beast::ssl_stream<beast::tcp_stream> stream;
    beast::flat_buffer buffer;
    http::request<http::empty_body> req;
    http::response<http::string_body> res;
    std::string host, port, target;
    std::function<void(int, std::string)> done;

    Get(net::io_context& io, std::function<void(int, std::string)> d) : resolver(io), stream(io, ssl), done(std::move(d)) {
        ssl.set_default_verify_paths();
        ssl.set_verify_mode(net::ssl::verify_peer);
    }

    void run(const std::string& url) {
        if (url.rfind("https://", 0) != 0) { done(0, "only https:// is supported: " + url); return; }
        const std::string rest = url.substr(8);
        const size_t slash = rest.find('/');
        const std::string hostport = rest.substr(0, slash);
        target = slash == std::string::npos ? "/" : rest.substr(slash);
        const size_t colon = hostport.find(':');
        host = hostport.substr(0, colon);
        port = colon == std::string::npos ? "443" : hostport.substr(colon + 1);
        if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) { done(0, "cannot set SNI host"); return; }
        req.version(11);
        req.method(http::verb::get);
        req.target(target);
        req.set(http::field::host, host);
        req.set(http::field::user_agent, "spwsgw");
        auto self = shared_from_this();
        resolver.async_resolve(host, port, [self](beast::error_code ec, tcp::resolver::results_type results) {
            if (ec) { self->done(0, "resolve: " + ec.message()); return; }
            beast::get_lowest_layer(self->stream).expires_after(std::chrono::seconds(10));
            beast::get_lowest_layer(self->stream).async_connect(results, [self](beast::error_code ec2, tcp::resolver::results_type::endpoint_type) {
                if (ec2) { self->done(0, "connect: " + ec2.message()); return; }
                self->stream.async_handshake(net::ssl::stream_base::client, [self](beast::error_code ec3) {
                    if (ec3) { self->done(0, "tls: " + ec3.message()); return; }
                    http::async_write(self->stream, self->req, [self](beast::error_code ec4, size_t) {
                        if (ec4) { self->done(0, "write: " + ec4.message()); return; }
                        http::async_read(self->stream, self->buffer, self->res, [self](beast::error_code ec5, size_t) {
                            if (ec5) { self->done(0, "read: " + ec5.message()); return; }
                            self->done(static_cast<int>(self->res.result_int()), std::move(self->res.body()));
                            beast::error_code ignored;
                            beast::get_lowest_layer(self->stream).socket().shutdown(tcp::socket::shutdown_both, ignored);
                        });
                    });
                });
            });
        });
    }
};

} // namespace

void https_get(net::io_context& io, const std::string& url, std::function<void(int, std::string)> done) {
    std::make_shared<Get>(io, std::move(done))->run(url);
}

} // namespace feeds
