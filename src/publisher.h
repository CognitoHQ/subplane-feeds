#pragma once

// Where canonical messages go: Aeron (production) or a text stream (tests, dry runs).

#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>

namespace feeds {

class Publisher {
public:
    virtual ~Publisher() = default;
    // One whole canonical message. False = dropped (counted by the gateway).
    virtual bool publish(const uint8_t* msg, size_t len) noexcept = 0;
    virtual std::string describe() const = 0;
    virtual void close() noexcept {}
};

// One line per message: "<Message> iid=<n> seq=<n> ets=<ns> <fields...>", integers only (exact, diffable).
class StdoutPublisher final : public Publisher {
public:
    explicit StdoutPublisher(std::ostream& out) : out_(out) {}
    bool publish(const uint8_t* msg, size_t len) noexcept override;
    std::string describe() const override { return "stdout"; }
private:
    std::ostream& out_;
};

#if FEEDS_WITH_AERON
struct AeronPublisherConfig {
    std::string channel = "aeron:ipc";
    int32_t     stream_id = 1001;
    std::string aeron_dir;              // empty = Aeron default
    bool        embedded_driver = false;
    uint64_t    driver_timeout_ms = 10000;
    uint64_t    offer_retry_us = 2000;  // spin on back pressure for this long, then drop
};
std::unique_ptr<Publisher> make_aeron_publisher(const AeronPublisherConfig& cfg, std::string& err);
#endif

} // namespace feeds
