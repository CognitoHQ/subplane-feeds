#include "gateway.h"
#include "snap/gateway_link.h"
#include "snap/recap_server.h"
#include "snap/snap.h"

#include "canonical_fields.h"
#include "doctest.h"

#include <cstring>
#include <set>

using namespace feeds;
using sp_proto::SourceStateCode;

namespace {

// The gateway's two outputs wired straight into a SnapCore, as Aeron would
// deliver them; `drop` loses the next n data offers (a gap).
struct DataTap final : Publisher {
    SnapCore& core; int drop = 0;
    uint64_t btc_iseq = 0;   // InstrumentSeq of the last message about instrument 1 offered
    explicit DataTap(SnapCore& c) : core(c) {}
    Offer publish(const uint8_t* m, size_t n) noexcept override {
        if (drop > 0) { --drop; return Offer::OK; }
        for (size_t at = 0; at + sp_canonical::kHeaderSize <= n;) {
            uint16_t block; std::memcpy(&block, m + at, 2);
            uint32_t iid; std::memcpy(&iid, m + at + kPrefixInstrumentId, 4);
            if (iid == 1) std::memcpy(&btc_iseq, m + at + kPrefixInstrumentSeq, 8);
            at += sp_canonical::kHeaderSize + block;
        }
        core.on_data(m, n);
        return Offer::OK;
    }
    std::string describe() const override { return "tap"; }
};
struct StateTap final : Publisher {
    SnapCore& core;
    explicit StateTap(SnapCore& c) : core(c) {}
    Offer publish(const uint8_t* m, size_t n) noexcept override { core.on_state(m, n); return Offer::OK; }
    std::string describe() const override { return "tap"; }
};

struct Rig {
    Pack pack; Instruments ins; SnapCore core; DataTap data{core}; StateTap state{core};
    explicit Rig(const char* pack_name = "binance-spot", std::vector<std::string> symbols = {"BTCUSDT", "ETHUSDT"}, uint32_t snap_levels = Book::kDefaultMaxLevels)
        : core(snap_levels) {
        std::string err;
        REQUIRE_MESSAGE(Pack::load_with_registry(std::string(FEEDS_SOURCE_DIR) + "/packs/" + pack_name + ".yaml", "", pack, err), err);
        REQUIRE(ins.load_symbols(symbols, pack.connect.symbol_lower, err));
    }
    static GatewayOptions opts(uint64_t epoch = 7) { GatewayOptions o; o.heartbeat_ms = 0; o.definitions_ms = 0; o.log_errors = false; o.snapshot_every = 0; o.source_epoch = epoch; return o; }
};

const char* kDiff157 = R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1672515782136,"s":"BTCUSDT","U":157,"u":160,"b":[["0.0024","10"]],"a":[["0.0026","100"]]}})";
const char* kSnap158 = R"({"lastUpdateId":158,"bids":[["0.0024","5"],["0.0023","1"]],"asks":[["0.0026","7"]]})";
const char* kDiff161 = R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1672515782236,"s":"BTCUSDT","U":161,"u":161,"b":[["0.0023","0"],["0.0022","4"]],"a":[]}})";

void sync_btc(Gateway& gw) {
    gw.on_connected(1);
    gw.on_frame(kDiff157, 10);
    gw.on_snapshot("BTCUSDT", kSnap158, 11);
}

std::vector<std::pair<int64_t, int64_t>> levels(const Book& b, Side s) {
    std::vector<std::pair<int64_t, int64_t>> v;
    b.for_each(s, [&](int64_t px, int64_t q) { v.emplace_back(px, q); });
    return v;
}
using Lv = std::vector<std::pair<int64_t, int64_t>>;

// Splits u32-length-prefixed SPP messages.
std::vector<std::vector<uint8_t>> messages(const std::vector<uint8_t>& f) {
    std::vector<std::vector<uint8_t>> out;
    size_t at = 0;
    while (at + 4 <= f.size()) { uint32_t n; std::memcpy(&n, f.data() + at, 4); at += 4; out.emplace_back(f.data() + at, f.data() + at + n); at += n; }
    return out;
}
uint16_t template_of(const std::vector<uint8_t>& m) { uint16_t t; std::memcpy(&t, m.data() + 2, 2); return t; }

std::vector<uint8_t> state_frame(uint64_t epoch, uint64_t instrument, uint64_t seq, SourceStateCode code) {
    std::vector<uint8_t> f(4 + 64);
    sp_proto::SourceStateEncoder e(f.data() + 4, 64);
    e.SourceEpoch(epoch).Instrument(instrument).Sequence(seq).Venue(10).State(code);
    const uint32_t n = static_cast<uint32_t>(e.finish());
    std::memcpy(f.data(), &n, 4);
    f.resize(4 + n);
    return f;
}

} // namespace

TEST_CASE("snap: Unknown until the gateway publishes the book whole, then Ok with the gateway's book") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.data, Rig::opts(), nullptr, &r.state);
    gw.on_connected(1);
    gw.on_frame(kDiff157, 10);
    CHECK(r.core.state(1, 10) == SourceStateCode::Unknown);
    gw.on_snapshot("BTCUSDT", kSnap158, 11);
    CHECK(r.core.state(1, 10) == SourceStateCode::Ok);
    CHECK(r.core.state(1, 0) == SourceStateCode::Ok);           // venue 0: any venue
    CHECK(r.core.state(2, 10) == SourceStateCode::Unknown);    // ETHUSDT never synced
    CHECK(r.core.source_epoch() == 7);
    gw.on_frame(kDiff161, 12);
    const Book* b = r.core.book(1, 10);
    REQUIRE(b);
    CHECK(levels(*b, Side::Buy) == Lv{{2400000, 10000000000}, {2200000, 4000000000}});
    CHECK(levels(*b, Side::Sell) == Lv{{2600000, 100000000000}});
    CHECK(r.core.last_sequence() == gw.sequence());
    CHECK(r.core.stats().gaps == 0);
}

TEST_CASE("snap: a gap makes every book Stale; a republish brings it back to Ok") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.data, Rig::opts(), nullptr, &r.state);
    sync_btc(gw);
    REQUIRE(r.core.state(1, 10) == SourceStateCode::Ok);
    r.data.drop = 1;
    gw.on_frame(kDiff161, 12);                                 // lost
    gw.on_frame(R"({"stream":"btcusdt@depth@100ms","data":{"E":1672515782336,"U":162,"u":162,"b":[["0.0021","1"]],"a":[]}})", 13);
    CHECK(r.core.stats().gaps == 1);
    CHECK(r.core.state(1, 10) == SourceStateCode::Stale);
    // The asks: "all" once at start, then a Stale instrument after one quiet wait.
    SnapCore::Ask ask;
    REQUIRE(r.core.next_ask(100, 1000, ask)); CHECK(ask.all);
    CHECK_FALSE(r.core.next_ask(100, 1000, ask));              // the first wait
    CHECK_FALSE(r.core.next_ask(500, 1000, ask));
    std::set<uint64_t> asked;                                   // BTCUSDT (Stale) and ETHUSDT (never Ok), one per call
    while (r.core.next_ask(1200, 1000, ask)) { CHECK_FALSE(ask.all); CHECK(ask.venue == 10); asked.insert(ask.instrument); }
    CHECK(asked == std::set<uint64_t>{1, 2});
    CHECK(gw.republish(1, 10, 5) == RepublishResult::QUEUED);
    gw.tick(1'000'000'000);
    CHECK(r.core.state(1, 10) == SourceStateCode::Ok);
    CHECK(levels(*r.core.book(1, 10), Side::Buy) == Lv{{2400000, 10000000000}, {2200000, 4000000000}, {2100000, 1000000000}});
}

TEST_CASE("snap: an Ok for a book not seen whole since the last gap is ignored") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.data, Rig::opts(), nullptr, &r.state);
    sync_btc(gw);
    r.data.drop = 1;
    gw.on_frame(kDiff161, 12);
    gw.on_frame(R"({"stream":"btcusdt@depth@100ms","data":{"E":1672515782336,"U":162,"u":162,"b":[["0.0021","1"]],"a":[]}})", 13);
    REQUIRE(r.core.state(1, 10) == SourceStateCode::Stale);
    const auto f = state_frame(7, 1, r.core.last_sequence(), SourceStateCode::Ok);   // e.g. a repeat of an older Ok
    r.core.on_state(f.data(), f.size());
    CHECK(r.core.state(1, 10) == SourceStateCode::Stale);
    CHECK(r.core.stats().states_ignored == 1);
}

TEST_CASE("snap: a state waits until the data stream reaches its Sequence") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.data, Rig::opts(), nullptr, &r.state);
    sync_btc(gw);
    const uint64_t s = r.core.last_sequence();
    const auto f = state_frame(7, 1, s + 2, SourceStateCode::Stale);
    r.core.on_state(f.data(), f.size());
    CHECK(r.core.state(1, 10) == SourceStateCode::Ok);         // not yet: the data is behind
    gw.on_frame(kDiff161, 12);                                 // two BookUpdates: s+1, s+2
    CHECK(r.core.state(1, 10) == SourceStateCode::Stale);
}

TEST_CASE("snap: a gateway restart (new epoch, Sequence from 1) forgets everything and asks for all again") {
    Rig r;
    {
        Gateway gw(r.pack, r.ins, r.data, Rig::opts(7), nullptr, &r.state);
        sync_btc(gw);
    }
    SnapCore::Ask ask;
    REQUIRE(r.core.next_ask(1, 1000, ask)); CHECK(ask.all);
    REQUIRE(r.core.state(1, 10) == SourceStateCode::Ok);
    Gateway gw2(r.pack, r.ins, r.data, Rig::opts(9), nullptr, &r.state);
    gw2.on_connected(100);                                     // definitions from Sequence 1
    CHECK(r.core.stats().restarts >= 1);
    CHECK(r.core.state(1, 10) == SourceStateCode::Unknown);
    REQUIRE(r.core.next_ask(2, 1000, ask)); CHECK(ask.all);
    gw2.on_frame(kDiff157, 110);
    gw2.on_snapshot("BTCUSDT", kSnap158, 111);
    CHECK(r.core.state(1, 10) == SourceStateCode::Ok);
    CHECK(r.core.source_epoch() == 9);
}

TEST_CASE("snap: a recap reply is Begin, the definition, Clear, every level, End, all at AsOf") {
    Rig r;
    Gateway gw(r.pack, r.ins, r.data, Rig::opts(), nullptr, &r.state);
    sync_btc(gw);
    gw.on_frame(kDiff161, 12);
    std::vector<uint8_t> out;
    r.core.answer(77, 1, 0, out);
    const auto ms = messages(out);
    REQUIRE(ms.size() >= 3);
    sp_proto::RecapBeginDecoder b;
    REQUIRE(b.wrap(ms.front().data(), ms.front().size()));
    CHECK(b.CorrelationId() == 77); CHECK(b.AsOf() == gw.sequence()); CHECK(b.SourceEpoch() == 7);
    sp_proto::RecapEndDecoder e;
    REQUIRE(e.wrap(ms.back().data(), ms.back().size()));
    CHECK(b.RecordCount() == ms.size() - 2); CHECK(e.RecordCount() == ms.size() - 2);
    std::vector<uint16_t> kinds;
    std::vector<uint8_t> actions;
    for (size_t i = 1; i + 1 < ms.size(); ++i) {
        sp_proto::RecapRecordDecoder d;
        REQUIRE(d.wrap(ms[i].data(), ms[i].size()));
        const uint8_t* p = d.Payload_data();
        uint16_t t; std::memcpy(&t, p + 2, 2); kinds.push_back(t);
        uint64_t seq; std::memcpy(&seq, p + kPrefixSequence, 8);
        CHECK(seq == gw.sequence());
        uint32_t iid; std::memcpy(&iid, p + kPrefixInstrumentId, 4);
        CHECK(iid == 1);
        uint64_t iseq; std::memcpy(&iseq, p + kPrefixInstrumentSeq, 8);
        CHECK(iseq == r.data.btc_iseq);   // every record says the instrument as of S
        if (const auto* u = sp_canonical::view<sp_canonical::BookUpdate>(p, static_cast<uint32_t>(d.Payload_size()))) actions.push_back(u->Action);
    }
    CHECK(r.data.btc_iseq > 1);
    CHECK(kinds.front() == sp_canonical::InstrumentDefinition::kTemplateId);
    CHECK(actions == std::vector<uint8_t>{4, 1, 1, 1});        // Clear, two bids, one ask
    out.clear();
    r.core.answer(78, 2, 10, out);                             // ETHUSDT: known (its definition), never Ok
    sp_proto::RecapRejectDecoder rj;
    REQUIRE(rj.wrap(out.data() + 4, out.size() - 4));
    CHECK(rj.Reason() == sp_proto::RejectReason::NotReady);
    out.clear();
    r.core.answer(79, 99, 0, out);
    REQUIRE(rj.wrap(out.data() + 4, out.size() - 4));
    CHECK(rj.Reason() == sp_proto::RejectReason::UnknownInstrument);
    CHECK(r.core.stats().replies == 1); CHECK(r.core.stats().rejects_not_ready == 1); CHECK(r.core.stats().rejects_unknown == 1);
}

TEST_CASE("snap: a book deeper than the snap holds overflows to Stale, and a reply rejects it") {
    Rig r("binance-spot", {"BTCUSDT"}, 16);
    Gateway gw(r.pack, r.ins, r.data, Rig::opts(), nullptr, &r.state);
    gw.on_connected(1);
    std::string body = R"({"lastUpdateId":158,"bids":[)";
    for (int i = 0; i < 17; ++i) body += (i ? "," : "") + std::string("[\"0.00") + std::to_string(30 + i) + "\",\"1\"]";
    body += R"(],"asks":[]})";
    gw.on_snapshot("BTCUSDT", body, 2);                        // the gateway holds 17 (its default depth)
    CHECK(r.core.stats().book_overflows == 1);
    CHECK(r.core.state(1, 10) != SourceStateCode::Ok);         // its Ok is ignored: this copy is not whole
    std::vector<uint8_t> out;
    r.core.answer(1, 1, 10, out);
    CHECK(template_of(messages(out)[0]) == sp_proto::RecapRejectDecoder::kTemplateId);
}

TEST_CASE("snap: RecapServer answers Hello as Snap and unknown requests as Unsupported") {
    SnapCore core;
    RecapServer s(core, "spwsgw-snap", 5);
    std::vector<uint8_t> req(4 + 128), reply;
    sp_proto::HelloEncoder he(req.data() + 4, 128);
    he.CorrelationId(3).ProtoVersion(1).MinProtoVersion(1).Role(sp_proto::Role::Agent);
    uint32_t n = static_cast<uint32_t>(he.finish());
    std::memcpy(req.data(), &n, 4); req.resize(4 + n);
    s.on_frame(req.data(), req.size(), reply);
    auto ms = messages(reply);
    REQUIRE(ms.size() == 1);
    sp_proto::HelloDecoder hd;
    REQUIRE(hd.wrap(ms[0].data(), ms[0].size()));
    CHECK(hd.Role() == sp_proto::Role::Snap); CHECK(hd.PeerEpoch() == 5); CHECK(hd.CorrelationId() == 3);
    // A Republish is a gateway's business, not a snap's.
    req.assign(4 + 128, 0); reply.clear();
    sp_proto::RepublishEncoder re(req.data() + 4, 128);
    re.CorrelationId(4).Instrument(1);
    n = static_cast<uint32_t>(re.finish());
    std::memcpy(req.data(), &n, 4); req.resize(4 + n);
    s.on_frame(req.data(), req.size(), reply);
    ms = messages(reply);
    REQUIRE(ms.size() == 1);
    sp_proto::StatusDecoder sd;
    REQUIRE(sd.wrap(ms[0].data(), ms[0].size()));
    CHECK(sd.Code() == sp_proto::StatusCode::Unsupported); CHECK(sd.CorrelationId() == 4);
    CHECK(s.stats().unsupported == 1);
}

TEST_CASE("snap: the gateway link says Hello, then asks for all, and reads the gateway's replies") {
    SnapCore core;
    GatewayLink link("spwsgw-snap", 5);
    std::vector<uint8_t> f;
    REQUIRE(link.next(core, 1, 1000, f));
    CHECK(template_of(messages(f)[0]) == sp_proto::HelloDecoder::kTemplateId);
    REQUIRE(link.next(core, 1, 1000, f));                      // not sent: the same Hello again
    CHECK(template_of(messages(f)[0]) == sp_proto::HelloDecoder::kTemplateId);
    link.sent();
    REQUIRE(link.next(core, 1, 1000, f));
    sp_proto::RepublishDecoder rd;
    auto m = messages(f)[0];
    REQUIRE(rd.wrap(m.data(), m.size()));
    CHECK(rd.Instrument() == 0); CHECK(rd.CorrelationId() == 2);
    link.sent();
    CHECK_FALSE(link.next(core, 1, 1000, f));                  // nothing Stale to ask for
    CHECK(link.stats().hellos_sent == 1); CHECK(link.stats().republish_sent == 1);
    // The gateway's Hello and a Status{Ok}, in one frame.
    std::vector<uint8_t> reply(256);
    sp_proto::HelloEncoder he(reply.data() + 4, 120);
    he.CorrelationId(1).PeerEpoch(42).ProtoVersion(1).MinProtoVersion(1).Role(sp_proto::Role::Gateway);
    uint32_t n1 = static_cast<uint32_t>(he.finish());
    std::memcpy(reply.data(), &n1, 4);
    sp_proto::StatusEncoder se(reply.data() + 8 + n1, 64);
    se.CorrelationId(2).Code(sp_proto::StatusCode::Ok);
    uint32_t n2 = static_cast<uint32_t>(se.finish());
    std::memcpy(reply.data() + 4 + n1, &n2, 4);
    reply.resize(8 + n1 + n2);
    link.on_reply(reply.data(), reply.size());
    CHECK(link.gateway_epoch() == 42); CHECK(link.stats().status_ok == 1); CHECK(link.stats().malformed == 0);
    link.disconnected();
    REQUIRE(link.next(core, 2, 1000, f));
    CHECK(template_of(messages(f)[0]) == sp_proto::HelloDecoder::kTemplateId);   // Hello again on a new connection
}
