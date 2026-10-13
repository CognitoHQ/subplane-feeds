#include "book/binance_diff.h"

#include "doctest.h"

using namespace feeds;

namespace {
Pack pack_with_depth() {
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load(std::string(FEEDS_SOURCE_DIR) + "/packs/binance-spot.yaml", p, err), err);
    return p;
}
BinanceDiff::Event ev(uint64_t U, uint64_t u, std::vector<Level> bids, std::vector<Level> asks) {
    BinanceDiff::Event e; e.first_id = U; e.last_id = u; e.bids = std::move(bids); e.asks = std::move(asks); e.exch_ts = u * 1000; return e;
}
size_t count(const std::vector<BookEmit>& v, BookEmit::Kind k) { size_t n = 0; for (auto& e : v) n += e.kind == k; return n; }
}

TEST_CASE("Book keeps sides sorted and reports actions with level indexes") {
    Book b;
    auto r = b.apply(Side::Buy, 100, 5); CHECK(r.action == Action::Add); CHECK(r.level == 0);
    r = b.apply(Side::Buy, 101, 5); CHECK(r.action == Action::Add); CHECK(r.level == 0);      // better bid goes on top
    r = b.apply(Side::Buy, 99, 5); CHECK(r.level == 2);
    r = b.apply(Side::Buy, 100, 7); CHECK(r.action == Action::Update); CHECK(r.level == 1);
    r = b.apply(Side::Buy, 100, 0); CHECK(r.action == Action::Delete); CHECK(r.level == 1); CHECK(r.changed);
    r = b.apply(Side::Buy, 100, 0); CHECK_FALSE(r.changed);
    CHECK(b.bids().size() == 2); CHECK(b.bids()[0].px == 101); CHECK(b.bids()[1].px == 99);
    b.apply(Side::Sell, 105, 1); b.apply(Side::Sell, 103, 1); b.apply(Side::Sell, 104, 1);
    CHECK(b.asks()[0].px == 103); CHECK(b.asks()[2].px == 105);
}

TEST_CASE("binance_diff: buffer, snapshot, apply, then track update ids") {
    Pack p = pack_with_depth();
    const StreamDef& sd = *p.find_by_suffix("@depth@100ms");
    BinanceDiff s(p, sd, 0);
    std::vector<BookEmit> out;
    CHECK(s.take_snapshot_request());          // wants one snapshot
    CHECK_FALSE(s.take_snapshot_request());    // not twice while pending
    // Events arrive while the snapshot is in flight.
    CHECK(s.apply_event(ev(150, 152, {{100, 1}}, {}), out));   // entirely before the snapshot: dropped
    CHECK(s.apply_event(ev(159, 161, {{100, 2}}, {{110, 3}}), out)); // overlaps lastUpdateId+1 = 161
    CHECK(s.apply_event(ev(162, 163, {{101, 4}}, {}), out));
    CHECK(out.empty());
    CHECK(s.apply_snapshot(160, {{100, 9}, {99, 1}}, {{110, 9}}, out));
    CHECK(s.synced());
    // Only the snapshot is emitted (the book already reflects the buffered diffs).
    REQUIRE(out.size() == 1); CHECK(out[0].kind == BookEmit::SNAPSHOT); CHECK_FALSE(out[0].resynced);
    CHECK(s.book().bids()[0].px == 101); CHECK(s.book().bids()[0].qty == 4);
    CHECK(s.book().bids()[1].qty == 2);     // 100 overwritten by the overlapping event
    CHECK(s.book().asks()[0].qty == 3);
    CHECK(s.resyncs() == 0);
    out.clear();
    CHECK(s.apply_event(ev(164, 165, {{100, 0}}, {{111, 1}}), out));
    REQUIRE(out.size() == 2);
    CHECK(out[0].kind == BookEmit::UPDATE); CHECK(out[0].side == Side::Buy); CHECK(out[0].action == Action::Delete); CHECK(out[0].level == 1);
    CHECK(out[1].side == Side::Sell); CHECK(out[1].action == Action::Add); CHECK(out[1].level == 1); CHECK(out[1].px == 111);
    out.clear();
    // A gap: resync, drop the event into the buffer, want a new snapshot.
    CHECK_FALSE(s.apply_event(ev(170, 171, {}, {}), out));
    CHECK_FALSE(s.synced()); CHECK(s.resyncs() == 1); CHECK(s.take_snapshot_request());
    CHECK(s.apply_snapshot(169, {{100, 1}}, {{110, 1}}, out));
    REQUIRE(out.size() == 1); CHECK(out[0].resynced);
}

TEST_CASE("binance_diff: a snapshot older than the buffer asks for another one") {
    Pack p = pack_with_depth();
    BinanceDiff s(p, *p.find_by_suffix("@depth@100ms"), 0);
    std::vector<BookEmit> out;
    CHECK(s.take_snapshot_request());
    CHECK(s.apply_event(ev(200, 201, {{1, 1}}, {}), out));
    CHECK_FALSE(s.apply_snapshot(100, {}, {}, out));   // 200 > 100 + 1: cannot bridge
    CHECK_FALSE(s.synced());
    CHECK(s.take_snapshot_request());
    CHECK(s.apply_snapshot(199, {}, {}, out));
    CHECK(s.synced());
}

TEST_CASE("binance_diff: periodic snapshots and level 255 beyond the top ten") {
    Pack p = pack_with_depth();
    BinanceDiff s(p, *p.find_by_suffix("@depth@100ms"), 3);
    std::vector<BookEmit> out;
    s.take_snapshot_request();
    std::vector<Level> bids;
    for (int i = 0; i < 12; ++i) bids.push_back({1000 - i, 1});
    CHECK(s.apply_snapshot(10, bids, {}, out));
    out.clear();
    CHECK(s.apply_event(ev(11, 11, {{980, 5}}, {}), out));   // 12th bid: level 255
    REQUIRE(out.size() == 1); CHECK(out[0].level == 255);
    out.clear();
    CHECK(s.apply_event(ev(12, 12, {{1000, 2}, {999, 2}}, {}), out));
    CHECK(count(out, BookEmit::UPDATE) == 2); CHECK(count(out, BookEmit::SNAPSHOT) == 1);   // third update triggers a snapshot
}
