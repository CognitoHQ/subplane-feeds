#include "book/binance_diff.h"

#include "doctest.h"

#include <atomic>
#include <cstdlib>
#include <new>

// Counts heap allocations while `g_count_allocs` is set, to prove the book's data path makes none.
namespace { std::atomic<bool> g_count_allocs{false}; std::atomic<size_t> g_allocs{0}; }
void* operator new(std::size_t n) {
    if (g_count_allocs.load(std::memory_order_relaxed)) g_allocs.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
// INTENT: noinline, or GCC 16 inlines these into callers whose pointer came from the standard
// `operator new` and fails the build with -Wmismatched-new-delete (both sides are ours here).
[[gnu::noinline]] void operator delete(void* p) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace feeds;

namespace {
Pack pack_with_depth() {
    Pack p; std::string err;
    REQUIRE_MESSAGE(Pack::load_with_registry(std::string(FEEDS_SOURCE_DIR) + "/packs/binance-spot.yaml", "", p, err), err);
    return p;
}
const StreamDef& depth_stream(const Pack& p) { std::string_view s; return *p.find_by_name("btcusdt@depth@100ms", s); }
BinanceDiff::Event ev(uint64_t U, uint64_t u, std::vector<LevelRow> bids, std::vector<LevelRow> asks) {
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
    BinanceDiff s(p, depth_stream(p), 0);
    std::vector<BookEmit> out;
    CHECK(s.take_snapshot_request());          // wants one snapshot
    CHECK_FALSE(s.take_snapshot_request());    // not twice while pending
    // Events arrive while the snapshot is in flight.
    CHECK(s.apply_event(ev(150, 152, {{Side::Unknown, 100, 1}}, {}), out));   // entirely before the snapshot: dropped
    CHECK(s.apply_event(ev(159, 161, {{Side::Unknown, 100, 2}}, {{Side::Unknown, 110, 3}}), out)); // overlaps lastUpdateId+1 = 161
    CHECK(s.apply_event(ev(162, 163, {{Side::Unknown, 101, 4}}, {}), out));
    CHECK(out.empty());
    CHECK(s.apply_snapshot(160, {{Side::Unknown, 100, 9}, {Side::Unknown, 99, 1}}, {{Side::Unknown, 110, 9}}, out));
    CHECK(s.synced());
    // Only the whole book and its top-ten snapshot are emitted (the book already reflects the buffered diffs).
    REQUIRE(out.size() == 2); CHECK(out[0].kind == BookEmit::FULL); CHECK(out[1].kind == BookEmit::SNAPSHOT); CHECK_FALSE(out[1].resynced);
    CHECK(s.book().bids()[0].px == 101); CHECK(s.book().bids()[0].qty == 4);
    CHECK(s.book().bids()[1].qty == 2);     // 100 overwritten by the overlapping event
    CHECK(s.book().asks()[0].qty == 3);
    CHECK(s.resyncs() == 0);
    out.clear();
    CHECK(s.apply_event(ev(164, 165, {{Side::Unknown, 100, 0}}, {{Side::Unknown, 111, 1}}), out));
    REQUIRE(out.size() == 2);
    CHECK(out[0].kind == BookEmit::UPDATE); CHECK(out[0].side == Side::Buy); CHECK(out[0].action == Action::Delete); CHECK(out[0].level == 1);
    CHECK(out[1].side == Side::Sell); CHECK(out[1].action == Action::Add); CHECK(out[1].level == 1); CHECK(out[1].px == 111);
    out.clear();
    // A gap: resync, drop the event into the buffer, want a new snapshot.
    CHECK_FALSE(s.apply_event(ev(170, 171, {}, {}), out));
    CHECK_FALSE(s.synced()); CHECK(s.resyncs() == 1); CHECK(s.take_snapshot_request());
    CHECK(s.apply_snapshot(169, {{Side::Unknown, 100, 1}}, {{Side::Unknown, 110, 1}}, out));
    REQUIRE(out.size() == 2); CHECK(out[0].kind == BookEmit::FULL); CHECK(out[0].resynced); CHECK(out[1].resynced);
}

TEST_CASE("binance_diff: a snapshot older than the buffer asks for another one") {
    Pack p = pack_with_depth();
    BinanceDiff s(p, depth_stream(p), 0);
    std::vector<BookEmit> out;
    CHECK(s.take_snapshot_request());
    CHECK(s.apply_event(ev(200, 201, {{Side::Unknown, 1, 1}}, {}), out));
    CHECK_FALSE(s.apply_snapshot(100, {}, {}, out));   // 200 > 100 + 1: cannot bridge
    CHECK_FALSE(s.synced());
    CHECK(s.take_snapshot_request());
    CHECK(s.apply_snapshot(199, {}, {}, out));
    CHECK(s.synced());
}

TEST_CASE("binance_diff: periodic snapshots and level 255 beyond the top ten") {
    Pack p = pack_with_depth();
    BinanceDiff s(p, depth_stream(p), 3);
    std::vector<BookEmit> out;
    s.take_snapshot_request();
    std::vector<LevelRow> bids;
    for (int i = 0; i < 12; ++i) bids.push_back({Side::Unknown, 1000 - i, 1});
    CHECK(s.apply_snapshot(10, bids, {}, out));
    out.clear();
    CHECK(s.apply_event(ev(11, 11, {{Side::Unknown, 980, 5}}, {}), out));   // 12th bid: level 255
    REQUIRE(out.size() == 1); CHECK(out[0].level == 255);
    out.clear();
    CHECK(s.apply_event(ev(12, 12, {{Side::Unknown, 1000, 2}, {Side::Unknown, 999, 2}}, {}), out));
    CHECK(count(out, BookEmit::UPDATE) == 2); CHECK(count(out, BookEmit::SNAPSHOT) == 1);   // third update triggers a snapshot
}

TEST_CASE("Book::for_each walks every level best first, beyond the top ten") {
    Book b;
    for (int i = 0; i < 15; ++i) { b.apply(Side::Buy, 100 + i, 1); b.apply(Side::Sell, 200 + i, 2); }
    std::vector<int64_t> bids, asks;
    b.for_each(Side::Buy, [&](int64_t px, int64_t qty) { bids.push_back(px); CHECK(qty == 1); });
    b.for_each(Side::Sell, [&](int64_t px, int64_t qty) { asks.push_back(px); CHECK(qty == 2); });
    REQUIRE(bids.size() == 15); REQUIRE(asks.size() == 15);
    CHECK(bids.front() == 114); CHECK(bids.back() == 100);
    CHECK(asks.front() == 200); CHECK(asks.back() == 214);
}

TEST_CASE("Book: levels come from a fixed pool; a full side refuses a new level (overflow)") {
    Book b(16);
    CHECK(b.max_levels() == 16);
    for (int i = 0; i < 16; ++i) CHECK(b.apply(Side::Buy, 100 + i, 1).changed);
    Book::Result r = b.apply(Side::Buy, 200, 1);      // even a better price: no level is evicted
    CHECK(r.overflow); CHECK_FALSE(r.changed);
    CHECK(b.depth(Side::Buy) == 16); CHECK(b.bids()[0].px == 115); CHECK(b.overflows() == 1);
    CHECK(b.apply(Side::Buy, 100, 9).action == Action::Update);   // existing levels still update
    CHECK(b.apply(Side::Sell, 300, 1).changed);                   // each side has its own pool
    CHECK(b.apply(Side::Buy, 105, 0).action == Action::Delete);   // a delete gives its node back
    r = b.apply(Side::Buy, 200, 1);
    CHECK(r.changed); CHECK(r.level == 0); CHECK_FALSE(r.overflow);
    CHECK(Book(3).max_levels() == Book::kMinMaxLevels);
}

TEST_CASE("Book: apply, delete and clear never allocate once constructed") {
    Book b(64);
    g_allocs = 0;
    g_count_allocs = true;
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 64; ++i) { b.apply(Side::Buy, 1000 - i, i + 1); b.apply(Side::Sell, 2000 + i, i + 1); }
        for (int i = 0; i < 64; i += 2) b.apply(Side::Buy, 1000 - i, 0);
        for (int i = 0; i < 64; i += 2) b.apply(Side::Buy, 1000 - i, 7);
        b.apply(Side::Buy, 5, 1);                      // overflow
        b.clear();
    }
    g_count_allocs = false;
    CHECK(g_allocs == 0);
    CHECK(b.depth(Side::Buy) == 0);
}

TEST_CASE("Book: a moved book keeps its levels and its pool") {
    Book a(16);
    a.apply(Side::Buy, 100, 1); a.apply(Side::Sell, 101, 2);
    Book b(std::move(a));
    REQUIRE(b.bids().size() == 1); CHECK(b.asks()[0].qty == 2);
    for (int i = 0; i < 15; ++i) CHECK(b.apply(Side::Buy, 50 + i, 1).changed);
    CHECK(b.apply(Side::Buy, 10, 1).overflow);
}

TEST_CASE("binance_diff: an overflowing snapshot or diff resyncs instead of publishing a partial book") {
    Pack p = pack_with_depth();
    BinanceDiff s(p, depth_stream(p), 0, 16);
    std::vector<BookEmit> out;
    CHECK(s.take_snapshot_request());
    std::vector<LevelRow> bids;
    for (int i = 0; i < 17; ++i) bids.push_back({Side::Unknown, 100 - i, 1});
    CHECK_FALSE(s.apply_snapshot(10, bids, {}, out));
    CHECK_FALSE(s.synced()); CHECK(out.empty());
    CHECK(s.take_snapshot_request());                 // asks again
    bids.pop_back();
    CHECK(s.apply_snapshot(10, bids, {}, out));       // 16 fit
    CHECK(s.synced());
    out.clear();
    CHECK_FALSE(s.apply_event(ev(11, 11, {{Side::Unknown, 200, 1}}, {}), out));   // a 17th level
    CHECK_FALSE(s.synced()); CHECK(s.book().depth(Side::Buy) == 0);
    CHECK(s.take_snapshot_request());
    CHECK(s.book().overflows() == 2);
}
