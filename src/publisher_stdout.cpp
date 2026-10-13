#include "publisher.h"

#include "subplane_canonical_views.h"

#include <cstdio>
#include <cstring>
#include <ostream>

namespace feeds {
namespace {
using namespace sp_canonical;

std::string chars(const char* p, size_t n) {
    size_t len = 0;
    while (len < n && p[len] != '\0') ++len;
    return std::string(p, len);
}

template <class T>
void prefix(std::ostream& o, const char* name, const T& m) {
    o << name << " iid=" << m.InstrumentId << " venue=" << m.VenueId << " seq=" << m.Sequence << " ets=" << m.ExchangeTs << " flags=" << m.Flags;
}
} // namespace

bool StdoutPublisher::publish(const uint8_t* buf, size_t total) noexcept {
    size_t off = 0;
    while (off + kHeaderSize <= total) {
        uint16_t block = 0;
        std::memcpy(&block, buf + off, 2);
        const size_t len = kHeaderSize + block;
        if (len < kHeaderSize + 32 || off + len > total) { out_ << "Unknown len=" << (total - off) << '\n'; return false; }
        if (!publish_one(buf + off, len)) return false;
        off += len;
    }
    return off == total;
}

bool StdoutPublisher::publish_one(const uint8_t* p, size_t len) noexcept {
    const uint32_t n = static_cast<uint32_t>(len);
    if (const Trade* m0 = view<Trade>(p, n)) {
        prefix(out_, "Trade", *m0);
        out_ << " px=" << m0->Price << " qty=" << m0->Qty << " id=" << m0->TradeId << " aggr=" << unsigned(m0->Aggressor) << '\n';
    } else if (const Quote* m1 = view<Quote>(p, n)) {
        prefix(out_, "Quote", *m1);
        out_ << " bid=" << m1->BidPx << " bidqty=" << m1->BidQty << " ask=" << m1->AskPx << " askqty=" << m1->AskQty << '\n';
    } else if (const BookUpdate* m2 = view<BookUpdate>(p, n)) {
        prefix(out_, "BookUpdate", *m2);
        out_ << " side=" << unsigned(m2->Side) << " action=" << unsigned(m2->Action) << " level=" << unsigned(m2->Level) << " px=" << m2->Price << " qty=" << m2->Qty << '\n';
    } else if (const BookSnapshot* m3 = view<BookSnapshot>(p, n)) {
        prefix(out_, "BookSnapshot", *m3);
        out_ << " depth=" << unsigned(m3->Depth);
        for (unsigned i = 0; i < m3->Depth && i < 10; ++i) out_ << " b" << i << '=' << m3->BidPx[i] << 'x' << m3->BidQty[i];
        for (unsigned i = 0; i < m3->Depth && i < 10; ++i) out_ << " a" << i << '=' << m3->AskPx[i] << 'x' << m3->AskQty[i];
        out_ << '\n';
    } else if (const InstrumentDefinition* m4 = view<InstrumentDefinition>(p, n)) {
        prefix(out_, "InstrumentDefinition", *m4);
        out_ << " symbol=" << chars(m4->Symbol, 32) << " exchange=" << chars(m4->Exchange, 16) << " base=" << chars(m4->Base, 16) << " quote=" << chars(m4->Quote, 16) << '\n';
    } else if (const Heartbeat* m5 = view<Heartbeat>(p, n)) {
        prefix(out_, "Heartbeat", *m5);
        out_ << '\n';
    } else if (const Status* m6 = view<Status>(p, n)) {
        prefix(out_, "Status", *m6);
        out_ << " status=" << unsigned(m6->TradingStatus) << '\n';
    } else if (const Ohlcv* m7 = view<Ohlcv>(p, n)) {
        prefix(out_, "Ohlcv", *m7);
        out_ << " o=" << m7->Open << " h=" << m7->High << " l=" << m7->Low << " c=" << m7->Close << " v=" << m7->Volume << '\n';
    } else {
        out_ << "Unknown len=" << len << '\n';
        return false;
    }
    return true;
}

} // namespace feeds
