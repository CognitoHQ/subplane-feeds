#include "book.h"

#include "binance_diff.h"
#include "l2_inband.h"
#include "snapshot_only.h"

#include <algorithm>

namespace feeds {

Book::Result Book::apply(Side side, int64_t px, int64_t qty) noexcept {
    std::vector<Level>& v = side == Side::Buy ? bids_ : asks_;
    // bids descend, asks ascend: find the first level not better than px.
    auto it = side == Side::Buy
        ? std::lower_bound(v.begin(), v.end(), px, [](const Level& l, int64_t p) { return l.px > p; })
        : std::lower_bound(v.begin(), v.end(), px, [](const Level& l, int64_t p) { return l.px < p; });
    const uint32_t idx = static_cast<uint32_t>(it - v.begin());
    if (it != v.end() && it->px == px) {
        if (qty == 0) { v.erase(it); return {Action::Delete, idx, true}; }
        it->qty = qty;
        return {Action::Update, idx, true};
    }
    if (qty == 0) return {Action::Delete, idx, false};
    v.insert(it, Level{px, qty});
    return {Action::Add, idx, true};
}

namespace {
bool side_of(const LevelLayout& lay, std::string_view tok, Side& out, std::string& err) noexcept {
    auto it = lay.side_map->find(std::string(tok));
    if (it == lay.side_map->end()) { err = "level side '" + std::string(tok) + "' not in map " + lay.side_map_name; return false; }
    uint8_t e = 0;
    if (!enum_value(it->second, e) || e > 2) { err = "level side maps to '" + it->second + "', not Buy|Sell"; return false; }
    out = static_cast<Side>(e);
    return true;
}
} // namespace

bool parse_levels(simdjson::ondemand::object& rec, const FieldMap& role, std::vector<LevelRow>& out, std::string& err) noexcept {
    out.clear();
    const LevelLayout& lay = *role.levels;
    simdjson::ondemand::value v;
    if (!walk(rec, role.path, v)) { err = role.target + ": path '" + role.path.text + "' not found"; return false; }
    simdjson::ondemand::array arr;
    if (v.get_array().get(arr) != simdjson::SUCCESS) { err = role.target + ": not an array"; return false; }
    for (auto el : arr) {
        LevelRow row;
        if (lay.object) {
            simdjson::ondemand::object o;
            if (el.get_object().get(o) != simdjson::SUCCESS) { err = role.target + ": level is not an object"; return false; }
            simdjson::ondemand::value x;
            if (o[lay.price_key].get(x) != simdjson::SUCCESS || !read_decimal(x, row.px)) { err = role.target + ": level '" + lay.price_key + "' missing or not a decimal"; return false; }
            if (o[lay.qty_key].get(x) != simdjson::SUCCESS || !read_decimal(x, row.qty)) { err = role.target + ": level '" + lay.qty_key + "' missing or not a decimal"; return false; }
            if (lay.has_side) {
                std::string_view tok;
                if (o[lay.side_key].get(x) != simdjson::SUCCESS || !read_token(x, tok)) { err = role.target + ": level '" + lay.side_key + "' missing"; return false; }
                if (!side_of(lay, tok, row.side, err)) return false;
            }
        } else {
            simdjson::ondemand::array cols;
            if (el.get_array().get(cols) != simdjson::SUCCESS) { err = role.target + ": level is not an array"; return false; }
            uint32_t i = 0, seen = 0;
            for (auto x : cols) {
                simdjson::ondemand::value xv;
                if (x.get(xv) != simdjson::SUCCESS) { err = role.target + ": bad level element"; return false; }
                if (i == lay.price_idx) { if (!read_decimal(xv, row.px)) { err = role.target + ": price is not a decimal"; return false; } ++seen; }
                else if (i == lay.qty_idx) { if (!read_decimal(xv, row.qty)) { err = role.target + ": qty is not a decimal"; return false; } ++seen; }
                else if (lay.has_side && i == lay.side_idx) { std::string_view tok; if (!read_token(xv, tok) || !side_of(lay, tok, row.side, err)) { if (err.empty()) err = role.target + ": bad side"; return false; } ++seen; }
                ++i;
            }
            if (seen < 2u + (lay.has_side ? 1u : 0u)) { err = role.target + ": level has too few columns for layout " + lay.name; return false; }
        }
        out.push_back(row);
    }
    return true;
}

bool parse_time_role(simdjson::ondemand::object& rec, const FieldMap* role, uint64_t& out, std::string& err) noexcept {
    out = 0;
    if (!role) return true;
    simdjson::ondemand::value v;
    if (!walk(rec, role->path, v)) return true;
    std::string_view tok;
    if (!read_token(v, tok) || !parse_time(tok, role->unit, out)) { err = "ExchangeTs: not a timestamp in the given format"; return false; }
    return true;
}

void BookBase::apply_rows(Side side, const std::vector<LevelRow>& rows, uint64_t ts, std::vector<BookEmit>& out) noexcept {
    for (const LevelRow& l : rows) {
        const Side s = l.side == Side::Unknown ? side : l.side;
        const Book::Result r = book_.apply(s, l.px, l.qty);
        if (!r.changed) continue;
        BookEmit e;
        e.kind = BookEmit::UPDATE; e.side = s; e.action = r.action;
        e.level = r.level < 10 ? static_cast<uint8_t>(r.level) : 255;
        e.px = l.px; e.qty = l.qty; e.exch_ts = ts;
        out.push_back(e);
        ++updates_since_snapshot_;
    }
}

void BookBase::emit_snapshot(uint64_t ts, bool resynced, std::vector<BookEmit>& out) noexcept {
    BookEmit e;
    e.kind = BookEmit::SNAPSHOT; e.exch_ts = ts; e.resynced = resynced;
    out.push_back(e);
    updates_since_snapshot_ = 0;
}

void BookBase::maybe_periodic_snapshot(uint64_t ts, std::vector<BookEmit>& out) noexcept {
    if (snapshot_every_ && updates_since_snapshot_ >= snapshot_every_) emit_snapshot(ts, false, out);
}

std::unique_ptr<BookStrategy> make_strategy(const Pack& pack, const StreamDef& sd, uint32_t snapshot_every, std::string& err) {
    if (sd.book == "binance_diff") return std::make_unique<BinanceDiff>(pack, sd, snapshot_every);
    if (sd.book == "l2_inband") return std::make_unique<L2Inband>(pack, sd, snapshot_every);
    if (sd.book == "snapshot_only") return std::make_unique<SnapshotOnly>(pack, sd, snapshot_every);
    err = "unknown book strategy '" + sd.book + "'";
    return nullptr;
}

} // namespace feeds
