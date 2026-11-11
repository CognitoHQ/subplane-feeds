#pragma once

// The canonical messages as a field table (name, payload offset, size, kind), so
// a pack can name a target field and the mapper can write it without generated
// setters. Offsets come from the generated structs; tests/test_contract.cpp pins
// them against contract/subplane_canonical.xml.

#include "subplane_canonical_views.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace feeds {

enum class FieldKind : uint8_t { I64, U64, U32, U16, U8, CHARS, I64_ARRAY };

struct CanonicalField {
    std::string_view name;
    uint16_t offset;   // from the start of the message (header included)
    uint16_t size;
    FieldKind kind;
    uint16_t count = 1; // elements for arrays / chars
};

struct CanonicalMessage {
    std::string_view name;
    uint16_t template_id;
    uint16_t size;              // header + block
    const CanonicalField* fields;
    size_t nfields;
};

// The 40-byte prefix shared by every message, at the same offsets.
inline constexpr uint16_t kPrefixInstrumentId = static_cast<uint16_t>(offsetof(sp_canonical::Trade, InstrumentId));
inline constexpr uint16_t kPrefixVenueId      = static_cast<uint16_t>(offsetof(sp_canonical::Trade, VenueId));
inline constexpr uint16_t kPrefixFlags        = static_cast<uint16_t>(offsetof(sp_canonical::Trade, Flags));
inline constexpr uint16_t kPrefixSequence     = static_cast<uint16_t>(offsetof(sp_canonical::Trade, Sequence));
inline constexpr uint16_t kPrefixInstrumentSeq = static_cast<uint16_t>(offsetof(sp_canonical::Trade, InstrumentSeq));
inline constexpr uint16_t kPrefixExchangeTs   = static_cast<uint16_t>(offsetof(sp_canonical::Trade, ExchangeTs));
inline constexpr uint16_t kPrefixRecvTs       = static_cast<uint16_t>(offsetof(sp_canonical::Trade, RecvTs));

#define FEEDS_FIELD(T, F, K) CanonicalField{#F, static_cast<uint16_t>(offsetof(sp_canonical::T, F)), static_cast<uint16_t>(sizeof(sp_canonical::T::F)), FieldKind::K}
#define FEEDS_ARRAY(T, F, K, N) CanonicalField{#F, static_cast<uint16_t>(offsetof(sp_canonical::T, F)), static_cast<uint16_t>(sizeof(sp_canonical::T::F)), FieldKind::K, N}

inline constexpr CanonicalField kTradeFields[] = {
    FEEDS_FIELD(Trade, ExchangeTs, U64), FEEDS_FIELD(Trade, Flags, U16),
    FEEDS_FIELD(Trade, Price, I64), FEEDS_FIELD(Trade, Qty, I64), FEEDS_FIELD(Trade, TradeId, U64), FEEDS_FIELD(Trade, Aggressor, U8),
};
inline constexpr CanonicalField kQuoteFields[] = {
    FEEDS_FIELD(Quote, ExchangeTs, U64), FEEDS_FIELD(Quote, Flags, U16),
    FEEDS_FIELD(Quote, BidPx, I64), FEEDS_FIELD(Quote, BidQty, I64), FEEDS_FIELD(Quote, AskPx, I64), FEEDS_FIELD(Quote, AskQty, I64),
};
inline constexpr CanonicalField kBookUpdateFields[] = {
    FEEDS_FIELD(BookUpdate, ExchangeTs, U64), FEEDS_FIELD(BookUpdate, Flags, U16),
    FEEDS_FIELD(BookUpdate, Price, I64), FEEDS_FIELD(BookUpdate, Qty, I64), FEEDS_FIELD(BookUpdate, Side, U8),
    FEEDS_FIELD(BookUpdate, Action, U8), FEEDS_FIELD(BookUpdate, Level, U8),
};
inline constexpr CanonicalField kBookSnapshotFields[] = {
    FEEDS_FIELD(BookSnapshot, ExchangeTs, U64), FEEDS_FIELD(BookSnapshot, Flags, U16),
    FEEDS_ARRAY(BookSnapshot, BidPx, I64_ARRAY, 10), FEEDS_ARRAY(BookSnapshot, BidQty, I64_ARRAY, 10),
    FEEDS_ARRAY(BookSnapshot, AskPx, I64_ARRAY, 10), FEEDS_ARRAY(BookSnapshot, AskQty, I64_ARRAY, 10), FEEDS_FIELD(BookSnapshot, Depth, U8),
};
inline constexpr CanonicalField kOhlcvFields[] = {
    FEEDS_FIELD(Ohlcv, ExchangeTs, U64), FEEDS_FIELD(Ohlcv, Flags, U16),
    FEEDS_FIELD(Ohlcv, Open, I64), FEEDS_FIELD(Ohlcv, High, I64), FEEDS_FIELD(Ohlcv, Low, I64), FEEDS_FIELD(Ohlcv, Close, I64),
    FEEDS_FIELD(Ohlcv, Volume, I64), FEEDS_FIELD(Ohlcv, IntervalSec, U32),
};
inline constexpr CanonicalField kStatusFields[] = {
    FEEDS_FIELD(Status, ExchangeTs, U64), FEEDS_FIELD(Status, Flags, U16),
    FEEDS_FIELD(Status, TradingStatus, U8), FEEDS_FIELD(Status, Reason, U8),
};
inline constexpr CanonicalField kInstrumentDefinitionFields[] = {
    FEEDS_FIELD(InstrumentDefinition, ExchangeTs, U64), FEEDS_FIELD(InstrumentDefinition, Flags, U16),
    FEEDS_ARRAY(InstrumentDefinition, Symbol, CHARS, 32), FEEDS_ARRAY(InstrumentDefinition, Exchange, CHARS, 16),
    FEEDS_ARRAY(InstrumentDefinition, Base, CHARS, 16), FEEDS_ARRAY(InstrumentDefinition, Quote, CHARS, 16),
    FEEDS_FIELD(InstrumentDefinition, TickSize, I64), FEEDS_FIELD(InstrumentDefinition, LotSize, I64), FEEDS_FIELD(InstrumentDefinition, Status, U8),
};
inline constexpr CanonicalField kHeartbeatFields[] = {
    FEEDS_FIELD(Heartbeat, ExchangeTs, U64), FEEDS_FIELD(Heartbeat, Flags, U16),
    FEEDS_FIELD(Heartbeat, StreamId, U32),   // int32 in the schema, always > 0 (topic_id <= 32767); the gateway writes it (--topic)
};
#undef FEEDS_FIELD
#undef FEEDS_ARRAY

inline constexpr CanonicalMessage kCanonicalMessages[] = {
    {"Trade", sp_canonical::Trade::kTemplateId, static_cast<uint16_t>(sp_canonical::Trade::kSize), kTradeFields, std::size(kTradeFields)},
    {"Quote", sp_canonical::Quote::kTemplateId, static_cast<uint16_t>(sp_canonical::Quote::kSize), kQuoteFields, std::size(kQuoteFields)},
    {"BookUpdate", sp_canonical::BookUpdate::kTemplateId, static_cast<uint16_t>(sp_canonical::BookUpdate::kSize), kBookUpdateFields, std::size(kBookUpdateFields)},
    {"BookSnapshot", sp_canonical::BookSnapshot::kTemplateId, static_cast<uint16_t>(sp_canonical::BookSnapshot::kSize), kBookSnapshotFields, std::size(kBookSnapshotFields)},
    {"Ohlcv", sp_canonical::Ohlcv::kTemplateId, static_cast<uint16_t>(sp_canonical::Ohlcv::kSize), kOhlcvFields, std::size(kOhlcvFields)},
    {"Status", sp_canonical::Status::kTemplateId, static_cast<uint16_t>(sp_canonical::Status::kSize), kStatusFields, std::size(kStatusFields)},
    {"InstrumentDefinition", sp_canonical::InstrumentDefinition::kTemplateId, static_cast<uint16_t>(sp_canonical::InstrumentDefinition::kSize), kInstrumentDefinitionFields, std::size(kInstrumentDefinitionFields)},
    {"Heartbeat", sp_canonical::Heartbeat::kTemplateId, static_cast<uint16_t>(sp_canonical::Heartbeat::kSize), kHeartbeatFields, std::size(kHeartbeatFields)},
};

inline const CanonicalMessage* find_message(std::string_view name) noexcept {
    for (const auto& m : kCanonicalMessages) if (m.name == name) return &m;
    return nullptr;
}
inline const CanonicalField* find_field(const CanonicalMessage& m, std::string_view name) noexcept {
    for (size_t i = 0; i < m.nfields; ++i) if (m.fields[i].name == name) return &m.fields[i];
    return nullptr;
}

// Enum names a pack may use in `map:` tables, resolved to their wire values.
inline bool enum_value(std::string_view name, uint8_t& out) noexcept {
    struct E { std::string_view n; uint8_t v; };
    static constexpr E kEnums[] = {
        {"Unknown", 0}, {"Buy", 1}, {"Sell", 2},                       // Side
        {"Add", 1}, {"Update", 2}, {"Delete", 3}, {"Clear", 4},         // BookAction
        {"Trading", 1}, {"Halted", 2}, {"Closed", 3}, {"PreOpen", 4}, {"Auction", 5}, // TradingStatus
    };
    for (const auto& e : kEnums) if (e.n == name) { out = e.v; return true; }
    return false;
}

} // namespace feeds
