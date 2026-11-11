#pragma once

// Fills the SBE header and the 40-byte canonical prefix of any message. The
// pack never sets these: InstrumentId, VenueId, Sequence, InstrumentSeq and
// RecvTs are the gateway's (Sequence and InstrumentSeq at publish), ExchangeTs
// and Flags are written by the mapper when mapped.

#include "canonical_fields.h"

#include <cstring>

namespace feeds {

inline void begin_message(uint8_t* out, const CanonicalMessage& m, uint32_t instrument, uint16_t venue,
                          uint64_t seq, uint64_t recv_ts) noexcept {
    std::memset(out, 0, m.size);
    const uint16_t block = static_cast<uint16_t>(m.size - sp_canonical::kHeaderSize);
    const uint16_t tid = m.template_id, sid = sp_canonical::kSchemaId, ver = sp_canonical::kSchemaVersion;
    std::memcpy(out + 0, &block, 2);
    std::memcpy(out + 2, &tid, 2);
    std::memcpy(out + 4, &sid, 2);
    std::memcpy(out + 6, &ver, 2);
    std::memcpy(out + kPrefixInstrumentId, &instrument, 4);
    std::memcpy(out + kPrefixVenueId, &venue, 2);
    std::memcpy(out + kPrefixSequence, &seq, 8);
    std::memcpy(out + kPrefixRecvTs, &recv_ts, 8);
}

template <class T> inline void put(uint8_t* msg, uint16_t off, T v) noexcept { std::memcpy(msg + off, &v, sizeof v); }
inline void put_u64(uint8_t* msg, uint16_t off, uint64_t v) noexcept { put(msg, off, v); }
inline void put_i64(uint8_t* msg, uint16_t off, int64_t v) noexcept { put(msg, off, v); }

} // namespace feeds
