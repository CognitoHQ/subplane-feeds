# Pack format

A pack is one YAML file per exchange (`packs/<exchange>.yaml`). It is loaded once at start; every key is validated and an unknown key or an impossible mapping is a startup error with the path to the offending line.

## Top level

| Key | Meaning |
|---|---|
| `exchange` | Name written into `InstrumentDefinition.Exchange` (16 chars max). |
| `venue_id` | `VenueId` on every message, 1..65535. |
| `connect` | How to connect and subscribe. |
| `streams` | The streams to subscribe per instrument and how each maps. |

## `connect`

| Key | Meaning |
|---|---|
| `url` | `wss://host[:port]/path`. Combined-stream endpoints are expected: every frame is a wrapper object naming its stream. |
| `wrapper` | `{ stream: <key>, data: <key> }`, the wrapper's field names (defaults `stream` and `data`). |
| `subscribe` | A YAML mapping emitted as JSON once after the handshake. The string `{streams}` is replaced by a JSON array of stream names. |
| `symbol_case` | `lower` (default) or `upper`: how venue symbols appear in stream names. |
| `reconnect_before_s` | Reconnect proactively after this many seconds (Binance closes connections at 24 h). 0 = never. |

## `streams[]`

Each entry has `name` with a `{symbol}` placeholder (the part without the placeholder routes incoming frames), and exactly one of:

- `message: <Trade|Quote|BookUpdate|BookSnapshot|Ohlcv|Status|InstrumentDefinition>` with `fields`: a flat mapping, one canonical message per frame.
- `book: <strategy>` with `fields` naming the strategy's roles, plus `snapshot: { url, fields }` when the strategy needs one. Known strategies: `binance_diff`.

## Field mapping

A field is either a path string or a mapping:

| Key | Meaning |
|---|---|
| `path` | Dot path into the frame's data object: `p`, `k.c`, `b.0.1` (array index). At most 4 steps. Lookups are unordered, so fields may be listed in any order. |
| `type` | `decimal` (default for int64 price/quantity fields: a decimal string or number becomes int64 at 1e-9, exact, no floating point), `int`, `uint`, `bool` (1/0), `string` (char arrays). |
| `unit` | For timestamps: `s`, `ms`, `us`, `ns`; the value is converted to nanoseconds. |
| `scale` | Integer multiplier applied after parsing (overflow is an error). |
| `map` | Token -> value table for enums. Tokens are the JSON scalar's text (`true`, `BUY`, `1`); values are canonical enum names (`Buy`, `Sell`, `Add`, `Delete`, `Trading`, ...) or integers. A token not in the table is a mapping error and the message is dropped (counted). |

The prefix fields (`InstrumentId`, `VenueId`, `Sequence`, `RecvTs`) are never mapped: the gateway sets them. `ExchangeTs` and `Flags` may be mapped.

Anything the vocabulary cannot express (multi-message frames, conditional fields, stateful decoding) is a strategy in code, not a richer language. That boundary is deliberate: a pack stays a description, not a program.

## Strategy `binance_diff`

Roles: `first_id` (U), `last_id` (u), `bids`, `asks` (arrays of `[price, qty]` with absolute quantities, 0 deletes), optional `ExchangeTs`. Snapshot roles: `last_id`, `bids`, `asks`; the snapshot URL may use `{SYMBOL}` (venue case) or `{symbol}` (stream case).

Sync rule: events are buffered until a snapshot arrives; events with `u <= lastUpdateId` are dropped; the first applied event must satisfy `U <= lastUpdateId + 1 <= u`; afterwards every event must have `U == previous u + 1`, otherwise the book is dropped and a new snapshot requested (a resync, counted). After a sync the gateway publishes one `BookSnapshot` (`Flags` bit 0 set when it followed a resync), then one `BookUpdate` per changed level, and another `BookSnapshot` every `--snapshot-every` updates.

## Known gaps (from the draft packs under `packs/draft/`)

Written 2026-10-13 against Coinbase, Kraken v2, OKX v5 and Bitstamp v2 frames to see where the vocabulary stops. In order of how many exchanges need them:

1. **Routing by fields, not stream name** (`connect.route: { type: <path>, symbol: <path> }` plus `match:` per stream): Coinbase (`type` + `product_id`), Kraken (`channel` + `data.0.symbol`), OKX (`arg.channel` + `arg.instId`). Only Binance and Bitstamp name the stream in a wrapper.
2. **Several records per frame** (`each: <path>`): Kraken and OKX put an array of records under `data`; one frame must yield one canonical message per element, with frame-level fields (OKX `action`, Kraken `type`) still reachable.
3. **ISO-8601 timestamps** (`type: iso8601`): Coinbase and Kraken. Exact parse to nanoseconds, no floating point.
4. **One subscribe message per channel** and non-string stream entries: Kraken and Bitstamp send one subscribe per channel; OKX subscribes with objects `{channel, instId}`; Coinbase wants `product_ids` plus a channel list. `subscribe` must become a list of templates with `{symbols}` / `{stream}` placeholders, not one message with `{streams}`.
5. **Symbol anywhere in the stream name** (`live_trades_{symbol}`): Bitstamp; routing by prefix and suffix instead of suffix only.
6. **Application-level ping** (`ping: { text: ping, every_s: 20 }`): OKX closes idle connections unless a text "ping" is sent; Binance uses WebSocket ping frames.
7. **Book strategies**: `l2_inband` (Coinbase: snapshot on the socket, then `[side, price, size]` changes), `l2_inband_checksum` (Kraken: levels as `{price, qty}` objects, CRC32 of the top ten), `okx_seq` (seqId/prevSeqId chain, levels with extra columns), `snapshot_only` (Bitstamp: full top-100 every event). Each is code; the pack only names it and binds its roles.

Not needed by any of the four: a richer expression language. Every value is still a path plus a conversion.
