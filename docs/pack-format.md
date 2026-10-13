# Pack format

A pack is one YAML file per exchange (`packs/<exchange>.yaml`). It is loaded once at start; every key is validated and an unknown key or an impossible mapping is a startup error with the path to the offending line. What packs share lives in the registry (`packs/registry.yaml`); what no YAML can say (order-book sync protocols) is a compiled-in strategy named in the pack.

## Models and the registry

The models are the canonical schema itself: `spwsgw models` lists every message with its fields and kinds. A pack names a field and gives it a path; the conversion follows from the field's kind (decimal fields parse a decimal string or number into int64 at 1e-9, exact; unsigned fields parse integers; char fields take strings). A pack names a conversion only when the wire needs one, as `path@name`:

```yaml
ExchangeTs: T@ms                       # a time format
Aggressor:  m@taker_from_maker_bool    # an enum map
bids:       bids@objects               # a level layout (book roles)
```

`name` resolves against the registry by what the target needs. `packs/registry.yaml` ships the defaults; it is loaded from next to the pack (or `--registry`), and a pack may add entries under its own `registry:` key.

| Section | Entries | Built in |
|---|---|---|
| `times` | `name: <time format>` aliases | `s`, `ms`, `us`, `ns`, `iso8601` (`YYYY-MM-DDTHH:MM:SS[.f{1,9}]Z`, exact, nothing else accepted) |
| `maps` | `name: { token: value, ... }`; tokens are the JSON scalar's text (`true`, `BUY`, `1`), values canonical enum names (`Buy`, `Sell`, `Add`, `Delete`, `Trading`, ...) or integers | none; the registry file has `side`, `taker_from_maker_side`, `taker_from_maker_bool`, `bitstamp_type` |
| `levels` | how one price level is laid out: `{ kind: array, price: 0, qty: 1 [, side: 2, side_map: side] }` or `{ kind: object, price: <key>, qty: <key> [, side: <key>, side_map: <map>] }` | `pairs` (`[price, qty]`); the file adds `objects` (Kraken) and `side_pairs` (Coinbase changes) |

The long form stays for the rare case: `{ path, type: decimal|int|uint|bool|string|iso8601, unit: <time>, scale: <int>, map: <name> | { inline } , levels: <name> }`.

## Top level

| Key | Meaning |
|---|---|
| `exchange` | Name written into `InstrumentDefinition.Exchange` (16 chars max). |
| `venue_id` | `VenueId` on every message, 1..65535. |
| `registry` | Local additions to the registry (same sections). |
| `connect` | How to connect, subscribe and route frames. |
| `streams` | The streams to subscribe per instrument and how each maps. |

## `connect`

| Key | Meaning |
|---|---|
| `url` | `wss://host[:port]/path`. |
| `wrapper` | `{ stream: <key>, data: <key> [, type: <key>] }`: frames carry their stream name (Binance `stream`, Bitstamp `channel`) and the record under `data`. `type` names a field whose value a stream must `match` (Bitstamp `event`: `trade`, `data`; subscription acks do not match and are counted). |
| `route` | `{ type: <path>, symbol: <path> }`: no wrapper; the frame's `type` value selects the stream whose `match` lists it, and `symbol` names the instrument (Coinbase `type` + `product_id`, Kraken `channel` + `symbol`, OKX `arg.channel` + `arg.instId`). Exactly one of `wrapper` and `route`. |
| `subscribe` | One JSON template (a YAML mapping) or a list of them, sent in order after the handshake. Placeholders: `"{streams}"` becomes the array of stream names (or of `stream_object`s), `"{symbols}"` the array of venue symbols, a template with `{stream}` is sent once per stream name, one with `{channel}` once per stream definition. Quoted YAML scalars stay JSON strings, bare numbers stay numbers. |
| `stream_object` | A JSON template for each `{streams}` entry when the exchange subscribes with objects: OKX `{ channel: "{channel}", instId: "{SYMBOL}" }`. |
| `ping` | `{ text: ping, every_s: 20, reply: pong }`: send an application-level text ping on a timer and drop the reply (OKX). WebSocket ping frames are always answered. |
| `symbol_case` | `lower` (default) or `upper`: how venue symbols appear in stream names. |
| `reconnect_before_s` | Reconnect proactively after this many seconds (Binance closes connections at 24 h). 0 = never. |

Placeholders in names and URLs: `{symbol}` (stream case), `{SYMBOL}` (venue spelling), `{channel}`.

## `streams[]`

| Key | Meaning |
|---|---|
| `name` | Template with `{symbol}` anywhere: `{symbol}@trade`, `live_trades_{symbol}`. In wrapper mode the parts around the placeholder route incoming frames; in route mode the name is only used for subscribing. |
| `match` | Frame type value(s) this stream accepts (`route` mode, required; `wrapper.type`, optional). One stream may take several (`[snapshot, l2update]`). |
| `channel` | The `{channel}` value for subscribe templates; defaults to the name without `{symbol}` and separators (`{symbol}@trades` gives `trades`). |
| `each` | A path to an array on the frame: one record per element (Kraken, OKX `data`). Field paths then address the element; a path starting with `$.` addresses the frame (`kind: $.type`). With `route`, the symbol resolves per record unless its path starts with `$.`. |
| `message` | A canonical message with `fields`: one message per record. |
| `book` | A strategy with `fields` naming its roles, plus `snapshot: { url, fields }` when it needs one. |
| `snapshot_kind` | For `l2_inband`: the `kind` value that carries a full book (default `snapshot`). |

Exactly one of `message` and `book`. The prefix fields (`InstrumentId`, `VenueId`, `Sequence`, `RecvTs`) are never mapped: the gateway sets them. `ExchangeTs` and `Flags` may be mapped.

## Paths

Dot paths into the record: `p`, `arg.channel`, `b.0.1` (array index). At most 4 steps. Lookups are unordered, so fields may be listed in any order. `$.` prefixes a frame-scope path under `each`.

Anything the vocabulary cannot express (conditional fields, stateful decoding) is a strategy in code, not a richer language. That boundary is deliberate: a pack stays a description, not a program.

## Strategies

Every strategy keeps a local book per instrument, emits one `BookUpdate` per changed level (absolute quantity, 0 deletes, level index 0..9 or 255 beyond the top ten), a `BookSnapshot` of the top ten after each sync (`Flags` bit 0 set when it followed a resync) and another every `--snapshot-every` updates. Book roles that are level arrays take a level layout (`@pairs` by default); `ExchangeTs` takes a time format and is 0 when the record lacks it.

### `binance_diff`

Roles: `first_id` (U), `last_id` (u), `bids`, `asks`, optional `ExchangeTs`. Snapshot roles: `last_id`, `bids`, `asks`; the snapshot URL may use `{SYMBOL}` or `{symbol}`.

Sync rule: events are buffered until a snapshot arrives; events with `u <= lastUpdateId` are dropped; the first applied event must satisfy `U <= lastUpdateId + 1 <= u`; afterwards every event must have `U == previous u + 1`, otherwise the book is dropped and a new snapshot requested (a resync, counted). Only the `BookSnapshot` is published after a sync, never the diffs applied while catching up.

### `l2_inband`

The socket delivers a full book first, then absolute-size changes. The stream's `match` values (Coinbase `[snapshot, l2update]`) or the `kind` role (Kraken `kind: $.type`) tell the two apart; the one equal to `snapshot_kind` replaces the book and emits a snapshot (a second one is a resync). Update roles: `changes` (rows carrying their side, Coinbase `[side, price, size]` via `@side_pairs`) or `bids`/`asks` (Kraken, `@objects`). Updates before the first snapshot are dropped. No REST. Kraken's CRC32 checksum and its depth truncation rule are not applied yet (see below).

### `snapshot_only`

Every event is the whole book (Bitstamp `order_book_*`): replace it, emit one `BookSnapshot`. Roles: `bids`, `asks`, optional `ExchangeTs`.

## Still missing

Written against Coinbase, Kraken v2, OKX v5 and Bitstamp v2 on 2026-10-13. What the shipped packs do not do:

- **OKX books**: the `books` channel chains frames by `seqId`/`prevSeqId` with `action: snapshot|update` on the frame and a checksum. Needs an `okx_seq` strategy; the pack keeps the stream commented out with the reason.
- **Kraken checksum and depth**: `l2_inband` does not verify the CRC32 of the top ten nor drop levels past the subscribed depth; a mismatch would go unnoticed until the next snapshot.
- **Authentication**: private streams need a signing strategy; not before a user asks.
- A richer expression language: none of the four needed it. Every value is still a path plus a named conversion.
