# subplane-feeds

Gateways that turn an exchange's WebSocket feed into canonical SBE for the [subplane](https://github.com/satyan-g/subplane) agent. One `spwsgw` binary; each exchange is a YAML pack. The gateway opens one connection per box, normalizes trades, quotes and the order book into the canonical schema, and publishes them over Aeron. The agent on the same box (or on any box that can reach the stream) ingests that as an ordinary `aeron` partition and fans it out to every app.

```
                     exchange box                               the application box
 ┌────────────┐               ┌─────────────────────┐          ┌────────────────────────────┐      ┌───────────┐
 │  Exchange  │   WSS / JSON  │  spwsgw             │  Aeron   │  subplaned                 │ SHM  │  app      │
 │  Binance   │──────────────▶│  pack: binance-spot │─────────▶│  partition: crypto         │─────▶│  views    │
 │  Coinbase  │  exchange's   │  registry           │ canonical│   transport: aeron         │ ring │  codec    │
 │  Kraken    │  own format,  │  JSON → canonical   │  SBE,    │   framing: packet          │ per  │  filter   │
 │  ...       │  REST         │  book strategies    │  one msg │   decoder: sbe_routing     │ sess │  by id    │
 └────────────┘  snapshots    │  instruments → ids  │  per     │   (InstrumentId, Sequence, │      └───────────┘
                              │  Sequence, Heartbeat│  offer,  │    ExchangeTs, VenueId)    │ ┌───────────┐
      one process per pack    │  InstrumentDefinition  gap-free│  sessions: filter + ring   │ │  app 2    │
                              └─────────────────────┘          │  LVC, control, HTTP        │ └───────────┘
                                                               └────────────────────────────┘
 ┌────────────┐  UDP multicast, the firm's own SBE schema                           ▲
 │ internal   │──────────────────────────────────────────────────────────────────────┘
 │ SBE bus    │  the agent's primary ingest: route on the schema, deliver the bytes untouched
 └────────────┘
```

Two ways into the agent. The firm's internal SBE bus is what `subplaned` was built for. Every external feed comes in through a gateway like this one, normalised to the canonical schema, so apps see one schema whatever the source; today that is exchange WebSocket feeds, later FIX, Kafka and vendor feeds as sibling handlers in this repo. Bytes change shape once, in the gateway; from Aeron onward every hop carries the same canonical SBE with the 32-byte prefix at fixed offsets, and the agent and the ring never decode a body. (The agent can also ingest some external formats directly, DBN and FlatBuffers, because its routing core is encoding-blind; that is an option for a feed already on the wire, not the recommended route.)

## Why

Every crypto shop rewrites the same layer in Python per process: connect, parse JSON, keep a book, hand ticks to a strategy. Exchanges cap connections per IP and JSON parsing is the whole cost. This does it once per box in C++ with a microsecond handoff, and apps read typed canonical messages without knowing which exchange they came from.

## Run it

```bash
sudo apt install libboost-dev libssl-dev libyaml-cpp-dev   # CMake 3.30+ (see scripts/deps-2204.sh on Ubuntu 22.04)
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j

# Dry run against Binance, canonical messages as text:
build/spwsgw --pack packs/binance-spot.yaml --symbols BTCUSDT,ETHUSDT --out stdout --seconds 10

# For real: Aeron IPC into an agent whose partition is `transport: {type: aeron, channel: "aeron:ipc", stream_id: 1001}`
build/spwsgw --pack packs/binance-spot.yaml --instruments site/instruments.yaml --channel aeron:ipc --stream-id 1001
```

`--instruments` gives each venue symbol a stable `InstrumentId` (`site/instruments.example.yaml`); `--symbols` auto-numbers for quick runs. `--batch-bytes N` (default 1376, one Aeron MTU after its header) lets the messages one frame produced share an offer; the batch is flushed when the frame ends, never on a timer. `--out null` counts and discards, for measuring (`frame_to_offer_ns` on the stats line). `--record frames.jsonl` saves every raw frame and snapshot; `--replay frames.jsonl` plays a recording through the same pack without the exchange, which is how the tests work.

## What a pack is

```yaml
exchange: binance-spot
venue_id: 10
connect:
  url: wss://data-stream.binance.vision/stream
  wrapper: { stream: stream, data: data }       # frames name their stream; Coinbase/Kraken/OKX route by fields instead
  subscribe: { method: SUBSCRIBE, params: "{streams}", id: 1 }
streams:
  - name: "{symbol}@trade"
    message: Trade
    fields:
      ExchangeTs: T@ms                            # path@conversion; the conversion comes from the registry
      Price:      p                               # "65432.10000000" -> int64 65432100000000 (1e-9 units), no floating point
      Aggressor:  m@taker_from_maker_bool
  - name: "{symbol}@depth@100ms"
    book: binance_diff                            # a compiled-in strategy for what YAML cannot say
    fields: { first_id: U, last_id: u, bids: b, asks: a }
    snapshot: { url: "https://data-api.binance.vision/api/v3/depth?symbol={SYMBOL}&limit=1000", fields: { last_id: lastUpdateId, bids: bids, asks: asks } }
```

The models are the canonical schema (`spwsgw models` lists them); conversions default from each field's kind and the named ones (time formats, enum maps, level layouts) live in `packs/registry.yaml`, so packs do not repeat them. The vocabulary is closed on purpose: a path, a named conversion, and for frames that carry several records `each`. Order-book protocols, authentication and reconnect semantics are strategies in code, named in the pack. `docs/pack-format.md` has the full reference, `docs/gateway.md` the runtime rules (sequence, heartbeats, instrument definitions, snapshots, recording).

## Contract with the agent

Only two things: `contract/subplane_canonical.xml` (schema id 100) with its generated header, both copied from the agent repo, and the framing rule: an Aeron offer carries one or more whole canonical messages back to back, each self-framed by its SBE header, `Sequence` in every message. The agent's partition uses `framing: { length: schema, sequence: message.Sequence }` and walks them apart; no envelope. No shared library in either direction.

## Status

| Pack | Trades | Quotes | Book | Notes |
|---|---|---|---|---|
| `binance-spot` | yes | bookTicker | `binance_diff` (REST snapshot + diffs) | `*.binance.vision` hosts |
| `coinbase-exchange` | yes | ticker | `l2_inband` (snapshot on the socket, then changes) | ISO-8601 times, routed by `type` |
| `kraken` | yes | ticker | `l2_inband` (checksum not verified) | v2, several records per frame |
| `bitstamp` | yes | no | `snapshot_only` (full top 100 per event) | symbol at the end of the channel name |
| `okx` | yes | tickers | not yet (`okx_seq`, see docs) | text ping/pong |

Each pack has a recorded fixture (`fixtures/<pack>/`) replayed byte-for-byte in `ctest`, and the same replay runs end to end through a local agent build (`-DSUBPLANE_BUILD_DIR=...`): gateway, Aeron IPC, `subplaned`, ring, `views_app`. All recorded live on 2026-10-13 with zero mapping, book or parse errors and zero resyncs.

Licence: Apache-2.0.
