# subplane-feeds

Gateways that turn an exchange's WebSocket feed into canonical SBE for the [subplane](https://github.com/CognitoHQ/subplane) agent. One `spwsgw` binary; each exchange is a YAML pack. The gateway opens one connection per box, normalizes trades, quotes and the order book into the canonical schema, and publishes them over Aeron. The agent on the same box (or on any box that can reach the stream) ingests that as an ordinary `aeron` partition and fans it out to every app.

```
                     exchange box                               the application box
 ┌────────────┐               ┌─────────────────────┐          ┌────────────────────────────┐      ┌───────────┐
 │  Exchange  │   WSS / JSON  │  spwsgw             │  Aeron   │  subplaned                 │ SHM  │  app      │
 │  Binance   │──────────────▶│  pack: binance-spot │─────────▶│  partition: crypto         │─────▶│  views    │
 │  Coinbase  │  exchange's   │  registry           │ canonical│   transport: aeron         │ ring │  codec    │
 │  Kraken    │  own format,  │  JSON → canonical   │  SBE,    │   framing: schema walk     │ per  │  filter   │
 │  ...       │  REST         │  book strategies    │  whole   │   decoder: sbe_routing     │ sess │  by id    │
 └────────────┘  snapshots    │  instruments → ids  │  msgs per│   (InstrumentId, Sequence, │      └───────────┘
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
scripts/dev.sh test        # build ./build in the toolchain image (Ubuntu 26.04, GCC 16), then ctest
# The binaries need 26.04's glibc: on another host, prefix each command with scripts/dev.sh run.
# On an Ubuntu 26.04 host, sudo scripts/deps-2604.sh installs the toolchain and plain cmake works.

# Dry run against Binance, canonical messages as text:
scripts/dev.sh run build/spwsgw --pack packs/binance-spot.yaml --symbols BTCUSDT,ETHUSDT --out stdout --seconds 10

# For real: Aeron IPC into an agent whose partition is `transport: {type: aeron, channel: "aeron:ipc", stream_id: 1001}`
scripts/dev.sh run build/spwsgw --pack packs/binance-spot.yaml --instruments site/instruments.example.yaml --channel aeron:ipc --stream-id 1001
```

`--instruments` gives each venue symbol a stable `InstrumentId`: copy `site/instruments.example.yaml` (Binance's spelling, `BTCUSDT`) and add each venue's symbols as that venue spells them (`BTC-USD`); `--symbols` auto-numbers for quick runs. `--spin` keeps the core hot between frames (trades and quotes reach the agent in about 1 µs instead of 5). `--batch-bytes N` (default 1376, one Aeron MTU after its header) lets the messages one frame produced share an offer; the batch is flushed when the frame ends, never on a timer. `--out null` counts and discards, for measuring (`frame_to_offer_ns` on the stats line). `--record frames.jsonl` saves every raw frame and snapshot; `--replay frames.jsonl` plays a recording through the same pack without the exchange, which is how the tests work.

`--http 127.0.0.1:9101` serves `/metrics` (Prometheus), `/topology` and `/health` from a separate thread, with the agent's observability contract (`docs/observability.md`).

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

The models are the canonical schema (`spwsgw models` lists them); conversions default from each field's kind and the named ones live in `packs/registry.yaml` (enum maps, level layouts; the time units `s`, `ms`, `us`, `ns` and `iso8601` are built in), so packs do not repeat them. The vocabulary is closed on purpose: a path, a named conversion, and for frames that carry several records `each`. Order-book protocols, authentication and reconnect semantics are strategies in code, named in the pack. `docs/pack-format.md` has the full reference, `docs/gateway.md` the runtime rules (sequence, heartbeats, instrument definitions, snapshots, recording). `spwsgw-snap` is the gateway's snapshot service: it rebuilds every instrument from the gateway's stream and answers the agent's recap requests (`docs/snap.md`).

## Contract with the agent

For data, only two things: `contract/subplane_canonical.xml` (schema id 100) with its generated header, both copied from the agent repo, and the framing rule: an Aeron offer carries one or more whole canonical messages back to back, each self-framed by its SBE header, `Sequence` in every message. The agent's partition uses `framing: { length: schema, sequence: message.Sequence }` and walks them apart; no envelope. No shared library in either direction. For operations, the observability contract (`docs/observability.md`) and its endpoint-key vectors (`contract/topology-keys.json`), also copies; `src/obs/` holds copies of the agent's Prometheus writer and key rules.

## Status

| Pack | Trades | Quotes | Book | Notes |
|---|---|---|---|---|
| `binance-spot` | yes | bookTicker | `binance_diff` (REST snapshot + diffs) | `*.binance.vision` hosts |
| `coinbase-exchange` | yes | ticker | `l2_inband` (snapshot on the socket, then changes) | ISO-8601 times, routed by `type` |
| `kraken` | yes | ticker | `l2_inband` (checksum not verified) | v2, several records per frame |
| `bitstamp` | yes | no | `snapshot_only` (full top 100 per event) | symbol at the end of the channel name |
| `okx` | yes | tickers | not yet (`okx_seq`, see docs) | text ping/pong |

Each pack is tested on a recording of the live exchange (`recordings/<pack>/`, local and gitignored), replayed byte-for-byte in `ctest` and end to end through a local agent build (`-DSUBPLANE_BUILD_DIR=...`): gateway, Aeron IPC, `subplaned`, ring, `views_app`. All five were validated live on 2026-10-13 with zero mapping, book or parse errors and zero resyncs. Recordings are not committed, since the exchanges' terms do not allow redistributing their data; `scripts/record-fixture.sh <pack>` records your own in a few seconds (`fixtures/README.md`), and the replay tests skip until you do.

## Licence

Apache License 2.0 (see `LICENSE` and `NOTICE`): use, modify and ship the gateways as you like. The agent they feed, [subplane](https://github.com/CognitoHQ/subplane), is source-available under FSL-1.1-ALv2, with its app contract (the canonical schema and views copied into `contract/`) under Apache-2.0. Contribution guidelines and the CLA are in `CONTRIBUTING.md`.
