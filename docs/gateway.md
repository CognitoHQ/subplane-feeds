# Gateway runtime

## Data path

One I/O thread. A WebSocket frame is parsed with simdjson On Demand, routed to a stream definition and an instrument (by the wrapper's stream name, or by the frame's type and symbol fields), split into records when the stream has `each`, mapped into a canonical struct on the stack, and offered to Aeron. Frame-scope values (`$.` paths, the symbol under `each`) are read before the records are iterated because On Demand only walks forward. No queue, no second thread on the path, no allocation per message after warm-up. REST snapshot fetches are async HTTPS requests on the same thread. The optional embedded Aeron media driver runs its own threads.

## Sequence

One counter for the whole gateway, every published message, gap-free, assigned at publish time. A message the publisher drops (no subscriber, sustained back pressure) still consumes its number, so the agent sees a real gap for a real loss. The agent partition is configured `framing: { length: schema, sequence: message.Sequence }`: an offer carries several messages back to back (see Batching).

## Heartbeats and definitions

`Heartbeat` (InstrumentId 0) every `--heartbeat-ms` (default 1000) whatever the market does, so the agent's heartbeat timeout stays meaningful. `InstrumentDefinition` for every instrument at connect, after every reconnect, and every `--definitions-ms` (default 60000): Symbol, Exchange, Base and Quote (from the instruments file), Status Trading. TickSize and LotSize are 0 until a pack learns them from the exchange's instrument endpoint.

## Instruments

`site/instruments.yaml` maps venue symbols to ids and optional base/quote. Ids are what apps filter on, so keep them stable; `--symbols` auto-numbers and warns.

## Books

Each (book stream, instrument) pair owns a strategy and a local book. Strategies that need a REST snapshot (`binance_diff`) ask for it through the gateway; the network loop fetches it. In-band strategies (`l2_inband`, `snapshot_only`) get everything from the socket. On disconnect every book is dropped and resynced after reconnect. Every sync (first sync, resync after a gap, in-band resync, republish) publishes the whole book: `BookUpdate{Action=Clear}`, then every level as `Add`, bids then asks, best first (`Level` is the index up to 9, 255 beyond), then the top-ten `BookSnapshot`. A reader that applies BookUpdates holds the exchange's full book from the Clear on. `--snapshot-every N` (default 100) republishes the top ten levels every N updates per instrument so late joiners and the agent's last-value cache converge quickly. `snapshot_only` books (Bitstamp) are whole in every event and publish only the `BookSnapshot`.

## Recovery

Whether an instrument's in-band state can be trusted goes on a **state stream** of its own (`--state-stream`, default 1002, same channel; `0` turns it off), as SPP `SourceState` (`contract/subplane_proto.xml`, template 30; spec `docs/protocol/subplane-protocol.md` in the agent repo). Never in the canonical data: recovery stays independent of the data schema, and apps see only market data.

| State | When |
|---|---|
| `Unknown` | from start until the instrument is first whole: connected, and every book of it synced |
| `Ok` (`Startup`, `Resync`, `Republish`) | once it is whole, after the burst that made it so |
| `Stale` (`Gap`, `Disconnect`) | when it was Ok and a book lost sync, or the connection dropped |

- **Ordering.** `Sequence` is the last data Sequence published before the state; the data batch is flushed first. A subscriber applies the state once its copy of the data stream reached that Sequence.
- **The beat.** On the heartbeat timer, a `SourceState` with InstrumentId 0: Ok while connected, Stale otherwise, carrying `SourceEpoch` (the process start; `--source-epoch` overrides, a replay uses 1), so a late subscriber learns the epoch and a restart without asking.
- **Republish.** `Gateway::republish(instrument | 0, venue, correlation)` queues instruments (a queued one joins, taking the newer correlation); `tick()` republishes at most `--republish-per-second` (default 20), one per tick: the definition, every synced book whole, then `SourceState` with the correlation. An unsynced book is not republished; its state says why. The control input that calls it (SPP `Republish` over Aeron) is the next step.
- **On `--out stdout`** the state stream is printed in the same output, in publish order: `SourceState iid=<n> venue=<n> seq=<n> state=<..> reason=<..> corr=<n> epoch=<n>`. Fixtures include these lines.
- **Cost.** A Binance resync is about 2,000 BookUpdates (64 B each, 93 offers of 1376 B) in one burst. On back pressure the rest of the burst is dropped and counted; downstream sees the gap and recovers by Republish.

## Recording and replay

`--record file.jsonl` writes `{"ts":<recv ns>,"frame":<frame>}`, `{"ts":..,"snapshot":"<SYMBOL>","body":<json>}` and `{"ts":..,"event":"connected"|"disconnected"}` lines. `--replay file.jsonl` feeds them back in order with the recorded timestamps, so the output is byte-for-byte deterministic; `--out stdout` prints one line per canonical message with raw integer fields. The fixture test and the agent end-to-end test are both replays.

## Subscribing and keeping the connection

Subscribe messages (one or several, from the pack's templates) are written one after another right after the handshake; `--subscribe` prints them without connecting. WebSocket ping frames are answered by the client; an application-level text ping (`connect.ping`) is sent on its own timer and its reply dropped before parsing.

## Counters and latency

Printed on exit and every minute: frames, published, dropped, per-type counts, mapping/book/parse errors, unmatched streams, unknown symbols, acks, resyncs, last sequence, and `frame_to_offer_ns`: a log-linear histogram (a copy of the agent's, `src/latency_hist.h`, so both repos report on the same bucket edges; p50, p99, p999, max, mean) of the wall time from entering `on_frame` to the last offer of that frame, over frames that published something. It includes the publisher's cost, so measure the gateway itself with `--out null` (counts and discards). On the 2026-10-13 fixtures replayed on the dev host with `--out null`: Binance p50 0.7 µs, p99 10 to 15 µs (depth diffs with many levels), Kraken p50 0.7 to 1 µs, Coinbase p50 1.7 µs, OKX p50 0.9 to 1.4 µs; `--out stdout` roughly doubles the p50 with iostream formatting.

The counters are one-writer relaxed atomics (`OneWriter`, `src/one_writer.h`): the gateway thread writes them with a plain load and store, as cheap as a `uint64_t`, and the HTTP thread reads them. A drop is counted by the publisher's reason: back pressure that outlasted the retry spin, no subscriber yet, or a failed publication.

## Observability

`--http 127.0.0.1:9101` starts an HTTP server on its own thread and io_context, never the gateway thread, serving `/metrics` (Prometheus text, or `?format=json`), `/topology` and `/health`, the contract shared with the agent (`docs/observability.md`, a copy of the agent's). `--instance`, `--site`, `--env` and `--labels k=v,...` set the identity. The server reads only the atomic counters and histograms, the immutable pack and instruments, and the publication's connected flag; it stops before the publication closes. On `--replay` with `--http` the gateway keeps serving the final numbers until `--seconds` or a signal, which is how `tests/http_test.sh` checks the routes.

## End to end, measured live

2026-10-13, Binance spot, BTCUSDT/ETHUSDT/SOLUSDT, 30 s per run: `spwsgw` (core 3) -> Aeron IPC, embedded driver in the agent, shared mode -> `subplaned` (pipeline on core 2, its other threads on core 5) -> ring -> `views_app --spin --latency` (core 4), Ryzen 5 3600, Ubuntu 22.04 container with `--shm-size=2g`, no core isolation, one CLOCK_REALTIME on all three. Hops are differences of three stamps per message: the gateway's `RecvTs` (frame receive), the agent's `ingest_ts` (poll), the app's read.

Per message type, gateway receive to agent poll, with the gateway spinning (`--spin`):

| Message | p50 | p90 | Why |
|---|---|---|---|
| Trade | 1.3 µs | 1.5 µs | one message per frame: parse, map, offer |
| Quote | 0.7 µs | 1.0 µs | same |
| BookUpdate | 28 µs | 28 µs | one depth frame yields a median of 13 and p90 of 72 BookUpdates, about 400 ns of work per level, and every message of the frame leaves when the frame is done |
| BookSnapshot (initial) | 570 µs | | a 1000-level REST body parsed and applied |

Without `--spin` the gateway's core sleeps in `epoll_wait` between frames and wakes cold: Trade and Quote were 4 to 6 µs p50 under the same conditions. Agent poll to app read is 0.7 µs p50 for single-message packets and 5 µs for the records of a batched depth frame (the agent decodes the batch's messages in order; each record's read waits for its predecessors). The Aeron IPC hop itself, measured with a synthetic publisher at 20 to 1000 messages per second, is 1.3 µs p50 publisher stamp to agent poll and 2 to 2.6 µs to the app read, in the container and on the host alike. What was first read as a slow Aeron hop (16 to 20 µs p50 over all messages) is the message-weighted cost of depth frames; the book's data structure (a sorted vector of a thousand levels, replaced by a map on 2026-10-13) was not the dominant term: replay numbers barely moved.

What would move the BookUpdate number, in order of effect: not emitting updates deeper than the published depth (most changes are deep; a strategy option), cutting the per-level work (decimal parse, map insert with allocation, a 72-byte message built from scratch and copied into the batch), and a schema change that carries a frame's levels in one message. None is done; the choice is a design decision.

## On the path

One I/O thread, no queue, no lock. Per frame: one indirect call from the WebSocket client (a function pointer with a context, not `std::function`), one simdjson On Demand parse, string views into the frame for frame-scope values, no allocation after warm-up (error strings are built only when a record fails). Per book record one virtual call into the strategy; per offer one virtual call into the publisher. `--spin` polls the io_context instead of sleeping so the core stays hot between frames; pin it with `taskset`.

## Batching

The messages one input frame produces are appended back to back into one buffer and offered once when the frame has been processed, or earlier when the next message would exceed `--batch-bytes` (default 1376, one Aeron MTU of 1408 minus the 32-byte frame header, so an offer never fragments). Heartbeats and definitions from the timer are flushed at the end of the tick. There is no timer-driven flush: the last message of a frame leaves at the same moment it would have unbatched, so batching adds no latency; it only amortises the Aeron frame header and the offer. Each message is self-framed by its SBE header, so the agent walks a batch apart with `framing: { length: schema }` and any other subscriber does the same; there is no envelope. `--batch-bytes 0` offers one message at a time. On the fixtures with `--out null`: Binance 7048 messages in 2729 offers instead of 7048, Coinbase 628 in 111, Kraken 832 in 632, OKX unchanged (one record per frame); frame-to-offer p50 rose by about 0.3 µs on Binance for the copy into the batch buffer, which the Aeron offer it saves costs more than.
