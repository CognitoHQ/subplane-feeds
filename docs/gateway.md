# Gateway runtime

## Data path

One I/O thread. A WebSocket frame is parsed with simdjson On Demand, routed to a stream definition and an instrument (by the wrapper's stream name, or by the frame's type and symbol fields), split into records when the stream has `each`, mapped into a canonical struct on the stack, and offered to Aeron. Frame-scope values (`$.` paths, the symbol under `each`) are read before the records are iterated because On Demand only walks forward. No queue, no second thread on the path, no allocation per message after warm-up. REST snapshot fetches are async HTTPS requests on the same thread. The optional embedded Aeron media driver runs its own threads.

## Sequence

One counter for the whole gateway, every published message, gap-free, assigned at publish time. A message the publisher drops (no subscriber, sustained back pressure) still consumes its number, so the agent sees a real gap for a real loss. The agent partition is configured `framing: { length: packet, sequence: message.Sequence }`.

## Heartbeats and definitions

`Heartbeat` (InstrumentId 0) every `--heartbeat-ms` (default 1000) whatever the market does, so the agent's heartbeat timeout stays meaningful. `InstrumentDefinition` for every instrument at connect, after every reconnect, and every `--definitions-ms` (default 60000): Symbol, Exchange, Base and Quote (from the instruments file), Status Trading. TickSize and LotSize are 0 until a pack learns them from the exchange's instrument endpoint.

## Instruments

`site/instruments.yaml` maps venue symbols to ids and optional base/quote. Ids are what apps filter on, so keep them stable; `--symbols` auto-numbers and warns.

## Books

Each (book stream, instrument) pair owns a strategy and a local book. Strategies that need a REST snapshot (`binance_diff`) ask for it through the gateway; the network loop fetches it. In-band strategies (`l2_inband`, `snapshot_only`) get everything from the socket. On disconnect every book is dropped and resynced after reconnect. `--snapshot-every N` (default 100) republishes the top ten levels every N updates per instrument so late joiners and the agent's last-value cache converge quickly.

## Recording and replay

`--record file.jsonl` writes `{"ts":<recv ns>,"frame":<frame>}`, `{"ts":..,"snapshot":"<SYMBOL>","body":<json>}` and `{"ts":..,"event":"connected"|"disconnected"}` lines. `--replay file.jsonl` feeds them back in order with the recorded timestamps, so the output is byte-for-byte deterministic; `--out stdout` prints one line per canonical message with raw integer fields. The fixture test and the agent end-to-end test are both replays.

## Subscribing and keeping the connection

Subscribe messages (one or several, from the pack's templates) are written one after another right after the handshake; `--subscribe` prints them without connecting. WebSocket ping frames are answered by the client; an application-level text ping (`connect.ping`) is sent on its own timer and its reply dropped before parsing.

## Counters and latency

Printed on exit and every minute: frames, published, dropped, per-type counts, mapping/book/parse errors, unmatched streams, unknown symbols, acks, resyncs, last sequence, and `frame_to_offer_ns`: a log-linear histogram (a copy of the agent's, `src/latency_hist.h`, so both repos report on the same bucket edges; p50, p99, p999, max, mean) of the wall time from entering `on_frame` to the last offer of that frame, over frames that published something. It includes the publisher's cost, so measure the gateway itself with `--out null` (counts and discards). On the 2026-10-13 fixtures replayed on the dev host with `--out null`: Binance p50 0.7 µs, p99 10 to 15 µs (depth diffs with many levels), Kraken p50 0.7 to 1 µs, Coinbase p50 1.7 µs, OKX p50 0.9 to 1.4 µs; `--out stdout` roughly doubles the p50 with iostream formatting.

## End to end, measured live

2026-10-13, Binance spot, BTCUSDT/ETHUSDT/SOLUSDT, 30 s, 7921 messages: `spwsgw` (core 3) -> Aeron IPC, embedded driver in the agent, shared mode -> `subplaned` (core 2) -> ring -> `views_app --spin --latency` (core 4), Ryzen 5 3600, Ubuntu 22.04 container, no core isolation, one CLOCK_REALTIME on all three.

| Hop | p50 | p90 | p99 | max |
|---|---|---|---|---|
| gateway frame receive to Aeron offer (`frame_to_offer_ns`, live) | 4.1 µs | | 49 µs | 530 µs |
| gateway receive to agent receive (`gw_to_agent`) | 20 µs | 57 µs | 131 µs | 710 µs |
| agent receive to ring write (agent `recv_to_publish`) | 1.5 µs | 8 µs | 25 µs | 48 µs |
| agent receive to app read (`agent_to_app`) | 6.1 µs | 20 µs | 41 µs | 2.8 ms |
| gateway receive to app read (`gw_to_app`) | 28.7 µs | 65 µs | 164 µs | 2.8 ms |

The Aeron IPC hop (gateway offer to agent poll) is about 16 µs p50 by subtraction, larger than an IPC log hand-off should be; candidates are the publisher's offer path, the agent's `sched_yield` idle on a shared core, and the container's lack of isolation. Not yet explained. The TLS and WebSocket read in front of `RecvTs` is not in these numbers. One Binance depth resync occurred in the 30 s (0 in the earlier 6 s run).

## On the path

One I/O thread, no queue, no lock. Per frame: one indirect call from the WebSocket client (a function pointer with a context, not `std::function`), one simdjson On Demand parse, string views into the frame for frame-scope values, no allocation after warm-up (error strings are built only when a record fails). Per book record one virtual call into the strategy; per offer one virtual call into the publisher.

## Batching

The messages one input frame produces are appended back to back into one buffer and offered once when the frame has been processed, or earlier when the next message would exceed `--batch-bytes` (default 1376, one Aeron MTU of 1408 minus the 32-byte frame header, so an offer never fragments). Heartbeats and definitions from the timer are flushed at the end of the tick. There is no timer-driven flush: the last message of a frame leaves at the same moment it would have unbatched, so batching adds no latency; it only amortises the Aeron frame header and the offer. Each message is self-framed by its SBE header, so the agent walks a batch apart with `framing: { length: schema }` and any other subscriber does the same; there is no envelope. `--batch-bytes 0` offers one message at a time. On the fixtures with `--out null`: Binance 7048 messages in 2729 offers instead of 7048, Coinbase 628 in 111, Kraken 832 in 632, OKX unchanged (one record per frame); frame-to-offer p50 rose by about 0.3 µs on Binance for the copy into the batch buffer, which the Aeron offer it saves costs more than.
