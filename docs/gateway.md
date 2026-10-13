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

## Counters

Printed on exit and every minute: frames, published, dropped, per-type counts, mapping/book/parse errors, unmatched streams, unknown symbols, acks, resyncs, last sequence.
