# spwsgw-snap: the gateway's snapshot service

`spwsgw-snap` answers the agent's recap requests for one gateway. It subscribes to that gateway's canonical data stream and SPP state stream, untethered (`tether=false`, so it can never hold the gateway back), and rebuilds every instrument from the stream alone. It never talks to the exchange. It is also the first consumer that must rebuild exact books from the gateway's output: if it cannot, apps cannot either.

Source: `src/snap/` (network-free core, tested from the gateway's own output in `tests/test_snap.cpp`), `src/snap_main.cpp` (the process), `src/aeron_io.{h,cpp}` (Aeron pieces shared with the gateway).

## Running

```
spwsgw-snap --recap-channel aeron:udp?endpoint=SNAPHOST:40500 --recap-response SNAPHOST:40501 \
            --gateway-control aeron:udp?endpoint=GWHOST:40600 --gateway-response GWHOST:40601 \
            [--data-channel aeron:ipc] [--data-stream 1001] [--state-stream 1002] \
            [--recap-stream 1101] [--recap-response-stream 1102] [--gateway-control-stream 1003] [--gateway-response-stream 1004] \
            [--recap-term-length 16777216] [--recap-max-pending 16777216] [--book-levels 5000] [--ask-every-ms 1000] \
            [--name spwsgw-snap] [--aeron-dir D] [--embedded-driver] [--seconds N] [--spin] [--quiet]
            [--http 127.0.0.1:9102] [--site S] [--env E] [--labels k=v,...]
```

- **Data and state:** the gateway's `--channel`, `--stream-id` and `--state-stream`. The state stream is on the same channel.
- **Recap:** the agent's partition `recap.request` (`--recap-channel`, `--recap-stream`) and `recap.response` (`--recap-response` is its `control=` address, `--recap-response-stream`). Each requesting agent gets its own response publication.
- **Gateway control** (optional, but without it nothing is ever republished): the gateway's `--control-channel` and `--control-response`.
- **`--book-levels`:** at least the gateway's `max_levels` for every book stream. A deeper book overflows here, goes Stale and is asked for again.
- **`--recap-term-length` and `--recap-max-pending`:** a reply is a whole book, about 100 bytes a level, so both must hold the largest one. The agent's `recap.reply_slab_bytes` (1 MiB by default) bounds what it accepts, about 10,000 levels.

- **`--http`:** `/metrics`, `/metrics?format=json`, `/topology` and `/health` ([observability.md](observability.md), "The snapshot service's metrics"). `--name` is the `instance`. Needs the network build (Boost.Beast), as the gateway's does.

Stats are printed every minute and on exit.

**Deploying:** `deploy/systemd/spwsgw-snap@.service`, one instance per gateway (`spwsgw-snap@binance-spot`), its flags from `/etc/subplane/snap-<instance>.env` (`deploy/systemd/snap.env.example`). With an `aeron:ipc` data channel the snap shares the gateway's driver directory, so it runs on the gateway's box. Restarting it is safe: it starts Unknown, asks for a Republish, and answers NotReady until it is whole.

## State per instrument

Each instrument (VenueId, InstrumentId) has a book, its last InstrumentDefinition, Quote, Trade, Status and BookSnapshot, and a state:

- **Unknown** at start and after a gateway restart.
- **Ok** when the gateway's `SourceState{Ok}` is applied. States are held until the data stream reaches their `Sequence`, so an Ok never covers data this copy has not seen.
- **An Ok is ignored** (`states_ignored`) when this copy did not see the book whole since its last gap: a full-depth book needs a `BookUpdate{Clear}` after the last gap, with no overflow since. Joining mid-stream counts as a gap.
- **Stale** on:
  - `SourceState{Stale}`;
  - an in-band `Clear` (the book is being rebuilt; Ok follows the burst);
  - a sequence gap (every instrument);
  - the data image going away (every instrument);
  - a book overflow.
- **A restart:** a new `SourceEpoch`, or `Sequence` starting again, forgets everything.

Books published only as snapshots (`snapshot_only`, Bitstamp) are replaced by each `BookSnapshot`. Full-depth books (those with a `Clear`) ignore their `BookSnapshot` for the book.

## Asking the gateway

On connecting to the gateway's control channel the snap sends `Hello`, then `Republish{Instrument=0}` (all) once after a start or a restart. After that, every instrument that is not Ok is asked for again, one per call, at most once per `--ask-every-ms`, after one quiet wait (the gateway may be resyncing on its own). The gateway queues the instruments and republishes them at its `--republish-per-second`.

## Recap replies

For an Ok instrument, one reply (each message with the request's `CorrelationId`):

1. `RecapBegin{AsOf = S, SourceEpoch, RecordCount}`. S is the last data `Sequence` applied.
2. `RecapRecord`s, each one canonical message with `Sequence = S`:
   - the InstrumentDefinition;
   - for a full-depth book, `BookUpdate{Clear}` then every level as `Add` (bids then asks, best first, `Level` up to 9, 255 beyond);
   - for a snapshot-only book, its last `BookSnapshot`;
   - the last Quote, Trade and Status.
3. `RecapEnd{RecordCount}`.

An app applies these with the same code as the live stream, and the agent merges the live messages after S.

Otherwise `RecapReject{NotReady}` (known but not Ok; the agent retries with backoff) or `RecapReject{UnknownInstrument}`. `Hello` is answered `Hello{Role=Snap}`; anything else gets the SPP evolution rules (`Status{Unsupported}` when it has a CorrelationId).

## Tested

- `tests/test_snap.cpp` drives the real gateway into a `SnapCore`:
  - sync, gap, republish, restart;
  - states waiting for their data, and Ok ignored without a whole book;
  - overflow;
  - the reply layout through the vendored decoders;
  - the recap server and the gateway link.
- `tests/snap_e2e.sh` (ctest `snap_e2e_binance_spot`, needs `-DSUBPLANE_BUILD_DIR`) runs the gateway replay, the agent with a recap partition, and spwsgw-snap:
  - a recap session added after the replay (`POST /reload`) ends with the same book as a session that read the stream from the start, level by level (about 2,000 levels on a Binance recording);
  - then the gateway runs again (a restart: new epoch, `Sequence` from 1). The snap counts one restart, answers NotReady while it rebuilds (the agent retries), and a second recap session again ends with the live session's book;
  - in a network build (every `scripts/dev.sh` build) it also scrapes the snap's `/metrics`, lints it and the agent's with promtool (installed in the image and CI) and checks that its `/topology` joins the agent's on the recap and data keys.
- `tests/recap_storm_e2e.sh` (ctest `recap_storm_coinbase`, needs `-DSUBPLANE_BUILD_DIR`): 16 recap sessions added by one reload, all on the same 2 instruments, reach the snap as exactly 2 requests (the agent's `recap.coalesce_ms`), and every session ends with the live session's books.
