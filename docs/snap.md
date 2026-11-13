# spwsgw-snap: the gateway's snapshot service

`spwsgw-snap` serves recap for one gateway: one partition of a topic, since each partition has its own gateway. It subscribes to that gateway's canonical data stream and SPP state stream, untethered (`tether=false`, so it can never hold the gateway back), and rebuilds every instrument from the stream alone. It never talks to the exchange. It is also the first consumer that must rebuild exact books from the gateway's output: if it cannot, apps cannot either.

Source: `src/snap/` (network-free core, tested from the gateway's own output in `tests/test_snap.cpp`), `src/snap_main.cpp` (the process), `src/aeron_io.{h,cpp}` (Aeron pieces shared with the gateway).

## Running

```
spwsgw-snap --recap-channel aeron:udp?endpoint=SNAPHOST:40500 --recap-response SNAPHOST:40501 \
            --section-channel "aeron:udp?endpoint=239.1.2.3:40502|interface=10.0.0.0/8" [--section-stream 1103] \
            [--section-term-length 4194304] [--section-offer-bytes 65536] [--section-rate 25000000] [--section-burst 1048576] \
            [--gather-ms 20] [--bulk-gather-ms 1000] \
            --gateway-control aeron:udp?endpoint=GWHOST:40600 --gateway-response GWHOST:40601 \
            [--data-channel aeron:ipc] [--data-stream 1001] [--state-stream 1002] \
            [--recap-stream 1101] [--recap-response-stream 1102] [--gateway-control-stream 1003] [--gateway-response-stream 1004] \
            [--recap-term-length 65536] [--recap-max-pending 65536] [--book-levels 5000] [--ask-every-ms 1000] \
            [--name spwsgw-snap] [--aeron-dir D] [--embedded-driver] [--seconds N] [--spin] [--quiet]
            [--http 127.0.0.1:9102] [--site S] [--env E] [--labels k=v,...] \
            [--instruments F --pack P [--registry R] --topic T --topic-registry F [--partition 1]]
```

- **Data and state:** the gateway's `--channel`, its stream (`--stream-id`, or the one `--topic` derives: `topic_id << 16 | partition_num`, e.g. 720897 for `crypto.binance-spot`) and `--state-stream`. The state stream is on the same channel; keep it below 65536, where no topic's stream can be.
- **Recap:** the agent's partition `recap.request` (`--recap-channel`, `--recap-stream`) and `recap.response` (`--recap-response` is its `control=` address, `--recap-response-stream`). Each requesting agent gets its own response publication, which carries only Hello and rejects.
- **Sections:** the agent's partition `recap.sections` (`--section-channel`, `--section-stream`). One publication shared by every agent of the partition: multicast, or Aeron multi-destination, or `aeron:ipc` on one box. The channel's flow control decides who sets the pace: `fc=max` (the multicast default) lets a slow agent lose a section and ask again instead of holding the others back. `--section-term-length` is added as `term-length=` unless the channel sets one.
- **Gateway control** (optional, but without it nothing is ever republished): the gateway's `--control-channel` and `--control-response`.
- **`--book-levels`:** at least the gateway's `max_levels` for every book stream. A deeper book overflows here, goes Stale and is asked for again.
- **Pacing:** `--section-rate` bytes per second (0 = unlimited) with `--section-burst` saved up; a section goes in offers of at most `--section-offer-bytes`, cut at message boundaries, each taken from the budget. A section is a whole book, about 100 bytes a level; the agent's `recap.reply_slab_bytes` (1 MiB by default) bounds what it accepts, about 10,000 levels.
- **Gather windows:** `--gather-ms` (20) for an instrument's first single request, `--bulk-gather-ms` (1000) for a bulk; interest that arrives within the window shares the one section.

- **Resolver** (optional): the gateway's own `--instruments` file (every partition's instruments), `--pack` (and `--registry`) for the venue id it stamps, `--topic` and `--topic-registry` for the topic id, and `--partition`, this snap's. With them the snap answers SPP `Resolve` (below), takes interest in its own partition's instruments before it has seen them, and refuses at once (`not_here`) an instrument the file puts on another partition or does not list. Any of a topic's snaps can be its resolver, since each reads the whole file; point the agent's topic `resolver:` at one of them, on its recap request channel.

- **`--http`:** `/metrics`, `/metrics?format=json`, `/topology` and `/health` ([observability.md](observability.md), "The snapshot service's metrics"). `--name` is the `instance`. Needs the network build (Boost.Beast), as the gateway's does.

Stats are printed every minute and on exit.

**Deploying:** `deploy/systemd/spwsgw-snap@.service`, one instance per gateway (`spwsgw-snap@binance-spot`), its flags from `/etc/subplane/snap-<instance>.env` (`deploy/systemd/snap.env.example`). With an `aeron:ipc` data channel the snap shares the gateway's driver directory, so it runs on the gateway's box. Restarting it is safe: it starts Unknown and asks for a Republish; interest in an instrument it knows waits until that instrument is whole, and one it has not seen yet is refused `UnknownInstrument` (the agent retries). With `--instruments`, its partition's instruments count as known from the start, so their interest waits instead.

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

## Sections

Recap here is interest, not request and reply. A `RecapRequest` for an instrument, or with the Bulk flag for every instrument, says "publish this"; the snap publishes each wanted instrument once, as a **section** on the section stream, however many agents asked within its gather window, and every agent waiting on the instrument takes it.

- **Single** interest is due `--gather-ms` after the first request for the instrument; more requests for it meanwhile join (`coalesced`). It goes out ahead of bulk.
- **Bulk** opens one window of `--bulk-gather-ms` for the partition: every agent sees a gap or a gateway restart at about the same time, and those that ask within the window share one section per instrument. An instrument already wanted keeps its earlier due time.
- **Not Ok yet** (a restart, a resync): its interest waits (`subplane_snap_recap_waiting`) and the section goes out once the instrument is Ok. No `NotReady`, so no retry from every agent.
- **Unknown** (never seen: another partition's, or not published yet): `RecapReject{UnknownInstrument}` on the requester's response channel. On a topic with several partitions the agent asks each partition until it knows where an instrument lives (from the data, or the resolver's list), and treats this as "not here". With `--instruments`, an instrument of another partition, or not in the file, is refused this way at once (`not_here`), and one of this partition's waits for its data.
- **One at a time:** the next section is chosen when the last is out, so a single request that arrives during a long bulk still goes next.

A section, each message with the section id as `CorrelationId`:

1. `RecapBegin{AsOf = S, SourceEpoch, RecordCount, Instrument, Venue}`. S is the last data `Sequence` applied.
2. `RecapRecord`s, each one canonical message with `Sequence = S` and `InstrumentSeq` = the instrument's last one applied (the instrument as of S, so an agent orders the section and the live messages after it per instrument):
   - the InstrumentDefinition;
   - for a full-depth book, `BookUpdate{Clear}` then every level as `Add` (bids then asks, best first, `Level` up to 9, 255 beyond);
   - for a snapshot-only book, its last `BookSnapshot`;
   - the last Quote, Trade and Status.
3. `RecapEnd{RecordCount}`.

An app applies these with the same code as the live stream, and the agent merges the live messages after S. `Hello` is answered `Hello{Role=Snap}`; anything else gets the SPP evolution rules (`Status{Unsupported}` when it has a CorrelationId).

## Resolve

With `--instruments`, `Resolve` is answered from the file (`docs/protocol/subplane-protocol.md` in subplane, "Resolve"):
- **By `Name`** (the venue symbol as in the file, `BTCUSDT`) or by `Instrument` (and `Venue`): one `ResolveReply` with that instrument and Last set.
- **The whole topic** (`Instrument` 0, empty `Name`): every instrument of the file, 128 to a reply (about 6 KiB, under the response term's maximum message), the last with Last.
- Every reply carries `TopicId` and `Partitions` (the highest `partition:` in the file). Each instrument carries its `partition:` (its home), the pack's venue id, and its symbol.
- An unknown name or id: `RecapReject{UnknownInstrument}`. Without `--instruments`: `Status{Unsupported}`.

## Tested

- `tests/test_snap.cpp` drives the real gateway into a `SnapCore`:
  - sync, gap, republish, restart;
  - states waiting for their data, and Ok ignored without a whole book;
  - overflow;
  - the section layout through the vendored decoders;
  - interest: the gather windows, single ahead of bulk, waiting until Ok, unknown instruments;
  - the token bucket;
  - the recap server and the gateway link;
  - Resolve: paged whole topic, by name, by id, unknown; another partition's instrument refused at once.
- `tests/snap_e2e.sh` (ctest `snap_e2e_binance_spot`, needs `-DSUBPLANE_BUILD_DIR`) runs the gateway replay, the agent with a recap partition, and spwsgw-snap:
  - a recap session added after the replay (`POST /reload`) ends with the same book as a session that read the stream from the start, level by level (about 2,000 levels on a Binance recording);
  - then the gateway runs again (a restart: new epoch, `Sequence` from 1). The snap counts one restart, holds interest while it rebuilds, and a second recap session again ends with the live session's book;
  - in a network build (every `scripts/dev.sh` build) it also scrapes the snap's `/metrics`, lints it and the agent's with promtool (installed in the image and CI) and checks that its `/topology` joins the agent's on the recap and data keys.
- `tests/recap_storm_e2e.sh` (ctest `recap_storm_coinbase`, needs `-DSUBPLANE_BUILD_DIR`): 16 recap sessions added by one reload, all on the same 2 instruments, reach the snap as exactly 2 requests (the agent's `recap.coalesce_ms`) and 2 sections, and every session ends with the live session's books.
- `tests/partition_recap_e2e.sh` (ctest `partition_recap_coinbase`, needs `-DSUBPLANE_BUILD_DIR`): one topic in two partitions, a gateway and a snap each (BTC-USD on /1, ETH-USD on /2), three agents, with snap /1 the topic's resolver. Every agent gets the list (two instruments, two partitions). Before any data, each adds a recap session by name (`BTC-USD`, `ETH-USD`) and asks each instrument's home partition only, once; each snap holds the interest until its book is whole and publishes its instrument once for all three. Then gateway /1 restarts: every agent sends one bulk resync to /1 and takes the section the snap publishes once more, with no timeout, and /2 is not touched. No snap refuses a request as another partition's. Every recap session ends with its live session's books.
