# Fixtures

Each pack's replay tests (`fixture_*`, `agent_e2e_*`, and for Binance and Coinbase the recap tests) run on a recording of the live exchange: `frames.jsonl`, the raw frames and snapshots, and `expected.txt`, what the pack makes of them. Replay is deterministic, so a later code change that alters the output shows as a diff.

**No recordings are committed.** The exchanges' API terms do not allow their market data to be redistributed (OKX, Kraken and Coinbase say so explicitly), so a recording is made on your machine, for your own use, and stays there: both files are gitignored. Only `instruments.yaml` (the symbols to record and their ids) is in the repository. Without a recording the tests report as skipped.

## Record

```bash
scripts/dev.sh build
scripts/dev.sh run scripts/record-fixture.sh binance-spot        # 5 s; or: <pack> <seconds>
for p in binance-spot coinbase-exchange kraken bitstamp okx; do scripts/dev.sh run scripts/record-fixture.sh $p; done
scripts/dev.sh test                                              # the replay tests now run
```

(On an Ubuntu 26.04 host, drop the `scripts/dev.sh run` prefix.) The script records with `spwsgw --record`, replays the recording with `--out stdout` to write `expected.txt`, and refuses a recording whose replay has mapping, book or parse errors.

Keep a recording as long as it is useful: it is your regression baseline. Re-record when a pack changes what it subscribes to, or when you want to check the pack against what the exchange sends today; a change in `expected.txt` after re-recording is the exchange's data, not the code. To check a code change, replay the same recording before and after.

## A new pack

Add `fixtures/<pack>/instruments.yaml` with one or two liquid symbols, record, and check the output by eye (`expected.txt` is one line per canonical message). The pull request carries the pack and `instruments.yaml`, never the recording; say in it what you recorded and that the fixture test passed.
