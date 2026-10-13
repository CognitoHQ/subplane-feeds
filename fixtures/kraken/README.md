About 4 s of BTC/USD and ETH/USD from `wss://ws.kraken.com/v2` (trade, ticker, book depth 25), recorded on 2026-10-13 with `subplane-gateway --record` inside the Ubuntu 22.04 container; the whole recording, nothing cut. Status and heartbeat frames are in it (counted as unmatched). `expected.txt` is the replay output with `--out stdout`; `tests/fixture_test.sh` diffs them. Regenerate with:

    subplane-gateway --pack packs/kraken.yaml --instruments fixtures/kraken/instruments.yaml --out stdout --replay fixtures/kraken/frames.jsonl --quiet > fixtures/kraken/expected.txt
