About 5.4 s of BTC-USDT and ETH-USDT from `wss://ws.okx.com:8443/ws/v5/public` (trades, tickers), recorded on 2026-10-13 with `spwsgw --record` inside the Ubuntu 22.04 container; the whole recording. `expected.txt` is the replay output with `--out stdout`; `tests/fixture_test.sh` diffs them. Regenerate with:

    spwsgw --pack packs/okx.yaml --instruments fixtures/okx/instruments.yaml --out stdout --replay fixtures/okx/frames.jsonl --quiet > fixtures/okx/expected.txt
