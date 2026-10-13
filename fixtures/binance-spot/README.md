About 2.5 s of BTCUSDT from `wss://data-stream.binance.vision/stream` (trade, bookTicker, depth@100ms) plus the REST depth snapshot, recorded on 2026-10-13 with `subplane-gateway --record`. `expected.txt` is the replay output with `--out stdout`; `tests/fixture_test.sh` diffs them. Regenerate with:

    subplane-gateway --pack packs/binance-spot.yaml --instruments fixtures/binance-spot/instruments.yaml --out stdout --replay fixtures/binance-spot/frames.jsonl --quiet > fixtures/binance-spot/expected.txt
