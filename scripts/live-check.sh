#!/usr/bin/env bash
# Ten seconds against the live exchange: counts per message type, no resyncs expected. Not run in CI.
# usage: scripts/live-check.sh [build dir] [pack] [symbols]
set -u
B=${1:-build}; PACK=${2:-packs/binance-spot.yaml}; SYMS=${3:-BTCUSDT,ETHUSDT}
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
"$B/subplane-gateway" --pack "$PACK" --symbols "$SYMS" --out stdout --seconds 10 > "$TMP/out.txt" 2> "$TMP/err.txt"
echo "trades=$(grep -c '^Trade ' "$TMP/out.txt") quotes=$(grep -c '^Quote ' "$TMP/out.txt") book_updates=$(grep -c '^BookUpdate ' "$TMP/out.txt") snapshots=$(grep -c '^BookSnapshot ' "$TMP/out.txt") heartbeats=$(grep -c '^Heartbeat ' "$TMP/out.txt")"
tail -1 "$TMP/err.txt"
