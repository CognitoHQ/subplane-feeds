#!/usr/bin/env bash
# Recorded frames -> pack -> stdout must equal the expected text byte for byte.
# usage: fixture_test.sh <spwsgw> <pack.yaml> <fixture dir>
# Skipped (77) without a local recording: exchange data is not redistributed (fixtures/README.md).
set -u
GW=$1; PACK=$2; DIR=$3
REC="${FEEDS_RECORDINGS:-$(cd "$DIR/../.." && pwd)/recordings}/$(basename "$DIR")"   # the local recording (fixtures/README.md)
[ -f "$REC/frames.jsonl" ] && [ -f "$REC/expected.txt" ] || { echo "SKIP: no recording in $REC (record one: scripts/record-fixture.sh $(basename "$DIR"))"; exit 77; }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
"$GW" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out stdout --replay "$REC/frames.jsonl" --quiet > "$TMP/out.txt" 2> "$TMP/err.txt" || { echo "FAIL: gateway exit $?"; cat "$TMP/err.txt"; exit 1; }
if ! diff -u "$REC/expected.txt" "$TMP/out.txt" > "$TMP/diff.txt"; then
  echo "FAIL: output differs from expected.txt"; head -40 "$TMP/diff.txt"; exit 1
fi
grep -E "map_errors=0 book_errors=0 parse_errors=0" "$TMP/err.txt" >/dev/null || { echo "FAIL: errors reported"; cat "$TMP/err.txt"; exit 1; }
echo "PASS: $(wc -l < "$TMP/out.txt") messages"
