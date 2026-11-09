#!/usr/bin/env bash
# Record a fixture from the live exchange, then its expected replay output, for the replay tests.
# The recording stays local (gitignored): exchange terms bar redistributing their data (fixtures/README.md).
# usage: scripts/record-fixture.sh <pack> [seconds, default 5] [build dir, default build]
# On a host without Ubuntu 26.04: scripts/dev.sh run scripts/record-fixture.sh <pack>
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NAME=${1:?usage: scripts/record-fixture.sh <pack> [seconds] [build dir]}; SECS=${2:-5}; B=${3:-$ROOT/build}
PACK="$ROOT/packs/$NAME.yaml"; DIR="$ROOT/fixtures/$NAME"
[ -f "$PACK" ] || { echo "record-fixture: no pack $PACK" >&2; exit 2; }
[ -f "$DIR/instruments.yaml" ] || { echo "record-fixture: no $DIR/instruments.yaml" >&2; exit 2; }
[ -x "$B/spwsgw" ] || { echo "record-fixture: no $B/spwsgw (scripts/dev.sh build first)" >&2; exit 2; }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

"$B/spwsgw" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out null \
            --record "$TMP/frames.jsonl" --seconds "$SECS" --quiet 2> "$TMP/rec.err"
grep -q '"frame"' "$TMP/frames.jsonl" || { echo "record-fixture: no frames from the exchange" >&2; cat "$TMP/rec.err" >&2; exit 1; }
"$B/spwsgw" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out stdout \
            --replay "$TMP/frames.jsonl" --quiet > "$TMP/expected.txt" 2> "$TMP/replay.err"
# INTENT: a recording that maps with errors would make the fixture test fail for the wrong reason;
# refuse it here, with the gateway's own counters, so the cause is the exchange's data and not the code.
grep -q "map_errors=0 book_errors=0 parse_errors=0" "$TMP/replay.err" || {
  echo "record-fixture: the replay reported errors; not keeping it" >&2; cat "$TMP/replay.err" >&2; exit 1; }
mv "$TMP/frames.jsonl" "$DIR/frames.jsonl"; mv "$TMP/expected.txt" "$DIR/expected.txt"
echo "record-fixture: $NAME: $(grep -c '"frame"' "$DIR/frames.jsonl") frames, $(wc -l < "$DIR/expected.txt") messages in fixtures/$NAME/"
