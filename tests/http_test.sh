#!/usr/bin/env bash
# The observability routes on a real HTTP server: replay a fixture with --http, then
# scrape /metrics (text and JSON), /topology and /health while the gateway lingers.
# promtool lints the exposition when installed (always in the toolchain image and CI) or PROMTOOL is set.
# usage: http_test.sh <spwsgw> <pack.yaml> <fixture dir>
set -u
GW=$1; PACK=$2; DIR=$3
REC="${FEEDS_RECORDINGS:-$(cd "$DIR/../.." && pwd)/recordings}/$(basename "$DIR")"   # the local recording (fixtures/README.md)
[ -f "$REC/frames.jsonl" ] || { echo "SKIP: no recording in $REC (record one: scripts/record-fixture.sh $(basename "$DIR"))"; exit 77; }
command -v curl >/dev/null || { echo "SKIP: no curl"; exit 77; }
TMP=$(mktemp -d)
"$GW" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out null --replay "$REC/frames.jsonl" \
      --http 127.0.0.1:0 --instance fx1 --site lab --labels team=md --seconds 20 > /dev/null 2> "$TMP/err.txt" &
PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$TMP"' EXIT
PORT=""
for _ in $(seq 100); do
  PORT=$(sed -n 's/^spwsgw: http on port \([0-9]*\)$/\1/p' "$TMP/err.txt")
  [ -n "$PORT" ] && grep -q '^replayed' "$TMP/err.txt" && break
  sleep 0.05
done
[ -n "$PORT" ] || { echo "FAIL: no http port"; cat "$TMP/err.txt"; exit 1; }
U=http://127.0.0.1:$PORT
fail() { echo "FAIL: $*"; exit 1; }

curl -sf -D "$TMP/h.txt" "$U/metrics" > "$TMP/m.txt" || fail "/metrics"
grep -qi '^content-type: text/plain; version=0.0.4' "$TMP/h.txt" || fail "/metrics content type"
H=$(hostname)
grep -q "^subplane_service_info{service=\"spwsgw\",instance=\"fx1\",host=\"$H\",.*site=\"lab\".*team=\"md\"} 1$" "$TMP/m.txt" || fail "service_info"
FRAMES=$(wc -l < "$REC/frames.jsonl")
grep -Eq '^subplane_gateway_frames_total\{.*\} [1-9][0-9]*$' "$TMP/m.txt" || fail "frames_total"
grep -q 'subplane_gateway_dropped_total{.*reason="not_connected"} 0' "$TMP/m.txt" || fail "dropped by reason"
grep -q 'subplane_gateway_frame_to_offer_seconds_bucket{.*le="+Inf"}' "$TMP/m.txt" || fail "histogram"
grep -q 'subplane_topology_edge{.*direction="upstream",role="data",key="file://' "$TMP/m.txt" || fail "upstream edge"
PROMTOOL=${PROMTOOL:-$(command -v promtool || true)}
if [ -n "$PROMTOOL" ]; then "$PROMTOOL" check metrics < "$TMP/m.txt" || fail "promtool check metrics"; fi

curl -sf "$U/metrics?format=json" | grep -q '"counters":{"frames":' || fail "/metrics?format=json"
curl -sf "$U/topology" > "$TMP/t.json" || fail "/topology"
grep -q "\"schema\":\"subplane.topology/1\",\"node\":{\"id\":\"$H/spwsgw/fx1\"" "$TMP/t.json" || fail "topology node"
grep -q '"downstream":\[\]' "$TMP/t.json" || fail "null output has no downstream edge"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$U/health")" = 200 ] || fail "/health on replay"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$U/nope")" = 404 ] || fail "404"
echo "PASS: $FRAMES frames replayed, routes served on port $PORT"
