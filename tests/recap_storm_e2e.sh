#!/usr/bin/env bash
# Recap storm: 16 recap sessions added at once (one POST /reload), each asking for the same 2
# instruments, must reach spwsgw-snap as exactly 2 RecapRequests: the agent coalesces per
# (partition, symbol), and its recap.coalesce_ms window gathers sessions that ask together (a
# reload creates them a few ms apart, about 80 ms for all 16, so the test uses 200 ms).
# Every session must still end with the live session's book for both.
# usage: recap_storm_e2e.sh <spwsgw> <spwsgw-snap> <agent build dir or empty> <pack.yaml> <fixture dir> <schema.xml> <book levels>
# Skipped (77) without an agent build directory.
set -u
GW=$1; SNAP=$2; AGENT_DIR=$3; PACK=$4; DIR=$5; SCHEMA=$6; LEVELS=$7
[ -n "$AGENT_DIR" ] && [ -x "$AGENT_DIR/subplaned" ] && [ -x "$AGENT_DIR/examples/views_app" ] || { echo "SKIP: no agent build (configure with -DSUBPLANE_BUILD_DIR=...)"; exit 77; }
AGENT="$AGENT_DIR/subplaned"; APP="$AGENT_DIR/examples/views_app"
SESSIONS=16; IIDS="1 2"
TMP=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null; rm -rf "$TMP"; rm -f /dev/shm/subplane.storme2e.*.ring' EXIT
fail() { echo "FAIL: $*"; for f in "$TMP"/*.log; do echo "--- $f"; tail -15 "$f"; done; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
http_post() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'POST %s HTTP/1.0\r\nContent-Length: 0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
counter() { echo "$1" | grep -o "\"$2\":[0-9]*" | head -1 | cut -d: -f2; }
BASE=$(( 20000 + (RANDOM % 4000) * 5 ))
API=$BASE; RECAP_REQ=$((BASE + 1)); RECAP_RSP=$((BASE + 2)); CTL_REQ=$((BASE + 3)); CTL_RSP=$((BASE + 4))
SOCK="$TMP/ctl.sock"; ADIR="$TMP/aeron"

config() {   # $1 = with the storm sessions
cat <<YAML
agent: { name: storme2e, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [crypto] } ] }
partitions:
  - name: crypto
    transport: { type: aeron, channel: "aeron:ipc", stream_id: 1001, aeron_dir: "$ADIR", driver: { embedded: true, threading_mode: shared } }
    framing: { length: schema, sequence: message.Sequence }
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, exchange_ts: ExchangeTs, venue: VenueId }
      symbol_id_mode: passthrough
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 3, msg_type: BOOK_UPDATE }, { template: 4, msg_type: BOOK_SNAPSHOT }, { template: 10, msg_type: STATUS }, { template: 100, admin: HEARTBEAT } ]
    heartbeat_timeout_ms: 5000
    recap:
      type: aeron
      aeron_dir: "$ADIR"
      request:  { channel: "aeron:udp?endpoint=127.0.0.1:$RECAP_REQ", stream_id: 1101 }
      response: { channel: "aeron:udp?control-mode=response|control=127.0.0.1:$RECAP_RSP", stream_id: 1102 }
      timeout_ms: 1000
      coalesce_ms: 200
sessions:
  - { name: live, ring: { capacity_mb: 16, max_payload: 4096 }, partitions: [crypto], symbols: ["*"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }
YAML
if [ "$1" = yes ]; then
  for i in $(seq 1 $SESSIONS); do
    echo "  - { name: s$i, recap: true, ring: { capacity_mb: 4, max_payload: 4096 }, partitions: [crypto], symbols: [1, 2], msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
  done
fi
cat <<YAML
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML
}
config no > "$TMP/config.yaml"
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 &
for _ in $(seq 1 150); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $! 2>/dev/null || fail "agent exited early"

"$SNAP" --aeron-dir "$ADIR" --data-channel aeron:ipc --recap-channel "aeron:udp?endpoint=127.0.0.1:$RECAP_REQ" --recap-response "127.0.0.1:$RECAP_RSP" \
        --gateway-control "aeron:udp?endpoint=127.0.0.1:$CTL_REQ" --gateway-response "127.0.0.1:$CTL_RSP" --recap-term-length 4194304 \
        --book-levels "$LEVELS" --ask-every-ms 200 > "$TMP/snap.log" 2>&1 &
SNAP_PID=$!
sleep 0.5
kill -0 $SNAP_PID 2>/dev/null || fail "snap exited early"

"$GW" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out aeron --channel aeron:ipc --stream-id 1001 --aeron-dir "$ADIR" --replay "$DIR/frames.jsonl" \
      --control-channel "aeron:udp?endpoint=127.0.0.1:$CTL_REQ" --control-response "127.0.0.1:$CTL_RSP" --seconds 6 > "$TMP/gw.log" 2>&1 &
GW_PID=$!
sleep 2

# The storm: every session at once, all on the same two instruments.
config yes > "$TMP/config.yaml"
http_post "$API" /reload > "$TMP/reload.out" 2>&1 || fail "reload failed: $(cat "$TMP/reload.out")"
# recap_merged counts merged replies (one per request), not sessions: the books below show
# that every session got its instruments.
OK=""
for _ in $(seq 1 100); do
  sleep 0.1
  P=$(http_get "$API" /partitions)
  [ "$(counter "$P" recap_merged)" -ge 2 ] 2>/dev/null && [ "$(counter "$P" recap_inflight)" = 0 ] && [ "$(counter "$P" recap_queued)" = 0 ] && { OK=1; break; }
done
[ -n "$OK" ] || fail "the recaps did not complete: $P"
wait $GW_PID || fail "gateway exit $?"
P=$(http_get "$API" /partitions)
REQ=$(counter "$P" recap_requests); COAL=$(counter "$P" recap_coalesced); REJ=$(counter "$P" recap_rejects)
[ "$REQ" = 2 ] || fail "the agent sent $REQ recap requests for 2 instruments (coalesced $COAL, rejects $REJ): $P"

book() {   # views_app lines -> the final book of instrument $1, one "side px qty" line per level, sorted
  awk -v iid="$1" '$1 == "book" {
      sym = ""; side = ""; act = ""; px = ""; qty = ""
      for (i = 2; i <= NF; ++i) { split($i, kv, "="); if (kv[1] == "sym") sym = kv[2]; else if (kv[1] == "side") side = kv[2];
                                   else if (kv[1] == "action") act = kv[2]; else if (kv[1] == "px") px = kv[2]; else if (kv[1] == "qty") qty = kv[2] }
      if (sym != iid) next
      if (act == 4) { delete lv; next }
      k = side " " px
      if (act == 3) delete lv[k]; else lv[k] = qty
    } END { for (k in lv) print k, lv[k] }' | sort
}
"$APP" /dev/shm/subplane.storme2e.live.ring --from-start --seconds 1 > "$TMP/live.out" 2>"$TMP/live.log" || fail "views_app live exit $?"
TOTAL=0
for iid in $IIDS; do
  book "$iid" < "$TMP/live.out" > "$TMP/live.$iid.book"
  [ -s "$TMP/live.$iid.book" ] || fail "the live session has no book for instrument $iid"
  TOTAL=$(( TOTAL + $(wc -l < "$TMP/live.$iid.book") ))
done
for i in $(seq 1 $SESSIONS); do
  "$APP" /dev/shm/subplane.storme2e.s$i.ring --from-start --seconds 1 > "$TMP/s$i.out" 2>"$TMP/s$i.log" || fail "views_app s$i exit $?"
  for iid in $IIDS; do
    book "$iid" < "$TMP/s$i.out" | diff -u "$TMP/live.$iid.book" - > "$TMP/diff" || fail "session s$i, instrument $iid: the book differs from the live one: $(head -20 "$TMP/diff")"
  done
done
kill -INT $SNAP_PID; wait $SNAP_PID 2>/dev/null
grep -q " requests=2 " "$TMP/snap.log" || fail "the snap did not see exactly 2 requests: $(tail -1 "$TMP/snap.log")"
echo "$(tail -1 "$TMP/snap.log")"
echo "PASS: $SESSIONS sessions x 2 instruments -> 2 requests at the snap; every book equals the live one ($TOTAL levels)"
