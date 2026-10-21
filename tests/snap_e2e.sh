#!/usr/bin/env bash
# Recap end to end: gateway (replaying the fixture, control input on) -> Aeron IPC -> subplaned
# (embedded driver, a recap partition) and spwsgw-snap (untethered, asks the gateway to Republish).
# A recap session added after the replay (POST /reload) must end with the same book as a session
# that read the stream from the start, level by level.
# usage: snap_e2e.sh <spwsgw> <spwsgw-snap> <agent build dir or empty> <pack.yaml> <fixture dir> <schema.xml> <instrument id>
# Skipped (77) without an agent build directory.
set -u
GW=$1; SNAP=$2; AGENT_DIR=$3; PACK=$4; DIR=$5; SCHEMA=$6; IID=$7
[ -n "$AGENT_DIR" ] && [ -x "$AGENT_DIR/subplaned" ] && [ -x "$AGENT_DIR/examples/views_app" ] || { echo "SKIP: no agent build (configure with -DSUBPLANE_BUILD_DIR=...)"; exit 77; }
AGENT="$AGENT_DIR/subplaned"; APP="$AGENT_DIR/examples/views_app"
TMP=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null; rm -rf "$TMP"; rm -f /dev/shm/subplane.snape2e.*.ring' EXIT
fail() { echo "FAIL: $*"; for f in "$TMP"/*.log; do echo "--- $f"; tail -15 "$f"; done; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
http_post() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'POST %s HTTP/1.0\r\nContent-Length: 0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
BASE=$(( 20000 + (RANDOM % 4000) * 5 ))
API=$BASE; RECAP_REQ=$((BASE + 1)); RECAP_RSP=$((BASE + 2)); CTL_REQ=$((BASE + 3)); CTL_RSP=$((BASE + 4))
SOCK="$TMP/ctl.sock"; ADIR="$TMP/aeron"

sessions() {   # $1 = with the late recap session
  echo "sessions:"
  echo "  - { name: live, ring: { capacity_mb: 16, max_payload: 4096 }, partitions: [crypto], symbols: [\"*\"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
  [ "$1" = yes ] && echo "  - { name: late, recap: true, ring: { capacity_mb: 16, max_payload: 4096 }, partitions: [crypto], symbols: [$IID], msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
  return 0
}
config() {
cat <<YAML
agent: { name: snape2e, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [crypto] } ] }
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
YAML
sessions "$1"
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
AGENT_PID=$!
for _ in $(seq 1 150); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"

"$SNAP" --aeron-dir "$ADIR" --data-channel aeron:ipc --recap-channel "aeron:udp?endpoint=127.0.0.1:$RECAP_REQ" --recap-response "127.0.0.1:$RECAP_RSP" \
        --gateway-control "aeron:udp?endpoint=127.0.0.1:$CTL_REQ" --gateway-response "127.0.0.1:$CTL_RSP" --recap-term-length 1048576 --ask-every-ms 200 \
        > "$TMP/snap.log" 2>&1 &
SNAP_PID=$!
sleep 0.5
kill -0 $SNAP_PID 2>/dev/null || fail "snap exited early"

# The replay, then the gateway keeps answering control (Republish) for the rest of --seconds.
"$GW" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out aeron --channel aeron:ipc --stream-id 1001 --aeron-dir "$ADIR" --replay "$DIR/frames.jsonl" \
      --control-channel "aeron:udp?endpoint=127.0.0.1:$CTL_REQ" --control-response "127.0.0.1:$CTL_RSP" --seconds 6 > "$TMP/gw.log" 2>&1 &
GW_PID=$!
sleep 2

# The late recap session joins after everything was published once.
config yes > "$TMP/config.yaml"
http_post "$API" /reload > "$TMP/reload.out" 2>&1 || fail "reload failed: $(cat "$TMP/reload.out")"
OK=""
for _ in $(seq 1 50); do
  P=$(http_get "$API" /partitions)
  echo "$P" | grep -q '"recap_merged":[1-9]' && { OK=1; break; }
  sleep 0.1
done
[ -n "$OK" ] || fail "no recap merged: $(http_get "$API" /partitions)"
echo "$P" | grep -q '"recap_records":[1-9]' || fail "the recap delivered no records: $P"
wait $GW_PID || fail "gateway exit $?"

book() {   # views_app lines -> the final book of instrument $IID, one "side px qty" line per level, sorted
  awk -v iid="$IID" '$1 == "book" {
      sym = ""; side = ""; act = ""; px = ""; qty = ""
      for (i = 2; i <= NF; ++i) { split($i, kv, "="); if (kv[1] == "sym") sym = kv[2]; else if (kv[1] == "side") side = kv[2];
                                   else if (kv[1] == "action") act = kv[2]; else if (kv[1] == "px") px = kv[2]; else if (kv[1] == "qty") qty = kv[2] }
      if (sym != iid) next
      if (act == 4) { delete lv; next }
      k = side " " px
      if (act == 3) delete lv[k]; else lv[k] = qty
    } END { for (k in lv) print k, lv[k] }' | sort
}
"$APP" /dev/shm/subplane.snape2e.live.ring --from-start --seconds 1 > "$TMP/live.out" 2>"$TMP/live.log" || fail "views_app live exit $?"
"$APP" /dev/shm/subplane.snape2e.late.ring --from-start --seconds 1 > "$TMP/late.out" 2>"$TMP/late.log" || fail "views_app late exit $?"
book < "$TMP/live.out" > "$TMP/live.book"
book < "$TMP/late.out" > "$TMP/late.book"
LEVELS=$(wc -l < "$TMP/live.book")
[ "$LEVELS" -gt 0 ] || fail "the live session has no book for instrument $IID"
diff -u "$TMP/live.book" "$TMP/late.book" > "$TMP/book.diff" || fail "the recap session's book differs from the live one: $(head -20 "$TMP/book.diff")"
kill -INT $SNAP_PID; wait $SNAP_PID 2>/dev/null
grep -q "requests=[1-9]" "$TMP/snap.log" || fail "the snap served no request"
echo "$(tail -1 "$TMP/snap.log")"
echo "PASS: the recap session's book equals the live one ($LEVELS levels)"
