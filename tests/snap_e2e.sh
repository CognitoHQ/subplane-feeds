#!/usr/bin/env bash
# Recap end to end: gateway (replaying the fixture, control input on) -> Aeron IPC -> subplaned
# (embedded driver, a recap partition) and spwsgw-snap (untethered, asks the gateway to Republish).
# A recap session added after the replay (POST /reload) must end with the same book as a session
# that read the stream from the start, level by level. Then the gateway runs again (a restart:
# new epoch, Sequence from 1): the snap forgets, asks again, and a second recap session gets the
# new epoch's book.
# With HTTP=1 (a network build), also scrapes the snap's /metrics and /topology and joins its
# topology to the agent's on the recap and data keys.
# usage: snap_e2e.sh <spwsgw> <spwsgw-snap> <agent build dir or empty> <pack.yaml> <fixture dir> <schema.xml> <instrument id> [HTTP]
# Skipped (77) without an agent build directory.
set -u
GW=$1; SNAP=$2; AGENT_DIR=$3; PACK=$4; DIR=$5; SCHEMA=$6; IID=$7; HTTP=${8:-0}
REC="${FEEDS_RECORDINGS:-$(cd "$DIR/../.." && pwd)/recordings}/$(basename "$DIR")"   # the local recording (fixtures/README.md)
[ -f "$REC/frames.jsonl" ] || { echo "SKIP: no recording in $REC (record one: scripts/record-fixture.sh $(basename "$DIR"))"; exit 77; }
[ -n "$AGENT_DIR" ] && [ -x "$AGENT_DIR/subplaned" ] && [ -x "$AGENT_DIR/examples/views_app" ] || { echo "SKIP: no agent build (configure with -DSUBPLANE_BUILD_DIR=...)"; exit 77; }
AGENT="$AGENT_DIR/subplaned"; APP="$AGENT_DIR/examples/views_app"
TMP=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null; rm -rf "$TMP"; rm -f /dev/shm/subplane.snape2e.*.ring' EXIT
fail() { echo "FAIL: $*"; for f in "$TMP"/*.log; do echo "--- $f"; tail -15 "$f"; done; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
http_post() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'POST %s HTTP/1.0\r\nContent-Length: 0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
BASE=$(( 20000 + (RANDOM % 4000) * 5 ))
API=$BASE; RECAP_REQ=$((BASE + 1)); RECAP_RSP=$((BASE + 2)); CTL_REQ=$((BASE + 3)); CTL_RSP=$((BASE + 4))
SOCK="$TMP/ctl.sock"; ADIR="$TMP/aeron"

sessions() {   # $1 = how many late recap sessions: 0, 1 (late) or 2 (late, again)
  echo "sessions:"
  echo "  - { name: live, ring: { capacity_mb: 16, max_payload: 4096 }, topics: [crypto], symbols: [\"*\"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
  [ "$1" -ge 1 ] && echo "  - { name: late, recap: true, ring: { capacity_mb: 16, max_payload: 4096 }, topics: [crypto], symbols: [$IID], msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
  [ "$1" -ge 2 ] && echo "  - { name: again, recap: true, ring: { capacity_mb: 16, max_payload: 4096 }, topics: [crypto], symbols: [$IID], msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
  return 0
}
config() {
cat <<YAML
agent: { name: snape2e, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, topics: [crypto] } ] }
registry: [ { id: 11, name: crypto } ]
topics:
  - name: crypto
    transport: { type: aeron, channel: "aeron:ipc", stream_id: 1001, aeron_dir: "$ADIR", driver: { embedded: true, threading_mode: shared } }
    framing: { length: schema, sequence: message.Sequence }
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, instrument_sequence: InstrumentSeq, exchange_ts: ExchangeTs, venue: VenueId }
      symbol_id_mode: table
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 3, msg_type: BOOK_UPDATE }, { template: 4, msg_type: BOOK_SNAPSHOT }, { template: 5, msg_type: OHLCV }, { template: 6, msg_type: STATUS }, { template: 10, msg_type: DEFINITION }, { template: 100, admin: HEARTBEAT } ]
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
config 0 > "$TMP/config.yaml"
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 &
AGENT_PID=$!
for _ in $(seq 1 150); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"

"$SNAP" --aeron-dir "$ADIR" --data-channel aeron:ipc --recap-channel "aeron:udp?endpoint=127.0.0.1:$RECAP_REQ" --recap-response "127.0.0.1:$RECAP_RSP" \
        --gateway-control "aeron:udp?endpoint=127.0.0.1:$CTL_REQ" --gateway-response "127.0.0.1:$CTL_RSP" --recap-term-length 1048576 --ask-every-ms 200 \
        $([ "$HTTP" = 1 ] && echo --http 127.0.0.1:0 --site lab) > "$TMP/snap.log" 2>&1 &
SNAP_PID=$!
sleep 0.5
kill -0 $SNAP_PID 2>/dev/null || fail "snap exited early"

# The replay, then the gateway keeps answering control (Republish) for the rest of --seconds.
gateway() {
"$GW" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out aeron --channel aeron:ipc --stream-id 1001 --aeron-dir "$ADIR" --replay "$REC/frames.jsonl" \
      --control-channel "aeron:udp?endpoint=127.0.0.1:$CTL_REQ" --control-response "127.0.0.1:$CTL_RSP" --seconds 6 > "$TMP/gw$1.log" 2>&1 &
GW_PID=$!
}
gateway 1
sleep 2

# The late recap session joins after everything was published once.
config 1 > "$TMP/config.yaml"
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

# A gateway restart: the same replay again, Sequence from 1 and a new epoch. Its first sync
# clears and rebuilds the book in band, so the live session ends with the new epoch's book.
MERGED=$(http_get "$API" /partitions | grep -o '"recap_merged":[0-9]*' | cut -d: -f2)
gateway 2
sleep 2
config 2 > "$TMP/config.yaml"
http_post "$API" /reload > "$TMP/reload2.out" 2>&1 || fail "second reload failed: $(cat "$TMP/reload2.out")"
OK=""
for _ in $(seq 1 50); do
  P=$(http_get "$API" /partitions)
  N=$(echo "$P" | grep -o '"recap_merged":[0-9]*' | cut -d: -f2)
  [ "${N:-0}" -gt "${MERGED:-0}" ] && { OK=1; break; }
  sleep 0.1
done
[ -n "$OK" ] || fail "no recap merged after the gateway restart: $P"
wait $GW_PID || fail "second gateway exit $?"
"$APP" /dev/shm/subplane.snape2e.live.ring --from-start --seconds 1 > "$TMP/live2.out" 2>"$TMP/live2.log" || fail "views_app live exit $?"
"$APP" /dev/shm/subplane.snape2e.again.ring --from-start --seconds 1 > "$TMP/again.out" 2>"$TMP/again.log" || fail "views_app again exit $?"
book < "$TMP/live2.out" > "$TMP/live2.book"
book < "$TMP/again.out" > "$TMP/again.book"
[ -s "$TMP/again.book" ] || fail "the recap session after the restart has no book"
diff -u "$TMP/live2.book" "$TMP/again.book" > "$TMP/book2.diff" || fail "after the restart the recap session's book differs from the live one: $(head -20 "$TMP/book2.diff")"
if [ "$HTTP" = 1 ]; then
  SPORT=$(sed -n 's/^spwsgw-snap: http on port \([0-9]*\)$/\1/p' "$TMP/snap.log")
  [ -n "$SPORT" ] || fail "the snap printed no http port"
  http_get "$SPORT" /metrics > "$TMP/snap.metrics" || fail "snap /metrics"
  grep -Eq '^subplane_snap_recap_requests_total\{.*result="reply"\} [1-9]' "$TMP/snap.metrics" || fail "snap metrics: no replies counted"
  # The gateway has exited by now: once the snap sees its image go, the instrument is Stale, not Ok.
  grep -Eq '^subplane_snap_instrument_state\{.*state="(ok|stale)"\} 1$' "$TMP/snap.metrics" || fail "snap metrics: the instrument is neither ok nor stale"
  # promtool, when installed (always in the toolchain image and CI): the snap's metrics, and the
  # agent's with a recap partition, which no agent-side test serves.
  PROMTOOL=${PROMTOOL:-$(command -v promtool || true)}
  if [ -n "$PROMTOOL" ]; then
    "$PROMTOOL" check metrics < "$TMP/snap.metrics" || fail "promtool: snap"
    http_get "$API" /metrics > "$TMP/agent.metrics" || fail "agent /metrics"
    "$PROMTOOL" check metrics < "$TMP/agent.metrics" || fail "promtool: agent"
  fi
  http_get "$SPORT" /topology > "$TMP/snap.topo" || fail "snap /topology"
  http_get "$API" /topology > "$TMP/agent.topo" || fail "agent /topology"
  key() { grep -o "\"$1\":\[[^]]*\]" "$2" | grep -o '"key":"[^"]*"' | sed 's/"key":"\(.*\)"/\1/'; }
  RECAP_KEY=$(key downstream "$TMP/snap.topo")
  [ -n "$RECAP_KEY" ] || fail "the snap has no downstream recap edge: $(cat "$TMP/snap.topo")"
  key upstream "$TMP/agent.topo" | grep -qxF "$RECAP_KEY" || fail "the agent's upstream edges do not join the snap's recap key $RECAP_KEY: $(cat "$TMP/agent.topo")"
  DATA_KEY=$(key upstream "$TMP/snap.topo" | head -1)
  key upstream "$TMP/agent.topo" | grep -qxF "$DATA_KEY" || fail "the snap and the agent read different data keys: $DATA_KEY"
  echo "topology joins: recap $RECAP_KEY, data $DATA_KEY"
fi
kill -INT $SNAP_PID; wait $SNAP_PID 2>/dev/null
grep -q "requests=[1-9]" "$TMP/snap.log" || fail "the snap served no request"
grep -q "restarts=1 " "$TMP/snap.log" || fail "the snap did not see exactly one gateway restart: $(tail -1 "$TMP/snap.log")"
grep -Eq "replies=([2-9]|[1-9][0-9])" "$TMP/snap.log" || fail "the snap did not reply after the restart: $(tail -1 "$TMP/snap.log")"
echo "$(tail -1 "$TMP/snap.log")"
echo "PASS: the recap session's book equals the live one ($LEVELS levels), and again after a gateway restart ($(wc -l < "$TMP/again.book") levels)"
