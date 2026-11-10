#!/usr/bin/env bash
# Gateway (replaying the fixture) -> Aeron IPC (embedded driver in the agent) -> subplaned -> ring -> views_app.
# usage: agent_e2e.sh <spwsgw> <agent build dir or empty> <pack.yaml> <fixture dir> <schema.xml>
# Skipped (77) without an agent build directory.
set -u
GW=$1; AGENT_DIR=$2; PACK=$3; DIR=$4; SCHEMA=$5
REC="${FEEDS_RECORDINGS:-$(cd "$DIR/../.." && pwd)/recordings}/$(basename "$DIR")"   # the local recording (fixtures/README.md)
[ -f "$REC/frames.jsonl" ] || { echo "SKIP: no recording in $REC (record one: scripts/record-fixture.sh $(basename "$DIR"))"; exit 77; }
[ -n "$AGENT_DIR" ] && [ -x "$AGENT_DIR/subplaned" ] && [ -x "$AGENT_DIR/examples/views_app" ] || { echo "SKIP: no agent build (configure with -DSUBPLANE_BUILD_DIR=...)"; exit 77; }
AGENT="$AGENT_DIR/subplaned"; APP="$AGENT_DIR/examples/views_app"
TMP=$(mktemp -d); trap 'rm -rf "$TMP"; kill $(jobs -p) 2>/dev/null; rm -f /dev/shm/subplane.feedse2e.*.ring' EXIT
fail() { echo "FAIL: $*"; tail -30 "$TMP"/*.log 2>/dev/null; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
API=$(( 20000 + (RANDOM % 30000) )); SOCK="$TMP/ctl.sock"; ADIR="$TMP/aeron"
cat > "$TMP/config.yaml" <<YAML
agent: { name: feedse2e, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, partitions: [crypto] } ] }
partitions:
  - name: crypto
    transport: { type: aeron, channel: "aeron:ipc", stream_id: 1001, aeron_dir: "$ADIR", driver: { embedded: true, threading_mode: shared } }
    framing: { length: schema, sequence: message.Sequence }   # several canonical messages per offer, back to back
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, exchange_ts: ExchangeTs, venue: VenueId }
      symbol_id_mode: table
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 3, msg_type: BOOK_UPDATE }, { template: 4, msg_type: BOOK_SNAPSHOT }, { template: 5, msg_type: OHLCV }, { template: 6, msg_type: STATUS }, { template: 10, msg_type: DEFINITION }, { template: 100, admin: HEARTBEAT } ]
    heartbeat_timeout_ms: 5000
sessions:
  - { name: all, ring: { capacity_mb: 16, max_payload: 4096 }, partitions: [crypto], symbols: ["*"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }
fanout: { ring_dir: /dev/shm }
control: { socket_path: $SOCK, fallback_socket_path: $SOCK, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $API }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML
"$AGENT" --config "$TMP/config.yaml" > "$TMP/agent.log" 2>&1 & AGENT_PID=$!
for _ in $(seq 1 150); do http_get "$API" /status >/dev/null 2>&1 && break; sleep 0.1; done
kill -0 $AGENT_PID 2>/dev/null || fail "agent exited early"
sleep 0.5
"$GW" --pack "$PACK" --instruments "$DIR/instruments.yaml" --out aeron --channel aeron:ipc --stream-id 1001 --aeron-dir "$ADIR" --replay "$REC/frames.jsonl" --quiet > "$TMP/gw.log" 2>&1 || fail "gateway exit $?"
EXPECTED=$(grep -c -v "^SourceState" "$REC/expected.txt")   # data messages; SourceState lines are the state stream
PUBLISHED=$(grep -o "published=[0-9]*" "$TMP/gw.log" | cut -d= -f2)
[ "$PUBLISHED" = "$EXPECTED" ] || fail "gateway published $PUBLISHED, expected $EXPECTED: $(cat "$TMP/gw.log")"
RING=/dev/shm/subplane.feedse2e.all.ring
"$APP" "$RING" --from-start --quiet --seconds 2 > "$TMP/app.log" 2>&1 || fail "views_app exit $?"
SUMMARY=$(tail -1 "$TMP/app.log"); echo "$SUMMARY"
echo "$SUMMARY" | grep -q " bad=0$" || fail "views_app rejected records"
TRADES=$(grep -c "^Trade " "$REC/expected.txt"); echo "$SUMMARY" | grep -q "trades=$TRADES " || fail "trade count differs from the fixture ($TRADES)"
STATUS=$(http_get "$API" /partitions); echo "$STATUS" | grep -q '"gaps":0' || fail "agent reports gaps: $STATUS"
OFFERS=$(grep -o "offers=[0-9]*" "$TMP/gw.log" | cut -d= -f2)
echo "$STATUS" | grep -q "\"packets\":$OFFERS," || fail "agent packets != gateway offers ($OFFERS): $STATUS"
echo "$STATUS" | grep -q "\"messages\":$PUBLISHED," || fail "agent messages != gateway published ($PUBLISHED): $STATUS"
kill $AGENT_PID; wait $AGENT_PID 2>/dev/null
echo "PASS"
