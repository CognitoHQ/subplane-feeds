#!/usr/bin/env bash
# Recap per partition, end to end, with everything plural: one topic in two partitions, a gateway
# and a spwsgw-snap per partition, three agents. Both gateways replay the same recording, each
# publishing only its own instruments (instruments.yaml `partition:`): BTC-USD on /1, ETH-USD on /2.
# Each snap publishes sections on its own section stream, which all three agents read. Each snap
# reads the instruments file too, and /1's is the topic's resolver (SPP Resolve) for every agent.
#  0. Every agent gets the topic's list from the resolver: two instruments, two partitions.
#  1. Before any data, each agent adds a recap session for both instruments, by name (BTC-USD,
#     ETH-USD; agent c by id and name), one reload each, together. No agent has seen either
#     instrument, so the resolver's list is what sends each request to its home partition only.
#     The snaps hold the interest until their books are whole, then each publishes its instrument
#     once for all three agents (--gather-ms coalesces their requests).
#  2. Gateway /1 restarts. Every agent sees the publisher restart on /1 and sends one bulk resync to
#     /1's snap; the snap coalesces them (--bulk-gather-ms) and publishes BTC-USD once more, when its
#     book is whole again. ETH-USD on /2 is not touched.
# Every recap session ends with its live session's books, after each step.
# usage: partition_recap_e2e.sh <spwsgw> <spwsgw-snap> <agent build dir or empty> <pack.yaml> <fixture dir> <schema.xml> <book levels>
# Skipped (77) without an agent build directory.
set -u
GW=$1; SNAP=$2; AGENT_DIR=$3; PACK=$4; DIR=$5; SCHEMA=$6; LEVELS=$7
REC="${FEEDS_RECORDINGS:-$(cd "$DIR/../.." && pwd)/recordings}/$(basename "$DIR")"   # the local recording (fixtures/README.md)
[ -f "$REC/frames.jsonl" ] || { echo "SKIP: no recording in $REC (record one: scripts/record-fixture.sh $(basename "$DIR"))"; exit 77; }
[ -n "$AGENT_DIR" ] && [ -x "$AGENT_DIR/subplaned" ] && [ -x "$AGENT_DIR/examples/views_app" ] || { echo "SKIP: no agent build (configure with -DSUBPLANE_BUILD_DIR=...)"; exit 77; }
AGENT="$AGENT_DIR/subplaned"; APP="$AGENT_DIR/examples/views_app"
AGENTS="pre2ea pre2eb pre2ec"
TMP=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; wait 2>/dev/null; rm -rf "$TMP"; rm -f /dev/shm/subplane.pre2e?.*.ring' EXIT
fail() { echo "FAIL: $*"; for f in "$TMP"/*.log; do echo "--- $f"; tail -15 "$f"; done; exit 1; }
http_get() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'GET %s HTTP/1.0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
http_post() ( trap '' PIPE; exec 3<>"/dev/tcp/127.0.0.1/$1" 2>/dev/null || return 1; printf 'POST %s HTTP/1.0\r\nContent-Length: 0\r\n\r\n' "$2" >&3 || return 1; tr -d '\r' <&3 | sed '1,/^$/d'; exec 3<&- )
# A counter of one partition (crypto/1 or crypto/2) from /partitions.
pcounter() { echo "$1" | tr '{' '\n' | grep -A1 "\"name\":\"$2\"" | tr ',' '\n' | grep -o "\"$3\":[0-9]*" | head -1 | cut -d: -f2; }
# A counter in a partition's nested "resolver":{...} object (pcounter reads only the first two).
rcounter() { echo "$1" | sed 's/{"name":/\n&/g' | grep "\"name\":\"$2\"" | grep -o '"resolver":{[^}]*}' | grep -o "\"$3\":[0-9]*" | head -1 | cut -d: -f2; }
counter() { echo "$1" | grep -o "\"$3\":[0-9]*" | sed -n "$2p" | cut -d: -f2; }   # the Nth occurrence: partitions in order
BASE=$(( 20000 + (RANDOM % 3000) * 12 ))
REQ1=$((BASE + 3)); RSP1=$((BASE + 4)); REQ2=$((BASE + 5)); RSP2=$((BASE + 6))
CTL1=$((BASE + 7)); CTLR1=$((BASE + 8)); CTL2=$((BASE + 9)); CTLR2=$((BASE + 10))
ADIR="$TMP/aeron"
api() { case $1 in pre2ea) echo $BASE ;; pre2eb) echo $((BASE + 1)) ;; pre2ec) echo $((BASE + 2)) ;; esac; }

printf -- '- { id: 11, name: crypto }\n' > "$TMP/topics.yaml"
cat > "$TMP/instruments.yaml" <<YAML
instruments:
  BTC-USD: { id: 1, base: BTC, quote: USD, partition: 1 }
  ETH-USD: { id: 2, base: ETH, quote: USD, partition: 2 }
YAML

config() {   # $1 = agent name, $2 = with the recap session
  local driver=""
  [ "$1" = pre2ea ] && driver=', driver: { embedded: true, threading_mode: shared }'   # one driver for the box: the first agent's
cat <<YAML
agent: { name: $1, version: test, max_symbols: 65536, thread_groups: [ { name: g0, cpu: -1, topics: [crypto] } ] }
registry: [ { id: 11, name: crypto } ]
topics:
  - name: crypto
    transport: { type: aeron, channel: "aeron:ipc", aeron_dir: "$ADIR"$driver }
    framing: { length: schema, sequence: message.Sequence }
    decoder:
      type: sbe_routing
      schema: $SCHEMA
      fields: { symbol: InstrumentId, sequence: Sequence, instrument_sequence: InstrumentSeq, exchange_ts: ExchangeTs, venue: VenueId, stream_id: StreamId }
      symbol_id_mode: table
      messages: [ { template: 1, msg_type: TRADE }, { template: 2, msg_type: QUOTE }, { template: 3, msg_type: BOOK_UPDATE }, { template: 4, msg_type: BOOK_SNAPSHOT }, { template: 5, msg_type: OHLCV }, { template: 6, msg_type: STATUS }, { template: 10, msg_type: DEFINITION }, { template: 100, admin: HEARTBEAT } ]
    heartbeat_timeout_ms: 5000
    # hold_bytes: a pre-data session holds the replay burst (~80k messages) until the first section.
    recap: { type: aeron, aeron_dir: "$ADIR", timeout_ms: 10000, coalesce_ms: 0, reply_slab_bytes: 16777216, hold_bytes: 67108864 }
    resolver:
      type: aeron
      aeron_dir: "$ADIR"
      request:  { channel: "aeron:udp?endpoint=127.0.0.1:$REQ1", stream_id: 1101 }
      response: { channel: "aeron:udp?control-mode=response|control=127.0.0.1:$RSP1", stream_id: 1102 }
      refresh_ms: 2000
    partitions:
      - recap:
          request:  { channel: "aeron:udp?endpoint=127.0.0.1:$REQ1", stream_id: 1101 }
          response: { channel: "aeron:udp?control-mode=response|control=127.0.0.1:$RSP1", stream_id: 1102 }
          sections: { channel: "aeron:ipc", stream_id: 1103 }
      - recap:
          request:  { channel: "aeron:udp?endpoint=127.0.0.1:$REQ2", stream_id: 1101 }
          response: { channel: "aeron:udp?control-mode=response|control=127.0.0.1:$RSP2", stream_id: 1102 }
          sections: { channel: "aeron:ipc", stream_id: 1113 }
sessions:
  - { name: live, ring: { capacity_mb: 64, max_payload: 4096 }, topics: [crypto], symbols: ["*"], msg_types: [], deliver: { bus_admin: true, agent_control: true } }
YAML
local syms='[BTC-USD, ETH-USD]'
[ "$1" = pre2ec ] && syms='[1, ETH-USD]'
[ "$2" = yes ] && echo "  - { name: late, recap: true, ring: { capacity_mb: 64, max_payload: 4096 }, topics: [crypto], symbols: $syms, msg_types: [], deliver: { bus_admin: true, agent_control: true } }"
cat <<YAML
fanout: { ring_dir: /dev/shm }
control: { socket_path: $TMP/$1.sock, fallback_socket_path: $TMP/$1.sock, linger_ms: 1000 }
lvc: { max_payload: 512 }
api: { enabled: true, host: 127.0.0.1, port: $(api "$1") }
metrics: { enabled: false, report_interval_seconds: 60, output_format: json, console_output: false }
logging: { level: info, console_output: true, output_format: text }
YAML
}

for a in $AGENTS; do
  config "$a" no > "$TMP/$a.yaml"
  "$AGENT" --config "$TMP/$a.yaml" > "$TMP/$a.log" 2>&1 &
  for _ in $(seq 1 150); do http_get "$(api "$a")" /status >/dev/null 2>&1 && break; sleep 0.1; done
  http_get "$(api "$a")" /status >/dev/null 2>&1 || fail "agent $a did not start"
done

snap() {   # $1 = partition: its data stream, state stream, recap ports, section stream, gateway control
  local req rsp ctl ctlr state sec
  if [ "$1" = 1 ]; then req=$REQ1; rsp=$RSP1; ctl=$CTL1; ctlr=$CTLR1; state=1002; sec=1103; else req=$REQ2; rsp=$RSP2; ctl=$CTL2; ctlr=$CTLR2; state=1012; sec=1113; fi
  "$SNAP" --aeron-dir "$ADIR" --data-channel aeron:ipc --data-stream $(( (11 << 16) | $1 )) --state-stream $state --name "snap$1" \
          --recap-channel "aeron:udp?endpoint=127.0.0.1:$req" --recap-response "127.0.0.1:$rsp" --section-channel aeron:ipc --section-stream $sec \
          --gateway-control "aeron:udp?endpoint=127.0.0.1:$ctl" --gateway-response "127.0.0.1:$ctlr" \
          --instruments "$TMP/instruments.yaml" --pack "$PACK" --topic crypto --topic-registry "$TMP/topics.yaml" --partition "$1" \
          --book-levels "$LEVELS" --ask-every-ms 200 --gather-ms 1000 --bulk-gather-ms 1000 > "$TMP/snap$1.log" 2>&1 &
}
snap 1; SNAP1=$!
snap 2; SNAP2=$!
sleep 0.5
kill -0 $SNAP1 2>/dev/null && kill -0 $SNAP2 2>/dev/null || fail "a snap exited early"

gateway() {   # $1 = partition, $2 = log suffix
  local ctl ctlr state
  if [ "$1" = 1 ]; then ctl=$CTL1; ctlr=$CTLR1; state=1002; else ctl=$CTL2; ctlr=$CTLR2; state=1012; fi
  "$GW" --pack "$PACK" --instruments "$TMP/instruments.yaml" --topic crypto --topic-registry "$TMP/topics.yaml" --partition "$1" \
        --out aeron --channel aeron:ipc --state-stream $state --aeron-dir "$ADIR" --replay "$REC/frames.jsonl" \
        --control-channel "aeron:udp?endpoint=127.0.0.1:$ctl" --control-response "127.0.0.1:$ctlr" --seconds 8 > "$TMP/gw$1$2.log" 2>&1 &
}

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
compare() {   # $1 = step: every agent's late session has its live session's books
  for a in $AGENTS; do
    "$APP" /dev/shm/subplane.$a.live.ring --from-start --seconds 1 > "$TMP/$a.live.out" 2>/dev/null || fail "views_app $a live"
    "$APP" /dev/shm/subplane.$a.late.ring --from-start --seconds 1 > "$TMP/$a.late.out" 2>/dev/null || fail "views_app $a late"
    for iid in 1 2; do
      book $iid < "$TMP/$a.live.out" > "$TMP/live.book"
      [ -s "$TMP/live.book" ] || fail "$1: agent $a's live session has no book for instrument $iid"
      book $iid < "$TMP/$a.late.out" | diff -u "$TMP/live.book" - > "$TMP/diff" || fail "$1: agent $a, instrument $iid: the recap session's book differs: $(head -20 "$TMP/diff")"
    done
  done
}
# Waits until every agent's partition $1 has merged at least $2 recaps with nothing in flight or queued.
merged() {
  local a P ok
  for _ in $(seq 1 150); do
    ok=1
    for a in $AGENTS; do
      P=$(http_get "$(api "$a")" /partitions)
      [ "$(pcounter "$P" "crypto/$1" recap_merged)" -ge "$2" ] 2>/dev/null && [ "$(pcounter "$P" "crypto/$1" recap_inflight)" = 0 ] &&
        [ "$(pcounter "$P" "crypto/$1" recap_queued)" = 0 ] || { ok=""; break; }
    done
    [ -n "$ok" ] && return 0
    sleep 0.1
  done
  fail "partition /$1: not every agent merged $2 recaps: $(for a in $AGENTS; do http_get "$(api "$a")" /partitions; echo; done)"
}

# 0. Every agent has the topic's list from the resolver.
for a in $AGENTS; do
  for _ in $(seq 1 100); do P=$(http_get "$(api "$a")" /partitions); [ "$(rcounter "$P" crypto/1 resolver_lists)" -ge 1 ] 2>/dev/null && break; sleep 0.1; done
  [ "$(rcounter "$P" crypto/1 resolver_lists)" -ge 1 ] 2>/dev/null || fail "agent $a got no list from the resolver: $P"
  [ "$(rcounter "$P" crypto/1 resolver_instruments)" = 2 ] || fail "agent $a: the resolver listed $(rcounter "$P" crypto/1 resolver_instruments) instruments, not 2: $P"
  [ "$(rcounter "$P" crypto/1 resolver_mismatch)" = 0 ] || fail "agent $a: the resolver disagrees with the config: $P"
done

# 1. Before any data, the three agents add their recap sessions together, by name.
for a in $AGENTS; do config "$a" yes > "$TMP/$a.yaml"; done
for a in $AGENTS; do http_post "$(api "$a")" /reload > "$TMP/reload.$a.out" 2>&1 || fail "reload $a: $(cat "$TMP/reload.$a.out")"; done
sleep 0.5
for a in $AGENTS; do
  P=$(http_get "$(api "$a")" /partitions)
  for p in 1 2; do
    [ "$(pcounter "$P" "crypto/$p" recap_requests)" = 1 ] || fail "agent $a asked partition /$p $(pcounter "$P" "crypto/$p" recap_requests) times before any data (once: the list names each instrument's home): $P"
  done
done
gateway 1 a; GW1=$!
gateway 2 a; GW2=$!
merged 1 1
merged 2 1
for a in $AGENTS; do
  P=$(http_get "$(api "$a")" /partitions)
  for p in 1 2; do
    [ "$(pcounter "$P" "crypto/$p" recap_hold_overflows)" = 0 ] || fail "agent $a overflowed its hold on /$p waiting for the first section (raise hold_bytes): $P"
    [ "$(pcounter "$P" "crypto/$p" recap_requests)" = 1 ] || fail "agent $a asked partition /$p $(pcounter "$P" "crypto/$p" recap_requests) times (once: only its own instrument): $P"
    [ "$(pcounter "$P" "crypto/$p" recap_elsewhere)" = 0 ] || fail "agent $a asked /$p for an instrument it does not have: $P"
  done
done
wait $GW1 || fail "gateway /1 exit $?"
wait $GW2 || fail "gateway /2 exit $?"
compare "after the late sessions"

# 2. Gateway /1 restarts (Sequence from 1, a new epoch): a bulk resync of /1 from every agent.
gateway 1 b; GW1=$!
merged 1 2
for a in $AGENTS; do
  P=$(http_get "$(api "$a")" /partitions)
  [ "$(pcounter "$P" crypto/1 recap_bulk_requests)" -ge 1 ] || fail "agent $a sent no bulk resync to /1 after its restart: $P"
  [ "$(pcounter "$P" crypto/1 recap_timeouts)" = 0 ] || fail "agent $a timed out on /1: the restarted stream's section was not taken at once: $P"
  [ "$(pcounter "$P" crypto/2 recap_merged)" = 1 ] || fail "agent $a recapped /2 again, which did not restart: $P"
done
wait $GW1 || fail "gateway /1 (restarted) exit $?"
compare "after gateway /1 restarted"

kill -INT $SNAP1 $SNAP2; wait $SNAP1 2>/dev/null; wait $SNAP2 2>/dev/null
S1=$(tail -1 "$TMP/snap1.log"); S2=$(tail -1 "$TMP/snap2.log")
echo "$S1"; echo "$S2"
echo "$S1" | grep -q " requests=3 " || fail "snap /1 did not see one request per agent: $S1"
echo "$S1" | grep -q " coalesced=2 " || fail "snap /1 did not coalesce the agents' requests: $S1"
echo "$S1" | grep -q " bulk=3 " || fail "snap /1 did not see one bulk resync per agent: $S1"
echo "$S1" | grep -q " sections=2 " || fail "snap /1 did not publish BTC-USD once per step: $S1"
echo "$S2" | grep -q " requests=3 " || fail "snap /2 did not see one request per agent: $S2"
echo "$S2" | grep -q " bulk=0 " || fail "snap /2 was asked for a resync: $S2"
echo "$S2" | grep -q " sections=1 " || fail "snap /2 did not publish ETH-USD once: $S2"
for s in "$S1" "$S2"; do
  echo "$s" | grep -q " unknown_instrument=0 " || fail "a snap was asked for an instrument it does not have: $s"
  echo "$s" | grep -q " not_here=0 " || fail "a snap was asked for another partition's instrument: $s"
done
R1=$(echo "$S1" | grep -o ' resolves=[0-9]*' | cut -d= -f2)
[ "${R1:-0}" -ge 3 ] || fail "snap /1 (the resolver) answered ${R1:-0} Resolves, fewer than one per agent: $S1"
echo "PASS: 2 partitions x 3 agents: the resolver listed the topic for every agent, sessions by name asked each instrument's home only before any data, each snap published its instrument once per step, and every recap session's books equal the live ones"
