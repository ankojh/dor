#!/usr/bin/env bash
# F2·3 acceptance harness for the split-process runbook in docs/operations.md.
#
# Starts the documented one-process-per-agent layout on isolated ports with its
# own embedded NATS store and database, then proves:
#   - every process answers /healthz 200 with its selected agent running;
#   - the gateway process serves /api/places although no plot agent runs there;
#   - an alert created through the gateway matches a newly published story
#     (matcher process); the hit is readable through the alert API.
#
# The live stack (:8080/:8091/:4222/:5433 dor/:5173/:8090) is never touched.
set -euo pipefail
cd "$(dirname "$0")/.."

DB="${DOR_F23_SPLIT_DB:-dor_f2_3_split}"
DB_URL="postgres://dor:dor@127.0.0.1:5433/${DB}?sslmode=disable"
NATS_PORT="${DOR_F23_SPLIT_NATS_PORT:-14222}"
NATS_URL="nats://127.0.0.1:${NATS_PORT}"
GATEWAY="127.0.0.1:18080"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE="${DOR_F23_SPLIT_EVIDENCE:-$(pwd)/.dor/f2-3-split-${STAMP}}"
LOGDIR="$EVIDENCE/logs"
BIN="$(pwd)/bin/dor"
STORE="$(mktemp -d "${TMPDIR:-/tmp}/dor-f23-split-nats.XXXXXX")"
DB_CREATED=0
PIDS=()
NAMES=()
JAR="$EVIDENCE/cookies.txt"

mkdir -p "$LOGDIR"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

cleanup() {
  set +e
  for ((i=${#PIDS[@]}-1; i>=0; i--)); do
    pid="${PIDS[$i]}"
    kill -TERM "$pid" 2>/dev/null
  done
  for ((i=${#PIDS[@]}-1; i>=0; i--)); do
    wait "${PIDS[$i]}" 2>/dev/null
  done
  if (( DB_CREATED == 1 )); then
    PGPASSWORD=dor dropdb -h 127.0.0.1 -p 5433 -U dor --if-exists "$DB" >/dev/null 2>&1
  fi
  rm -rf "$STORE" "$JAR"
  echo "evidence kept in $EVIDENCE"
}
trap cleanup EXIT

for port in "$NATS_PORT" 18080 18091 18101 18102 18103 18104 18105 \
  18111 18112 18113 18114 18115 18121 18122 18123 18124 18125 18131 18132; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "port $port is already in use; refusing to collide with another stack"
  fi
done

if ! PGPASSWORD=dor psql -h 127.0.0.1 -p 5433 -U dor -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='${DB}'" | grep -q 1; then
  PGPASSWORD=dor createdb -h 127.0.0.1 -p 5433 -U dor "$DB" || fail "createdb $DB"
  DB_CREATED=1
fi
PGPASSWORD=dor psql -h 127.0.0.1 -p 5433 -U dor -d "$DB" -v ON_ERROR_STOP=1 \
  -c 'CREATE EXTENSION IF NOT EXISTS vector' >/dev/null || fail "vector extension"

if [[ ! -x "$BIN" ]]; then
  go build -o "$BIN" ./cmd/dor || fail "build bin/dor"
fi
go build -o "$EVIDENCE/f23smoke" ./scripts/f2-3-smoke/publish || fail "build f23smoke"

GEONAMES_ARGS=()
if [[ -n "${GEONAMES_DIR:-}" ]]; then
  GEONAMES_ARGS=(--geonames-dir "$GEONAMES_DIR")
fi

COMMON_ENV=(
  env -u TYPESAFE_API_KEY -u OPENROUTER_API_KEY -u ANTHROPIC_API_KEY -u DOR_LLM_BACKENDS
  DOR_DATABASE_URL="$DB_URL"
  DOR_TEI_URL=
  DOR_PHOTON_URL=
  DOR_NTFY_URL=
  DOR_VAPID_PUBLIC_KEY=
  DOR_VAPID_PRIVATE_KEY=
  DOR_PUSH_ENCRYPTION_KEY=
  DOR_GATEWAY_ADDR="$GATEWAY"
)

start_owner() {
  "${COMMON_ENV[@]}" DOR_NATS_URL= DOR_NATS_LISTEN="127.0.0.1:${NATS_PORT}" \
    DOR_NATS_STORE="$STORE" DOR_ADMIN_ADDR=127.0.0.1:18091 \
    "$BIN" run --agents=none --provision "${GEONAMES_ARGS[@]}" \
    >"$LOGDIR/owner.log" 2>&1 &
  PIDS+=($!)
  NAMES+=(owner)
}

start_agent() {
  local name="$1" admin="$2"
  shift 2
  "${COMMON_ENV[@]}" DOR_NATS_URL="$NATS_URL" DOR_ADMIN_ADDR="$admin" \
    "$BIN" run "$@" "${GEONAMES_ARGS[@]}" >"$LOGDIR/${name}.log" 2>&1 &
  PIDS+=($!)
  NAMES+=("$name")
}

wait_health() {
  local name="$1" admin="$2" i
  for i in $(seq 1 180); do
    if [[ "$(curl -sS -o /dev/null -w '%{http_code}' "http://${admin}/healthz" 2>/dev/null || true)" == "200" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "last ${name} health: $(curl -sS "http://${admin}/healthz" 2>/dev/null || true)" >&2
  tail -30 "$LOGDIR/${name}.log" >&2 || true
  return 1
}

echo "starting documented split stack on isolated ports; evidence: $EVIDENCE"
start_owner
wait_health owner 127.0.0.1:18091 || fail "owner did not become healthy"

# Step 2: fetchers. Network is used by the real producers; this is the same
# binary and the documented commands, only on isolated admin ports.
start_agent rss-0 127.0.0.1:18101 --agents=rss --rss-shard 0
start_agent rss-1 127.0.0.1:18102 --agents=rss --rss-shard 1
start_agent rss-2 127.0.0.1:18103 --agents=rss --rss-shard 2
start_agent gdelt 127.0.0.1:18104 --agents=gdelt
start_agent hazard 127.0.0.1:18105 --agents=hazard

# Step 3: enrichment and stories.
start_agent normalize 127.0.0.1:18111 --agents=normalize
start_agent plot 127.0.0.1:18112 --agents=plot
start_agent classify 127.0.0.1:18113 --agents=classify
start_agent embed 127.0.0.1:18114 --agents=embed
start_agent cluster 127.0.0.1:18115 --agents=cluster

# Step 4: relation shards, editor, publisher + gateway.
start_agent relation-0 127.0.0.1:18121 --agents=relation --relation-shards 3 --relation-shard 0
start_agent relation-1 127.0.0.1:18122 --agents=relation --relation-shards 3 --relation-shard 1
start_agent relation-2 127.0.0.1:18123 --agents=relation --relation-shards 3 --relation-shard 2
start_agent editor 127.0.0.1:18124 --agents=editor
start_agent publisher 127.0.0.1:18125 --agents=publisher

# Step 5: prompts/alerts evaluator and notification delivery.
start_agent matcher 127.0.0.1:18131 --agents=matcher
start_agent notifier 127.0.0.1:18132 --agents=notifier

while read -r name admin; do
  wait_health "$name" "$admin" || fail "$name health"
done <<'EOF'
rss-0 127.0.0.1:18101
rss-1 127.0.0.1:18102
rss-2 127.0.0.1:18103
gdelt 127.0.0.1:18104
hazard 127.0.0.1:18105
normalize 127.0.0.1:18111
plot 127.0.0.1:18112
classify 127.0.0.1:18113
embed 127.0.0.1:18114
cluster 127.0.0.1:18115
relation-0 127.0.0.1:18121
relation-1 127.0.0.1:18122
relation-2 127.0.0.1:18123
editor 127.0.0.1:18124
publisher 127.0.0.1:18125
matcher 127.0.0.1:18131
notifier 127.0.0.1:18132
EOF

# /api/places must not need the plot process that runs on 18112.
places="$(curl -fsS "http://${GATEWAY}/api/places?query=Paris&limit=3")" || fail "/api/places"
python3 - "$places" <<'PY'
import json, sys
body = json.loads(sys.argv[1])
assert body.get("available") is True, body
assert len(body.get("places") or []) > 0, body
print("places:", [(p["name"], p["country_code"]) for p in body["places"][:3]])
PY

# Create the alert before publishing so the matcher sees the first story.changed.
curl -fsS -c "$JAR" "http://${GATEWAY}/api/session" >/dev/null || fail "session"
PROMPT="F2-3 smoketest wildfire evacuation in California"
created="$(curl -fsS -b "$JAR" -H "Origin: http://${GATEWAY}" -H 'Content-Type: application/json' \
  -d "{\"prompt\":\"${PROMPT}\",\"enabled\":true,\"channels\":[]}" \
  "http://${GATEWAY}/api/alerts")" || fail "create alert"
ALERT_ID="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["alert"]["id"])' "$created")"
echo "alert: $ALERT_ID"

# The matcher refreshes saved alerts every 5s.
sleep 7
"$EVIDENCE/f23smoke" -url "$NATS_URL" -id "$STAMP" -title "$PROMPT" >/dev/null || fail "publish smoke article"

story=""
for _ in $(seq 1 60); do
  listing="$(curl -fsS "http://${GATEWAY}/api/stories?query=F2-3%20smoketest&limit=5" 2>/dev/null || true)"
  story="$(python3 - "$listing" <<'PY' 2>/dev/null || true
import json, sys
try:
    body = json.loads(sys.argv[1])
except Exception:
    raise SystemExit(0)
for row in body.get("stories") or []:
    if "F2-3 smoketest" in row.get("title", ""):
        print(row["id"])
        break
PY
)"
  [[ -n "$story" ]] && break
  sleep 1
done
[[ -n "$story" ]] || fail "story did not reach the durable store"
echo "story: $story"

hits=""
for _ in $(seq 1 60); do
  hits="$(curl -fsS -b "$JAR" "http://${GATEWAY}/api/alerts/hits?alert_id=${ALERT_ID}" 2>/dev/null || true)"
  if python3 - "$hits" <<'PY' 2>/dev/null
import json, sys
body = json.loads(sys.argv[1])
raise SystemExit(0 if body.get("hits") else 1)
PY
  then
    break
  fi
  hits=""
  sleep 1
done
[[ -n "$hits" ]] || fail "matcher did not record an alert hit"
python3 - "$hits" "$story" <<'PY'
import json, sys
body = json.loads(sys.argv[1])
hits = body["hits"]
assert any(h["story_id"] == sys.argv[2] for h in hits), (hits, sys.argv[2])
print("alert hits:", [(h["story_id"], h["level"], h.get("approx")) for h in hits])
PY

cat >"$EVIDENCE/summary.json" <<EOF
{
  "processes": $(printf '%s\n' "${NAMES[@]}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().split()))'),
  "all_healthz_200": true,
  "places_available": true,
  "alert_id": "${ALERT_ID}",
  "story_id": "${story}",
  "alert_hit": true,
  "passed": true
}
EOF
echo "split-process smoke passed: $EVIDENCE/summary.json"
