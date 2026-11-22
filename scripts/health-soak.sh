#!/usr/bin/env bash
# F2·3 acceptance harness: run one isolated stack for N minutes and prove
# /healthz stays 200 with every selected agent running, while the Supervisor
# logs no restart action (successful or advisory).
#
# The live stack (:8080/:8091/:4222/:5433 dor/:5173/:8090) is never touched.
# This uses its own ports, NATS store directory and database dor_f2_3.
#
# Usage:
#   scripts/health-soak.sh [minutes]       # default 30
#   KEEP_EVIDENCE=1 scripts/health-soak.sh 30
#   GEONAMES_DIR=/path/to/geonames scripts/health-soak.sh 30
set -euo pipefail
cd "$(dirname "$0")/.."

MINUTES="${1:-30}"
if ! [[ "$MINUTES" =~ ^[0-9]+$ ]] || (( MINUTES < 1 )); then
  echo "usage: $0 [minutes]" >&2
  exit 2
fi

DB="${DOR_F23_DB:-dor_f2_3}"
DB_URL="postgres://dor:dor@127.0.0.1:5433/${DB}?sslmode=disable"
NATS_PORT="${DOR_F23_NATS_PORT:-14222}"
GATEWAY_ADDR="${DOR_F23_GATEWAY_ADDR:-127.0.0.1:18080}"
ADMIN_ADDR="${DOR_F23_ADMIN_ADDR:-127.0.0.1:18091}"
SAMPLE_SECONDS="${DOR_F23_SAMPLE_SECONDS:-15}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE="${DOR_F23_EVIDENCE:-$(pwd)/.dor/f2-3-health-${STAMP}}"
LOG="${EVIDENCE}/dor.log"
HEALTH="${EVIDENCE}/healthz.jsonl"
OPS="${EVIDENCE}/ops-actions.jsonl"
SUMMARY="${EVIDENCE}/summary.json"
BIN="$(pwd)/bin/dor"
STORE="$(mktemp -d "${TMPDIR:-/tmp}/dor-f23-nats.XXXXXX")"
PID=""
DB_CREATED=0

mkdir -p "$EVIDENCE"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

cleanup() {
  set +e
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID" 2>/dev/null
    for _ in $(seq 1 60); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 1
    done
    kill -KILL "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
  fi
  if (( DB_CREATED == 1 )); then
    PGPASSWORD=dor dropdb -h 127.0.0.1 -p 5433 -U dor --if-exists "$DB" >/dev/null 2>&1
  fi
  rm -rf "$STORE"
  echo "evidence kept in $EVIDENCE"
}
trap cleanup EXIT

for port in "$NATS_PORT" "${GATEWAY_ADDR##*:}" "${ADMIN_ADDR##*:}"; do
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

GEONAMES_ARGS=()
if [[ -n "${GEONAMES_DIR:-}" ]]; then
  GEONAMES_ARGS=(--geonames-dir "$GEONAMES_DIR")
fi

echo "starting isolated all-agent stack: admin=$ADMIN_ADDR gateway=$GATEWAY_ADDR nats=$NATS_PORT db=$DB"
echo "evidence: $EVIDENCE"
env -u TYPESAFE_API_KEY -u OPENROUTER_API_KEY -u ANTHROPIC_API_KEY -u DOR_LLM_BACKENDS \
  DOR_AGENTS=all \
  DOR_NATS_URL= \
  DOR_NATS_LISTEN="127.0.0.1:${NATS_PORT}" \
  DOR_NATS_STORE="$STORE" \
  DOR_ADMIN_ADDR="$ADMIN_ADDR" \
  DOR_GATEWAY_ADDR="$GATEWAY_ADDR" \
  DOR_DATABASE_URL="$DB_URL" \
  DOR_TEI_URL= \
  DOR_PHOTON_URL= \
  DOR_NTFY_URL= \
  DOR_VAPID_PUBLIC_KEY= \
  DOR_VAPID_PRIVATE_KEY= \
  DOR_PUSH_ENCRYPTION_KEY= \
  DOR_RSS_CONCURRENCY=2 \
  DOR_GDELT_MAX_ITEMS=50 \
  "$BIN" run --agents=all "${GEONAMES_ARGS[@]}" >"$LOG" 2>&1 &
PID=$!

HEALTH_BODY="${EVIDENCE}/healthz-last.json"
health_code() {
  curl -sS -o "$HEALTH_BODY" -w '%{http_code}' \
    "http://${ADMIN_ADDR}/healthz" 2>/dev/null || echo 000
}

# Wait for the first fully healthy sample; startup may take a minute because
# every consumer and the gazetteer load before /healthz turns 200.
ready=0
for _ in $(seq 1 120); do
  if kill -0 "$PID" 2>/dev/null && [[ "$(health_code)" == "200" ]]; then
    ready=1
    break
  fi
  sleep 1
done
if (( ready == 0 )); then
  echo "last health body: $(cat "$HEALTH_BODY" 2>/dev/null || true)" >&2
  echo "last log lines:" >&2
  tail -40 "$LOG" >&2 || true
  fail "stack did not become healthy within 120s"
fi

DOR_F23_MINUTES="$MINUTES" \
DOR_F23_SAMPLE_SECONDS="$SAMPLE_SECONDS" \
DOR_F23_ADMIN="$ADMIN_ADDR" \
DOR_F23_HEALTH="$HEALTH" \
DOR_F23_OPS="$OPS" \
DOR_F23_SUMMARY="$SUMMARY" \
python3 - <<'PY'
import datetime, json, os, sys, time, urllib.request
admin = os.environ["DOR_F23_ADMIN"]
health_path = os.environ["DOR_F23_HEALTH"]
ops_path = os.environ["DOR_F23_OPS"]
summary_path = os.environ["DOR_F23_SUMMARY"]
minutes = int(os.environ["DOR_F23_MINUTES"])
sample_seconds = max(1, int(os.environ["DOR_F23_SAMPLE_SECONDS"]))
end = time.time() + minutes * 60
unhealthy = 0
samples = 0
restart_actions = []
seen_actions = set()

def get(path):
    req = urllib.request.Request(f"http://{admin}{path}", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.status, json.loads(r.read().decode())

with open(health_path, "a", buffering=1) as hf, open(ops_path, "a", buffering=1) as of:
    while time.time() < end:
        at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        samples += 1
        try:
            status, body = get("/healthz")
        except Exception as exc:
            status, body = 0, {"error": str(exc)}
        hf.write(json.dumps({"at": at, "http": status, "body": body}, sort_keys=True) + "\n")
        if status != 200:
            unhealthy += 1
        try:
            _, ops = get("/api/ops")
            for action in ops.get("actions") or []:
                if action.get("kind") == "restart":
                    key = (action.get("at"), action.get("target"), action.get("message"))
                    if key not in seen_actions:
                        seen_actions.add(key)
                        restart_actions.append(action)
                        of.write(json.dumps(action, sort_keys=True) + "\n")
        except Exception:
            pass
        time.sleep(sample_seconds)

summary = {
    "minutes": minutes,
    "samples": samples,
    "unhealthy_samples": unhealthy,
    "restart_actions": restart_actions,
    "passed": unhealthy == 0 and not restart_actions,
}
with open(summary_path, "w") as f:
    json.dump(summary, f, indent=2, sort_keys=True)
print(json.dumps(summary, sort_keys=True))
sys.exit(0 if summary["passed"] else 1)
PY

echo "health soak passed: $SUMMARY"
