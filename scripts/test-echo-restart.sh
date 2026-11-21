#!/usr/bin/env bash
# Acceptance test for the echo agent's durable resume and idempotent publish.
#
# A bus-only process owns the embedded NATS server. A separate echo process is
# killed -9 mid-stream, then restarted. echo.out must contain exactly COUNT
# unique messages once everything drains.
set -euo pipefail
cd "$(dirname "$0")/.."

COUNT="${COUNT:-300}"
RATE="${RATE:-300}"
PREFIX="${PREFIX:-e2e}"

pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

NATS_PORT="$(pick_port)"
NATS_ADDR="127.0.0.1:${NATS_PORT}"
NATS_URL="nats://${NATS_ADDR}"
SERVER_ADMIN="127.0.0.1:$(pick_port)"
ECHO_ADMIN="127.0.0.1:$(pick_port)"

TMP="$(mktemp -d)"
SERVER_PID=""
ECHO_PID=""
PUB_PID=""

cleanup() {
  set +e
  if [[ -n "$PUB_PID" ]]; then kill -9 "$PUB_PID" 2>/dev/null; fi
  if [[ -n "$ECHO_PID" ]]; then kill -9 "$ECHO_PID" 2>/dev/null; fi
  if [[ -n "$SERVER_PID" ]]; then kill -9 "$SERVER_PID" 2>/dev/null; fi
  wait 2>/dev/null
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "$TMP"
  else
    echo "logs kept in $TMP"
  fi
}
trap cleanup EXIT

wait_health() {
  local admin="$1"
  local name="$2"
  for _ in $(seq 1 200); do
    if curl -fsS "http://${admin}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "health check failed for ${name} on ${admin}" >&2
  return 1
}

echo "building dor and probe"
make --no-print-directory go-build

echo "starting bus server on ${NATS_ADDR}"
"$PWD/bin/dor" run \
  --agents=none \
  --nats-listen "$NATS_ADDR" \
  --nats-store "$TMP/nats" \
  --admin-addr "$SERVER_ADMIN" \
  >"$TMP/server.log" 2>&1 &
SERVER_PID=$!
wait_health "$SERVER_ADMIN" "bus server"

start_echo() {
  local round="$1"
  "$PWD/bin/dor" run \
    --agents=echo \
    --nats-url "$NATS_URL" \
    --admin-addr "$ECHO_ADMIN" \
    >"$TMP/echo-${round}.log" 2>&1 &
  ECHO_PID=$!
  wait_health "$ECHO_ADMIN" "echo round ${round}"
  echo "echo agent round ${round} is healthy (pid ${ECHO_PID})"
}

start_echo 1

echo "publishing ${COUNT} messages to echo.in at ${RATE}/s"
"$PWD/bin/probe" publish \
  --url "$NATS_URL" \
  --subject echo.in \
  --count "$COUNT" \
  --prefix "$PREFIX" \
  --rate "$RATE" \
  >"$TMP/publish.json" 2>&1 &
PUB_PID=$!

sleep 0.6
echo "kill -9 echo agent (pid ${ECHO_PID}) mid-stream"
kill -9 "$ECHO_PID"
wait "$ECHO_PID" 2>/dev/null || true
ECHO_PID=""
sleep 0.2

start_echo 2
wait "$PUB_PID"
PUB_PID=""
cat "$TMP/publish.json"

echo "collecting echo.out (expected ${COUNT})"
"$PWD/bin/probe" collect \
  --url "$NATS_URL" \
  --subject echo.out \
  --expected "$COUNT" \
  --idle 3s \
  --timeout 90s \
  --body-prefix "${PREFIX}-"

echo "test-echo-restart: PASS (${COUNT} unique messages, no duplicates)"
