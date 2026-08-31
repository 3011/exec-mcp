#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PORT_VALUE="${PORT:-18080}"
LOG_FILE="/tmp/exec-mcp-validate.log"
OUT_FILE="/tmp/exec-mcp-validate.sse"
FAKE_SSH="$PWD/scripts/fake-ssh.js"
rm -f "$LOG_FILE" "$OUT_FILE"
PORT="$PORT_VALUE" \
HOST=127.0.0.1 \
REMOTE_BIN="${REMOTE_BIN:-$(command -v node)}" \
REMOTE_BIN_ARGS="${REMOTE_BIN_ARGS:---no-warnings $FAKE_SSH}" \
REMOTE_HOST="${REMOTE_HOST:-fake-remote}" \
REMOTE_KEY_PATH="${REMOTE_KEY_PATH:-/tmp/fake-ssh-key}" \
node dist/src/server.js >"$LOG_FILE" 2>&1 &
PID=$!
cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
  wait "$PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

READY=0
for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${PORT_VALUE}/healthz" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.1
done
if [ "$READY" -ne 1 ]; then
  echo "runtime-smoke server did not become ready" >&2
  cat "$LOG_FILE" >&2 || true
  exit 1
fi

curl -fsS -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  --data '{"command":"echo hello; echo warn >&2","cwd":"/tmp"}' \
  "http://127.0.0.1:${PORT_VALUE}/exec" >"$OUT_FILE"

grep -q 'event: stdout' "$OUT_FILE"
grep -q 'event: stderr' "$OUT_FILE"
grep -q 'event: exit' "$OUT_FILE"
grep -q 'hello' "$OUT_FILE"
grep -q 'warn' "$OUT_FILE"

curl -fsS "http://127.0.0.1:${PORT_VALUE}/metrics" | grep -q 'exec_mcp_requests_total'
curl -fsS "http://127.0.0.1:${PORT_VALUE}/runtime" | grep -q 'Runtime Console'
curl -fsS "http://127.0.0.1:${PORT_VALUE}/runtime/api/overview" | grep -q '"health":"healthy"'
RUNTIME_POST_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:${PORT_VALUE}/runtime/api/overview")
test "$RUNTIME_POST_STATUS" = 405

echo "runtime-smoke-ok"
