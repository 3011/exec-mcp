#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

npm test
npm run build

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
node src/server.js >"$LOG_FILE" 2>&1 &
PID=$!
cleanup() {
  kill "$PID" >/dev/null 2>&1 || true
  wait "$PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:${PORT_VALUE}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

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

echo "validation ok"
