#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PORT_VALUE="${PORT:-18081}"
LOG_FILE="/tmp/exec-mcp-memory-smoke.log"
OUT_FILE="/tmp/exec-mcp-memory-smoke.sse"
rm -f "$LOG_FILE" "$OUT_FILE"
PORT="$PORT_VALUE" HOST=127.0.0.1 DEFAULT_MAX_OUTPUT_BYTES=1024 HARD_MAX_OUTPUT_BYTES=2048 RING_BUFFER_BYTES=64 node src/server.js >"$LOG_FILE" 2>&1 &
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

BEFORE_RSS=$(ps -o rss= -p "$PID" | tr -d ' ')
curl -fsS -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  --data '{"command":"node -e \"process.stdout.write('"'"'x'"'"'.repeat(5000000))\"","cwd":"/tmp","max_output_bytes":1024}' \
  "http://127.0.0.1:${PORT_VALUE}/exec" >"$OUT_FILE"
AFTER_RSS=$(ps -o rss= -p "$PID" | tr -d ' ')

grep -q 'event: truncated' "$OUT_FILE"
grep -q '"stdout_bytes":5000000' "$OUT_FILE"

echo "memory-smoke-ok before_rss_kib=${BEFORE_RSS} after_rss_kib=${AFTER_RSS} sse_bytes=$(wc -c < "$OUT_FILE")"
