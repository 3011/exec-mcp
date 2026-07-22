#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

PORT_VALUE="${PORT:-18081}"
LOG_FILE="/tmp/exec-mcp-memory-smoke.log"
OUT_FILE="/tmp/exec-mcp-memory-smoke.sse"
FAKE_SSH="$PWD/scripts/fake-ssh.js"
OUTPUT_BYTES="${MEMORY_SMOKE_OUTPUT_BYTES:-5000000}"
MAX_RSS_GROWTH_KIB="${MEMORY_SMOKE_MAX_RSS_GROWTH_KIB:-32768}"
MAX_SSE_BYTES="${MEMORY_SMOKE_MAX_SSE_BYTES:-16384}"

for value in "$OUTPUT_BYTES" "$MAX_RSS_GROWTH_KIB" "$MAX_SSE_BYTES"; do
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -le 0 ]; then
    echo "memory-smoke configuration values must be positive integers" >&2
    exit 2
  fi
done

rm -f "$LOG_FILE" "$OUT_FILE"
PORT="$PORT_VALUE" \
HOST=127.0.0.1 \
DEFAULT_MAX_OUTPUT_BYTES=1024 \
HARD_MAX_OUTPUT_BYTES=2048 \
RING_BUFFER_BYTES=64 \
REMOTE_BIN="${REMOTE_BIN:-$(command -v node)}" \
REMOTE_BIN_ARGS="${REMOTE_BIN_ARGS:---no-warnings $FAKE_SSH}" \
REMOTE_HOST="${REMOTE_HOST:-fake-remote}" \
REMOTE_KEY_PATH="${REMOTE_KEY_PATH:-/tmp/fake-ssh-key}" \
node dist/src/server.js >"$LOG_FILE" 2>&1 &
SERVER_PROCESS_ID=$!
cleanup() {
  kill "${SERVER_PROCESS_ID}" >/dev/null 2>&1 || true
  wait "${SERVER_PROCESS_ID}" >/dev/null 2>&1 || true
}
read_rss_kib() {
  ps -o rss= "${SERVER_PROCESS_ID}" | tr -d ' '
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
  echo "memory-smoke server did not become ready" >&2
  cat "$LOG_FILE" >&2 || true
  exit 1
fi

# Node-created pipe descriptors are non-blocking. Force the remote Python writer
# into blocking mode so it reliably emits the requested byte count instead of
# raising BlockingIOError/EAGAIN on one large write.
REQUEST_BODY=$(node -e '
const bytes = Number(process.argv[1]);
const command = `python3 -c "import os,sys; os.set_blocking(sys.stdout.fileno(), True); sys.stdout.buffer.write(b\x27x\x27 * ${bytes})"`;
process.stdout.write(JSON.stringify({ command, cwd: "/tmp", max_output_bytes: 1024 }));
' "$OUTPUT_BYTES")

BEFORE_RSS=$(read_rss_kib)
curl -fsS -N \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  --data-binary "$REQUEST_BODY" \
  "http://127.0.0.1:${PORT_VALUE}/exec" >"$OUT_FILE"
AFTER_RSS=$(read_rss_kib)

EXPECTED_STDOUT='"stdout_bytes":'"$OUTPUT_BYTES"
grep -q 'event: truncated' "$OUT_FILE"
grep -q "$EXPECTED_STDOUT" "$OUT_FILE"
grep -q '"stderr_bytes":0' "$OUT_FILE"
grep -q '"code":0' "$OUT_FILE"

RSS_GROWTH_KIB=$((AFTER_RSS - BEFORE_RSS))
SSE_BYTES=$(wc -c < "$OUT_FILE")
if [ "$RSS_GROWTH_KIB" -gt "$MAX_RSS_GROWTH_KIB" ]; then
  echo "memory-smoke RSS growth exceeded limit: ${RSS_GROWTH_KIB} KiB > ${MAX_RSS_GROWTH_KIB} KiB" >&2
  exit 1
fi
if [ "$SSE_BYTES" -gt "$MAX_SSE_BYTES" ]; then
  echo "memory-smoke SSE output exceeded limit: ${SSE_BYTES} bytes > ${MAX_SSE_BYTES} bytes" >&2
  exit 1
fi

echo "memory-smoke-ok output_bytes=${OUTPUT_BYTES} before_rss_kib=${BEFORE_RSS} after_rss_kib=${AFTER_RSS} rss_growth_kib=${RSS_GROWTH_KIB} sse_bytes=${SSE_BYTES}"
