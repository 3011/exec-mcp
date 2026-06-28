# exec-mcp validation design

## Goal

Build a minimal bounded streaming exec gateway. It exposes exactly one execution capability and leaves kubectl, helm, git, argocd and flux semantics to the remote shell.

The prototype is intentionally not a second GitOps API, not a file API, and not a kubectl wrapper.

## Runtime choice

The host currently has Node.js but no Go toolchain. This prototype uses dependency-free Node.js so it can build and test immediately on the target machine. The implementation avoids external packages and uses only:

- node:http
- node:child_process
- node:test
- node:crypto

Go would still be my preferred production implementation if the toolchain is available, because RSS and process control are easier to keep tight. The Node prototype is acceptable for validation because the design avoids buffering full command output.

## Interface

Current prototype endpoint:

```http
POST /exec
Accept: text/event-stream
Content-Type: application/json
```

Request:

```json
{
  "command": "kubectl get pods -A",
  "cwd": "/root/config-git",
  "timeout_seconds": 120,
  "max_output_bytes": 5242880,
  "env": {
    "NO_COLOR": "1"
  }
}
```

SSE events:

```json
{"type":"start","exec_id":"...","pid":123,"started_at":"...","cwd":"/root/config-git"}
{"type":"stdout","exec_id":"...","data":"...","seq":1}
{"type":"stderr","exec_id":"...","data":"...","seq":2}
{"type":"heartbeat","exec_id":"...","elapsed_ms":15000,"stdout_bytes":10,"stderr_bytes":0}
{"type":"truncated","exec_id":"...","stream":"combined","max_output_bytes":5242880}
{"type":"timeout","exec_id":"...","timeout_seconds":120,"action":"sigterm"}
{"type":"exit","exec_id":"...","code":0,"signal":null,"duration_ms":1234,"stdout_bytes":10,"stderr_bytes":0,"truncated":false,"timed_out":false,"stdout_tail":"...","stderr_tail":"..."}
```

## Bounds

Default configuration:

- allowed cwd: `/root`, `/root/config-git`, `/root/exec-mcp`, `/tmp`
- default timeout: 120s
- hard max timeout: 600s
- default max output: 5 MiB
- hard max output: 20 MiB
- concurrency: 2 active execs
- heartbeat: 15s
- ring buffer tail: 64 KiB per stream
- kill grace: 5s

## Critical behavior

1. stdout and stderr are drained until the child exits.
2. After `max_output_bytes`, the gateway stops forwarding excess bytes but keeps reading pipes.
3. Only tail buffers are retained in memory.
4. stderr events do not mean failure. The exit event is authoritative.
5. Timeout kills the process group, not just the shell child.
6. Client disconnect aborts the command and kills the process group.
7. Common secret patterns are redacted in streamed chunks and tail summaries.
8. Metrics never label by raw command.

## MCP integration plan

This prototype validates the exec core and HTTP/SSE behavior. The next step is to wrap the same runner behind MCP Streamable HTTP as a single `exec` tool.

Important MCP boundaries:

- MCP stdio stdout must only carry MCP JSON-RPC messages.
- Remote command stdout/stderr must be payload data, not server process stdout.
- Server stderr is implementation logging and should not be interpreted as tool failure.
- MCP cancellation should map to process-group termination and cleanup.
- If a client or bridge does not expose live progress, final response must include summary plus capped stdout/stderr tail.

## Validation matrix

| Case | Expected result |
|---|---|
| normal stdout | stdout event and exit code 0 |
| stderr warning | stderr event and exit code 0 |
| invalid cwd | error event `invalid_cwd` |
| large output | `truncated=true`, command still exits |
| timeout | timeout event, process group killed |
| client disconnect | active command killed and metric incremented |
| secrets in output | redacted before streaming/tail |
| concurrent overload | `too_many_active_execs` |

## Run

```bash
cd /root/exec-mcp
npm test
npm run build
PORT=18080 node src/server.js
```

Manual SSE validation:

```bash
curl -N -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  --data '{"command":"echo hello; echo warn >&2","cwd":"/tmp"}' \
  http://127.0.0.1:18080/exec
```

## Implemented test coverage

Current test count: 27 passing tests.

Additional production-risk cases implemented:

- hard timeout rejection
- hard output-limit rejection
- valid env injection and invalid env key filtering
- stderr with exit code 0 remains success
- non-zero exit code is preserved in summary
- redaction applies to stream chunks and tail summary
- concurrency overload rejects additional execs
- `ENV` and `BASH_ENV` are removed before shell spawn
- invalid JSON returns HTTP 400
- invalid cwd returns SSE error event
- metrics include exit and rejection counters
- client abort kills active command and decrements active count
- timeout kills the whole process group, including background child
- large output is drained, counted, truncated and tail-bounded
- stdout/stderr sequence numbers are monotonic
- heartbeat carries elapsed time and byte counters
- unknown HTTP path returns 404 JSON
- oversized HTTP request body returns 413
- SSE final exit includes tail summary for graceful degradation
- HTTP concurrency overload returns SSE error

## Memory smoke result

The `scripts/memory-smoke.sh` check runs a command that writes 5,000,000 bytes to stdout while `max_output_bytes` is 1024.

Observed result on the current host:

```text
memory-smoke-ok before_rss_kib=59524 after_rss_kib=63408 sse_bytes=1798
```

This validates the key behavior: the child output is drained and counted, but the gateway does not forward or retain the full output in memory.
