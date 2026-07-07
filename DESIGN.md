# exec-mcp design

## Goal

Build a minimal bounded SSH remote execution gateway for MCP agents.

The design exposes bounded remote shell execution plus base64 file upload/download. It leaves kubectl, helm, git, docker, argocd, flux, and other domain-specific semantics to the remote shell. The server is intentionally not a second GitOps API and not a Kubernetes wrapper.

## Runtime choice

The implementation is dependency-free Node.js. It uses only core modules for HTTP, process management, tests, and bounded buffering.

The important production property is not the language choice; it is the bounded behavior:

1. validate cwd and limits before spawning;
2. never buffer full stdout/stderr;
3. drain child pipes until exit;
4. return bounded tails and an explicit exec summary;
5. clean up local active-slot bookkeeping on every execution path.

## HTTP/SSE interface

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
{"type":"timeout","exec_id":"...","timeout_seconds":120,"action":"sigterm","reason":"exec_timeout"}
{"type":"exit","exec_id":"...","code":0,"signal":null,"duration_ms":1234,"stdout_bytes":10,"stderr_bytes":0,"truncated":false,"timed_out":false,"stdout_tail":"...","stderr_tail":"..."}
```

## MCP interface

```http
POST /mcp
Content-Type: application/json
```

The MCP server exposes these tools:

- `exec`
- `download_file`
- `upload_file`

Tool description requirements:

- State that commands run on the configured remote execution host, not inside the agent runtime.
- State that commands run through `/bin/sh -c` after cwd allowlist validation.
- State that `exec` is a general remote shell tool, not a GitOps/Kubernetes API.
- State that output may be truncated.
- State that `[exec summary]` is authoritative for exit code, signal, timeout, duration, byte counts, and truncation.
- State that stderr alone is not failure.
- State that `too_many_active_execs` reports active/max/oldest age/states.

MCP `tools/call` returns final text only. It does not expose live SSE events. The text is assembled from bounded stdout/stderr tails plus:

```text
[exec summary] exit=<code> signal=<signal|null> duration_ms=<ms> stdout_bytes=<n> stderr_bytes=<n> truncated=<true|false> timed_out=<true|false>
```

File tools return JSON text. `download_file` includes `data_base64`; `upload_file` accepts `data_base64`. Both validate paths against the remote `ALLOWED_CWDS` after resolving real paths, reject symlink escapes, and enforce file-size limits.

Success/failure semantics:

- success: `exit=0` and `timed_out=false`;
- failure: non-zero exit, signal, timeout, validation rejection, spawn failure, or concurrency rejection;
- stderr text alone does not make the result a failure.

## Bounds

Default configuration:

- allowed cwd: `/root`, `/root/config-git`, `/root/exec-mcp`, `/tmp`, `/app`
- default timeout: 120s
- hard max timeout: 600s
- default max output: 5 MiB
- hard max output: 20 MiB
- concurrency: 2 active execs
- heartbeat: 15s
- ring buffer tail: 64 KiB per stream
- kill grace: 5s

## Active slot lifecycle, v11

v11 hardens only the local active slot lifecycle. It does not attempt full remote process governance.

Implementation:

1. `active` is backed by `ExecRegistry`, a Map keyed by exec id.
2. Each record stores id, start time, timeout time, state, AbortController, timeout timer, and released flag.
3. `release(id)` is idempotent.
4. `runner.run()` releases through `finally` after successful acquire.
5. Timeout aborts the record controller.
6. Client close aborts the record controller.
7. A reaper first aborts overdue records, then releases them after a grace period.
8. `too_many_active_execs` includes `active`, `max`, `oldest_age_seconds`, and `states`.

Explicit non-goals for v11:

- remote pid file;
- remote process group protocol;
- startup cleanup;
- debug endpoint;
- cancel-by-exec-id API;
- queueing;
- business-specific scripts;
- GitOps or Kubernetes-specific logic.

## Critical behavior

1. stdout and stderr are drained until the child exits.
2. After `max_output_bytes`, the gateway stops forwarding excess bytes but keeps reading pipes.
3. Only tail buffers are retained in memory.
4. stderr events do not mean failure. The exit summary is authoritative.
5. Timeout kills the local process group used by the gateway, then escalates after grace.
6. Client disconnect aborts the command and releases the active slot.
7. Common secret patterns are redacted in streamed chunks and tail summaries.
8. Metrics never label by raw command.

## Validation matrix

| Case | Expected result |
|---|---|
| normal stdout | stdout event and exit code 0 |
| stderr warning | stderr event and exit code 0 |
| invalid cwd | error event `invalid_cwd` |
| large output | `truncated=true`, command still exits |
| timeout | timeout event, command aborted, active released |
| client disconnect | active command aborted and metric incremented |
| secrets in output | redacted before streaming/tail |
| concurrent overload | `too_many_active_execs` includes active/max/age/states |
| release race | release remains idempotent |
| reaper fallback | overdue active record is aborted then released after grace |

## Run

```bash
cd /root/exec-mcp
npm test
npm run build
PORT=18080 node src/server.js
```

Manual SSE validation:

```bash
curl -N -H 'content-type: application/json'   -H 'accept: text/event-stream'   --data '{"command":"echo hello; echo warn >&2","cwd":"/tmp"}'   http://127.0.0.1:18080/exec
```

Manual MCP validation:

```bash
curl -fsS -H 'content-type: application/json'   --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'   http://127.0.0.1:18080/mcp
```

## Implemented test coverage

Current test count: 32 passing tests.

Additional production-risk cases implemented:

- hard timeout rejection
- hard output-limit rejection
- valid env injection and invalid env key filtering
- stderr with exit code 0 remains success
- non-zero exit code is preserved in summary
- redaction applies to stream chunks and tail summary
- concurrency overload rejects additional execs with diagnostic context
- `ENV` and `BASH_ENV` are removed before shell spawn
- invalid JSON returns HTTP 400
- invalid cwd returns SSE error event
- metrics include exit and rejection counters
- client abort kills active command and decrements active count
- timeout kills the process group, including background child
- large output is drained, counted, truncated and tail-bounded
- stdout/stderr sequence numbers are monotonic
- heartbeat carries elapsed time and byte counters
- unknown HTTP path returns 404 JSON
- oversized HTTP request body returns 413
- SSE final exit includes tail summary for graceful degradation
- HTTP concurrency overload returns SSE error
- ExecRegistry release is idempotent
- ExecRegistry full acquire reports active age and states
- ExecRegistry timeout aborts the record controller
- ExecRegistry reaper aborts first and releases after grace period

## Memory smoke result

The `scripts/memory-smoke.sh` check runs a command that writes 5,000,000 bytes to stdout while `max_output_bytes` is 1024.

The expected result is a truncated SSE response with full byte counts and bounded tails, without retaining or forwarding the full output in memory.
