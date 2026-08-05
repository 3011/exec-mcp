# exec-mcp design

## Goal

`exec-mcp` is a minimal, bounded SSH remote execution and file-transfer gateway for trusted MCP clients.

It intentionally does not implement GitOps, Kubernetes, Docker, or domain-specific APIs. Those capabilities remain ordinary commands on the configured remote host. The gateway focuses on validation, bounded resource use, lifecycle control, and predictable results.

## Architecture

```text
trusted MCP or HTTP client
        |
        | authenticated/private transport supplied by operator
        v
exec-mcp HTTP service
        |
        | dedicated SSH identity + pinned host key
        v
remote non-interactive /bin/sh
```

The implementation is written in strict TypeScript, compiles to JavaScript for Node.js, and uses Node.js core modules only at runtime. Runtime state is in memory and is lost on restart.

The source is split by responsibility without adding runtime frameworks:

- `server.ts` composes dependencies and owns HTTP/SSE startup, routing, and shutdown;
- `mcp-handler.ts` owns MCP request tracking and JSON-RPC method dispatch;
- `tool-schemas.ts` contains the stable MCP tool schema objects;
- `artifact-transfer.ts` owns ChatGPT file references, binary SSH transfer, SHA-256 verification, atomic commits, and short-lived export resources;
- `metrics.ts` renders Prometheus text from the existing runner and registry state.

`server.ts` is the composition root. Lower-level modules do not import initialized server state or import `server.ts`.

## Public interfaces

### HTTP/SSE

```http
POST /exec
Accept: text/event-stream
Content-Type: application/json
```

Example request:

```json
{
  "command": "git status --short",
  "cwd": "/workspace",
  "timeout_seconds": 120,
  "max_output_bytes": 5242880,
  "env": {
    "NO_COLOR": "1"
  },
  "label": "inspect repository status"
}
```

Representative events:

```json
{"type":"start","exec_id":"...","pid":123,"started_at":"...","cwd":"/workspace"}
{"type":"stdout","exec_id":"...","data":"...","seq":1}
{"type":"stderr","exec_id":"...","data":"...","seq":2}
{"type":"heartbeat","exec_id":"...","elapsed_ms":15000,"stdout_bytes":10,"stderr_bytes":0}
{"type":"truncated","exec_id":"...","stream":"combined","max_output_bytes":5242880}
{"type":"timeout","exec_id":"...","timeout_seconds":120,"action":"sigterm","reason":"exec_timeout"}
{"type":"exit","exec_id":"...","code":0,"signal":null,"duration_ms":1234,"stdout_bytes":10,"stderr_bytes":0,"truncated":false,"timed_out":false,"stdout_tail":"...","stderr_tail":"..."}
```

### MCP Streamable HTTP

```http
POST /mcp
Content-Type: application/json
```

Exposed tools:

- `exec`
- `list_active_execs`
- `get_exec_status`
- `cancel_exec`
- `import_chatgpt_file`
- `export_remote_file`

MCP `tools/call` returns bounded final text and structured content. Live streaming is available only through `/exec`.

## Execution lifecycle

1. Validate command shape, limits, environment keys, and absolute `cwd`.
2. Resolve the remote working directory and verify its real path remains inside `ALLOWED_CWDS`.
3. Acquire an `ExecRegistry` slot.
4. Spawn an isolated local SSH transport process group.
5. Drain stdout and stderr continuously while forwarding only up to the configured limit.
6. Retain bounded, redacted tail buffers.
7. React to normal exit, timeout, HTTP disconnect, MCP cancellation, or `cancel_exec`.
8. Finalize exactly once, release capacity, and write a sanitized record to bounded recent history.

`release` and finalization are idempotent. Cancellation never frees capacity before the runner has finalized the transport.

## Bounds

Default configuration:

- allowed remote directories: `/workspace`, `/tmp`
- default timeout: 120 seconds
- hard timeout ceiling: 600 seconds
- default forwarded output: 5 MiB
- hard forwarded-output ceiling: 20 MiB
- active concurrency: 2
- heartbeat interval: 15 seconds
- retained tail: 64 KiB per stream
- termination grace: 5 seconds
- recent execution history: 100 records

## Control plane and history

`list_active_execs`, `get_exec_status`, and `cancel_exec` are operator-wide tools for a trusted single tenant. They do not consume execution slots.

Status records contain sanitized operational metadata rather than full commands or environments:

- execution ID and timestamps;
- state and abort source;
- command SHA-256 fingerprint;
- optional sanitized label;
- optional redacted command preview when explicitly enabled;
- transport process ID and final outcome;
- output byte counts and truncation state.

## Circuit breaker

A reaper first requests cancellation for overdue records. If the local SSH transport still does not confirm exit after the emergency grace window, the registry may force-finalize the stale record and open the execution circuit.

While the circuit is open:

- new `exec` calls are rejected;
- control-plane tools remain available;
- the diagnostic is retained even if normal history entries are evicted;
- a late confirmed transport close may clear the circuit;
- an operator may deliberately restart the service after investigation.

This avoids claiming safe capacity while an execution has an unresolved local transport lifecycle.

## File transfer

Two transfer classes are intentionally separated.

Artifact tools keep file bytes out of model-authored arguments and ordinary text results. Small exports may include a protocol-level MCP embedded resource that the host ingests directly without model copying:

- `import_chatgpt_file` receives a ChatGPT-authorized temporary file reference, downloads to a bounded local spool, computes SHA-256, streams raw bytes through SSH stdin, writes a same-directory temporary file remotely, fsyncs it, and atomically commits it.
- A retry to an existing destination succeeds only when size and SHA-256 are identical; different content requires explicit `overwrite=true`.
- `export_remote_file` streams raw remote bytes through SSH stdout, verifies size and SHA-256 locally, and stores an immutable temporary object behind a 256-bit capability token.
- Export results always contain structured metadata, including `embedded` and the explicit `delivery_mode` (`embedded_resource` or `resource_link_only`), plus an MCP HTTPS `resource_link`. Files no larger than `ARTIFACT_EMBED_MAX_BYTES` also contain an MCP embedded binary resource. Compatible ChatGPT hosts ingest that resource into the current working container while preserving the requested file name. Automatic large-file chunking is not currently provided.
- Capability URLs expire and have a bounded GET count. HEAD requests and byte ranges are supported.
- Relative remote paths resolve from `DEFAULT_CWD`; real paths and parents must remain inside `ALLOWED_CWDS`; symlinks and non-regular files are rejected.
- Artifact size, concurrency, and end-to-end duration are bounded independently from command execution.

Secure MCP Tunnel carries MCP JSON-RPC, not arbitrary artifact GET requests. A deployment using the tunnel therefore exposes only the capability-protected artifact data-plane paths through a separate HTTPS ingress: `/artifacts/<token>/...`, plus `/tool-container/<token>/current` when the fixed bridge is configured. Command, MCP, metrics, and health routes remain private.

## Security boundaries

The command is intentionally arbitrary shell text. The gateway does not attempt to classify commands as safe, and directory allowlisting is not a sandbox. The remote SSH account and its operating-system permissions are the primary authorization boundary.

The service does not provide built-in client authentication or TLS. Operators must supply those controls externally and keep the service on a trusted network. See [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md).

## Important residual limits

- A local SSH transport exit does not prove every independently detached remote process exited.
- Output redaction is defense in depth, not a guarantee that every secret format is recognized.
- State, history, and cancellation metadata are process-local and disappear after restart.
- There is no persistent queue or multi-tenant isolation.
- The gateway cannot make a broadly privileged remote account safe.

## Validation matrix

| Case | Expected result |
|---|---|
| normal stdout | stdout event and exit code 0 |
| stderr warning | stderr event and exit code 0 |
| invalid or escaped cwd | validation rejection before useful command execution |
| large output | output drains, `truncated=true`, tails remain bounded |
| timeout | process group termination requested and active slot finalized |
| client disconnect | matching execution cancelled and request mapping removed |
| MCP cancellation | only the matching session request is cancelled |
| manual cancellation | idempotent cancellation result |
| concurrent overload | diagnostic `too_many_active_execs` response |
| stale transport | circuit opens after unconfirmed emergency reap |
| secret-like output | redacted in forwarded chunks and retained tails |
| file symlink escape | rejected after realpath validation |

## Local validation

```bash
npm test
npm run build
npm run validate
```

The current suite contains 58 passing tests, including random-binary bidirectional artifact transfer, idempotent retry, checksum failure, HEAD, and byte-range coverage.
