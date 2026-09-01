# exec-mcp

[![CI](https://github.com/3011/exec-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/3011/exec-mcp/actions/workflows/ci.yml)
[![CodeQL](https://github.com/3011/exec-mcp/actions/workflows/codeql.yml/badge.svg)](https://github.com/3011/exec-mcp/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/3011/exec-mcp)](https://github.com/3011/exec-mcp/releases)
[![License](https://img.shields.io/github/license/3011/exec-mcp)](LICENSE)

A strict TypeScript Node.js gateway with no runtime npm dependencies that gives trusted MCP clients bounded remote command execution and file transfer over SSH.

`exec-mcp` deliberately stays small: it validates paths and resource limits, runs a non-interactive remote shell, streams or returns bounded output, and exposes execution lifecycle controls. Higher-level behavior remains in tools already installed on the remote host.

> [!CAUTION]
> This service is a remote command execution gateway. It has no built-in user authentication or TLS termination and is designed for a trusted, single-tenant connection. Never expose it directly to an untrusted network. Put it behind an authenticated transport or reverse proxy, restrict network access, use a dedicated low-privilege SSH account, and review the [threat model](docs/THREAT_MODEL.md) before deployment.

## Features

- MCP Streamable HTTP and an HTTP/SSE execution endpoint.
- Configurable command timeout, output limit, concurrency limit, and bounded tail buffers.
- Remote working-directory allowlist with realpath and symlink-escape checks.
- ChatGPT-native bidirectional artifact transfer using file references, binary SSH streaming, SHA-256 verification, and atomic remote commits.
- Embedded-resource export that materializes verified remote files into ChatGPT without model-authored Base64.
- Unified Exec Job Manager with synchronous exec, asynchronous start_exec, queued admission, recent status lookup, and idempotent cancellation.
- Incremental retained job logs with independent stdout/stderr cursors and bounded long-polling.
- One authoritative remote supervisor for process-group cleanup, command deadlines, cancellation, termination escalation, and final execution results.
- Secret-pattern redaction for streamed output and retained tails.
- Prometheus-compatible metrics and health endpoints.
- Execution-capacity gauges and duration histograms for latency percentiles.
- Strict TypeScript source compiled to JavaScript for production.
- No runtime npm dependencies.

## MCP tools

| Tool | Purpose |
|---|---|
| `begin_task` | Create one explicit logical task context and return a server-issued `task_handle` for conversation/task grouping. |
| `exec` | Run one bounded non-interactive command synchronously through the Job Manager; MCP callers must provide a `task_handle`. |
| `start_exec` | Submit one bounded background command with a `task_handle` and immediately return a queryable `exec_id`. |
| `list_active_execs` | List queued and running remote executions plus sync/async/global admission capacity. |
| `get_exec_status` | Read status, lightweight derived timings/diagnostics, and incremental redacted stdout/stderr with independent cursors and bounded long-polling. |
| `cancel_exec` | Idempotently cancel a queued or running execution; terminal states are immutable. |
| `import_chatgpt_file` | Transfer a current ChatGPT file into the remote environment with SHA-256 verification and atomic commit. |
| `export_remote_file` | Transfer one verified remote file to ChatGPT as an embedded resource for host-side materialization into `/mnt/data`; oversized files are rejected. |

The control-plane tools are operator-wide. They assume one trusted tenant and are intentionally available even when command capacity is full.

### Explicit task context

MCP protocol sessions are not treated as ChatGPT conversation identity. Before the first `exec` or `start_exec` in a new ChatGPT conversation or new logical task, call `begin_task` once. The server returns an opaque `task-...` handle; reuse that exact handle on every subsequent `exec` and `start_exec` in the same task. Different ChatGPT windows should create different handles.

`exec` and `start_exec` require a server-issued handle at the MCP schema and runtime-validation layers. An unknown or invented handle is rejected with `unknown_task_handle`, which instructs the caller to run `begin_task`. `get_exec_status` and `cancel_exec` need only `exec_id`; task ownership is already stored with the execution. The handle is correlation metadata only and grants no authorization. Task contexts are bounded and process-local, matching the existing Runtime/Job Manager retention model.

This follows the MCP explicit-handle pattern for carrying application state across otherwise stateless requests. It provides reliable Execution MCP grouping without pretending that the server knows ChatGPT's internal conversation ID.

### Choosing `exec` vs `start_exec`

Use `exec` for short, deterministic commands when the next reasoning step needs the result immediately. A useful rule of thumb is roughly five seconds or less: `pwd`, `ls`, `cat`/`grep`, `git status`/`git diff`, `kubectl get`, and similar probes.

Use `start_exec` when runtime is uncertain, may exceed a few seconds, or useful work can continue in parallel. Typical examples are dependency installation, test suites, builds, image builds, scans, migrations, and long scripts. Keep the returned `exec_id`, continue independent work, and use `get_exec_status` at the synchronization point (optionally with bounded `wait_seconds`) rather than busy-polling immediately after submission.

Do not emulate background execution inside `exec` with `nohup`, `disown`, or shell `&`. Pass the real foreground command to `start_exec` so timeout, cancellation, status, retained logs, and remote process-group cleanup remain owned by the Job Manager. Use concise `label` values when several independent jobs run concurrently.

## Quick start

### Requirements

- Node.js 20 or newer, or Docker.
- An SSH-reachable remote host with `/bin/sh` and Python 3.
- A dedicated SSH key and a pinned `known_hosts` file.

### Run with Docker

```bash
docker run --rm \
  --name exec-mcp \
  -p 127.0.0.1:8080:8080 \
  -p 127.0.0.1:9090:9090 \
  -e REMOTE_HOST=remote-host \
  -e REMOTE_USER=execmcp \
  -e REMOTE_KEY_PATH=/run/secrets/id_ed25519 \
  -e REMOTE_KNOWN_HOSTS_PATH=/run/secrets/known_hosts \
  -e REMOTE_STRICT_HOST_KEY_CHECKING=yes \
  -e ALLOWED_CWDS=/workspace,/tmp \
  -e DEFAULT_CWD=/workspace \
  -v "$PWD/id_ed25519:/run/secrets/id_ed25519:ro" \
  -v "$PWD/known_hosts:/run/secrets/known_hosts:ro" \
  ghcr.io/3011/exec-mcp:v0.7.0
```

The example binds only to loopback. Add authentication and TLS at the surrounding transport layer before making the service reachable from another machine.

Container tags follow the repository release model:

- `vX.Y.Z` is the versioned release tag; treat release tags as immutable by policy.
- `sha-<short-commit>` identifies an exact commit build; an image digest is the strongest immutable deployment reference.
- `main` tracks the latest successful default-branch build and should not be treated as an immutable production version.

### Run from source

```bash
git clone https://github.com/3011/exec-mcp.git
cd exec-mcp
npm ci
npm run validate

REMOTE_HOST=remote-host \
REMOTE_USER=execmcp \
REMOTE_KEY_PATH="$HOME/.ssh/id_ed25519" \
REMOTE_KNOWN_HOSTS_PATH="$HOME/.ssh/known_hosts" \
REMOTE_STRICT_HOST_KEY_CHECKING=yes \
ALLOWED_CWDS=/workspace,/tmp \
DEFAULT_CWD=/workspace \
npm start
```

## Interfaces

- `GET /healthz`
- `GET /metrics`
- `GET /runtime` read-only Runtime Console
- `GET /runtime/api/*` read-only runtime observation API
- `POST /exec` with `Accept: text/event-stream`
- `POST /mcp` for MCP Streamable HTTP / JSON-RPC
- Optional separate metrics listener on `METRICS_PORT`


### Runtime Console

`GET /runtime` serves a dependency-free, read-only execution viewer from the main listener. It is intentionally not exposed by the optional `METRICS_PORT` listener and provides no run, retry, cancel, kill, or other mutation endpoint.

The console focuses on current execution state rather than historical metrics: running/queued jobs, capacity, last runtime activity, last output time, retained stdout/stderr, origin metadata, a bounded lifecycle trace, and small lifecycle timing diagnostics. Executions are grouped under explicit Task Contexts. Activity and output are deliberately separate signals: a job can remain alive and observable while producing no output. Quiet periods are never labeled as hung or stuck without proof.

Lifecycle timings and diagnostics are derived at query time from timestamps the Runtime Observer already records; they add no timers, background workers, database, or persistent state. The derived fields cover queue wait, local transport/runner launch, time to first output, runtime, termination, total duration, coarse phase/activity, and a conservative failure phase. `transport_startup_ms` describes local runner/transport launch only and does not claim that SSH handshake or remote process startup has completed.

Trace origin records only facts the server actually receives. An MCP transport session identifier may be shown, but it is explicitly not treated as a ChatGPT conversation identifier. MCP executions now carry a server-issued explicit `task_handle` created by `begin_task`; the Runtime Console groups executions by that handle and displays the optional task label. The handle is a logical correlation context, not proof of ChatGPT's internal conversation ID.

Runtime state, traces, and retained output remain bounded and process-local, matching the existing Job Manager lifecycle. The console uses the same externally authenticated network boundary as the main service; there is no separate built-in Runtime Console authentication layer.

### MCP initialization

```bash
curl -fsS http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"example-client","version":"1.0.0"}}}'
```

### Execute a command

```json
{
  "command": "git status --short",
  "shell": "sh",
  "cwd": "/workspace",
  "timeout_seconds": 120,
  "max_output_bytes": 5242880,
  "env": {
    "NO_COLOR": "1"
  },
  "label": "inspect repository status"
}
```

Every command must explicitly select `shell: "sh"` or `shell: "bash"`. `sh` uses `/bin/sh -c`; `bash` uses `/bin/bash -c` for Bash-specific syntax. No default shell is applied, only these two interpreters are supported, and login/interactive shells are never used. The caller is intentionally allowed to supply arbitrary shell text; authorization must therefore happen before requests reach this service.

### Output semantics

- `/exec` emits SSE lifecycle events.
- MCP `tools/call` returns bounded final text plus structured content; it does not stream live command events.
- The final execution summary is authoritative for exit code, signal, timeout, duration, byte counts, and truncation.
- Stderr output alone does not indicate failure. A non-zero exit code, signal, or timeout does.
- Once the forwarding limit is reached, output is still drained so the child process cannot block on a full pipe.
- Synchronous exec keeps bounded final stdout/stderr tails for compatibility.
- get_exec_status reads bounded retained Job Manager logs incrementally with independent stdout/stderr cursors. It also returns query-time `timings` and `diagnostics` derived from existing lifecycle timestamps so an agent can distinguish queue, launch, running, quiet, and termination phases without a separate tracing service. `has_more_*` means another retained page is available; `*_log_truncated` means older bytes were permanently discarded.
- Runtime timeout starts only when a queued job enters execution; queue waiting time does not consume timeout_seconds.
- V1 Job Manager state and retained logs are process-local; service restart does not recover prior queued or running records.

### Cancellation boundary

Running executions use one SSH session to a remote Python supervisor. The supervisor is the authoritative owner of the command process group: it enforces the business timeout, receives cancellation requests over the same framed session, performs bounded SIGTERM-to-SIGKILL escalation, reaps the group, and returns the final execution reason. A small durable result journal is used only to recover that final fact if the primary SSH transport disappears before the result reaches exec-mcp. Queued jobs can still be cancelled before any process is spawned. If the remote final state cannot be authoritatively recovered, the execution circuit fails closed instead of claiming that termination succeeded.

## Configuration

| Variable | Default | Description |
|---|---:|---|
| `HOST` | `0.0.0.0` | Main HTTP listen address. |
| `PORT` | `8080` | Main HTTP port. |
| `METRICS_PORT` | `9090` | Optional separate metrics/health port. |
| `REMOTE_BIN` | `ssh` | SSH-compatible executable. |
| `REMOTE_BIN_ARGS` | empty | Additional arguments passed before generated SSH arguments. |
| `REMOTE_HOST` | empty | Required remote host. |
| `REMOTE_PORT` | `22` | Remote SSH port. |
| `REMOTE_USER` | `execmcp` | Remote SSH user. |
| `REMOTE_KEY_PATH` | empty | Required private-key path. |
| `REMOTE_KNOWN_HOSTS_PATH` | `/run/secrets/known_hosts` | Pinned SSH host-key file. |
| `REMOTE_STRICT_HOST_KEY_CHECKING` | `yes` | SSH host-key checking mode. |
| `ALLOWED_CWDS` | `/workspace,/tmp` | Comma-separated remote directory allowlist. |
| `DEFAULT_CWD` | first allowed path | Default remote working directory. |
| `DEFAULT_TIMEOUT_SECONDS` | `120` | Default command timeout. |
| `MAX_TIMEOUT_SECONDS` | `600` | Hard command timeout ceiling. |
| `DEFAULT_MAX_OUTPUT_BYTES` | `5242880` | Default combined forwarded-output limit. |
| `HARD_MAX_OUTPUT_BYTES` | `20971520` | Hard forwarded-output ceiling. |
| `MAX_CONCURRENT_EXECS` | `2` | Legacy/default concurrency value used as the fallback for sync, async, and global limits when their dedicated variables are omitted. |
| `SYNC_MAX_CONCURRENT_EXECS` | `MAX_CONCURRENT_EXECS` | Maximum running synchronous `exec` jobs. |
| `ASYNC_MAX_CONCURRENT_EXECS` | `MAX_CONCURRENT_EXECS` | Maximum running asynchronous `start_exec` jobs. |
| `GLOBAL_MAX_CONCURRENT_EXECS` | `MAX_CONCURRENT_EXECS` | Maximum running jobs across both admission classes. |
| `MAX_QUEUED_EXECS` | `20` | Maximum jobs waiting for an admission slot. |
| `JOB_LOG_BYTES` | `1048576` | Maximum retained Job Manager stdout bytes per stream and stderr bytes per stream. Older retained bytes are discarded. |
| `JOB_RETENTION_SECONDS` | `3600` | In-memory retention period for finalized Job Manager log buffers. |
| `STATUS_DEFAULT_MAX_OUTPUT_BYTES` | `32768` | Default combined incremental stdout/stderr bytes returned by one status query. |
| `STATUS_HARD_MAX_OUTPUT_BYTES` | `262144` | Hard ceiling for one incremental status-output page. |
| `STATUS_MAX_WAIT_SECONDS` | `30` | Maximum long-poll duration accepted by `get_exec_status`. |
| `RING_BUFFER_BYTES` | `65536` | Retained tail capacity per stream. |
| `HEARTBEAT_SECONDS` | `15` | SSE heartbeat interval. |
| `KILL_GRACE_SECONDS` | `5` | Delay between termination and forced kill. |
| `MCP_MAX_REQUEST_BYTES` | `16777216` | Maximum MCP request body size. |
| `ARTIFACT_MAX_BYTES` | `268435456` | Absolute artifact size ceiling. Imports may use the full value; exports are additionally capped by `ARTIFACT_EMBED_MAX_BYTES`. |
| `ARTIFACT_EMBED_MAX_BYTES` | `1450000` | Configurable remote-export ceiling, hard-capped at 1.45 MB (1,450,000 bytes). Lower values are allowed; larger files are rejected with no URL fallback. |
| `ARTIFACT_MAX_CONCURRENT_TRANSFERS` | `2` | Maximum concurrent artifact imports and exports. |
| `ARTIFACT_SPOOL_DIR` | `/tmp/exec-mcp-artifacts` | Local temporary/cache directory for artifact transfer. |
| `ARTIFACT_EMBED_URI_BASE` | `https://exec-mcp.invalid/embedded` | Identifier base placed in embedded-resource URIs. It is metadata only; the host receives bytes from the MCP `blob` field and must not fetch this URI. |
| `ARTIFACT_TRANSFER_TIMEOUT_SECONDS` | `600` | End-to-end artifact transfer timeout. |
| `ARTIFACT_IMPORT_ALLOWED_HOSTS` | empty | Optional comma-separated exact hosts or suffix rules. A leading dot matches the suffix and all subdomains, for example `.oaiusercontent.com` or `.blob.core.windows.net`. Empty permits any HTTPS host in the trusted single-tenant model. |
| `ARTIFACT_IMPORT_ALLOW_HTTP` | `false` | Allow HTTP file-reference URLs. Intended only for local tests. |
| `RECENT_EXEC_HISTORY_LIMIT` | `100` | Number of finalized executions retained in memory. |
| `EXPOSE_REDACTED_COMMAND_PREVIEW` | `false` | Expose a redacted command preview in operator status. |
| `LIFECYCLE_LOGS` | `true` | Emit structured execution lifecycle logs. |

For all lifecycle and circuit-breaker settings, see [DESIGN.md](DESIGN.md).


## ChatGPT artifact transfer

Use `import_chatgpt_file` for files attached to or generated in the current ChatGPT conversation. The tool declares `_meta["openai/fileParams"]`, so ChatGPT replaces the conversation-local file path with a temporary `{ download_url, file_id, mime_type?, file_name? }` reference. `exec-mcp` downloads the binary bytes to a bounded local spool, computes SHA-256, streams the bytes over SSH, verifies the remote hash, and commits the destination atomically. Retried calls are idempotent when the existing destination has identical bytes.

Use `export_remote_file` for the reverse direction. It streams the remote file into a bounded local spool, verifies size and SHA-256, reads the verified bytes, and returns exactly one MCP embedded binary resource plus structured metadata (`bytes`, `sha256`, `file_name`, `embedded=true`, and `delivery_mode=embedded_resource`). A compatible ChatGPT host can materialize that resource as a real file in `/mnt/data` while preserving `file_name`.

Exports larger than `ARTIFACT_EMBED_MAX_BYTES` are rejected with `file_too_large`; the service deliberately provides no `resource_link`, public download URL, or large-file fallback. The embedded `blob` is Base64 at the MCP protocol layer, so the practical ceiling must account for Base64 expansion, JSON framing, tunnel limits, host materialization limits, and gateway memory. The hard maximum and default are 1.45 MB (1,450,000 bytes). This conservative ceiling is covered by backend boundary tests and intentionally leaves margin below the platform-sensitive range observed during ChatGPT host ingestion/materialization testing.

Secure MCP Tunnel carries the embedded bytes inside MCP JSON-RPC, so remote-to-ChatGPT export requires no public artifact ingress. Keep `/mcp`, `/exec`, `/runtime`, `/runtime/api/*`, `/metrics`, and `/healthz` private behind the authenticated transport.

## Development

```bash
npm test             # strict build and regression tests
npm run build        # strict type-check and compile to dist/
npm run test:memory  # bounded-output and RSS smoke test
npm run validate     # canonical gate: build, tests, Runtime/HTTP/SSE, and memory smoke tests
```

Runtime source is organized by responsibility: `server.ts` for HTTP composition and lifecycle, `mcp-handler.ts` for JSON-RPC dispatch, `exec-runner.ts` for Job Manager admission/queue/status orchestration, `remote-execution-session.ts` for one remote execution session, `remote-supervisor.ts` for the authoritative remote process lifecycle, `ssh-transport.ts` for shared SSH transport primitives, `artifact-transfer.ts` for verified bidirectional file transfer, `runtime-observer.ts` for bounded execution observations/traces, `runtime-console.ts` for the read-only browser surface, and `metrics.ts` for Prometheus rendering.

CI runs the same `npm run validate` gate used for local release checks before building the container. The compiled Node test runner uses a bounded file-level concurrency of 4 to reduce timing contention in process/HTTP lifecycle tests. CodeQL and Dependabot configuration are included in the repository.

## Documentation

- [Design](DESIGN.md)
- [Operational runbook](RUNBOOK.md)
- [Threat model](docs/THREAT_MODEL.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## Versioning

The project uses Semantic Versioning. The version in `package.json`, MCP `serverInfo`, Git tags, GitHub Releases, and published container tags must match. Historical internal architecture labels are not part of the public version scheme.

## License

[MIT](LICENSE)
