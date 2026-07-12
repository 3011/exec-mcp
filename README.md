# exec-mcp

A dependency-free Node.js implementation for a bounded SSH remote execution MCP gateway.

It exposes bounded remote command execution plus base64 file upload/download over SSH. It is not a GitOps API and not a Kubernetes API. Higher-level tools such as kubectl, git, docker, helm, argocd, and flux are left to the remote shell.

## Current interface

- `GET /healthz`
- `GET /metrics`
- `POST /exec` with `Accept: text/event-stream` for SSE event streaming
- `POST /mcp` for MCP Streamable HTTP / JSON-RPC tool calls

The MCP server exposes these tools:

```text
exec
download_file
upload_file
```

All tools operate on the configured remote execution host. In the current dev deployment, the `ssh-mcp` connector runs commands and file transfers on `dev-debian` through SSH.

## MCP tool contract

Request arguments:

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

Field semantics:

- `command`: command string run through `/bin/sh -c` on the configured remote execution host. Use explicit quoting for pipelines, redirection, `&&`, and variable expansion. Avoid interactive or unbounded long-running commands.
- `cwd`: working directory on the remote execution host. It must be under the configured allowlist. If omitted, `DEFAULT_CWD` is used.
- `timeout_seconds`: maximum runtime. Values above `MAX_TIMEOUT_SECONDS` are rejected. On timeout, the server aborts the exec and sends `SIGTERM`, then `SIGKILL` after the configured kill grace period.
- `max_output_bytes`: maximum combined stdout/stderr bytes forwarded before truncation. The process is still drained until exit. Final byte counts and bounded tails are included in the summary.
- `env`: additional environment variables. Invalid variable names are ignored. `ENV` and `BASH_ENV` are removed before spawning.

File download arguments:

```json
{
  "path": "assets/logo.png",
  "max_bytes": 10485760
}
```

File download result:

```json
{
  "path": "/root/exec-mcp/assets/logo.png",
  "bytes": 12345,
  "mime_type": "image/png",
  "data_base64": "..."
}
```

File upload arguments:

```json
{
  "path": "uploads/report.xlsx",
  "data_base64": "...",
  "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "append": false
}
```

File upload result:

```json
{
  "path": "/root/exec-mcp/uploads/report.xlsx",
  "bytes": 12345,
  "action": "write",
  "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}
```

File tool semantics:

- `download_file` returns remote raw file bytes as `data_base64` after path allowlist validation.
- `download_file.max_bytes` is the maximum allowed file size. If omitted, `FILE_MAX_DOWNLOAD_BYTES` is used. Files over the limit are rejected, not truncated.
- `upload_file` writes remote raw file bytes from `data_base64` after path allowlist validation and decoded-size checks.
- `upload_file.data_base64` must decode to at most `FILE_MAX_UPLOAD_BYTES`.
- `upload_file.append=true` appends decoded bytes; otherwise it replaces the file.
- `MCP_MAX_REQUEST_BYTES` must be large enough for upload JSON bodies. Base64 expands raw bytes by roughly 33%.
- Parent directories must already exist.

Output semantics:

- MCP `tools/call` returns final text, not live SSE events.
- The final text contains bounded stdout/stderr tail content followed by `[exec summary]`.
- `[exec summary]` is authoritative for `exit`, `signal`, `duration_ms`, byte counts, `truncated`, and `timed_out`.
- stderr output alone is not failure. Non-zero exit code, signal, or `timed_out=true` is failure.
- Output may be truncated. Do not assume the returned text is the full command output.

Concurrency semantics:

- Active execs are bounded by `MAX_CONCURRENT_EXECS`.
- v12 adds operator-wide `list_active_execs`, `get_exec_status`, and idempotent `cancel_exec` control-plane tools. They do not consume execution slots and assume a trusted single-tenant MCP connection.
- Commands are fingerprinted with SHA-256. Full commands and environments are never stored in status/history; optional labels are sanitized and must not contain secrets. Redacted command previews remain disabled unless `EXPOSE_REDACTED_COMMAND_PREVIEW=true`.
- An unconfirmed emergency reap opens the execution circuit and rejects new execs until a late local SSH transport close is observed or the service is restarted. Control-plane tools remain available.
- When full, the tool returns `too_many_active_execs` with `active`, `max`, `oldest_age_seconds`, and `states`.
- `too_many_active_execs` usually means real concurrency pressure, not necessarily a service fault.
- v11 tracks active execs through `ExecRegistry` and releases slots through `finally`, timeout abort, client-close abort, and a reaper fallback.
- v12 keeps legacy metrics for one compatibility cycle and also exposes `exec_mcp_*` lifecycle, cancellation, history, and circuit-breaker metrics.

Cancellation boundary:

- The current connected request can be aborted by client close/disconnect.
- There is no cancel-by-exec-id API in v11.
- v11 protects local active-slot bookkeeping; it does not provide full remote process lifecycle governance.

## Commands

```bash
npm test
npm run build
npm run validate
npm start
```

## Validation status

Current local validation:

```text
node --test: 33 tests, 33 pass
npm run build: pass
npm run validate: pass
```

Memory smoke writes 5 MB from a child process while forwarding only 1 KiB plus summary/tail metadata. The gateway drains child output and keeps only bounded tails in memory.
