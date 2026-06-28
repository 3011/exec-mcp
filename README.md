# exec-mcp

A dependency-free Node.js implementation for a bounded remote exec MCP gateway.

It intentionally exposes one capability: run a single bounded remote shell command and return a capped result with stdout/stderr tails plus an authoritative exec summary. It is not a GitOps API, not a Kubernetes API, and not a file-management API. Higher-level tools such as kubectl, git, docker, helm, argocd, and flux are left to the remote shell.

## Current interface

- `GET /healthz`
- `GET /metrics`
- `POST /exec` with `Accept: text/event-stream` for SSE event streaming
- `POST /mcp` for MCP Streamable HTTP / JSON-RPC tool calls

The MCP server exposes exactly one tool:

```text
exec
```

The tool executes one command on the configured remote execution host. In the current dev deployment, the `ssh-mcp` connector runs commands on `dev-debian` through SSH.

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

Output semantics:

- MCP `tools/call` returns final text, not live SSE events.
- The final text contains bounded stdout/stderr tail content followed by `[exec summary]`.
- `[exec summary]` is authoritative for `exit`, `signal`, `duration_ms`, byte counts, `truncated`, and `timed_out`.
- stderr output alone is not failure. Non-zero exit code, signal, or `timed_out=true` is failure.
- Output may be truncated. Do not assume the returned text is the full command output.

Concurrency semantics:

- Active execs are bounded by `MAX_CONCURRENT_EXECS`.
- When full, the tool returns `too_many_active_execs` with `active`, `max`, `oldest_age_seconds`, and `states`.
- `too_many_active_execs` usually means real concurrency pressure, not necessarily a service fault.
- v11 tracks active execs through `ExecRegistry` and releases slots through `finally`, timeout abort, client-close abort, and a reaper fallback.

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
node --test: 32 tests, 32 pass
npm run build: pass
npm run validate: pass
```

Memory smoke writes 5 MB from a child process while forwarding only 1 KiB plus summary/tail metadata. The gateway drains child output and keeps only bounded tails in memory.
