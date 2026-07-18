# Threat model

## Scope

This document covers the `exec-mcp` HTTP service, its local SSH transport process, and interactions with one configured remote host.

The intended deployment is a trusted, single-tenant MCP connection. Multi-tenant isolation and direct exposure to untrusted networks are outside the design.

## Assets

- Integrity and availability of the remote host.
- Confidentiality of the SSH private key and remote files.
- Confidentiality of command output and operational metadata.
- Integrity of uploaded files and executed commands.
- Availability of gateway execution capacity.
- Integrity of the pinned remote SSH host identity.

## Trust boundaries

### Client to gateway

The gateway accepts arbitrary shell commands and operator-wide lifecycle controls. There is no built-in authentication, authorization, or TLS. An external authenticated transport, reverse proxy, or private network boundary is mandatory.

### Gateway to remote host

The gateway authenticates with an SSH key and trusts the server identity established by `known_hosts`. The remote SSH account and operating-system permissions are the main authorization boundary.

### Remote shell and filesystem

`ALLOWED_CWDS` limits accepted working directories and file-tool paths, but it is not a sandbox. Commands can access anything allowed by the remote account, including paths outside the cwd allowlist through ordinary shell arguments.

## Assumptions

- Every client allowed to reach the service is authorized to execute arbitrary commands as the configured SSH user.
- One tenant controls the connection; execution IDs and lifecycle controls are not isolated per user.
- The remote host provides `/bin/sh` and Python 3.
- SSH host-key verification is enabled with a trusted pinned key.
- The SSH identity is dedicated and has the minimum necessary remote privileges.
- Secrets are not intentionally placed in command labels or command previews.

## Threats and controls

### Unauthorized client executes commands

**Impact:** complete compromise within the privileges of the remote SSH account.

**Controls:** bind to loopback or a private interface; require authenticated transport; enforce firewall or network policy; never expose the service directly to the public internet.

### Overprivileged SSH identity

**Impact:** compromise expands to the whole remote host or adjacent infrastructure.

**Controls:** use a dedicated non-root account; restrict sudo; restrict filesystem permissions; use a dedicated key; consider `authorized_keys` restrictions such as source constraints where compatible.

### SSH man-in-the-middle

**Impact:** commands, files, or credentials are sent to an attacker-controlled host.

**Controls:** set `REMOTE_STRICT_HOST_KEY_CHECKING=yes`; provide a pinned `known_hosts` file out of band; treat host-key changes as security events.

### Path traversal or symlink escape in cwd/file tools

**Impact:** file operations or command startup occur outside the configured path allowlist.

**Controls:** require absolute command cwd; resolve real paths remotely; compare against normalized allowlist roots; reject file symlink escapes; require upload parent directories to exist.

**Residual risk:** an allowed command can still refer to any path accessible to the SSH user. The allowlist is input validation, not command sandboxing.

### Shell injection

**Impact:** arbitrary shell execution.

**Control boundary:** arbitrary shell execution is the intended function. The command is not interpolated into a second application-level command template; it is sent as the remote shell program. Only fully trusted callers may use the service.

### Resource exhaustion

**Impact:** memory pressure, blocked capacity, excessive remote processes, or large request bodies.

**Controls:** hard timeout ceiling; output forwarding ceiling; bounded ring buffers; request-size limits; file-size limits; active concurrency limit; bounded recent history; continuous pipe draining; emergency circuit breaker.

### Stuck or detached remote process

**Impact:** capacity leakage or a remote command surviving cancellation.

**Controls:** local SSH transport process groups; timeout and disconnect cancellation; SIGTERM/SIGKILL escalation; idempotent finalization; active registry; reaper; circuit breaker.

**Residual risk:** a deliberately detached remote descendant may survive after the local SSH transport exits. Remote operating-system controls remain necessary.

### Secret exposure in output or metadata

**Impact:** credentials appear in client responses, logs, metrics, or status history.

**Controls:** common secret-pattern redaction; no raw command or environment storage; command fingerprints; command previews disabled by default; sanitized labels; no raw command metric labels.

**Residual risk:** redaction cannot recognize every secret format. Avoid printing secrets and keep lifecycle logs protected.

### Malicious file upload or download

**Impact:** overwrite, exfiltration, disk consumption, or unsafe downstream processing.

**Controls:** trusted callers only; path allowlist and realpath checks; existing-parent requirement; regular-file download requirement; decoded-size limits; bounded remote protocol output.

**Residual risk:** the gateway does not inspect file content or prevent overwriting an allowed file. Remote permissions and caller authorization must enforce policy.

### Cross-tenant execution control

**Impact:** one caller lists or cancels another caller's command.

**Control boundary:** the service is single-tenant. Control-plane tools are intentionally operator-wide. Do not share one instance across mutually untrusted users.

### Supply-chain compromise

**Impact:** malicious build inputs or Actions alter releases.

**Controls:** dependency-free runtime; lockfile; CI; CodeQL; Dependabot for npm, Docker, and GitHub Actions; protected default branch; immutable release tags and container digests.

## Deployment checklist

- [ ] The service is not directly internet-accessible.
- [ ] Authentication and encrypted transport exist before the gateway.
- [ ] Network access is restricted to intended clients.
- [ ] The SSH user is dedicated and non-root where possible.
- [ ] The SSH key is mounted read-only and is not stored in the repository.
- [ ] Strict host-key checking is enabled with a pinned `known_hosts` file.
- [ ] `ALLOWED_CWDS` is minimal and reviewed.
- [ ] Timeout, output, file-size, request-size, and concurrency limits are set.
- [ ] Command previews are disabled unless explicitly required.
- [ ] Logs and metrics are access-controlled.
- [ ] Alerts cover circuit-open state, repeated timeouts, capacity exhaustion, and restarts.
- [ ] A tested rollback target exists.
