# exec-mcp operational runbook

This runbook is deployment-neutral. Adapt service, secret, and orchestration commands to your environment.

## Before deployment

- Place the HTTP service behind authentication and TLS, or keep it on an authenticated private transport.
- Permit inbound traffic only from the intended MCP bridge or operator network.
- Use a dedicated, low-privilege remote SSH account.
- Restrict the SSH key in `authorized_keys` where practical.
- Set `REMOTE_STRICT_HOST_KEY_CHECKING=yes` and mount a pinned `known_hosts` file.
- Set the smallest practical `ALLOWED_CWDS`, timeout, output, file-size, and concurrency limits.
- Keep command previews disabled unless the operational need outweighs metadata exposure.
- Review [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md).

## Readiness checks

1. `GET /healthz` returns HTTP 200.
2. `GET /metrics` returns Prometheus text.
3. MCP `initialize` reports the expected Semantic Version.
4. `tools/list` reports all seven tools, including `start_exec`.
5. A harmless `exec` such as `pwd` runs on the intended remote host and ends with a successful summary.
6. The returned working directory matches `DEFAULT_CWD` or the requested allowed path.
7. `list_active_execs` returns zero active tasks when idle.
8. The service has no unexpected restarts and its memory remains within the configured container limit.

## Release procedure

1. Update `package.json` using Semantic Versioning.
2. Update the matching version section in `CHANGELOG.md`.
3. Run `npm run validate` and build the container locally.
4. Open and merge a pull request after CI and CodeQL pass.
5. Create an annotated tag on the merged commit: `git tag -a v<package-version> -m "Release v<package-version>"`.
6. Push the tag. The existing CI workflow retests the tagged commit and publishes the matching `vX.Y.Z` and `sha-*` GHCR image tags.
7. The Release workflow extracts the matching `CHANGELOG.md` section and creates the GitHub Release after the tag is pushed.
8. Verify the CI and Release workflows succeeded and the matching GHCR release tag is pullable.
9. Roll out by the versioned release tag (treated as immutable by policy) or, preferably for strict pinning, by image digest.
10. Repeat the readiness checks.

The Release workflow is idempotent. Its main-branch path trigger exists to reconcile older tags that predate Release automation; normal releases are driven by `v*` tag pushes.

## Rollback

- Roll back to the previous known-good immutable image tag or digest.
- Verify health, MCP initialization, tool listing, and a harmless remote command.
- Record the rollback in the deployment source of truth.
- Preserve logs and recent execution diagnostics if the rollback was caused by a lifecycle or security issue.

## Troubleshooting

### HTTP 502 or connection failure

Check, in order:

1. service readiness and restarts;
2. reverse proxy or authenticated tunnel health;
3. network policy and service routing;
4. application logs;
5. main and metrics listener ports.

### Remote configuration error

The gateway requires both `REMOTE_HOST` and `REMOTE_KEY_PATH`. Also verify:

- the key is readable by the container user;
- `REMOTE_USER` and `REMOTE_PORT` are correct;
- the known-hosts path exists;
- strict host-key verification succeeds;
- the remote account can enter `DEFAULT_CWD`.

### `invalid_cwd` or file path rejection

- Use an absolute command `cwd`.
- Confirm the resolved real path is inside `ALLOWED_CWDS`.
- Check for symlinks that resolve outside the allowlist.
- For uploads, create the parent directory through an explicitly authorized workflow first.

### `exec_queue_full` or sustained queueing

Use `list_active_execs` and inspect running counts, queued count, execution class, and queue position.

- Queued jobs do not consume a running slot.
- Keep sync capacity available for short inspection commands.
- Use `cancel_exec` for a queued or running execution when appropriate.
- Do not raise sync, async, global, or queue limits until remote-host capacity and failure behavior are understood.

### `execution_circuit_open`

The registry observed an execution whose local SSH transport did not confirm exit before emergency reap.

1. Inspect active and recent status records.
2. Inspect the gateway process tree and logs.
3. Check the remote host for remaining processes related to the command.
4. Wait for a late transport-close diagnostic if one is expected.
5. Restart the service only after deciding the unresolved execution risk is acceptable.

### Missing output

Read the structured summary:

- `truncated=true` means the forwarding limit was reached;
- `stdout_bytes` and `stderr_bytes` are total observed byte counts;
- `stdout_tail` and `stderr_tail` are bounded retained tails, not complete logs.

### Command appears to survive cancellation

Cancellation terminates the isolated remote command process group and reports `remote_exit_confirmed`. If a job ends with `failure_reason=remote_termination_unconfirmed`, inspect the remote host before retrying. A program that deliberately creates a new session/process group can still escape the normal job boundary; constrain such behavior with remote-host policy and least privilege.

## Metrics

Metrics are available on `/metrics` and, when enabled, the separate metrics listener.

Important metric families include:

- active executions, configured concurrency capacity, and total executions;
- lifecycle transitions, final states, and execution-duration histograms;
- timeouts, cancellations, disconnects, and rejections;
- output bytes and truncation;
- recent-history and circuit-breaker state;
- process memory.

Alert on sustained capacity exhaustion, circuit-open state, repeated timeouts, unusual cancellation volume, and container restarts.
