# exec-mcp runbook

## Current state

- Connector service name stays unchanged.
- Backend image is the exec-mcp implementation.
- The container is independent.
- Commands run on dev-debian through remote shell access in the `ssh-mcp` deployment.
- Do not change dev-pod or mount its PVC.

## MCP context

The MCP server exposes one tool: `exec`.

Important context for agents:

- `command` runs on the configured remote execution host, not in the model or bridge runtime.
- The command runs through `/bin/sh -c`.
- `cwd` is a remote working directory and must pass the allowlist.
- MCP output is a final text response, not live streaming.
- The final `[exec summary]` is authoritative.
- stderr text alone does not mean failure.
- Output can be truncated; use byte counts and `truncated=true` to detect this.
- `too_many_active_execs` means the active limit is currently reached. The message includes `active`, `max`, `oldest_age_seconds`, and `states`.
- v11 has no cancel-by-exec-id API. Only the current connected request can be aborted by client disconnect.
- v12 provides `list_active_execs`, `get_exec_status`, and `cancel_exec`. These are operator-wide controls for a trusted single-tenant connector and remain callable when execution capacity is full.

## Validate

- `hostname` should be dev-debian when routed through `ssh-mcp`.
- `pwd` should match the requested/default remote cwd.
- `kubectl`, `git`, and `docker` should exist on the target host when expected.
- Output should end with `[exec summary]`.
- `exec_active` should return to 0 when no request is running. When checking through the same MCP connector, one active slot may be the check itself.
- Pod should be Ready with 0 restarts.
- Argo should be Synced Healthy.

## Release

1. Test the source.
2. Build and push a new image tag.
3. Update the GitOps deployment file.
4. Commit and push.
5. Wait for Argo sync.
6. Confirm both `exec-mcp` and `ssh-mcp` deployments use the intended image tag.

## Rollback

- Roll back to the previous known-good image tag.
- For emergency, use `kubectl rollout undo`, then write the rollback to GitOps.
- Always commit and push the desired rollback state.

## Troubleshooting

### 502

Check pod readiness, restarts, logs, tunnel health, and Argo status. A short 502 can happen during rollout while the MCP pod restarts.

### Missing output

Check whether the response is truncated. MCP final text includes only bounded tails plus `[exec summary]`.

### Remote failure

Check remote service, credential secret, key mount, known_hosts, and `REMOTE_HOST`.

### `too_many_active_execs`

Read the diagnostic fields:

- small `oldest_age_seconds`: real concurrency pressure; retry later;
- near timeout: command is still running or being aborted;
- far beyond timeout plus grace: check pod logs and consider restarting the deployment;
- `states=running:*`: active commands are still running;
- `states=timeout_aborting:*`: timeout abort has fired;
- `states=client_closed_aborting:*`: client disconnect abort has fired.

Recovery options:

1. wait for timeout/reaper;
2. restart `deploy/ssh-mcp` or `deploy/exec-mcp`;
3. inspect pod-local processes if a local child is stuck.

For v12, query `list_active_execs` first and use `cancel_exec` for a selected execution. If `execution_circuit_open` is returned, inspect recent status for `unconfirmed_reaped`. New executions remain blocked until a late SSH transport close clears the diagnostic or the deployment is deliberately restarted. A confirmed local SSH transport close does not prove every remote descendant exited.

### Argo revert

Commit and push the desired state to GitOps. Argo self-heal will revert manual changes that are not in Git.

## Metrics

Metrics are served on `/metrics`.

Important metrics:

- `exec_active`
- `exec_requests_total`
- `exec_timeout_total`
- `exec_truncated_total`
- `exec_stream_disconnect_total`
- `exec_exit_code_total`
- `exec_output_bytes_total`
- `process_resident_memory_bytes`
