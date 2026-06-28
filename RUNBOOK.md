run book

Current state
- Connector service name stays unchanged.
- Backend image is the new exec implementation.
- The container is independent.
- Commands run on dev-debian through remote shell access.
- Do not change dev-pod or mount its PVC.

Validate
- hostname should be dev-debian.
- pwd should be /root.
- kubectl and git should exist.
- output should end with exec summary.
- pod should be Ready with 0 restarts.
- Argo should be Synced Healthy.

Release
1. Test the source.
2. Build and push a new image tag.
3. Update the GitOps deployment file.
4. Commit and push.
5. Wait for Argo sync.

Rollback
- Roll back to the previous known-good image tag.
- For emergency, roll back to the old image tag.
- Always write rollback to GitOps.

Troubleshooting
- 502: check pod readiness, restarts, logs, tunnel health, and Argo status.
- Missing output: ensure content text includes stdout or stderr plus exec summary.
- Remote failure: check the remote service, credential secret, key mount, and known_hosts.
- Argo revert: commit and push the desired state.

Metrics
- Metrics are served on /metrics.
- Important: exec_active, exec_requests_total, exec_timeout_total, exec_truncated_total, exec_stream_disconnect_total, exec_exit_code_total, exec_output_bytes_total, process_resident_memory_bytes.
