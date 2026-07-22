import type { ExecRunner } from './exec-runner.js';

export function renderMetrics(runner: ExecRunner): string {
  const lines: string[] = [];
  lines.push('# HELP exec_mcp_active_execs Number of active exec calls');
  lines.push('# TYPE exec_mcp_active_execs gauge');
  lines.push(`exec_mcp_active_execs ${runner.active}`);
  lines.push('# HELP exec_mcp_max_concurrent_execs Configured maximum concurrent executions');
  lines.push('# TYPE exec_mcp_max_concurrent_execs gauge');
  lines.push(`exec_mcp_max_concurrent_execs ${runner.registry.maxActive}`);
  lines.push('# HELP exec_mcp_requests_total Total validated and rejected exec requests');
  lines.push('# TYPE exec_mcp_requests_total counter');
  lines.push(`exec_mcp_requests_total ${runner.metrics.requestsTotal}`);
  lines.push('# HELP exec_mcp_exec_started_total Total executions that acquired capacity and started');
  lines.push('# TYPE exec_mcp_exec_started_total counter');
  lines.push(`exec_mcp_exec_started_total ${runner.metrics.startedTotal}`);
  lines.push('# HELP exec_mcp_timeout_total Total executions aborted by their request timeout');
  lines.push('# TYPE exec_mcp_timeout_total counter');
  lines.push(`exec_mcp_timeout_total ${runner.metrics.timeoutTotal}`);
  lines.push('# HELP exec_mcp_truncated_total Total executions whose forwarded output exceeded its configured limit');
  lines.push('# TYPE exec_mcp_truncated_total counter');
  lines.push(`exec_mcp_truncated_total ${runner.metrics.truncatedTotal}`);
  lines.push('# HELP exec_mcp_stream_disconnect_total Total active execution streams interrupted by a client or cancellation signal');
  lines.push('# TYPE exec_mcp_stream_disconnect_total counter');
  lines.push(`exec_mcp_stream_disconnect_total ${runner.metrics.streamDisconnectTotal}`);
  lines.push('# HELP exec_mcp_output_bytes_total Total bytes read from remote command output');
  lines.push('# TYPE exec_mcp_output_bytes_total counter');
  lines.push(`exec_mcp_output_bytes_total{stream="stdout"} ${runner.metrics.outputBytesTotal.stdout}`);
  lines.push(`exec_mcp_output_bytes_total{stream="stderr"} ${runner.metrics.outputBytesTotal.stderr}`);
  lines.push('# HELP exec_mcp_execution_circuit_open Whether the execution safety circuit is open');
  lines.push('# TYPE exec_mcp_execution_circuit_open gauge');
  lines.push(`exec_mcp_execution_circuit_open ${runner.registry.circuitOpen ? 1 : 0}`);
  lines.push('# HELP exec_mcp_unconfirmed_reaped_total Total unconfirmed executions force-reaped from capacity accounting');
  lines.push('# TYPE exec_mcp_unconfirmed_reaped_total counter');
  lines.push(`exec_mcp_unconfirmed_reaped_total ${runner.registry.metrics.unconfirmedReapedTotal}`);
  lines.push('# HELP exec_mcp_unconfirmed_reaped_current Current unconfirmed force-reaped executions');
  lines.push('# TYPE exec_mcp_unconfirmed_reaped_current gauge');
  lines.push(`exec_mcp_unconfirmed_reaped_current ${runner.registry.unconfirmed.size}`);
  lines.push('# HELP exec_mcp_late_transport_close_total Total transport closes observed after force reaping');
  lines.push('# TYPE exec_mcp_late_transport_close_total counter');
  lines.push(`exec_mcp_late_transport_close_total ${runner.registry.metrics.lateTransportCloseTotal}`);
  lines.push('# HELP exec_mcp_registry_invariant_violation_total Total execution registry invariant violations');
  lines.push('# TYPE exec_mcp_registry_invariant_violation_total counter');
  lines.push(`exec_mcp_registry_invariant_violation_total ${runner.registry.metrics.invariantViolations}`);
  lines.push('# HELP exec_mcp_recent_history_size Current bounded execution history size');
  lines.push('# TYPE exec_mcp_recent_history_size gauge');
  lines.push(`exec_mcp_recent_history_size ${runner.registry.recent.length}`);
  for (const [reason, count] of runner.metrics.rejectedTotal.entries()) {
    lines.push(`exec_mcp_rejected_total{reason="${escapeLabel(reason)}"} ${count}`);
  }
  for (const [signal, count] of runner.metrics.killedTotal.entries()) {
    lines.push(`exec_mcp_killed_total{signal="${escapeLabel(signal)}"} ${count}`);
  }
  for (const [code, count] of runner.metrics.exitCodeTotal.entries()) {
    lines.push(`exec_mcp_exit_code_total{code="${escapeLabel(code)}"} ${count}`);
  }
  for (const [state, count] of runner.metrics.finishedTotal.entries()) {
    lines.push(`exec_mcp_exec_finished_total{final_state="${escapeLabel(state)}"} ${count}`);
  }
  lines.push('# HELP exec_mcp_exec_duration_seconds Execution duration from acquisition to finalization');
  lines.push('# TYPE exec_mcp_exec_duration_seconds histogram');
  for (const [state, histogram] of runner.metrics.durationSecondsByState.entries()) {
    runner.metrics.durationSecondsBuckets.forEach((upperBound, index) => {
      lines.push(`exec_mcp_exec_duration_seconds_bucket{final_state="${escapeLabel(state)}",le="${upperBound}"} ${histogram.buckets[index] ?? 0}`);
    });
    lines.push(`exec_mcp_exec_duration_seconds_bucket{final_state="${escapeLabel(state)}",le="+Inf"} ${histogram.count}`);
    lines.push(`exec_mcp_exec_duration_seconds_sum{final_state="${escapeLabel(state)}"} ${histogram.sum}`);
    lines.push(`exec_mcp_exec_duration_seconds_count{final_state="${escapeLabel(state)}"} ${histogram.count}`);
  }
  for (const [reason, count] of runner.metrics.abortRequestedTotal.entries()) {
    lines.push(`exec_mcp_abort_requested_total{reason="${escapeLabel(reason)}"} ${count}`);
  }
  for (const [result, count] of runner.metrics.cancelRequestsTotal.entries()) {
    lines.push(`exec_mcp_cancel_requests_total{result="${escapeLabel(result)}"} ${count}`);
  }
  if (process.memoryUsage) lines.push(`process_resident_memory_bytes ${process.memoryUsage().rss}`);
  return lines.join('\n') + '\n';
}

function escapeLabel(value: unknown): string {
  return String(value).replaceAll('\\', '\\\\').replaceAll('"', '\\"');
}
