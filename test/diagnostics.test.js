import test from 'node:test';
import assert from 'node:assert/strict';
import { deriveRuntimeDiagnostics } from '../dist/src/runtime-observer.js';

function activeTask(overrides = {}) {
  return {
    exec_id: 'exec-test', status: 'running', state: 'running', execution_class: 'async', task_handle: null,
    label: null, command_preview: null, command_sha256: null, command_length: 4, cwd: '/tmp', timeout_seconds: 60,
    elapsed_seconds: 5, created_at: '2026-01-01T00:00:00.000Z', transport_started_at: '2026-01-01T00:00:00.030Z',
    running_at: '2026-01-01T00:00:00.100Z', transport_pid: 1, remote_pid: null, remote_pgid: null,
    abort_reason: null, transport_exit_confirmed: false, remote_exit_confirmed: null, queue_position: null,
    ...overrides
  };
}

function observation(overrides = {}) {
  return {
    exec_id: 'exec-test', trace_id: 'trace-test', origin: { kind: 'mcp', tool: 'start_exec', transport_session_id: null, request_id: null, task_handle: null },
    created_at: '2026-01-01T00:00:00.000Z', last_activity_at: '2026-01-01T00:00:04.500Z', last_output_at: '2026-01-01T00:00:04.000Z',
    first_output_at: '2026-01-01T00:00:00.400Z', stdout_bytes: 10, stderr_bytes: 0, execution_class: 'async', label: null,
    cwd: '/tmp', command_preview: 'true', command_sha256: null, command_length: 4, timeout_seconds: 60,
    trace: [
      { id: 1, at: '2026-01-01T00:00:00.000Z', event: 'queued', level: 'info', detail: null },
      { id: 2, at: '2026-01-01T00:00:00.020Z', event: 'starting', level: 'info', detail: null },
      { id: 3, at: '2026-01-01T00:00:00.030Z', event: 'transport_started', level: 'info', detail: null },
      { id: 4, at: '2026-01-01T00:00:00.100Z', event: 'execution_running', level: 'info', detail: null },
      { id: 5, at: '2026-01-01T00:00:00.400Z', event: 'first_output', level: 'info', detail: 'stdout' }
    ],
    ...overrides
  };
}

test('derived diagnostics calculate lifecycle phase timings without storing new state', () => {
  const now = Date.parse('2026-01-01T00:00:05.000Z');
  const result = deriveRuntimeDiagnostics(activeTask(), observation(), now);
  assert.deepEqual(result.timings, {
    queue_ms: 20,
    transport_startup_ms: 80,
    time_to_first_output_ms: 300,
    runtime_ms: 4900,
    termination_ms: null,
    total_ms: 5000
  });
  assert.deepEqual(result.diagnostics, {
    phase: 'running', activity: 'active', failure_phase: null, last_activity_age_ms: 500, last_output_age_ms: 1000
  });
});

test('derived diagnostics report long quiet as observation rather than a hang claim', () => {
  const now = Date.parse('2026-01-01T00:10:00.000Z');
  const result = deriveRuntimeDiagnostics(activeTask(), observation({
    last_activity_at: '2026-01-01T00:09:59.000Z',
    last_output_at: '2026-01-01T00:01:00.000Z'
  }), now);
  assert.equal(result.diagnostics.phase, 'running');
  assert.equal(result.diagnostics.activity, 'long_quiet');
  assert.equal(result.diagnostics.failure_phase, null);
  assert.equal(result.diagnostics.last_activity_age_ms, 1000);
  assert.equal(result.diagnostics.last_output_age_ms, 540000);
});

test('derived diagnostics separate execution timeout from termination duration', () => {
  const task = {
    exec_id: 'exec-test', status: 'timed_out', execution_class: 'async', task_handle: null, label: null,
    command_sha256: null, command_length: 4, final_state: 'timed_out', abort_reason: 'request_timeout', abort_source: 'timeout',
    created_at: '2026-01-01T00:00:00.000Z', started_at: '2026-01-01T00:00:01.000Z', running_at: '2026-01-01T00:00:01.000Z',
    finished_at: '2026-01-01T00:00:06.000Z', duration_ms: 6000, exit_code: 143, signal: null, timed_out: true,
    transport_exit_confirmed: true, remote_exit_confirmed: true
  };
  const obs = observation({
    first_output_at: '2026-01-01T00:00:01.500Z', last_output_at: '2026-01-01T00:00:01.500Z', last_activity_at: '2026-01-01T00:00:06.000Z',
    trace: [
      { id: 1, at: '2026-01-01T00:00:00.000Z', event: 'queued', level: 'info', detail: null },
      { id: 2, at: '2026-01-01T00:00:00.010Z', event: 'starting', level: 'info', detail: null },
      { id: 3, at: '2026-01-01T00:00:00.020Z', event: 'transport_started', level: 'info', detail: null },
      { id: 4, at: '2026-01-01T00:00:01.000Z', event: 'execution_running', level: 'info', detail: null },
      { id: 5, at: '2026-01-01T00:00:01.500Z', event: 'first_output', level: 'info', detail: 'stdout' },
      { id: 6, at: '2026-01-01T00:00:05.000Z', event: 'abort_requested', level: 'error', detail: 'request_timeout' },
      { id: 7, at: '2026-01-01T00:00:06.000Z', event: 'timed_out', level: 'error', detail: 'exit 143' }
    ]
  });
  const result = deriveRuntimeDiagnostics(task, obs, Date.parse('2026-01-01T00:20:00.000Z'));
  assert.equal(result.timings.runtime_ms, 4000);
  assert.equal(result.timings.termination_ms, 1000);
  assert.equal(result.timings.total_ms, 6000);
  assert.equal(result.diagnostics.phase, 'finished');
  assert.equal(result.diagnostics.activity, 'unknown');
  assert.equal(result.diagnostics.failure_phase, 'execution');
  assert.equal(result.diagnostics.last_output_age_ms, 4500);
});
