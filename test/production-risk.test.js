import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { parseConfig } from '../dist/src/config.js';
import { ExecRunner, spawnRemoteProcess } from '../dist/src/exec-runner.js';
import { remoteTestEnv } from '../scripts/helpers.js';
import {
  REMOTE_SUPERVISOR_PROTOCOL_VERSION,
  REMOTE_SUPERVISOR_PY,
  RemoteSupervisorFrameDecoder,
  encodeSupervisorAck,
  encodeSupervisorConfig,
  parseSupervisorJson
} from '../dist/src/remote-supervisor.js';

function makeRunner(overrides = {}) {
  return new ExecRunner(parseConfig({
    ALLOWED_CWDS: '/tmp,/root/exec-mcp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '2',
    MAX_TIMEOUT_SECONDS: '5',
    DEFAULT_MAX_OUTPUT_BYTES: '65536',
    HARD_MAX_OUTPUT_BYTES: '131072',
    MAX_CONCURRENT_EXECS: '2',
    RING_BUFFER_BYTES: '64',
    KILL_GRACE_SECONDS: '1',
    ...remoteTestEnv(),
    ...overrides
  }));
}

test('timeout kills the whole process group including background child', async () => {
  const marker = `/tmp/exec-mcp-pg-${process.pid}-${Date.now()}`;
  const runner = makeRunner({ DEFAULT_TIMEOUT_SECONDS: '1', MAX_TIMEOUT_SECONDS: '2' });
  const summary = await runner.run({
    command: `sh -c 'sleep 3; touch ${marker}' & wait`,
    cwd: '/tmp',
    timeout_seconds: 1
  }, () => {});
  assert.equal(summary.timed_out, true);
  await new Promise((resolve) => setTimeout(resolve, 3500));
  const check = await runner.run({ command: `[ ! -e ${marker} ]`, cwd: '/tmp' }, () => {});
  assert.equal(check.code, 0, 'background process should not survive timeout');
});

test('timeout kills a foreground child process that ignores SIGTERM', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-timeout-foreground-'));
  const pidFile = join(root, 'child.pid');
  const runner = makeRunner({ ALLOWED_CWDS: root, DEFAULT_CWD: root, DEFAULT_TIMEOUT_SECONDS: '1', MAX_TIMEOUT_SECONDS: '2' });
  try {
    const summary = await runner.run({
      command: `python3 -c "import os, signal, sys, time; open(sys.argv[1], 'w').write(str(os.getpid())); signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)" ${pidFile}`,
      cwd: root,
      timeout_seconds: 1
    }, () => {});
    assert.equal(summary.timed_out, true);
    const pid = Number.parseInt(await readFile(pidFile, 'utf8'), 10);
    assert.equal(await waitForPidExit(pid), true, `foreground child should be gone after timeout: ${pid}`);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('timeout kills a background child process', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-timeout-background-'));
  const pidFile = join(root, 'child.pid');
  const runner = makeRunner({ ALLOWED_CWDS: root, DEFAULT_CWD: root, DEFAULT_TIMEOUT_SECONDS: '1', MAX_TIMEOUT_SECONDS: '2' });
  try {
    const summary = await runner.run({
      command: `sleep 60 & echo $! > ${pidFile}; wait`,
      cwd: root,
      timeout_seconds: 1
    }, () => {});
    assert.equal(summary.timed_out, true);
    const pid = Number.parseInt(await readFile(pidFile, 'utf8'), 10);
    assert.equal(await waitForPidExit(pid), true, `background child should be gone after timeout: ${pid}`);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('normal command exit cleans up leftover background process group members', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-normal-background-'));
  const pidFile = join(root, 'child.pid');
  const runner = makeRunner({ ALLOWED_CWDS: root, DEFAULT_CWD: root });
  try {
    const summary = await runner.run({
      command: `sleep 60 & echo $! > ${pidFile}; printf done`,
      cwd: root
    }, () => {});
    assert.equal(summary.code, 0);
    const pid = Number.parseInt(await readFile(pidFile, 'utf8'), 10);
    assert.equal(await waitForPidExit(pid), true, `leftover background child should be gone after normal exit: ${pid}`);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('cwd symlink realpath must remain inside allowlist', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-cwd-realpath-'));
  const link = join(root, 'var-link');
  await symlink('/var', link);
  const runner = makeRunner({ ALLOWED_CWDS: root, DEFAULT_CWD: root });
  try {
    const events = [];
    const summary = await runner.run({
      command: 'pwd -P',
      cwd: link
    }, (event) => events.push(event));
    assert.notEqual(summary.code, 0);
    assert.match(events.map((event) => event.data || event.stderr_tail || '').join('\n'), /invalid_cwd/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('large output is drained, counted, truncated, and tail is bounded', async () => {
  const runner = makeRunner({ DEFAULT_MAX_OUTPUT_BYTES: '1024', HARD_MAX_OUTPUT_BYTES: '2048', RING_BUFFER_BYTES: '32' });
  const events = [];
  const summary = await runner.run({
    command: `python3 -c "import sys; sys.stdout.write('x' * 100000)"`,
    cwd: '/tmp',
    max_output_bytes: 1024
  }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  assert.equal(summary.truncated, true);
  assert.equal(summary.stdout_bytes, 100000);
  assert.equal(summary.stdout_tail.length, 32);
  const forwarded = events.filter((e) => e.type === 'stdout').map((e) => e.data).join('');
  assert.equal(forwarded.length, 1024);
  assert.equal(events.filter((e) => e.type === 'truncated').length, 1);
});

test('tail summary is capped by max_output_bytes even when ring buffer is larger', async () => {
  const runner = makeRunner({ DEFAULT_MAX_OUTPUT_BYTES: '1024', HARD_MAX_OUTPUT_BYTES: '2048', RING_BUFFER_BYTES: '65536' });
  const summary = await runner.run({
    command: `python3 -c "import sys; sys.stdout.write('x' * 37000)"`,
    cwd: '/tmp',
    max_output_bytes: 1024
  }, () => {});
  assert.equal(summary.code, 0);
  assert.equal(summary.truncated, true);
  assert.equal(summary.stdout_bytes, 37000);
  assert.equal(Buffer.byteLength(summary.stdout_tail + summary.stderr_tail, 'utf8') <= 1024, true);
});

test('combined stdout and stderr tails are capped by max_output_bytes', async () => {
  const runner = makeRunner({ DEFAULT_MAX_OUTPUT_BYTES: '1024', HARD_MAX_OUTPUT_BYTES: '2048', RING_BUFFER_BYTES: '65536' });
  const summary = await runner.run({
    command: `python3 -c "import sys; sys.stdout.write('o' * 2000); sys.stderr.write('e' * 2000)"`,
    cwd: '/tmp',
    max_output_bytes: 1024
  }, () => {});
  assert.equal(summary.code, 0);
  assert.equal(summary.truncated, true);
  assert.equal(Buffer.byteLength(summary.stdout_tail + summary.stderr_tail, 'utf8') <= 1024, true);
  assert.equal(summary.stdout_tail.length > 0, true);
  assert.equal(summary.stderr_tail.length > 0, true);
});

test('stdout and stderr events carry monotonically increasing sequence numbers', async () => {
  const runner = makeRunner();
  const events = [];
  const summary = await runner.run({
    command: 'echo one; echo two >&2; echo three',
    cwd: '/tmp'
  }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  const seqs = events.filter((e) => e.type === 'stdout' || e.type === 'stderr').map((e) => e.seq);
  assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b));
  assert.equal(new Set(seqs).size, seqs.length);
});

test('heartbeat events include byte counters while command is running', async () => {
  const runner = makeRunner({ HEARTBEAT_SECONDS: '1', DEFAULT_TIMEOUT_SECONDS: '4' });
  const events = [];
  const summary = await runner.run({ command: 'printf start; sleep 2; printf end', cwd: '/tmp' }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  const heartbeats = events.filter((e) => e.type === 'heartbeat');
  assert.equal(heartbeats.length >= 1, true);
  assert.equal(typeof heartbeats[0].stdout_bytes, 'number');
  assert.equal(typeof heartbeats[0].elapsed_ms, 'number');
});

async function waitForPidExit(pid, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!existsSync(`/proc/${pid}`)) return true;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return !existsSync(`/proc/${pid}`);
}

test('manual cancel kills the whole async process group including background child', async () => {
  const root = await mkdtemp(join(tmpdir(), 'exec-mcp-cancel-background-'));
  const pidFile = join(root, 'child.pid');
  const runner = makeRunner({ ALLOWED_CWDS: root, DEFAULT_CWD: root, DEFAULT_TIMEOUT_SECONDS: '5' });
  try {
    const started = runner.start({
      command: `sleep 60 & echo $! > ${pidFile}; wait`,
      cwd: root,
      timeout_seconds: 5,
      label: 'cancel-process-group'
    });
    const deadline = Date.now() + 3000;
    while (!existsSync(pidFile) && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.equal(existsSync(pidFile), true, 'background child pid file should be created');
    const pid = Number.parseInt(await readFile(pidFile, 'utf8'), 10);
    const cancelled = runner.cancel(started.exec_id);
    assert.equal(cancelled.result, 'accepted');
    const final = await runner.getStatus(started.exec_id, { waitSeconds: 3 });
    assert.equal(final.found, true);
    assert.equal(final.task.status, 'cancelled');
    assert.equal(await waitForPidExit(pid), true, `background child should be gone after manual cancel: ${pid}`);
  } finally {
    runner.close();
    await rm(root, { recursive: true, force: true });
  }
});

test('manual cancel prevents a detached background marker from firing after transport isolation', async () => {
  const marker = `/tmp/exec-mcp-cancel-marker-${process.pid}-${Date.now()}`;
  const runner = makeRunner({ DEFAULT_TIMEOUT_SECONDS: '5', MAX_TIMEOUT_SECONDS: '6' });
  try {
    const started = runner.start({
      command: `sh -c '(sleep 2; touch ${marker}) & wait'`,
      cwd: '/tmp',
      timeout_seconds: 5,
      label: 'cancel-marker-regression'
    });
    const runningDeadline = Date.now() + 2000;
    while (Date.now() < runningDeadline) {
      const state = await runner.getStatus(started.exec_id);
      if (state.found && state.task.status === 'running') break;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.equal(runner.cancel(started.exec_id).result, 'accepted');
    const final = await runner.getStatus(started.exec_id, { waitSeconds: 3 });
    assert.equal(final.found, true);
    assert.equal(final.task.status, 'cancelled');
    assert.equal(final.task.remote_exit_confirmed, true);
    await new Promise((resolve) => setTimeout(resolve, 2300));
    assert.equal(existsSync(marker), false, 'cancelled remote process group must not create marker later');
  } finally {
    runner.close();
    await rm(marker, { force: true });
  }
});

test('remote supervisor timeout remains authoritative while the local event loop is stalled', async () => {
  const runner = makeRunner({ DEFAULT_TIMEOUT_SECONDS: '1', MAX_TIMEOUT_SECONDS: '2' });
  try {
    const execution = runner.run({ command: 'sleep 5', cwd: '/tmp', timeout_seconds: 1 }, () => {});
    await new Promise((resolve) => setTimeout(resolve, 250));

    const blockedUntil = Date.now() + 2200;
    while (Date.now() < blockedUntil) {
      // Intentionally block Node timers/I/O. The remote supervisor must enforce and report the deadline itself.
    }

    const summary = await execution;
    assert.equal(summary.timed_out, true);
    assert.equal(summary.code, 143);
    const record = runner.registry.recent.at(-1);
    assert.equal(record.final_state, 'timed_out');
    assert.equal(record.abort_reason, 'request_timeout');
    assert.equal(record.remote_exit_confirmed, true);
    assert.equal(record.transport_exit_confirmed, true);
  } finally {
    runner.close();
  }
});

test('a command that exits 143 by itself is a normal failed exit, not a timeout', async () => {
  const runner = makeRunner();
  try {
    const summary = await runner.run({ command: 'exit 143', cwd: '/tmp', timeout_seconds: 2 }, () => {});
    assert.equal(summary.code, 143);
    assert.equal(summary.timed_out, false);
    const record = runner.registry.recent.at(-1);
    assert.equal(record.final_state, 'failed');
    assert.equal(record.abort_reason, null);
    assert.equal(record.remote_exit_confirmed, true);
  } finally {
    runner.close();
  }
});


test('remote supervisor deadline is independent of stdout backpressure', async () => {
  const config = parseConfig({
    ALLOWED_CWDS: '/tmp,/root/exec-mcp',
    DEFAULT_CWD: '/tmp',
    ...remoteTestEnv()
  });
  const execId = `exec-${randomUUID()}`;
  const child = spawnRemoteProcess(config, ['python3', '-c', REMOTE_SUPERVISOR_PY]);
  const decoder = new RemoteSupervisorFrameDecoder();
  let result = null;
  let outputBytes = 0;
  let startedResolve;
  const started = new Promise((resolve) => { startedResolve = resolve; });
  child.stdin.on('error', () => {});
  child.stdout.on('data', (chunk) => {
    for (const frame of decoder.push(chunk)) {
      if (frame.type === 'O') outputBytes += frame.payload.length;
      if (frame.type === 'S') startedResolve();
      if (frame.type === 'R') {
        result = parseSupervisorJson(frame.payload);
        if (child.stdin.writable) child.stdin.write(encodeSupervisorAck(execId));
      }
    }
  });
  child.stderr.resume();
  child.stdin.write(encodeSupervisorConfig({
    protocol: REMOTE_SUPERVISOR_PROTOCOL_VERSION,
    exec_id: execId,
    command: `python3 -c 'import os; b=b"x"*65536\nwhile True: os.write(1,b)'`,
    cwd: '/tmp',
    timeout_seconds: 1,
    kill_grace_seconds: 1,
    allowed_cwds: ['/tmp', '/root/exec-mcp'],
    env: {}
  }));

  await started;
  const blockedUntil = Date.now() + 2500;
  while (Date.now() < blockedUntil) {
    // Deliberately stop consuming SSH output long enough to fill kernel/user-space buffers.
  }
  await new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });

  assert.ok(result, 'supervisor must emit an authoritative final result');
  assert.equal(result.reason, 'request_timeout');
  assert.equal(result.exit_code, 143);
  assert.equal(result.signal, 'SIGTERM');
  assert.equal(outputBytes > 0, true);
  assert.equal(result.decision_ms >= 900, true);
  assert.equal(result.decision_ms < 1800, true, `deadline decision was delayed by output backpressure: ${result.decision_ms}ms`);
});

test('normal supervisor result is acknowledged and leaves no result journal', async () => {
  const runner = makeRunner();
  try {
    const summary = await runner.run({ command: 'printf journal-ok', cwd: '/tmp' }, () => {});
    assert.equal(summary.code, 0);
    const path = `/tmp/exec-mcp-runtime-results-${process.geteuid()}/${summary.exec_id}.json`;
    assert.equal(existsSync(path), false, 'ACK should remove the durable result journal on the normal path');
  } finally {
    runner.close();
  }
});

test('lost SSH transport reconciles the durable remote result and kills the command group', async () => {
  const marker = `/tmp/exec-mcp-transport-loss-${process.pid}-${Date.now()}`;
  const runner = makeRunner({ DEFAULT_TIMEOUT_SECONDS: '10', MAX_TIMEOUT_SECONDS: '10' });
  try {
    const started = runner.start({
      command: `sleep 2; touch ${marker}`,
      cwd: '/tmp',
      timeout_seconds: 10,
      label: 'transport-loss-reconcile'
    });
    let active = null;
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const status = await runner.getStatus(started.exec_id);
      if (status.found && status.source === 'active' && status.task.transport_pid && status.task.remote_pid) {
        active = status.task;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.ok(active?.transport_pid, 'transport pid should become observable');
    process.kill(-active.transport_pid, 'SIGKILL');

    const final = await runner.getStatus(started.exec_id, { waitSeconds: 5 });
    assert.equal(final.found, true);
    assert.equal(final.source, 'recent');
    assert.equal(final.task.final_state, 'failed');
    assert.equal(final.task.failure_reason, 'remote_transport_closed');
    assert.equal(final.task.remote_exit_confirmed, true);
    assert.equal(final.task.transport_exit_confirmed, true);
    assert.equal(final.task.exit_code, 143);
    await new Promise((resolve) => setTimeout(resolve, 2300));
    assert.equal(existsSync(marker), false, 'command group must not survive the lost SSH transport');
    const journal = `/tmp/exec-mcp-runtime-results-${process.geteuid()}/${started.exec_id}.json`;
    assert.equal(existsSync(journal), false, 'successful reconcile should consume the durable result journal');
  } finally {
    runner.close();
    await rm(marker, { force: true });
  }
});
