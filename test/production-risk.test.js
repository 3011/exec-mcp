import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseConfig } from '../src/config.js';
import { ExecRunner } from '../src/exec-runner.js';
import { remoteTestEnv } from '../scripts/helpers.js';

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
