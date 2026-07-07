import test from 'node:test';
import assert from 'node:assert/strict';
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
