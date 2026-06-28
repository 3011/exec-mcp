import test from 'node:test';
import assert from 'node:assert/strict';
import { RingBuffer } from '../src/ring-buffer.js';
import { redact } from '../src/redact.js';
import { parseConfig } from '../src/config.js';
import { ExecRunner, ExecRejectedError } from '../src/exec-runner.js';

test('RingBuffer keeps only the last bytes', () => {
  const rb = new RingBuffer(5);
  rb.append('hello');
  rb.append(' world');
  assert.equal(rb.toString(), 'world');
});

test('redact masks common secret patterns', () => {
  assert.match(redact('Authorization: Bearer abc.def'), /Bearer \[REDACTED\]/);
  assert.match(redact('password: hunter2'), /password: \[REDACTED\]/);
  assert.match(redact('AWS_SECRET_ACCESS_KEY=abc123'), /AWS_SECRET_ACCESS_KEY=\[REDACTED\]/);
});

test('validate rejects cwd outside allowlist', () => {
  const config = parseConfig({ ALLOWED_CWDS: '/tmp', DEFAULT_CWD: '/tmp' });
  const runner = new ExecRunner(config);
  assert.throws(
    () => runner.validate({ command: 'pwd', cwd: '/etc' }),
    (err) => err instanceof ExecRejectedError && err.code === 'invalid_cwd'
  );
});

test('ExecRunner streams stdout/stderr and returns summary', async () => {
  const config = parseConfig({
    ALLOWED_CWDS: '/tmp,/root/exec-mcp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '5',
    DEFAULT_MAX_OUTPUT_BYTES: '1048576'
  });
  const runner = new ExecRunner(config);
  const events = [];
  const summary = await runner.run(
    { command: 'printf out; printf err >&2', cwd: '/tmp' },
    (event) => events.push(event)
  );
  assert.equal(summary.code, 0);
  assert.equal(events.some((e) => e.type === 'stdout' && e.data.includes('out')), true);
  assert.equal(events.some((e) => e.type === 'stderr' && e.data.includes('err')), true);
  assert.equal(events.at(-1).type, 'exit');
});

test('ExecRunner truncates forwarding but still drains output', async () => {
  const config = parseConfig({
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '5',
    DEFAULT_MAX_OUTPUT_BYTES: '16',
    RING_BUFFER_BYTES: '8'
  });
  const runner = new ExecRunner(config);
  const events = [];
  const summary = await runner.run(
    { command: 'printf 1234567890abcdefghij', cwd: '/tmp', max_output_bytes: 16 },
    (event) => events.push(event)
  );
  assert.equal(summary.code, 0);
  assert.equal(summary.truncated, true);
  assert.equal(events.some((e) => e.type === 'truncated'), true);
  assert.equal(summary.stdout_bytes, 20);
  assert.equal(summary.stdout_tail, 'cdefghij');
});

test('ExecRunner times out and kills process group', async () => {
  const config = parseConfig({
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '1',
    MAX_TIMEOUT_SECONDS: '2',
    KILL_GRACE_SECONDS: '1'
  });
  const runner = new ExecRunner(config);
  const events = [];
  const summary = await runner.run(
    { command: 'sleep 5', cwd: '/tmp', timeout_seconds: 1 },
    (event) => events.push(event)
  );
  assert.equal(summary.timed_out, true);
  assert.equal(events.some((e) => e.type === 'timeout'), true);
  assert.notEqual(summary.code, 0);
});
