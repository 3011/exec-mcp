import test from 'node:test';
import assert from 'node:assert/strict';
import { parseConfig } from '../src/config.js';
import { ExecRunner, ExecRejectedError } from '../src/exec-runner.js';
import { remoteTestEnv } from '../scripts/helpers.js';

function makeRunner(overrides = {}) {
  return new ExecRunner(parseConfig({
    ALLOWED_CWDS: '/tmp,/root/exec-mcp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '5',
    MAX_TIMEOUT_SECONDS: '10',
    DEFAULT_MAX_OUTPUT_BYTES: '1024',
    HARD_MAX_OUTPUT_BYTES: '2048',
    MAX_CONCURRENT_EXECS: '2',
    RING_BUFFER_BYTES: '32',
    KILL_GRACE_SECONDS: '1',
    ...remoteTestEnv(),
    ...overrides
  }));
}

test('timeout above hard maximum is rejected before spawn', () => {
  const runner = makeRunner();
  assert.throws(
    () => runner.validate({ command: 'echo ok', cwd: '/tmp', timeout_seconds: 999 }),
    (err) => err instanceof ExecRejectedError && err.code === 'timeout_too_large'
  );
});

test('output limit above hard maximum is rejected before spawn', () => {
  const runner = makeRunner();
  assert.throws(
    () => runner.validate({ command: 'echo ok', cwd: '/tmp', max_output_bytes: 999999 }),
    (err) => err instanceof ExecRejectedError && err.code === 'output_limit_too_large'
  );
});

test('invalid env names are ignored and valid env names are passed', async () => {
  const runner = makeRunner();
  const events = [];
  const summary = await runner.run({
    command: 'printf "%s:%s" "$GOOD_ENV" "$BAD-NAME"',
    cwd: '/tmp',
    env: { GOOD_ENV: 'yes', 'BAD-NAME': 'no' }
  }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  const stdout = events.filter((e) => e.type === 'stdout').map((e) => e.data).join('');
  assert.equal(stdout, 'yes:-NAME');
});

test('stderr output with exit code 0 is not treated as runner failure', async () => {
  const runner = makeRunner();
  const events = [];
  const summary = await runner.run({ command: 'echo warning >&2; exit 0', cwd: '/tmp' }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  assert.equal(summary.stderr_bytes > 0, true);
  assert.equal(events.some((e) => e.type === 'stderr'), true);
});

test('non-zero exit code is surfaced in exit summary', async () => {
  const runner = makeRunner();
  const events = [];
  const summary = await runner.run({ command: 'echo boom >&2; exit 7', cwd: '/tmp' }, (event) => events.push(event));
  assert.equal(summary.code, 7);
  assert.equal(events.at(-1).type, 'exit');
  assert.equal(events.at(-1).code, 7);
});

test('secret redaction applies to streamed data and tail summaries', async () => {
  const runner = makeRunner({ DEFAULT_MAX_OUTPUT_BYTES: '4096', HARD_MAX_OUTPUT_BYTES: '4096' });
  const events = [];
  const summary = await runner.run({
    command: 'echo "Authorization: Bearer abc.def"; echo "password: hunter2" >&2',
    cwd: '/tmp'
  }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  const streamed = events.map((e) => e.data || '').join('\n');
  assert.equal(streamed.includes('abc.def'), false);
  assert.equal(streamed.includes('hunter2'), false);
  assert.equal(summary.stdout_tail.includes('abc.def'), false);
  assert.equal(summary.stderr_tail.includes('hunter2'), false);
});

test('concurrency limit rejects extra active execs', async () => {
  const runner = makeRunner({ MAX_CONCURRENT_EXECS: '1', DEFAULT_TIMEOUT_SECONDS: '5' });
  const first = runner.run({ command: 'sleep 0.3', cwd: '/tmp' }, () => {});
  await new Promise((resolve) => setTimeout(resolve, 50));
  await assert.rejects(
    () => runner.run({ command: 'echo second', cwd: '/tmp' }, () => {}),
    (err) => err instanceof ExecRejectedError
      && err.code === 'too_many_active_execs'
      && /active=1 max=1 oldest_age_seconds=/.test(err.message)
      && /states=running:1/.test(err.message)
  );
  assert.equal(runner.active, 1);
  const summary = await first;
  assert.equal(summary.code, 0);
});

test('ENV and BASH_ENV are removed before spawning shell', async () => {
  const runner = makeRunner();
  const events = [];
  const summary = await runner.run({
    command: 'printf "%s:%s:%s" "$ENV" "$BASH_ENV" "$EXEC_MCP_CUSTOM"',
    cwd: '/tmp',
    env: {
      ENV: '/tmp/should_be_removed',
      BASH_ENV: '/tmp/should_be_removed',
      EXEC_MCP_CUSTOM: 'env_from_tool'
    }
  }, (event) => events.push(event));
  assert.equal(summary.code, 0);
  const stdout = events.filter((e) => e.type === 'stdout').map((e) => e.data).join('');
  assert.equal(stdout, '::env_from_tool');
});
