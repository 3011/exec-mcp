import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

async function withServer(overrides, fn) {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp,/root/exec-mcp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '5',
    MAX_TIMEOUT_SECONDS: '10',
    DEFAULT_MAX_OUTPUT_BYTES: '1024',
    HARD_MAX_OUTPUT_BYTES: '2048',
    MAX_CONCURRENT_EXECS: '1',
    ...remoteTestEnv(),
    ...overrides
  });
  const { server, runner } = createServer(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    await fn(base, runner);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

function parseSse(text) {
  return text.split('\n\n')
    .map((block) => block.trim())
    .filter((block) => block.startsWith('event:'))
    .map((block) => {
      const event = block.match(/^event: (.*)$/m)?.[1];
      const data = block.match(/^data: (.*)$/m)?.[1];
      return { event, data: data ? JSON.parse(data) : null };
    });
}

test('HTTP invalid JSON returns 400', async () => {
  await withServer({}, async (base) => {
    const resp = await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{not-json'
    });
    assert.equal(resp.status, 400);
    assert.equal((await resp.json()).error, 'invalid_json');
  });
});

test('HTTP invalid cwd returns SSE error event', async () => {
  await withServer({}, async (base) => {
    const resp = await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'pwd', cwd: '/etc' })
    });
    assert.equal(resp.status, 200);
    const events = parseSse(await resp.text());
    assert.equal(events.at(-1).event, 'error');
    assert.equal(events.at(-1).data.code, 'invalid_cwd');
  });
});

test('HTTP metrics include rejection and exit counters', async () => {
  await withServer({}, async (base) => {
    await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'exit 3', cwd: '/tmp' })
    }).then((r) => r.text());
    await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'pwd', cwd: '/etc' })
    }).then((r) => r.text());
    const metrics = await fetch(`${base}/metrics`).then((r) => r.text());
    assert.match(metrics, /exec_mcp_exit_code_total\{code="3"\} 1/);
    assert.match(metrics, /exec_mcp_rejected_total\{reason="invalid_cwd"\} 1/);
    assert.match(metrics, /exec_mcp_max_concurrent_execs 1/);
    assert.match(metrics, /exec_mcp_exec_duration_seconds_bucket\{final_state="failed",le="\+Inf"\} 1/);
    assert.match(metrics, /exec_mcp_exec_duration_seconds_count\{final_state="failed"\} 1/);
    assert.match(metrics, /exec_mcp_exec_duration_seconds_sum\{final_state="failed"\} [0-9.]+/);
    assert.match(metrics, /exec_mcp_requests_total 2/);
    assert.doesNotMatch(metrics, /(?:^|\n)exec_(?:active|requests_total|timeout_total|truncated_total|stream_disconnect_total|output_bytes_total|rejected_total|killed_total|exit_code_total|reaped_total)(?:\{| |$)/);
    assert.doesNotMatch(metrics, /(?:^|\n)exec_mcp_(?:reaped_total|disconnect_abort_total)(?:\{| |$)/);
  });
});

test('HTTP client abort kills running command and decrements active count', async () => {
  await withServer({ KILL_GRACE_SECONDS: '1' }, async (base, runner) => {
    const ac = new AbortController();
    const promise = fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'sleep 5', cwd: '/tmp', timeout_seconds: 10 }),
      signal: ac.signal
    }).catch((err) => err);
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(runner.active, 1);
    ac.abort();
    await promise;
    for (let i = 0; i < 20 && runner.active !== 0; i++) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    assert.equal(runner.active, 0);
    assert.equal(runner.metrics.streamDisconnectTotal >= 1, true);
  });
});
