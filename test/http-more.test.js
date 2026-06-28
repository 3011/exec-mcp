import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../src/server.js';
import { parseConfig } from '../src/config.js';

async function withServer(overrides, fn) {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp,/root/exec-mcp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '5',
    DEFAULT_MAX_OUTPUT_BYTES: '1024',
    HARD_MAX_OUTPUT_BYTES: '2048',
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

test('HTTP unknown path returns 404 JSON', async () => {
  await withServer({}, async (base) => {
    const resp = await fetch(`${base}/missing`);
    assert.equal(resp.status, 404);
    assert.equal((await resp.json()).error, 'not_found');
  });
});

test('HTTP request body over 1 MiB returns 413', async () => {
  await withServer({}, async (base) => {
    const resp = await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ command: 'echo ok', cwd: '/tmp', padding: 'x'.repeat(1024 * 1024) })
    });
    assert.equal(resp.status, 413);
    assert.equal((await resp.json()).error, 'body_too_large');
  });
});

test('HTTP SSE final exit contains graceful-degradation tail summary', async () => {
  await withServer({ DEFAULT_MAX_OUTPUT_BYTES: '10', HARD_MAX_OUTPUT_BYTES: '20', RING_BUFFER_BYTES: '8' }, async (base) => {
    const resp = await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'printf abcdefghijklmnopqrstuvwxyz', cwd: '/tmp', max_output_bytes: 10 })
    });
    const events = parseSse(await resp.text());
    const exit = events.at(-1);
    assert.equal(exit.event, 'exit');
    assert.equal(exit.data.truncated, true);
    assert.equal(exit.data.stdout_bytes, 26);
    assert.equal(exit.data.stdout_tail, 'stuvwxyz');
  });
});

test('HTTP concurrency overload returns SSE error', async () => {
  await withServer({ MAX_CONCURRENT_EXECS: '1' }, async (base) => {
    const first = fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'sleep 0.5', cwd: '/tmp' })
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    const second = await fetch(`${base}/exec`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
      body: JSON.stringify({ command: 'echo second', cwd: '/tmp' })
    });
    const events = parseSse(await second.text());
    assert.equal(events.at(-1).event, 'error');
    assert.equal(events.at(-1).data.code, 'too_many_active_execs');
    await (await first).text();
  });
});
