import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

const toolsListFixture = await readFile(new URL('./fixtures/tools-list.json', import.meta.url), 'utf8');
const metricsFixture = await readFile(new URL('./fixtures/metrics-initial.txt', import.meta.url), 'utf8');

async function withServer(fn) {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    MAX_CONCURRENT_EXECS: '1',
    ...remoteTestEnv()
  });
  const instance = createServer(config);
  instance.server.listen(0, '127.0.0.1');
  await once(instance.server, 'listening');
  try {
    await fn(`http://127.0.0.1:${instance.server.address().port}`, instance);
  } finally {
    instance.runner.registry.close();
    await new Promise((resolve) => instance.server.close(resolve));
  }
}

test('tools/list full response remains byte-stable', async () => {
  await withServer(async (base) => {
    const response = await fetch(`${base}/mcp`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 77, method: 'tools/list', params: {} })
    });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-type'), 'application/json');
    const actual = `${JSON.stringify(await response.json(), null, 2)}\n`;
    assert.equal(actual, toolsListFixture);
  });
});

test('initial metrics text and content type remain stable', async () => {
  await withServer(async (base) => {
    const response = await fetch(`${base}/metrics`);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-type'), 'text/plain; version=0.0.4');
    const actual = (await response.text()).replace(/process_resident_memory_bytes \d+/, 'process_resident_memory_bytes <rss>');
    assert.equal(actual, metricsFixture);
  });
});

test('MCP notification keeps the existing no-response behavior', async () => {
  await withServer(async (base) => {
    const response = await fetch(`${base}/mcp`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized', params: {} })
    });
    assert.equal(response.status, 202);
    assert.equal(response.headers.get('content-type'), 'application/json');
    assert.equal(await response.text(), '{}');
  });
});

test('standalone server exits cleanly on SIGTERM', async () => {
  const child = spawn(process.execPath, ['dist/src/server.js'], {
    cwd: new URL('..', import.meta.url),
    env: {
      ...process.env,
      HOST: '127.0.0.1',
      PORT: '0',
      METRICS_PORT: '0',
      ALLOWED_CWDS: '/tmp',
      DEFAULT_CWD: '/tmp',
      ...remoteTestEnv()
    },
    stdio: ['ignore', 'ignore', 'pipe']
  });

  let stderr = '';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => { stderr += chunk; });

  const deadline = Date.now() + 5000;
  while (!stderr.includes('exec-mcp listening on') && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.match(stderr, /exec-mcp listening on 127\.0\.0\.1:\d+/);

  child.kill('SIGTERM');
  const [code, signal] = await once(child, 'close');
  assert.equal(code, 0);
  assert.equal(signal, null);
  assert.match(stderr, /received SIGTERM, shutting down/);
});
