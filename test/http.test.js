import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../src/server.js';
import { parseConfig } from '../src/config.js';

test('HTTP /healthz and /exec SSE work', async () => {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    HEARTBEAT_SECONDS: '99',
    DEFAULT_TIMEOUT_SECONDS: '5'
  });
  const { server } = createServer(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();

  const health = await fetch(`http://127.0.0.1:${port}/healthz`);
  assert.equal(health.status, 200);
  assert.equal((await health.json()).ok, true);

  const resp = await fetch(`http://127.0.0.1:${port}/exec`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', accept: 'text/event-stream' },
    body: JSON.stringify({ command: 'echo hello', cwd: '/tmp' })
  });
  assert.equal(resp.status, 200);
  const text = await resp.text();
  assert.match(text, /event: stdout/);
  assert.match(text, /hello/);
  assert.match(text, /event: exit/);

  await new Promise((resolve) => server.close(resolve));
});
