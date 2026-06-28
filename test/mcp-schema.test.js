import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../src/server.js';
import { parseConfig } from '../src/config.js';

test('MCP exec tool schema includes operational context', async () => {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp'
  });
  const { server } = createServer(config);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;

  try {
    const resp = await fetch(`${base}/mcp`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} })
    });
    assert.equal(resp.status, 200);
    const body = await resp.json();
    const tool = body.result.tools.find((item) => item.name === 'exec');
    assert.ok(tool);
    assert.match(tool.description, /remote execution host/);
    assert.match(tool.description, /\/bin\/sh -c/);
    assert.match(tool.description, /\[exec summary\]/);
    assert.match(tool.description, /too_many_active_execs/);
    assert.match(tool.description, /oldest_age_seconds/);
    assert.match(tool.inputSchema.properties.command.description, /explicit quoting/);
    assert.match(tool.inputSchema.properties.cwd.description, /allowlist/);
    assert.match(tool.inputSchema.properties.timeout_seconds.description, /SIGTERM/);
    assert.match(tool.inputSchema.properties.max_output_bytes.description, /truncation/);
    assert.match(tool.inputSchema.properties.env.description, /BASH_ENV/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
