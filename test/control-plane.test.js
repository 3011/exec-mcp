import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

async function withServer(overrides, fn) {
  const config = parseConfig({ HOST: '127.0.0.1', PORT: '0', ALLOWED_CWDS: '/tmp', DEFAULT_CWD: '/tmp', ...remoteTestEnv(), ...overrides });
  const instance = createServer(config);
  instance.server.listen(0, '127.0.0.1');
  await once(instance.server, 'listening');
  try { await fn(`http://127.0.0.1:${instance.server.address().port}`, instance); }
  finally { instance.runner.registry.close(); await new Promise((resolve) => instance.server.close(resolve)); }
}

async function mcp(base, id, name, args = {}, headers = {}) {
  const response = await fetch(`${base}/mcp`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } })
  });
  return await response.json();
}

async function waitFor(predicate, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail('condition not reached before timeout');
}

test('control tools remain available when execution capacity is full', async () => {
  await withServer({ MAX_CONCURRENT_EXECS: '1' }, async (base, { runner }) => {
    const rec = runner.registry.acquire({ timeoutMs: 5000, metadata: { label: 'test', cwd: '/tmp' } });
    const list = await mcp(base, 1, 'list_active_execs');
    assert.equal(list.result.isError, false);
    assert.equal(list.result.structuredContent.active, 1);
    assert.equal(list.result.structuredContent.tasks[0].exec_id, rec.id);

    const cancel = await mcp(base, 2, 'cancel_exec', { exec_id: rec.id });
    assert.equal(cancel.result.structuredContent.result, 'accepted');
    assert.equal(runner.active, 1, 'cancel request must not release capacity');
    assert.equal((await mcp(base, 3, 'cancel_exec', { exec_id: rec.id })).result.structuredContent.result, 'idempotent');
    runner.registry.finalize(rec.id, { transportExitConfirmed: true, signal: 'SIGTERM' });
  });
});

test('get status reads active and recent history', async () => {
  await withServer({}, async (base, { runner }) => {
    const rec = runner.registry.acquire({ timeoutMs: 5000 });
    assert.equal((await mcp(base, 1, 'get_exec_status', { exec_id: rec.id })).result.structuredContent.source, 'active');
    runner.registry.finalize(rec.id, { exitCode: 0, transportExitConfirmed: true });
    assert.equal((await mcp(base, 2, 'get_exec_status', { exec_id: rec.id })).result.structuredContent.source, 'recent');
  });
});

test('batch rejects exec but permits control tools', async () => {
  await withServer({}, async (base) => {
    const response = await fetch(`${base}/mcp`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify([
        { jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'exec', arguments: { command: 'true' } } },
        { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'list_active_execs', arguments: {} } }
      ])
    });
    const body = await response.json();
    assert.equal(body[0].result.isError, true);
    assert.match(body[0].result.content[0].text, /exec_not_supported_in_batch/);
    assert.equal(body[1].result.structuredContent.active, 0);
  });
});

test('numeric and string request ids use distinct request registry keys', async () => {
  await withServer({}, async (_base, { mcpRequests }) => {
    mcpRequests.register('session', 1);
    mcpRequests.register('session', '1');
    assert.equal(mcpRequests.size, 2);
    mcpRequests.remove('session', 1);
    assert.equal(mcpRequests.size, 1);
    mcpRequests.remove('session', '1');
    assert.equal(mcpRequests.size, 0);
  });
});

test('MCP cancelled notification aborts only the matching session request', async () => {
  await withServer({ MAX_CONCURRENT_EXECS: '2', KILL_GRACE_SECONDS: '1' }, async (base, { runner, mcpRequests }) => {
    const first = mcp(base, 1, 'exec', { command: 'sleep 30', cwd: '/tmp', timeout_seconds: 30 }, { 'mcp-session-id': 'session-a' });
    const second = mcp(base, 1, 'exec', { command: 'sleep 30', cwd: '/tmp', timeout_seconds: 30 }, { 'mcp-session-id': 'session-b' });
    await waitFor(() => mcpRequests.size === 2 && runner.active === 2);

    await fetch(`${base}/mcp`, {
      method: 'POST', headers: { 'content-type': 'application/json', 'mcp-session-id': 'session-a' },
      body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 1, reason: 'test' } })
    });
    await waitFor(() => runner.registry.recent.some((item) => item.abort_reason === 'mcp_notification_cancel'));
    assert.equal(runner.active, 1);
    assert.equal(mcpRequests.get('session-b', 1)?.completed, false);

    const remaining = runner.listActive().tasks[0];
    runner.cancel(remaining.exec_id);
    await Promise.all([first, second]);
    assert.equal(runner.active, 0);
    assert.equal(mcpRequests.size, 0);
  });
});

test('MCP HTTP disconnect propagates abort and cleans request mapping', async () => {
  await withServer({ KILL_GRACE_SECONDS: '1' }, async (base, { runner, mcpRequests }) => {
    const controller = new AbortController();
    const request = fetch(`${base}/mcp`, {
      method: 'POST', signal: controller.signal,
      headers: { 'content-type': 'application/json', 'mcp-session-id': 'disconnect-session' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 9, method: 'tools/call', params: { name: 'exec', arguments: { command: 'sleep 30', cwd: '/tmp', timeout_seconds: 30 } } })
    });
    await waitFor(() => runner.active === 1 && mcpRequests.size === 1);
    controller.abort();
    await assert.rejects(request, /abort/i);
    await waitFor(() => runner.active === 0 && mcpRequests.size === 0);
    assert.equal(runner.registry.recent.at(-1).final_state, 'client_closed');
  });
});
