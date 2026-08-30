import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

async function withServer(fn, extra = {}) {
  const config = parseConfig({
    HOST: '127.0.0.1',
    PORT: '0',
    ALLOWED_CWDS: '/tmp',
    DEFAULT_CWD: '/tmp',
    DEFAULT_TIMEOUT_SECONDS: '5',
    JOB_RETENTION_SECONDS: '60',
    RECENT_EXEC_HISTORY_LIMIT: '50',
    ...remoteTestEnv(),
    ...extra
  });
  const instance = createServer(config);
  instance.server.listen(0, '127.0.0.1');
  await once(instance.server, 'listening');
  try {
    await fn(`http://127.0.0.1:${instance.server.address().port}`, instance);
  } finally {
    await instance.runner.shutdown(2);
    await new Promise((resolve) => instance.server.close(resolve));
  }
}

async function mcpCall(base, id, name, args = {}, sessionId = 'runtime-test-session') {
  const response = await fetch(`${base}/mcp`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'mcp-session-id': sessionId },
    body: JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } })
  });
  assert.equal(response.status, 200);
  return await response.json();
}

async function waitForFinished(base, execId, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const response = await fetch(`${base}/runtime/api/executions/${execId}`);
    if (response.ok) {
      const detail = await response.json();
      if (detail.source !== 'active') return detail;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`execution did not finish: ${execId}`);
}

test('Runtime Console serves dependency-free assets with strict read-only security headers', async () => {
  await withServer(async (base) => {
    const html = await fetch(`${base}/runtime`);
    assert.equal(html.status, 200);
    assert.match(html.headers.get('content-type') || '', /^text\/html/);
    assert.equal(html.headers.get('x-frame-options'), 'DENY');
    assert.equal(html.headers.get('x-content-type-options'), 'nosniff');
    assert.equal(html.headers.get('cache-control'), 'no-store, max-age=0');
    assert.match(html.headers.get('content-security-policy') || '', /default-src 'none'/);
    const body = await html.text();
    assert.match(body, /Execution MCP/);
    assert.match(body, /Runtime Console/);
    assert.match(body, /Read only/);

    const js = await fetch(`${base}/runtime/assets/app.js`);
    assert.equal(js.status, 200);
    const script = await js.text();
    assert.doesNotThrow(() => new Function(script));
    assert.doesNotMatch(script, /\.innerHTML\s*=/);

    const denied = await fetch(`${base}/runtime/api/overview`, { method: 'POST' });
    assert.equal(denied.status, 405);
    assert.equal(denied.headers.get('allow'), 'GET');
    assert.deepEqual(await denied.json(), { error: 'method_not_allowed', message: 'Runtime Console is read-only.' });
  });
});

test('Runtime API correlates an MCP start_exec with origin, lifecycle trace, activity and redacted logs', async () => {
  await withServer(async (base) => {
    const secret = 'runtime-super-secret-93817';
    const session = 'chatgpt-transport-session-test';
    const begun = await mcpCall(base, 900, 'begin_task', { label: 'runtime task grouping' }, session);
    const taskHandle = begun.result.structuredContent.task_handle;
    assert.match(taskHandle, /^task-/);
    const started = await mcpCall(base, 901, 'start_exec', {
      task_handle: taskHandle,
      command: 'printf "hello-runtime\\n%s\\n" "$RUNTIME_SECRET"; sleep 0.12; printf "done\\n"',
      cwd: '/tmp',
      env: { RUNTIME_SECRET: secret },
      label: 'runtime-observer-test'
    }, session);
    const execId = started.result.structuredContent.exec_id;
    assert.match(execId, /^exec-/);

    const activeList = await (await fetch(`${base}/runtime/api/executions?limit=50`)).json();
    const listed = activeList.executions.find((item) => item.exec_id === execId);
    assert.ok(listed);
    assert.equal(listed.label, 'runtime-observer-test');
    assert.equal(listed.origin.kind, 'mcp');
    assert.equal(listed.origin.tool, 'start_exec');
    assert.equal(listed.origin.transport_session_id, session);
    assert.equal(listed.origin.request_id, 'number:901');
    assert.equal(listed.origin.task_handle, taskHandle);
    assert.equal(listed.task_handle, taskHandle);
    assert.equal(listed.task_context.label, 'runtime task grouping');
    assert.match(listed.trace_id, /^trace-/);
    assert.doesNotMatch(JSON.stringify(listed), new RegExp(secret));

    const detail = await waitForFinished(base, execId);
    assert.equal(detail.found, true);
    assert.equal(detail.task.status, 'completed');
    assert.equal(detail.observation.origin.task_handle, taskHandle);
    assert.equal(detail.task.task_handle, taskHandle);
    assert.equal(detail.task_context.task_handle, taskHandle);
    assert.ok(detail.observation.last_activity_at);
    assert.ok(detail.observation.last_output_at);
    assert.ok(detail.observation.stdout_bytes > 0);

    const events = detail.observation.trace.map((event) => event.event);
    for (const expected of ['tool_request_received', 'request_validated', 'job_registered', 'queued', 'starting', 'transport_started', 'execution_running', 'first_output', 'transport_closed', 'completed']) {
      assert.ok(events.includes(expected), `missing trace event: ${expected}`);
    }
    assert.ok(events.indexOf('tool_request_received') < events.indexOf('job_registered'));
    assert.ok(events.indexOf('execution_running') < events.indexOf('completed'));

    const logsResponse = await fetch(`${base}/runtime/api/executions/${execId}/logs?max_output_bytes=65536`);
    assert.equal(logsResponse.status, 200);
    const logs = await logsResponse.json();
    assert.equal(logs.logs_available, true);
    assert.match(logs.stdout, /hello-runtime/);
    assert.match(logs.stdout, /done/);
    assert.doesNotMatch(logs.stdout, new RegExp(secret));
    assert.match(logs.stdout, /\[REDACTED\]/);
  });
});

test('Runtime overview is observation-only and leaves existing MCP and metrics surfaces independent', async () => {
  await withServer(async (base) => {
    const overview = await fetch(`${base}/runtime/api/overview`);
    assert.equal(overview.status, 200);
    const data = await overview.json();
    assert.equal(data.health, 'healthy');
    assert.equal(data.counts.running, 0);
    assert.equal(data.counts.queued, 0);
    assert.equal(typeof data.version, 'string');

    const metrics = await fetch(`${base}/metrics`);
    assert.equal(metrics.status, 200);
    assert.match(await metrics.text(), /exec_mcp_requests_total/);
  });
});

test('explicit task handles isolate concurrent ChatGPT task groups and reject unknown handles', async () => {
  await withServer(async (base) => {
    const sessionA = 'chat-window-a';
    const sessionB = 'chat-window-b';
    const taskA = (await mcpCall(base, 1001, 'begin_task', { label: 'Window A task' }, sessionA)).result.structuredContent;
    const taskB = (await mcpCall(base, 2001, 'begin_task', { label: 'Window B task' }, sessionB)).result.structuredContent;
    assert.notEqual(taskA.task_handle, taskB.task_handle);

    const unknown = await mcpCall(base, 1002, 'start_exec', {
      task_handle: 'task-11111111-1111-4111-8111-111111111111',
      command: 'true', cwd: '/tmp'
    }, sessionA);
    assert.equal(unknown.result.isError, true);
    assert.equal(unknown.result.structuredContent.code, 'unknown_task_handle');
    assert.match(unknown.result.content[0].text, /call begin_task/);

    const a1 = await mcpCall(base, 1003, 'start_exec', {
      task_handle: taskA.task_handle, command: 'sleep 0.08; printf A1', cwd: '/tmp', label: 'A1'
    }, sessionA);
    const a2 = await mcpCall(base, 1004, 'start_exec', {
      task_handle: taskA.task_handle, command: 'sleep 0.08; printf A2', cwd: '/tmp', label: 'A2'
    }, sessionA);
    const b1 = await mcpCall(base, 2002, 'start_exec', {
      task_handle: taskB.task_handle, command: 'sleep 0.08; printf B1', cwd: '/tmp', label: 'B1'
    }, sessionB);

    for (const result of [a1, a2, b1]) assert.equal(result.result.isError, false);
    assert.equal(a1.result.structuredContent.task_handle, taskA.task_handle);
    assert.equal(a2.result.structuredContent.task_handle, taskA.task_handle);
    assert.equal(b1.result.structuredContent.task_handle, taskB.task_handle);

    await Promise.all([
      waitForFinished(base, a1.result.structuredContent.exec_id),
      waitForFinished(base, a2.result.structuredContent.exec_id),
      waitForFinished(base, b1.result.structuredContent.exec_id)
    ]);

    const list = await (await fetch(`${base}/runtime/api/executions?limit=50`)).json();
    const byId = new Map(list.executions.map((item) => [item.exec_id, item]));
    assert.equal(byId.get(a1.result.structuredContent.exec_id).task_context.label, 'Window A task');
    assert.equal(byId.get(a2.result.structuredContent.exec_id).task_context.label, 'Window A task');
    assert.equal(byId.get(b1.result.structuredContent.exec_id).task_context.label, 'Window B task');
    assert.equal(byId.get(a1.result.structuredContent.exec_id).task_context.execution_count, 2);
    assert.equal(byId.get(b1.result.structuredContent.exec_id).task_context.execution_count, 1);
  });
});
