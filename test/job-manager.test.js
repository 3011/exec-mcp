import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from '../dist/src/server.js';
import { parseConfig } from '../dist/src/config.js';
import { remoteTestEnv } from '../scripts/helpers.js';

async function withServer(overrides, fn) {
  const config = parseConfig({
    HOST: '127.0.0.1', PORT: '0',
    ALLOWED_CWDS: '/tmp', DEFAULT_CWD: '/tmp',
    MAX_CONCURRENT_EXECS: '2',
    SYNC_MAX_CONCURRENT_EXECS: '2',
    ASYNC_MAX_CONCURRENT_EXECS: '2',
    GLOBAL_MAX_CONCURRENT_EXECS: '2',
    MAX_QUEUED_EXECS: '4',
    JOB_LOG_BYTES: '128',
    STATUS_DEFAULT_MAX_OUTPUT_BYTES: '32',
    STATUS_HARD_MAX_OUTPUT_BYTES: '256',
    STATUS_MAX_WAIT_SECONDS: '30',
    HEARTBEAT_SECONDS: '99',
    ...remoteTestEnv(),
    ...overrides
  });
  const instance = createServer(config);
  instance.server.listen(0, '127.0.0.1');
  await once(instance.server, 'listening');
  try { await fn(`http://127.0.0.1:${instance.server.address().port}`, instance); }
  finally {
    instance.runner.close();
    await new Promise((resolve) => instance.server.close(resolve));
  }
}

async function mcp(base, id, name, args = {}) {
  const response = await fetch(`${base}/mcp`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } })
  });
  assert.equal(response.status, 200);
  return await response.json();
}

async function waitFor(check, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail('condition not reached before timeout');
}

test('start_exec registers a queryable job before returning', async () => {
  await withServer({}, async (base) => {
    const started = await mcp(base, 1, 'start_exec', { command: 'sleep 0.2; printf done', label: 'queryable' });
    assert.equal(started.result.isError, false);
    const info = started.result.structuredContent;
    assert.match(info.exec_id, /^exec-/);
    assert.ok(['queued', 'running', 'completed'].includes(info.status));

    const status = await mcp(base, 2, 'get_exec_status', { exec_id: info.exec_id });
    assert.equal(status.result.structuredContent.found, true);
    assert.equal(status.result.structuredContent.task.exec_id, info.exec_id);
    assert.equal(status.result.structuredContent.task.label, 'queryable');
  });
});

test('async pool queues excess jobs and queued jobs do not consume slots', async () => {
  await withServer({ ASYNC_MAX_CONCURRENT_EXECS: '1', GLOBAL_MAX_CONCURRENT_EXECS: '2' }, async (base) => {
    const a = (await mcp(base, 1, 'start_exec', { command: 'sleep 0.4', label: 'a' })).result.structuredContent;
    const b = (await mcp(base, 2, 'start_exec', { command: 'sleep 0.4', label: 'b' })).result.structuredContent;

    const list = await waitFor(async () => {
      const value = (await mcp(base, 3, 'list_active_execs')).result.structuredContent;
      return value.active === 1 && value.queued === 1 ? value : null;
    });
    assert.equal(list.async_running, 1);
    assert.equal(list.total_active, 2);
    assert.equal(list.tasks.find((task) => task.exec_id === b.exec_id).status, 'queued');
    assert.equal(list.tasks.find((task) => task.exec_id === b.exec_id).queue_position, 1);

    await mcp(base, 4, 'get_exec_status', { exec_id: a.exec_id, wait_seconds: 2 });
    const bStatus = await waitFor(async () => {
      const value = (await mcp(base, 5, 'get_exec_status', { exec_id: b.exec_id })).result.structuredContent;
      return value.task.status === 'running' || value.task.status === 'completed' ? value : null;
    });
    assert.notEqual(bStatus.task.status, 'queued');
  });
});

test('queued timeout starts when execution starts, not while waiting', async () => {
  await withServer({ ASYNC_MAX_CONCURRENT_EXECS: '1', GLOBAL_MAX_CONCURRENT_EXECS: '1', MAX_TIMEOUT_SECONDS: '3' }, async (base) => {
    const first = (await mcp(base, 1, 'start_exec', { command: 'sleep 1.2', timeout_seconds: 2 })).result.structuredContent;
    const second = (await mcp(base, 2, 'start_exec', { command: 'sleep 0.1; printf ok', timeout_seconds: 1 })).result.structuredContent;
    const queued = (await mcp(base, 3, 'get_exec_status', { exec_id: second.exec_id })).result.structuredContent;
    assert.equal(queued.task.status, 'queued');
    await mcp(base, 4, 'get_exec_status', { exec_id: first.exec_id, wait_seconds: 2 });
    const done = (await mcp(base, 5, 'get_exec_status', { exec_id: second.exec_id, wait_seconds: 2 })).result.structuredContent;
    assert.equal(done.task.status, 'completed');
    assert.equal(done.task.timed_out, false);
  });
});

test('cancel_exec terminalizes queued jobs and terminal state is immutable', async () => {
  await withServer({ ASYNC_MAX_CONCURRENT_EXECS: '1', GLOBAL_MAX_CONCURRENT_EXECS: '1' }, async (base) => {
    const first = (await mcp(base, 1, 'start_exec', { command: 'sleep 0.5' })).result.structuredContent;
    const queued = (await mcp(base, 2, 'start_exec', { command: 'sleep 0.5' })).result.structuredContent;
    assert.equal((await mcp(base, 3, 'cancel_exec', { exec_id: queued.exec_id })).result.structuredContent.result, 'accepted');
    const status = (await mcp(base, 4, 'get_exec_status', { exec_id: queued.exec_id })).result.structuredContent;
    assert.equal(status.task.status, 'cancelled');
    const again = (await mcp(base, 5, 'cancel_exec', { exec_id: queued.exec_id })).result.structuredContent;
    assert.equal(again.result, 'already_finished');
    assert.equal(again.final_state, 'cancelled');
    await mcp(base, 6, 'cancel_exec', { exec_id: first.exec_id });
  });
});

test('get_exec_status returns incremental stdout/stderr with independent cursors', async () => {
  await withServer({}, async (base) => {
    const started = (await mcp(base, 1, 'start_exec', { command: 'printf first; printf err1 >&2; sleep 0.25; printf second; printf err2 >&2' })).result.structuredContent;
    const first = await waitFor(async () => {
      const value = (await mcp(base, 2, 'get_exec_status', { exec_id: started.exec_id, max_output_bytes: 32 })).result.structuredContent;
      return value.stdout.includes('first') && value.stderr.includes('err1') ? value : null;
    });
    assert.equal(first.logs_available, true);
    assert.equal(first.stdout_log_truncated, false);
    assert.equal(first.stderr_log_truncated, false);

    const second = (await mcp(base, 3, 'get_exec_status', {
      exec_id: started.exec_id,
      stdout_cursor: first.stdout_cursor,
      stderr_cursor: first.stderr_cursor,
      max_output_bytes: 32,
      wait_seconds: 2
    })).result.structuredContent;
    assert.equal(second.task.status, 'completed');
    assert.match(second.stdout, /second/);
    assert.match(second.stderr, /err2/);
    assert.doesNotMatch(second.stdout, /first/);
    assert.doesNotMatch(second.stderr, /err1/);
  });
});

test('env values never appear in list, status metadata, history, or retained job logs', async () => {
  await withServer({}, async (base) => {
    const secret = 'super-secret-env-value-123';
    const started = (await mcp(base, 1, 'start_exec', {
      command: 'printf "%s" "$SECRET_TOKEN"',
      env: { SECRET_TOKEN: secret },
      label: 'env-redaction'
    })).result.structuredContent;
    const listText = JSON.stringify((await mcp(base, 2, 'list_active_execs')).result.structuredContent);
    assert.equal(listText.includes(secret), false);

    const done = (await mcp(base, 3, 'get_exec_status', { exec_id: started.exec_id, wait_seconds: 2, max_output_bytes: 128 })).result.structuredContent;
    const statusText = JSON.stringify(done);
    assert.equal(statusText.includes(secret), false);
    assert.match(done.stdout, /REDACTED/);
  });
});

test('async_max leaves sync capacity available while respecting global_max', async () => {
  await withServer({
    SYNC_MAX_CONCURRENT_EXECS: '2',
    ASYNC_MAX_CONCURRENT_EXECS: '2',
    GLOBAL_MAX_CONCURRENT_EXECS: '4'
  }, async (base, { runner }) => {
    const a = (await mcp(base, 1, 'start_exec', { command: 'sleep 0.4' })).result.structuredContent;
    const b = (await mcp(base, 2, 'start_exec', { command: 'sleep 0.4' })).result.structuredContent;
    await waitFor(() => runner.active === 2);

    const sync = mcp(base, 3, 'exec', { command: 'printf sync-ok' });
    await waitFor(() => runner.active === 3);
    assert.ok(runner.active <= 4);
    const syncResult = await sync;
    assert.equal(syncResult.result.structuredContent.code, 0);
    assert.match(syncResult.result.structuredContent.stdout_tail, /sync-ok/);
    await mcp(base, 4, 'cancel_exec', { exec_id: a.exec_id });
    await mcp(base, 5, 'cancel_exec', { exec_id: b.exec_id });
  });
});

test('status distinguishes response pagination from permanent log truncation', async () => {
  await withServer({ JOB_LOG_BYTES: '32', STATUS_DEFAULT_MAX_OUTPUT_BYTES: '8' }, async (base) => {
    const started = (await mcp(base, 101, 'start_exec', { command: "printf 'abcdefghijklmnopqrstuvwxyz0123456789'" })).result.structuredContent;
    const done = (await mcp(base, 102, 'get_exec_status', { exec_id: started.exec_id, wait_seconds: 2, max_output_bytes: 8 })).result.structuredContent;
    assert.equal(done.task.status, 'completed');
    assert.equal(done.stdout_log_truncated, true, 'old stdout bytes were permanently discarded by the retained log limit');
    assert.equal(done.has_more_stdout, true, 'more retained stdout remains after this response page');
    assert.equal(Buffer.byteLength(done.stdout, 'utf8') <= 8, true);

    const next = (await mcp(base, 103, 'get_exec_status', {
      exec_id: started.exec_id,
      stdout_cursor: done.stdout_cursor,
      stderr_cursor: done.stderr_cursor,
      max_output_bytes: 64
    })).result.structuredContent;
    assert.equal(next.stdout_log_truncated, true);
    assert.equal(next.has_more_stdout, false);
  });
});

test('wait_seconds above the server hard limit is rejected', async () => {
  await withServer({}, async (base) => {
    const started = (await mcp(base, 111, 'start_exec', { command: 'sleep 0.2' })).result.structuredContent;
    const status = await mcp(base, 112, 'get_exec_status', { exec_id: started.exec_id, wait_seconds: 31 });
    assert.equal(status.result.isError, true);
    assert.match(status.result.content[0].text, /wait_seconds_too_large/);
    await mcp(base, 113, 'cancel_exec', { exec_id: started.exec_id });
  });
});

test('queue limit rejects only after all running and queued capacity is occupied', async () => {
  await withServer({ ASYNC_MAX_CONCURRENT_EXECS: '1', GLOBAL_MAX_CONCURRENT_EXECS: '1', MAX_QUEUED_EXECS: '1' }, async (base) => {
    const running = (await mcp(base, 121, 'start_exec', { command: 'sleep 0.5' })).result.structuredContent;
    const queued = (await mcp(base, 122, 'start_exec', { command: 'sleep 0.5' })).result.structuredContent;
    const rejected = await mcp(base, 123, 'start_exec', { command: 'printf rejected' });
    assert.equal(rejected.result.isError, true);
    assert.match(rejected.result.content[0].text, /exec_queue_full/);
    const list = (await mcp(base, 124, 'list_active_execs')).result.structuredContent;
    assert.equal(list.active, 1);
    assert.equal(list.queued, 1);
    await mcp(base, 125, 'cancel_exec', { exec_id: running.exec_id });
    await mcp(base, 126, 'cancel_exec', { exec_id: queued.exec_id });
  });
});

test('retained job logs redact an env value split across output chunks', async () => {
  await withServer({}, async (base) => {
    const value = 'abcdef';
    const started = (await mcp(base, 131, 'start_exec', {
      command: "printf 'abc'; sleep 0.15; printf 'def'",
      env: { TEST_VALUE: value }
    })).result.structuredContent;
    const done = (await mcp(base, 132, 'get_exec_status', {
      exec_id: started.exec_id,
      wait_seconds: 2,
      max_output_bytes: 128
    })).result.structuredContent;
    assert.equal(done.task.status, 'completed');
    assert.equal(done.stdout.includes(value), false);
    assert.match(done.stdout, /REDACTED/);
  });
});
