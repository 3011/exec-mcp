import test from 'node:test';
import assert from 'node:assert/strict';
import { ExecRegistry, TooManyActiveExecsError } from '../src/exec-registry.js';

test('ExecRegistry release is idempotent', () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 1000 });
    assert.equal(registry.activeCount, 1);
    assert.equal(registry.release(rec.id), true);
    assert.equal(registry.release(rec.id), false);
    assert.equal(registry.activeCount, 0);
  } finally {
    registry.close();
  }
});

test('ExecRegistry full acquire reports active age and states', () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 1000 });
    rec.startedAt -= 2000;

    assert.throws(
      () => registry.acquire({ timeoutMs: 1000 }),
      (err) => err instanceof TooManyActiveExecsError
        && err.code === 'too_many_active_execs'
        && /active=1 max=1 oldest_age_seconds=2/.test(err.message)
        && /states=running:1/.test(err.message)
    );
    assert.equal(registry.activeCount, 1);
  } finally {
    registry.close();
  }
});

test('ExecRegistry timeout aborts the record controller', async () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 20 });
    rec.timer.ref?.();
    await new Promise((resolve) => rec.controller.signal.addEventListener('abort', resolve, { once: true }));
    assert.equal(rec.state, 'timeout_aborting');
    assert.equal(rec.controller.signal.reason.message, 'exec_timeout');
    assert.equal(registry.activeCount, 1);
    registry.release(rec.id);
    assert.equal(registry.activeCount, 0);
  } finally {
    registry.close();
  }
});

test('ExecRegistry reaper aborts first and releases after grace period', () => {
  const registry = new ExecRegistry({ maxActive: 1, reapGraceMs: 1000 });
  try {
    const rec = registry.acquire({ timeoutMs: 10000 });
    const now = Date.now();
    rec.timeoutAt = now - 1;

    registry.reap(now);
    assert.equal(rec.state, 'reaper_aborting');
    assert.equal(rec.controller.signal.aborted, true);
    assert.equal(rec.controller.signal.reason.message, 'exec_reaper_abort');
    assert.equal(registry.activeCount, 1);

    registry.reap(now + 1001);
    assert.equal(registry.activeCount, 0);
  } finally {
    registry.close();
  }
});
