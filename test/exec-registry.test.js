import test from 'node:test';
import assert from 'node:assert/strict';
import { ExecRegistry, ExecutionCircuitOpenError, TooManyActiveExecsError } from '../src/exec-registry.js';

test('finalize is idempotent and writes history once', () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 1000 });
    assert.equal(registry.finalize(rec.id, { exitCode: 0, transportExitConfirmed: true }).finalized, true);
    assert.equal(registry.finalize(rec.id, { exitCode: 0, transportExitConfirmed: true }).finalized, false);
    assert.equal(registry.activeCount, 0);
    assert.equal(registry.recent.length, 1);
    assert.equal(registry.recent[0].final_state, 'completed');
  } finally { registry.close(); }
});

test('full registry reports active age and starting state', () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    registry.acquire({ timeoutMs: 1000 });
    assert.throws(() => registry.acquire({ timeoutMs: 1000 }), (err) => err instanceof TooManyActiveExecsError && /states=starting:1/.test(err.message));
  } finally { registry.close(); }
});

test('timeout preserves first abort reason', async () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 10 });
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(rec.controller.signal.aborted, true);
    assert.equal(rec.controller.signal.reason.message, 'request_timeout');
    assert.equal(registry.requestAbort(rec.id, 'manual_cancel').accepted, false);
    assert.equal(rec.abortReason, 'request_timeout');
  } finally { registry.close(); }
});

test('manual cancel is idempotent and finalizes as cancelled', () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 1000 });
    assert.equal(registry.requestCancel(rec.id).result, 'accepted');
    assert.equal(registry.requestCancel(rec.id).result, 'idempotent');
    registry.finalize(rec.id, { transportExitConfirmed: true, signal: 'SIGTERM' });
    assert.equal(registry.requestCancel(rec.id).result, 'already_finished');
    assert.equal(registry.recent[0].final_state, 'cancelled');
  } finally { registry.close(); }
});

test('emergency force reap opens circuit until late transport close', () => {
  const registry = new ExecRegistry({ maxActive: 1 });
  try {
    const rec = registry.acquire({ timeoutMs: 1000 });
    assert.equal(registry.forceReap(rec.id), true);
    assert.equal(registry.circuitOpen, true);
    assert.throws(() => registry.acquire({ timeoutMs: 1000 }), ExecutionCircuitOpenError);
    assert.equal(registry.recent[0].final_state, 'unconfirmed_reaped');
    assert.equal(registry.observeLateTransportClose(rec.id, { exitCode: null, signal: 'SIGKILL' }), true);
    assert.equal(registry.circuitOpen, false);
    assert.equal(registry.recent[0].transport_exit_confirmed, true);
  } finally { registry.close(); }
});

test('history ring evicts oldest records', () => {
  const registry = new ExecRegistry({ maxActive: 1, historyLimit: 2 });
  try {
    const ids = [];
    for (let i = 0; i < 3; i++) {
      const rec = registry.acquire({ timeoutMs: 1000 });
      ids.push(rec.id);
      registry.finalize(rec.id, { exitCode: 0, transportExitConfirmed: true });
    }
    assert.equal(registry.recent.length, 2);
    assert.equal(registry.status(ids[0]).found, false);
    assert.equal(registry.status(ids[2]).found, true);
  } finally { registry.close(); }
});

test('history eviction never clears an unresolved circuit diagnostic', () => {
  const registry = new ExecRegistry({ maxActive: 3, historyLimit: 1 });
  try {
    const stuck = registry.acquire({ timeoutMs: 1000 });
    const finishing = registry.acquire({ timeoutMs: 1000 });
    registry.forceReap(stuck.id);
    registry.finalize(finishing.id, { exitCode: 0, transportExitConfirmed: true });
    assert.equal(registry.recent.length, 1);
    assert.equal(registry.circuitOpen, true);
    assert.equal(registry.status(stuck.id).source, 'unconfirmed');
    registry.observeLateTransportClose(stuck.id);
    assert.equal(registry.circuitOpen, false);
  } finally { registry.close(); }
});
