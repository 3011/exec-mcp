import { randomUUID } from 'node:crypto';

export class TooManyActiveExecsError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TooManyActiveExecsError';
    this.code = 'too_many_active_execs';
  }
}

export class ExecRegistry {
  constructor({ maxActive, reapIntervalMs = 10000, reapGraceMs = 30000 } = {}) {
    if (!Number.isInteger(maxActive) || maxActive <= 0) {
      throw new Error('maxActive must be a positive integer');
    }

    this.maxActive = maxActive;
    this.reapGraceMs = reapGraceMs;
    this.active = new Map();

    this.reaper = setInterval(() => this.reap(), reapIntervalMs);
    this.reaper.unref?.();
  }

  get activeCount() {
    return this.active.size;
  }

  acquire({ timeoutMs }) {
    if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
      throw new Error('timeoutMs must be a positive integer');
    }

    if (this.active.size >= this.maxActive) {
      throw new TooManyActiveExecsError(this.tooManyActiveMessage());
    }

    const id = randomUUID();
    const now = Date.now();
    const controller = new AbortController();
    const rec = {
      id,
      startedAt: now,
      timeoutAt: now + timeoutMs,
      timeoutMs,
      controller,
      state: 'running',
      released: false,
      timer: null
    };

    rec.timer = setTimeout(() => {
      if (rec.released || controller.signal.aborted) return;
      rec.state = 'timeout_aborting';
      controller.abort(new Error('exec_timeout'));
    }, timeoutMs);
    rec.timer.unref?.();

    this.active.set(id, rec);
    return rec;
  }

  abort(id, state, reason) {
    const rec = this.active.get(id);
    if (!rec || rec.released || rec.controller.signal.aborted) return false;
    rec.state = state;
    rec.controller.abort(reason instanceof Error ? reason : new Error(String(reason || state)));
    return true;
  }

  release(id) {
    const rec = this.active.get(id);
    if (!rec) return false;

    rec.released = true;
    if (rec.timer) clearTimeout(rec.timer);
    this.active.delete(id);
    return true;
  }

  reap(now = Date.now()) {
    for (const rec of this.active.values()) {
      if (rec.released) continue;

      if (now > rec.timeoutAt && rec.state === 'running') {
        rec.state = 'reaper_aborting';
        if (!rec.controller.signal.aborted) {
          rec.controller.abort(new Error('exec_reaper_abort'));
        }
      }

      if (now > rec.timeoutAt + this.reapGraceMs) {
        rec.state = 'reaped';
        this.release(rec.id);
      }
    }
  }

  snapshot(now = Date.now()) {
    return [...this.active.values()].map((rec) => ({
      id: rec.id,
      state: rec.state,
      ageSeconds: Math.floor((now - rec.startedAt) / 1000),
      timeoutInSeconds: Math.floor((rec.timeoutAt - now) / 1000)
    }));
  }

  tooManyActiveMessage() {
    const now = Date.now();
    const records = this.snapshot(now);
    const oldest = records.reduce((max, rec) => Math.max(max, rec.ageSeconds), 0);
    const states = records.reduce((acc, rec) => {
      acc.set(rec.state, (acc.get(rec.state) || 0) + 1);
      return acc;
    }, new Map());
    const statesText = [...states.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([state, count]) => `${state}:${count}`)
      .join(',');

    return `too_many_active_execs: active=${this.active.size} max=${this.maxActive} oldest_age_seconds=${oldest}${statesText ? ` states=${statesText}` : ''}`;
  }

  close() {
    clearInterval(this.reaper);
  }
}
