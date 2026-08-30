import { randomUUID } from 'node:crypto';

export const TASK_HANDLE_PATTERN = /^task-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export interface TaskContext {
  task_handle: string;
  label: string | null;
  created_at: string;
  last_attached_at: string;
  execution_count: number;
}

interface MutableTaskContext {
  taskHandle: string;
  label: string | null;
  createdAt: number;
  lastAttachedAt: number;
  executionIds: Set<string>;
}

export class TaskContextStore {
  private readonly contexts = new Map<string, MutableTaskContext>();
  private readonly maxContexts: number;

  constructor(maxContexts = 500) {
    this.maxContexts = Math.max(20, maxContexts);
  }

  create(label: string | null, now = Date.now()): TaskContext {
    const taskHandle = `task-${randomUUID()}`;
    const context: MutableTaskContext = {
      taskHandle,
      label,
      createdAt: now,
      lastAttachedAt: now,
      executionIds: new Set()
    };
    this.contexts.set(taskHandle, context);
    this.prune();
    return toPublic(context);
  }

  has(taskHandle: string): boolean {
    return this.contexts.has(taskHandle);
  }

  attach(taskHandle: string, execId: string, now = Date.now()): void {
    const context = this.contexts.get(taskHandle);
    if (!context) return;
    context.executionIds.add(execId);
    context.lastAttachedAt = Math.max(context.lastAttachedAt, now);
  }

  get(taskHandle: string | null | undefined): TaskContext | null {
    if (!taskHandle) return null;
    const context = this.contexts.get(taskHandle);
    return context ? toPublic(context) : null;
  }

  list(): TaskContext[] {
    return [...this.contexts.values()]
      .sort((a, b) => b.lastAttachedAt - a.lastAttachedAt)
      .map(toPublic);
  }

  private prune(): void {
    if (this.contexts.size <= this.maxContexts) return;
    const oldest = [...this.contexts.values()].sort((a, b) => a.lastAttachedAt - b.lastAttachedAt);
    while (this.contexts.size > this.maxContexts) {
      const context = oldest.shift();
      if (!context) break;
      this.contexts.delete(context.taskHandle);
    }
  }
}

function toPublic(context: MutableTaskContext): TaskContext {
  return {
    task_handle: context.taskHandle,
    label: context.label,
    created_at: new Date(context.createdAt).toISOString(),
    last_attached_at: new Date(context.lastAttachedAt).toISOString(),
    execution_count: context.executionIds.size
  };
}
