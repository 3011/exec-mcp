export interface JobLogRead {
  data: string;
  nextCursor: number;
  hasMore: boolean;
  logTruncated: boolean;
}

export class JobLogBuffer {
  readonly maxBytes: number;
  private readonly storage: Buffer<ArrayBufferLike>;
  private startIndex = 0;
  private length = 0;
  private baseOffset = 0;
  private endOffset = 0;

  constructor(maxBytes: number) {
    if (!Number.isInteger(maxBytes) || maxBytes <= 0) throw new Error('maxBytes must be a positive integer');
    this.maxBytes = maxBytes;
    this.storage = Buffer.allocUnsafe(maxBytes);
  }

  append(value: string | Buffer): void {
    const incoming = Buffer.isBuffer(value) ? value : Buffer.from(value, 'utf8');
    if (incoming.length === 0) return;

    const nextEnd = this.endOffset + incoming.length;
    if (incoming.length >= this.maxBytes) {
      const tail = incoming.subarray(incoming.length - this.maxBytes);
      tail.copy(this.storage, 0);
      this.startIndex = 0;
      this.length = this.maxBytes;
      this.endOffset = nextEnd;
      this.baseOffset = this.endOffset - this.length;
      return;
    }

    const writeIndex = (this.startIndex + this.length) % this.maxBytes;
    this.copyIntoRing(incoming, writeIndex);

    const overflow = Math.max(0, this.length + incoming.length - this.maxBytes);
    if (overflow > 0) this.startIndex = (this.startIndex + overflow) % this.maxBytes;
    this.length = Math.min(this.maxBytes, this.length + incoming.length);
    this.endOffset = nextEnd;
    this.baseOffset = this.endOffset - this.length;
  }

  availableFrom(cursor: number): number {
    const normalized = normalizeCursor(cursor);
    const start = Math.max(this.baseOffset, Math.min(normalized, this.endOffset));
    return Math.max(0, this.endOffset - start);
  }

  read(cursor: number, maxBytes: number): JobLogRead {
    const normalized = normalizeCursor(cursor);
    const effective = Math.max(this.baseOffset, Math.min(normalized, this.endOffset));
    const budget = Math.max(0, Math.floor(maxBytes));
    const available = Math.max(0, this.endOffset - effective);
    const take = Math.min(available, budget);
    const relative = effective - this.baseOffset;
    const index = (this.startIndex + relative) % this.maxBytes;
    const data = this.readFromRing(index, take).toString('utf8');
    const nextCursor = effective + take;
    return {
      data,
      nextCursor,
      hasMore: nextCursor < this.endOffset,
      logTruncated: this.baseOffset > 0
    };
  }

  get startCursor(): number { return this.baseOffset; }
  get endCursor(): number { return this.endOffset; }
  get truncated(): boolean { return this.baseOffset > 0; }

  private copyIntoRing(source: Buffer, index: number): void {
    const first = Math.min(source.length, this.maxBytes - index);
    source.copy(this.storage, index, 0, first);
    if (first < source.length) source.copy(this.storage, 0, first);
  }

  private readFromRing(index: number, bytes: number): Buffer<ArrayBufferLike> {
    if (bytes <= 0) return Buffer.alloc(0);
    const first = Math.min(bytes, this.maxBytes - index);
    if (first === bytes) return this.storage.subarray(index, index + bytes);
    const output = Buffer.allocUnsafe(bytes);
    this.storage.copy(output, 0, index, index + first);
    this.storage.copy(output, first, 0, bytes - first);
    return output;
  }
}

function normalizeCursor(value: number): number {
  if (!Number.isFinite(value) || value < 0) return 0;
  return Math.floor(value);
}
