export class RingBuffer {
  readonly maxBytes: number;
  private chunks: Buffer[] = [];
  private bytes = 0;

  constructor(maxBytes = 65536) {
    if (!Number.isInteger(maxBytes) || maxBytes <= 0) {
      throw new Error('maxBytes must be a positive integer');
    }
    this.maxBytes = maxBytes;
  }

  append(input: unknown): void {
    const buffer = Buffer.isBuffer(input) ? input : Buffer.from(String(input));
    if (buffer.length === 0) return;

    if (buffer.length >= this.maxBytes) {
      this.chunks = [buffer.subarray(buffer.length - this.maxBytes)];
      this.bytes = this.maxBytes;
      return;
    }

    this.chunks.push(buffer);
    this.bytes += buffer.length;
    while (this.bytes > this.maxBytes && this.chunks.length > 0) {
      const first = this.chunks[0];
      if (!first) break;
      const overflow = this.bytes - this.maxBytes;
      if (first.length <= overflow) {
        this.chunks.shift();
        this.bytes -= first.length;
      } else {
        this.chunks[0] = first.subarray(overflow);
        this.bytes -= overflow;
      }
    }
  }

  toString(encoding: BufferEncoding = 'utf8'): string {
    return Buffer.concat(this.chunks, this.bytes).toString(encoding);
  }
}
