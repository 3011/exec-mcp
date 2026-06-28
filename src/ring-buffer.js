export class RingBuffer {
  constructor(maxBytes = 65536) {
    if (!Number.isInteger(maxBytes) || maxBytes <= 0) {
      throw new Error('maxBytes must be a positive integer');
    }
    this.maxBytes = maxBytes;
    this.chunks = [];
    this.bytes = 0;
  }

  append(input) {
    const buf = Buffer.isBuffer(input) ? input : Buffer.from(String(input));
    if (buf.length === 0) return;

    if (buf.length >= this.maxBytes) {
      this.chunks = [buf.subarray(buf.length - this.maxBytes)];
      this.bytes = this.maxBytes;
      return;
    }

    this.chunks.push(buf);
    this.bytes += buf.length;
    while (this.bytes > this.maxBytes && this.chunks.length > 0) {
      const first = this.chunks[0];
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

  toString(encoding = 'utf8') {
    return Buffer.concat(this.chunks, this.bytes).toString(encoding);
  }
}
