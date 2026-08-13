import { StringDecoder } from 'node:string_decoder';
import { redact } from './redact.js';

export class JobOutputRedactor {
  private readonly decoder = new StringDecoder('utf8');
  private secrets: string[];
  private maxSecretLength: number;
  private pending = '';

  constructor(secretValues: string[]) {
    this.secrets = [...new Set(secretValues.filter((value) => value.length > 0))]
      .sort((a, b) => b.length - a.length);
    this.maxSecretLength = this.secrets.reduce((max, value) => Math.max(max, value.length), 0);
  }

  push(chunk: Buffer): string {
    return this.consume(this.decoder.write(chunk), false);
  }

  flush(): string {
    return this.consume(this.decoder.end(), true);
  }

  clearSecrets(): void {
    this.secrets = [];
    this.maxSecretLength = 0;
    this.pending = '';
  }

  private consume(decoded: string, flush: boolean): string {
    this.pending += decoded;
    if (this.pending.length === 0) return '';

    if (this.secrets.length === 0) {
      const output = redact(this.pending);
      this.pending = '';
      return output;
    }

    const safeLimit = flush
      ? this.pending.length
      : Math.max(0, this.pending.length - this.maxSecretLength + 1);
    let index = 0;
    let output = '';

    while (index < safeLimit) {
      const secret = this.secrets.find((value) => this.pending.startsWith(value, index));
      if (secret) {
        output += '[REDACTED]';
        index += secret.length;
      } else {
        output += this.pending[index] ?? '';
        index++;
      }
    }

    if (flush) {
      while (index < this.pending.length) {
        const secret = this.secrets.find((value) => this.pending.startsWith(value, index));
        if (secret) {
          output += '[REDACTED]';
          index += secret.length;
        } else {
          output += this.pending[index] ?? '';
          index++;
        }
      }
    }

    this.pending = this.pending.slice(index);
    return redact(output);
  }
}
