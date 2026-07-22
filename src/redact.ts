const RULES: readonly RegExp[] = [
  /(Authorization:\s*Bearer\s+)[A-Za-z0-9._~+\/=:-]+/gi,
  /((?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)\s*=\s*)[^\s]+/gi,
  /((?:password|passwd|token|secret|api[_-]?key)\s*[:=]\s*)[^\s'\"]+/gi,
  /((?:--password|--passwd|--token|--secret|-p)\s+)(?:'[^']*'|"[^"]*"|[^\s]+)/gi,
  /(https?:\/\/[^:\s/@]+:)[^@\s/]+@/gi,
  /(-----BEGIN [A-Z ]*PRIVATE KEY-----)[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----)/g
];

export function redact(input: unknown): string {
  let text = String(input);
  for (const rule of RULES) {
    text = text.replace(rule, (_match: string, ...captures: unknown[]): string => {
      const first = typeof captures[0] === 'string' ? captures[0] : '';
      const second = typeof captures[1] === 'string' ? captures[1] : '';
      return second ? `${first}[REDACTED]${second}` : `${first}[REDACTED]`;
    });
  }
  return text;
}
