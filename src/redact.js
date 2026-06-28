const RULES = [
  /(Authorization:\s*Bearer\s+)[A-Za-z0-9._~+\/=:-]+/gi,
  /((?:AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)\s*=\s*)[^\s]+/gi,
  /((?:password|passwd|token|secret|api[_-]?key)\s*[:=]\s*)[^\s'\"]+/gi,
  /(-----BEGIN [A-Z ]*PRIVATE KEY-----)[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----)/g
];

export function redact(input) {
  let text = String(input);
  for (const rule of RULES) {
    text = text.replace(rule, (...args) => {
      if (args.length >= 4 && typeof args[1] === 'string' && typeof args[2] === 'string') {
        return `${args[1]}[REDACTED]${args[2]}`;
      }
      return `${args[1] || ''}[REDACTED]`;
    });
  }
  return text;
}
