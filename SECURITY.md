# Security policy

## Supported versions

Security fixes are provided for the latest released minor version.

| Version | Supported |
|---|---|
| `0.3.x` | Yes |
| `< 0.3` | No |

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub private vulnerability reporting:

https://github.com/3011/exec-mcp/security/advisories/new

Include, when available:

- affected version or commit;
- deployment assumptions and configuration relevant to the issue;
- clear reproduction steps or a minimal proof of concept;
- impact and expected security boundary;
- suggested mitigation;
- whether the issue is already public.

Please avoid accessing data that is not yours, disrupting live systems, or retaining secrets obtained during testing. Reports will be investigated privately and coordinated disclosure will be preferred.

## Security expectations

`exec-mcp` is a remote command execution gateway for a trusted single tenant. It does not provide built-in client authentication, authorization, TLS termination, or a remote-host sandbox. Deployers are responsible for supplying those boundaries.

Before deployment, read [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) and follow its checklist.
