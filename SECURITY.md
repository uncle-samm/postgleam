# Security Policy

## Supported Versions

Security fixes are provided for the latest published `0.x` release and the `main` branch.

## Reporting a Vulnerability

Please do not open a public issue for suspected vulnerabilities. Email the maintainer or use GitHub private vulnerability reporting when available.

Include:

- A description of the issue and affected API surface.
- A minimal reproduction, if possible.
- Whether credentials, TLS, query construction, COPY, LISTEN/NOTIFY, or replication are involved.

## Security Notes

- Use parameterized queries for user-controlled values.
- Use `config.SslVerified` for production network connections.
- Treat `config.SslUnverified`, MD5 authentication, and cleartext authentication as compatibility modes only.
- Do not commit Hex API keys, database passwords, `.env` files, crash dumps, or generated build output.
