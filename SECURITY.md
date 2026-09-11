# Security Policy

## Supported versions

| Version | Receives security updates |
|---|---|
| `main` | ✅ |

## Reporting a vulnerability

If you discover a security vulnerability in HData, **do not open a public Issue or PR**, to avoid abuse.

Report it privately via one of the following:

- Email: **opensource@hdata.example** (replace with the maintainer's real address)
- Or use GitHub's **Private vulnerability reporting** (Security → Report a vulnerability)

When reporting, please include:

1. The vulnerability type and impact (e.g. arbitrary file write, dependency RCE, credential leakage).
2. Steps to reproduce, or a minimal pipeline example.
3. The affected version / commit.
4. If known, a suggested fix direction.

We will acknowledge receipt, then assess and fix as soon as possible. Details stay limited to maintainers
until a fix is released.

## Security-related design conventions

HData already follows several security conventions in code; please do not break them when contributing:

- **Dead letter does not leak secrets**: dead-letter records carry only the error cause and the original
  row data; never print plaintext passwords in logs or output.
- **No credentials in config files**: sensitive fields such as passwords in a pipeline should use
  `${VAR}` / `${VAR:-default}` placeholders, injected via system properties or environment variables —
  do not write plaintext into the repo or example files.
- **No silent security downgrade**: capabilities with security implications (e.g. exactly-once) that are
  not supported should fail explicitly in `validate`, not silently degrade to an unsafe behavior.
- **No non-serializable / unauthorized-networking objects**: DoFns are serialized and sent to workers at
  job submission, so capturing non-serializable objects crashes at submission time.

## Dependency security

Dependencies come from Maven Central. If you find a known vulnerability in a transitive dependency, please report it too.
