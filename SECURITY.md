# Security

## Reporting a problem

Please don't open a public issue for a security problem. Report it privately through GitHub instead:
**Security → Report a vulnerability** on this repository.

Include what you found, how to reproduce it, and what an attacker could do with it. You'll get a reply within a few days. Once a fix is released, you'll be credited in the release notes unless you'd rather not be.

## What's in scope

- Getting past `ingest_token` or `api_token` checks
- Reading or changing another tenant's data, rules, receivers or alerts
- Leaking tokens or webhook URLs through logs, the API or alert messages
- Making signalbox crash or stop alerting with crafted input

## Supported versions

Fixes go into the latest commit on `main` and the image built from it.
