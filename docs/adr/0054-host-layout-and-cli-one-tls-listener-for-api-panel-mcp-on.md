# ADR 0054: Host layout and CLI: one TLS listener for API/panel/MCP on 8443, self-signed primary certificate pinned by join commands, /var/lib/quoro + /etc/quoro/quoro.toml, one-time setup token for /setup

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0054
- **Tags:** installer, control-plane, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

No document fixed the listener port and its coexistence with ingress on 80/443, the primary's TLS certificate source, data/config paths, the config format or the CLI command set; POST /api/v1/setup is public while the users table is empty, so anyone who reaches the listener first can claim owner.

## Decision

Host layout and CLI: one TLS listener for API/panel/MCP on 8443, self-signed primary certificate pinned by join commands, /var/lib/quoro + /etc/quoro/quoro.toml, one-time setup token for /setup.

Listener: one HTTPS port (default 8443, configurable) for API, panel and MCP; ingress owns 80/443 on the same host. Primary TLS: a self-signed certificate generated at install; the join command carries its SHA-256 fingerprint (the pin model of architecture section 5); rotating it invalidates outstanding join commands, which the CLI states; an operator may replace it with an ACME certificate later. Paths: data /var/lib/quoro (SQLite, key files 0600), config /etc/quoro/quoro.toml (TOML), logs via journald. CLI (binary quoro): 'quoro serve --role node|witness|edge' (systemd), 'quoro version', 'quoro join <url> --token --fingerprint', 'quoro mcp stdio' (MCP over stdio with an agent key), 'quoro setup-token' (prints the one-time token). First run: the installer prints a one-time setup token; POST /api/v1/setup requires it (header Quoro-Setup-Token); single use, expires after 1 hour; add a threat-model row for an unclaimed setup. Alternatives: ACME for the primary listener from install (needs a public name, fails behind NAT); /setup bound to localhost only (breaks remote installs). Accepted as stated.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0054 in the project decision ledger. Do not edit by hand; changes go through a new decision.
