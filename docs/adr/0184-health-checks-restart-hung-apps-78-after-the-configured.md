# ADR 0184: Health checks restart hung apps (#78): after the configured number of consecutive failed probes outside a start grace period, the node restarts the VM with the crash backoff; opt-out per app

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0184
- **Tags:** m5b, node-agent, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The node already probes and emits app.down and app.up (crates/node-agent/src/health.rs) but never restarts; node-agent-protocol 6.3 says report only, so a restart is a contract change. Health-gated restarts are also the base for D-0155's automatic rebuild and rollback.

## Decision

Health checks restart hung apps (#78): after the configured number of consecutive failed probes outside a start grace period, the node restarts the VM with the crash backoff; opt-out per app.

Contract: health_check gains start_period_s (default 30: failures in that window after a start do not count) and restart (default true). After `failures` consecutive failures outside the start period the node reports failing, emits app.down and restarts the VM in place with the crash backoff (1 s doubling to 5 min); a stateful VM restarts only while its lease is held with the matching token (6.4). restart false keeps today's report-only behaviour. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0184 in the project decision ledger. Do not edit by hand; changes go through a new decision.
