# ADR 0193: health_check.start_period_s (minimum 0, default 30) and restart (default true) are optional in requests and always present in responses; a health restart keeps restart_generation; the edge ignores both

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0193
- **Tags:** m5b, contracts, node-agent
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0184 names the two fields and their defaults; the first M5b contract change must fix bounds, request and response shape, and how they interact with the crash backoff, the human restart counter and the edge's probes.

## Decision

health_check.start_period_s (minimum 0, default 30) and restart (default true) are optional in requests and always present in responses; a health restart keeps restart_generation; the edge ignores both.

openapi HealthCheck: start_period_s integer minimum 0 default 30, restart boolean default true, neither in required; the API stores and returns both (validation_failed for start_period_s < 0). Protocol spec.vms[].health_check gains both as optional (absent = default). Section 6.3: failed probes within start_period_s after each start do not count; after `failures` consecutive failures outside it the node reports failing, emits app.down and restarts the VM in place with the crash backoff (1 s doubling to 5 min, reset after 10 min healthy); a stateful VM restarts only under 6.4; restart_generation (human restarts) is not changed. The edge's probes (6.8) use only path, interval_s, timeout_s and failures.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0193 in the project decision ledger. Do not edit by hand; changes go through a new decision.
