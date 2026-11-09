# ADR 0179: Change-log follow (#74): every machine with role node follows and verifies the change log; a promotion replays it into a fresh database; followers do not serve reads in M5b

- **Status:** Accepted; refined by [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0179
- **Tags:** m5b, node-agent, control-plane, sync
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md)

## Context

Promotion needs a verified copy of state on the candidate. Today the node discards log_entries (crates/node-agent/src/session.rs:865) and the primary never sends them or reads log_ack (crates/control-plane/src/session/mod.rs:1733). Audit records travel in the log (audit-log-format 3.2), so follow also carries the audit chain.

## Decision

Change-log follow (#74): every machine with role node follows and verifies the change log; a promotion replays it into a fresh database; followers do not serve reads in M5b.

Primary: after desired_state, send log_entries from the node's acked seq (up to 500 per message), then continuously; track nodes.log_seq from log_ack; show each node's lag in the API and the HA view. Node: verify hash and prev_hash, store, ack; on log_diverged quarantine the copy and refollow (section 7, unchanged). Edge-only and witness-only machines do not follow. Promotion replays the stored log into a new quoro.db (a lab spike measures how long that takes); LOCAL tables (browser sessions, access tokens, webhook delivery cursors, the vulnerability cache) start empty, so users log in again and the cache refetches. A live replica database and read serving (cp_status replica) stay for M6. The maintainer accepted this on 2026-11-06.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) refines durable acknowledgements, the high-water mark, the refollowing state, and acknowledgements reset on a divergence. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0179 in the project decision ledger. Do not edit by hand; changes go through a new decision.
