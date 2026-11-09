# ADR 0227: Change-log follow sends one log_entries batch at a time and keeps each node's acknowledged seq in memory; Node.log_lag shows the gap

- **Status:** Accepted; refined by [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md), [ADR 0301](0301-node-log-lag-is-absent-only-until-change-log-follow-ships.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0227
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md), [ADR 0301](0301-node-log-lag-is-absent-only-until-change-log-follow-ships.md)

## Context

ADR 0179 says the primary tracks nodes.log_seq from log_ack, but every write to the nodes table is itself a change-log entry that every follower must then receive and acknowledge, so writing each acknowledgement would never settle. Flow control and how a node learns its lag are open.

## Decision

Change-log follow sends one log_entries batch at a time and keeps each node's acknowledged seq in memory; Node.log_lag shows the gap.

The primary keeps at most one log_entries message per session unacknowledged (up to 500 entries); the node acknowledges each message once, with its highest entry. log_entries gains head_seq, the primary's newest seq. The primary keeps each node's newest ack in memory and reports it as Node.log_seq, with Node.log_lag (head minus log_seq, null for a machine that does not follow); it writes nodes.log_seq only together with the writes it already makes when a session starts and ends. log_diverged is also sent when the node's log_seq is above the primary's head, the case of a node that followed an older primary further than the promoted one. A message whose entries do not verify is answered with bad_message and resent from the last ack.

## Consequences

- [ADR 0261](0261-every-node-holds-a-lease-row-s-local-write-before-an.md) refines durable acknowledgements, the high-water mark, the refollowing state, and acknowledgements reset on a divergence. The rest of this decision stands.
- [ADR 0301](0301-node-log-lag-is-absent-only-until-change-log-follow-ships.md) refines when Node.log_lag appears: once change-log follow ships. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0227 in the project decision ledger. Do not edit by hand; changes go through a new decision.
