# ADR 0218: Changing ha_policy changes the lease mode with a lease_grant at the same token; expiring only with majority support

- **Status:** Accepted; refined by [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0218
- **Tags:** m5b, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md)

## Context

warm uses expiring leases and none/cold use local ones; how an existing app changes mode is open.

## Decision

Changing ha_policy changes the lease mode with a lease_grant at the same token; expiring only with majority support.

When the policy becomes warm the primary sends the holder a lease_grant with mode expiring, the current token and a duration, only while it holds majority support; when it leaves warm, a lease_grant with mode local and the current token. Neither is an ownership change, so the token stays. Until the expiring grant the lease stays local: the app runs on, and a failover needs the source connected or declared lost (failover answers lease_held). Written in node-agent-protocol 8.4 'Changing the mode'.

## Consequences

- [ADR 0241](0241-a-lease-converted-from-local-to-expiring-opens-no-failover.md) refines what the grant that converts a lease to expiring sets on the node and counts toward the failover gate. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0218 in the project decision ledger. Do not edit by hand; changes go through a new decision.
