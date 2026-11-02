# ADR 0047: Leases have two modes: local (HA none/cold, never expire on control-plane loss) and expiring (warm/hot, self-fence on expiry)

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0047
- **Tags:** reliability, node-agent, fencing
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0019 says a node that loses its lease stops its stateful apps and must self-fence from M0. D-0011 and the M2 demo say apps keep running when the control plane is gone. With one control plane (M1-M4), a lease that expires when the control plane is unreachable would stop every stateful app whenever the panel is down.

## Decision

Leases have two modes: local (HA none/cold, never expire on control-plane loss) and expiring (warm/hot, self-fence on expiry).

Every stateful app has a lease from M0, and the node-agent protocol carries lease_grant / lease_renew / lease_revoke and the self-fence path from M0. mode=local: issued for HA policy none or cold, where no other copy can start; it never expires because the arbiter is unreachable; it ends only by explicit lease_revoke (move, delete, planned switchover) or by an operator declaring the node lost (manual fence acknowledgement, required before a cold restore elsewhere). mode=expiring (M5b+): issued for warm/hot; duration-based on the node's monotonic clock with a safety margin; node states held -> uncertain (renewal failing, arbiter unreachable: keep running, start nothing new) -> expired (self-fence: stop stateful VMs) ; revoked = explicit. Fencing tokens are monotonic per lease and carried on every write to shared storage (chunk store, object store, replication stream); receivers reject stale tokens. Stateless apps need no lease. Detailed in docs/contracts/node-agent-protocol.md.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0047 in the project decision ledger. Do not edit by hand; changes go through a new decision.
