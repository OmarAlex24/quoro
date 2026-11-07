# ADR 0241: A lease converted from local to expiring opens no failover gate until a renewal is answered, and the node fences from its first renewal

- **Status:** Accepted; refined by [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md), [ADR 0254](0254-a-converted-lease-s-first-gate-counts-the-conversion-grant.md), [ADR 0256](0256-an-expiring-grant-that-carries-re-never-converts-a-lease.md), [ADR 0258](0258-a-primary-changes-a-lease-s-mode-only-with-the-same-token.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0241
- **Tags:** m5b, leases, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0218](0218-changing-ha-policy-changes-the-lease-mode-with-a-lease.md), [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md), [ADR 0254](0254-a-converted-lease-s-first-gate-counts-the-conversion-grant.md), [ADR 0256](0256-an-expiring-grant-that-carries-re-never-converts-a-lease.md), [ADR 0258](0258-a-primary-changes-a-lease-s-mode-only-with-the-same-token.md)

## Context

A safety review of the renewal loop found this. ADR 0218 (section 8.4 'Changing the mode') makes a same-token lease_grant switch a held local lease to expiring. The VMs already run, but the conversion grant answers no renewal, so per section 8.4 it sets no valid_until. The primary, per 'What the authority must enforce', counts every grant it sent the holder toward the gate, the conversion grant included: gate = T_conv + D + 2M. Now take a partition right after the conversion. The node has running VMs and no deadline. The primary opens the gate and fails over. Result: double run. Nor can the node fence from the conversion grant's receipt time: D-0072 removed exactly that assumption about delivery time. A second gap: if the session drops before the first answer, the lease is uncertain with no valid_until. The node then stops the VMs, which contradicts 'the app runs on'.

## Decision

A lease converted from local to expiring opens no failover gate until a renewal is answered, and the node fences from its first renewal.

It completes ADR 0218, in four parts:
(1) Primary: the conversion grant counts toward no gate. A converted lease's gate opens only from grants that answer a renewal (T_grant >= that renewal's t_send). Until the first such answer the lease is local for failover: the source must be connected or declared lost (failover answers lease_held).
(2) Node: from the conversion grant it renews at once. Until a real valid_until exists, it keeps a provisional deadline: the first renewal's t_send + D - M (the earliest t_send, so the earliest possible gate). It self-fences at that deadline unless an answer sets a real valid_until first. Any answer sets a valid_until no earlier than that, so the deadline only moves later on proof.
(3) Node: before its first renewal leaves, a converted lease behaves as local. A lost session neither makes it uncertain nor stops its VMs. With no renewal sent, the primary has no gate, so the app runs on as ADR 0218 says.
(4) The node accepts a same-token grant whose mode differs from the held lease's. Today it refuses one with bad_message, which leaves a converted lease local on the node and expiring on the primary.
No wire change.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0218](0218-changing-ha-policy-changes-the-lease-mode-with-a-lease.md): what the grant that converts a lease to expiring sets on the node and counts toward the failover gate. The rest of [ADR 0218](0218-changing-ha-policy-changes-the-lease-mode-with-a-lease.md) stands.
- [ADR 0244](0244-a-converted-lease-s-row-keeps-mode-local-until-the-primary.md) refines what the lease row records during a conversion: mode stays local until the primary answers the first renewal. The rest of this decision stands.
- [ADR 0254](0254-a-converted-lease-s-first-gate-counts-the-conversion-grant.md) refines the gate that the first answer after a conversion opens. The rest of this decision stands.
- [ADR 0256](0256-an-expiring-grant-that-carries-re-never-converts-a-lease.md) refines which same-token grant converts a lease: one without re. The rest of this decision stands.
- [ADR 0258](0258-a-primary-changes-a-lease-s-mode-only-with-the-same-token.md) refines which grants may change a lease's mode: only the same-token conversion. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0241 in the project decision ledger. Do not edit by hand; changes go through a new decision.
