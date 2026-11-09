# ADR 0198: The candidate waits for each voter's lease horizon, which covers the longest lease the old primary declared (max_lease_ms), plus a vote pre-check (refines ADR 0169)

- **Status:** Accepted; refined by [ADR 0305](0305-section-8-8-restarts-after-a-reboot-a-voter-restarts-its.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0198
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0169](0169-m5b-lease-authority-the-primary-may-grant-or-renew-expiring.md), [ADR 0305](0305-section-8-8-restarts-after-a-reboot-a-voter-restarts-its.md)

## Context

D-0169's wait of S + D + 2M is unsafe once D-0171's maintenance window lets a grant carry W up to 600 s; the voters must know the longest duration the primary may grant.

## Decision

The candidate waits for each voter's lease horizon, which covers the longest lease the old primary declared (max_lease_ms), plus a vote pre-check (refines ADR 0169).

Each support_request declares L = max_lease_ms (D, or W during a maintenance window); the primary may send an expiring grant of duration d only while a majority supports it with L >= d. The voter keeps a lease horizon v_recv + S + L + 2*max(1000, L/10), never lowered within a term, and its vote answer carries lease_wait_ms = horizon - now; the candidate waits until the latest receipt time + lease_wait_ms over its majority (unless the fence is acknowledged, D-0182). Defaults give 33 s (the D-0169 bound), 231 s with W = 180 s. Vote request: cluster_id, term, candidate_node_id, voters_version, optional check (true = answer without changing anything, so a promotion that cannot win moves no voter). Answer: node_id, term, vote granted|refused, reason stale_term|support_held|voters_changed|not_allowed, retry_after_ms, lease_wait_ms, voters_version, voters. A voter grants only after its promise ran out, or when the candidate is the primary it supports; a repeated grant of the same term to the same candidate is harmless. After a host reboot the voter restarts its promise and horizon from its first start (later, so safe). The proof sketch is in node-agent-protocol 8.9.

## Consequences

- Refines [ADR 0169](0169-m5b-lease-authority-the-primary-may-grant-or-renew-expiring.md): the candidate's wait before it starts at the new term (each voter's lease horizon instead of S + D + 2M). The rest of [ADR 0169](0169-m5b-lease-authority-the-primary-may-grant-or-renew-expiring.md) stands.
- [ADR 0305](0305-section-8-8-restarts-after-a-reboot-a-voter-restarts-its.md) refines where a voter's promise and lease horizon start again after a reboot: from its first start, each for the longer of the rule's span and what was left of it at the last grant. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0198 in the project decision ledger. Do not edit by hand; changes go through a new decision.
