# ADR 0268: A grant longer than a lease's stored duration_ms waits until every node-role machine holds the raise

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0268
- **Tags:** m5b, leases, contracts, warm
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0254](0254-a-converted-lease-s-first-gate-counts-the-conversion-grant.md), [ADR 0171](0171-expiring-lease-defaults-for-warm-d-15-s-m-1-5-s-t-grace-1-s.md)

## Context

Found while checking the design of ADR 0261. The primary raises leases.duration_ms before a conversion grant (ADR 0254) and before a maintenance window's W (section 8.11), but sends the longer grant without waiting for any node to receive the raise. A primary promoted with --ack-fence (ADR 0236) skips the voters' wait and counts each expiring lease's gate from its first start with the stored duration_ms; if it never received the raise it counts D while the holder runs for W (up to 600 s), so an operator's failover after about 18 s can start the app on a second node. Issue #528.

## Decision

A grant longer than a lease's stored duration_ms waits until every node-role machine holds the raise.

Scenario: (1) P opens a maintenance window with W = 180 s, raises duration_ms to W for every expiring lease and answers holder X's renewal of app A with W. (2) P's host fails before node Q receives the raise. (3) The operator promotes Q with --ack-fence; Q counts A's gate as start + D + 2M, about 18 s. (4) After that gate an operator's failover starts A on Y, while X, cut off from Q, runs A until its W grant ends more than two minutes later. The fix: the primary sends a grant longer than the stored duration_ms only once every enrolled, non-removed machine with the node role has acknowledged the raise entry (the replication rule of ADR 0261; only such a machine can be promoted, so every possible new primary counts the longer gate). While a node is unreachable it keeps granting the shorter duration: the maintenance window fails to open, as it already does when no majority supports W, and a conversion keeps the stored duration. Lowering duration_ms needs nothing new (a primary that lacks the lowering waits longer, which is safe). Alternative rejected: a primary promoted with --ack-fence counts every first gate at the 600 s cap; safe with no new rule, but costs every failover after such a promotion about twelve minutes. Cost of the fix: a maintenance window or a conversion at a longer duration waits for every node. Issue #528 tracks the contract change (sections 8.4, 8.11, 7.3). It uses the replication rule of ADR 0261.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0254](0254-a-converted-lease-s-first-gate-counts-the-conversion-grant.md): when a conversion grant longer than the stored duration_ms is sent: only once the raise before it is replicated. The rest of [ADR 0254](0254-a-converted-lease-s-first-gate-counts-the-conversion-grant.md) stands.
- Refines [ADR 0171](0171-expiring-lease-defaults-for-warm-d-15-s-m-1-5-s-t-grace-1-s.md): when a maintenance window's W is answered: only once the raise of duration_ms to W is replicated. The rest of [ADR 0171](0171-expiring-lease-defaults-for-warm-d-15-s-m-1-5-s-t-grace-1-s.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0268 in the project decision ledger. Do not edit by hand; changes go through a new decision.
