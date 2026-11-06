# ADR 0240: Node measures expiring-lease validity on CLOCK_BOOTTIME, so a host suspend shortens it

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0240
- **Tags:** m5b, leases, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A safety review of the renewal loop (#75) found this. Section 8.4 puts every lease time on 'the node's monotonic clock'. On Linux CLOCK_MONOTONIC stops during a host suspend, but the primary's gate keeps running. A node suspended for longer than 3M resumes with almost D - M of validity left, while the gate (T_grant + D + 2M) passed during the suspend and another node may now run the app. With the session dead, nothing revokes the resumed VMs for up to D - M. The same gap reaches the stored deadline: it is converted to CLOCK_BOOTTIME when the answer is handled, so a suspend between a renewal and its answer stores a deadline that is later by the suspend. Section 8.4 says 'a node whose clock steps by more than M must be treated as a failed node', but nothing says how the node notices.

## Decision

Node measures expiring-lease validity on CLOCK_BOOTTIME, so a host suspend shortens it.

In three parts:
(1) Section 8.4 'Definitions': the node's times (t_send, valid_until, the renewal interval) are measured on a clock that counts suspend: CLOCK_BOOTTIME on Linux. The node takes t_send on that clock when it hands the renewal to the session.
(2) The node checks, at least every M/4 and at once on any wake-up, how far CLOCK_BOOTTIME has moved past its monotonic timers. When it has, every expiring lease's valid_until is brought earlier by that amount, so a lease whose deadline passed during the suspend expires and self-fences at once.
(3) The stored deadline (section 8.4 'Agent restart') is CLOCK_BOOTTIME(t_send) + D - M, taken when the renewal is sent, not when its answer arrives.
No wire change.
Why not 'mark the node failed' instead: a suspend is detected precisely, so fencing what passed its deadline is enough, and a failed mark would need a human acknowledgement for a laptop lid.
The renewal loop (#75) documents the gap without closing it. Nothing is exposed today: the primary grants no expiring lease until warm apps ship, or with the dev-only feature of ADR 0223. This needs to land before the test-lab runs of that feature.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0240 in the project decision ledger. Do not edit by hand; changes go through a new decision.
