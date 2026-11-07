# ADR 0263: The primary measures its support window (p_send) on CLOCK_BOOTTIME

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0263
- **Tags:** m5b, leases, contracts, election
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Found while reviewing the lease design. Section 8.8 defines p_send as 'the primary's monotonic time' without naming the clock; the voter keeps its promise on CLOCK_BOOTTIME and the node its deadlines (ADR 0240). On Linux CLOCK_MONOTONIC stops during a host suspend, so a primary whose host was suspended longer than S still counts support after the voters' promises ran out: it answers a renewal with D after a candidate has won and waited the voters' lease horizon, and the app can run on two nodes. A support window that runs long is the unsafe direction. Issue #527.

## Decision

The primary measures its support window (p_send) on CLOCK_BOOTTIME.

Scenario: (1) primary P holds majority support; its host is suspended for longer than S. (2) The voters' promises run out on CLOCK_BOOTTIME; candidate Q wins a majority, waits the lease horizon (section 8.9) and starts. (3) P wakes; on a clock that stopped, p_send + S - M_S has not passed, so P still counts support. (4) Holder X, which has not seen Q's term, sends P a renewal; P answers with D. (5) The horizon did not cover a grant sent after the promises ran out; Q may have granted the lease elsewhere. Double run until X's lease runs out. The fix: section 8.8 states that the primary reads p_send on CLOCK_BOOTTIME (a clock that counts suspend) and compares against that clock at each expiring grant it sends, as the voter and the node already do (ADR 0240). No other clock changes: the authority's gate (section 8.4) and the candidate's horizon wait (section 8.9) only run long across a suspend, which is safe. Cost: none on the wire; implementation reads a different clock. Issue #527 tracks the contract change.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0263 in the project decision ledger. Do not edit by hand; changes go through a new decision.
