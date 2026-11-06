# ADR 0243: Both stop modes carry a caller-supplied bound, and a forced stop the engine began finishes after a dropped call

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0243
- **Tags:** m5b, leases, engine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A self-fence must finish within T_grace + T_force <= M (node-agent-protocol 8.5), with M as low as 1000 ms. StopMode::Force has no bound today and the Cloud Hypervisor backend waits about 5 s before SIGKILL; Graceful's fallback is itself a forced stop, so it leaks the fence budget the same way. When the fence drops the stop future, the kill is lost (#434). The bound #434 needs on Force goes into the contract change for ADR 0242 rather than a second one.

## Decision

Both stop modes carry a caller-supplied bound, and a forced stop the engine began finishes after a dropped call.

Written into the contract change for ADR 0242 (#435): StopMode::Force { within } and StopMode::Graceful { timeout, force_within }. Force escalates to SIGKILL and reaps inside within, returns Ok only when the VMM process is gone, and Timeout when it is not gone once within has passed (VM may still run). Graceful returns within timeout + force_within. A forced stop the engine began, from stop or from a start that passed its bound, goes on to the kill and the reap in an engine task after the call returned Timeout or its future was dropped; a later stop of the same VM joins it within its own bound. The self-fence calls Graceful { timeout: T_grace, force_within: T_force }. Alternative: keep StopMode as is and fix #434 inside the engine with internal timeouts below the smallest T_force; rejected because M scales with D, and the caller is the one that knows the fence budget. It goes beyond ADR 0242, which bounds only start.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0243 in the project decision ledger. Do not edit by hand; changes go through a new decision.
