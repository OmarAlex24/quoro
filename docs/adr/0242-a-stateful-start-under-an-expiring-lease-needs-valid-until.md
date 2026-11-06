# ADR 0242: A stateful start under an expiring lease needs valid_until - now >= start bound + M, and the engine's start takes a caller-supplied bound (3 s for expiring leases)

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0242
- **Tags:** m5b, leases, engine, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A start that takes its permit just before valid_until could hold the fence past the gate and run an app twice under a partition. A renewed lease always has about 8.5 s left, so the check costs nothing in steady state.

## Decision

A stateful start under an expiring lease needs valid_until - now >= start bound + M, and the engine's start takes a caller-supplied bound (3 s for expiring leases).

Node: start_permit for an expiring lease refuses a start unless valid_until - now >= start bound + M. Engine: start takes a caller-supplied bound and gives up (stopping what it started) when it passes; the node passes 3 s for stateful starts under expiring leases. The engine-trait change is a contract change; the engine work goes with issue #434 (forced stop bound and cancellation safety).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0242 in the project decision ledger. Do not edit by hand; changes go through a new decision.
