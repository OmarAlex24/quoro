# ADR 0022: Failback is never automatic or immediate; returning node rejoins as replica and handover is planned

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0022
- **Tags:** reliability, failback
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Node A does not have the data B collected while A was down; A may also hold writes B never received (two histories).

## Decision

Failback is never automatic or immediate; returning node rejoins as replica and handover is planned.

Sequence: (1) A returns fenced, checks lease, starts nothing stateful. (2) A joins as replica of B (B is now source of truth). (3) A's divergent writes are quarantined in a separate snapshot and recorded in the audit log, never silently dropped. (4) Incremental resync from the last common point; A becomes warm. (5) Default: no failback, roles simply swap. (6) If A is preferred: planned switchover ('relevo inteligente'): wait for lag ~0, wait for a low-traffic window (user-chosen or agent-proposed with approval), freeze writes for seconds, final sync, move lease, LB shifts traffic, drain connections. Planned switchover loses no data; data loss of seconds only happens in surprise failures. Stateless: LB slow-starts traffic back to A.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0022 in the project decision ledger. Do not edit by hand; changes go through a new decision.
