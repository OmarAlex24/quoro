# ADR 0133: Keep an app's last failed or cancelled build in spec.builds so the node keeps its log

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0133
- **Tags:** builds, m4, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

node-agent-protocol.md 6.6 keeps an app's build log and cache disk only while the app appears in spec.vms or spec.builds, and 6.6 line 682 removes a build from spec.builds once its terminal status is recorded. A Git app whose build failed has no VM, so on the lab the failed build's log was deleted seconds after the failure (GET /builds/{id}/logs answered 404). That breaks the M4 DoD 'a failing build shows the failing step and its log in the panel'.

## Decision

Keep an app's last failed or cancelled build in spec.builds so the node keeps its log.

Implemented in #173 without changing contract text: line 682 only gives a lower bound for how long a build stays and an exit rule for succeeded builds. The primary keeps, per app, the newest build that reached the node (started_at set) when it ended failed or cancelled. That entry is sent marked cancelled and without secrets, so the node never runs it again, even after losing its row. The primary ignores reports about it, because it is terminal. It leaves when a newer build of the app starts (the primary re-sends the desired state on a build's first start), or when the app is deleted. Builds that failed on the primary, or were cancelled before starting, are not kept: they have no log on the node. Needs a decision: accept this reading, or tighten 6.6 wording in a separate contract PR.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0133 in the project decision ledger. Do not edit by hand; changes go through a new decision.
