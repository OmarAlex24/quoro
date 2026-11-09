# ADR 0301: Node.log_lag is absent only until change-log follow ships, not until the promotion and custody work

- **Status:** Accepted
- **Date:** 2026-11-09
- **Ledger id:** D-0301
- **Tags:** m5b, contracts, follow
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md)

## Context

The maintainer, 2026-11-09. openapi.yaml says Node.log_lag is absent until the promotion and custody work ships, but change-log follow ships before custody and shows the lag in the API and the panel. The sentence is a contract statement, and contract changes are standalone PRs.

## Decision

Node.log_lag is absent only until change-log follow ships, not until the promotion and custody work.

A one-sentence standalone contract PR changes Node.log_lag's description to say it is absent until change-log follow ships (or drops the clause). The API and panel change that ships change-log follow then fills log_lag. Rejected: filling it with the contract unchanged; waiting for the custody work.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md): when Node.log_lag appears: once change-log follow ships. The rest of [ADR 0227](0227-change-log-follow-sends-one-log-entries-batch-at-a-time-and.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0301 in the project decision ledger. Do not edit by hand; changes go through a new decision.
