# ADR 0200: Voter-set changes go one voter at a time, versioned by change-log seq, and the next waits for a majority to store the current one

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0200
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0169's majority argument needs the candidate's majority and the primary's to share a voter; that fails when the voter set changes by more than one voter between them.

## Decision

Voter-set changes go one voter at a time, versioned by change-log seq, and the next waits for a majority to store the current one.

spec.votes.version = the change-log seq of the entry that last changed the voter set (enrollment of a node or witness machine, a roles change that adds or removes a vote, removal of a voter), so it needs no column and a promoted primary inherits it. Rules: (1) one voter per change; (2) the next change only after a majority of current voters acknowledged a desired_state carrying the current version; until then updateNodeRoles and removeNode answer 409 conflict and enrollNode of a voter answers 409 without consuming the token; (3) the primary counts support only from support_grants carrying its current version; (4) a voter votes only at its own version (voters_changed, with its voters list). D-0170's 'fewer than three votes while expiring leases exist, unless forced' guard applies to the roles operation only; removeNode is not refused for it (no force parameter on an implemented operation), and the HA view reports the missing vote.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0200 in the project decision ledger. Do not edit by hand; changes go through a new decision.
