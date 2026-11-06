# ADR 0234: After a promotion the new primary sends each node a desired_state rev above the node's stored rev

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0234
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

A node ignores a desired_state whose rev is not above its stored one (node-agent-protocol 6.1). The new primary's nodes.desired_rev can be below what the old primary last sent if the candidate's log lags.

## Decision

After a promotion the new primary sends each node a desired_state rev above the node's stored rev.

The new primary sends each node a desired_state whose rev is above both its own nodes.desired_rev and the stored_rev in the node's hello. No node change is needed. Rejected: letting a higher term override rev on the node, which changes the node's acceptance rule.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0234 in the project decision ledger. Do not edit by hand; changes go through a new decision.
