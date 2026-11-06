# ADR 0202: Declare-lost sets nodes.lost_at (not a new status), is refused for a connected node, and leaves fence_failure and the gate in place

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0202
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0183 says 'mark the node lost (it stays enrolled)' without saying how lost is stored, whether a connected node can be declared lost, or how it interacts with failed fences and expiring gates.

## Decision

Declare-lost sets nodes.lost_at (not a new status), is refused for a connected node, and leaves fence_failure and the gate in place.

Migration 0010 adds nodes.lost_at (nullable); Node.lost_at is required and nullable. A new Node.status value would need a nodes rebuild and changes the session lifecycle enum, so a timestamp is used. declareNodeLost: no body; 200 with the node; 409 while the node is connected (revoke over the session instead) or removed; a repeat on a lost node answers 200 and changes nothing. In one transaction: every lease of the node gets token + 1 and revoked_at, the node stays the last holder; lost_at is set; audit record declareNodeLost. It never clears fence_failure (leases listed there stay ungrantable until fence-ack, so the operator never covers a failure unseen), and an expiring lease still waits for its gate. A lost node stays enrolled and a voter until removed. On its next hello its leases are revoked with reason declared_lost, and lost_at is cleared once those revokes are answered. No new event.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0202 in the project decision ledger. Do not edit by hand; changes go through a new decision.
