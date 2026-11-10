# ADR 0321: Four lease and voter readings: every waiting grant is marked, the token cap answers 409, the vote-removal guard counts only live expiring leases, and a stuck voter gate is repaired by removing the machine

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0321
- **Tags:** m5b, leases, voters, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0170](0170-witness-role-any-enrolled-machine-can-add-the-witness-role.md), [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md), [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md)

## Context

Work on held grants and on roles found four places where the contract text and the code did not say the same thing. Each was filed as an issue with the safe reading as its proposal (issues 766 to 769). The maintainer accepted the four safe readings on 2026-11-10. None of them lets an app run twice.

## Decision

Four lease and voter readings: every waiting grant is marked, the token cap answers 409, the vote-removal guard counts only live expiring leases, and a stuck voter gate is repaired by removing the machine.

1. The unsettled mark (issue 766). Every grant that must wait is written marked, whoever makes it: a failover's raise, a regrant, a restore, createVolume. The code already does this; the contract gets a line that says so. A marked row moves only as a local lease does, so marking more rows can delay a move and never lets an app run twice.

2. The token cap (issue 767). A lease row at the highest token the change log carries stays revoked and cannot be granted again. A fence acknowledgement that would raise it writes nothing and answers 409 conflict with a reason naming the exhausted token, not an internal error. Recovery stays a human action.

3. The vote-removal guard (issue 768). The guard that refuses removing a vote below three voters while an expiring lease exists counts an expiring row only while it is not revoked, or while it is revoked and its gate has not passed. A revoked row whose gate has passed does not count. Before this, one warm app in the cluster's history made every later removal need force; that was a defect. Force still bypasses only this guard, never the voter-set gate.

4. A voter-set gate that cannot clear (issue 769). The gate keeps refusing every voter change while the nodes rows and the change log disagree on a machine. The repair is removing that machine with removeNode: its removal entry ends the disagreement and the gate lets that one change through. There is no other bypass and no force for the gate.

Contract lines for 1, 3 and 4 go in standalone contract PRs; 2 and 3 need code changes; 4 depends on removeNode being served.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0170](0170-witness-role-any-enrolled-machine-can-add-the-witness-role.md): which `expiring` leases the vote-removal guard counts: a row that is not revoked, or revoked with its gate not yet passed. The rest of [ADR 0170](0170-witness-role-any-enrolled-machine-can-add-the-witness-role.md) stands.
- Refines [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md): which grants are written marked: every grant that must wait, whoever makes it. The rest of [ADR 0280](0280-every-grant-of-a-marked-lease-waits-until-every-node-holds.md) stands.
- Refines [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md): what happens at the token cap: the row stays revoked, and a fence acknowledgement that would raise it answers `409` and writes nothing. The rest of [ADR 0285](0285-a-volume-restore-that-turns-an-expiring-lease-row-local-at.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0321 in the project decision ledger. Do not edit by hand; changes go through a new decision.
