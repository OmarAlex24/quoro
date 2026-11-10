# ADR 0170: Witness role: any enrolled machine can add the witness role without re-enrolling; voters are machines with role node or witness; Cluster reports the votes and offers a witness below three

- **Status:** Accepted; refined by [ADR 0321](0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md)
- **Date:** 2026-11-06
- **Ledger id:** D-0170
- **Tags:** m5b, witness, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0321](0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md)

## Context

The witness exists only as an enum value today (contracts README 40-42, enroll roles) and roles are fixed at enrolment. On the test lab the third machine is already enrolled as the edge, so making it the witness needs a role change, not a re-enrolment that would drop its tunnel and mesh.

## Decision

Witness role: any enrolled machine can add the witness role without re-enrolling; voters are machines with role node or witness; Cluster reports the votes and offers a witness below three.

Contract: POST /api/v1/nodes/{node_id}/roles {add|remove: [witness]}, human-only and audited (removing a voter is refused when it would leave fewer than three votes while expiring leases exist, unless forced). The machine learns its roles from desired_state (spec.witness present) and keeps only its vote state in its local store: current term, the candidate it supports and the support deadline; no volumes, no app data. Voters: role node or witness; an edge-only machine does not vote. Cluster gains votes {voters[{node_id, roles, connected}], count, required: 3, ha_ready}. The panel HA view says 'HA needs a third vote' below three and offers a witness: a join token with role witness for a new machine, or the roles action for an enrolled one (D-0018: edge+witness). The maintainer accepted this on 2026-11-06; the milestone's definition of done reads that a machine already enrolled as the edge gains the witness role.

_Some wording was changed for the public repository._

## Consequences

- [ADR 0321](0321-four-lease-and-voter-readings-every-waiting-grant-is-marked.md) refines which `expiring` leases the vote-removal guard counts: a row that is not revoked, or revoked with its gate not yet passed. The rest of this decision stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0170 in the project decision ledger. Do not edit by hand; changes go through a new decision.
