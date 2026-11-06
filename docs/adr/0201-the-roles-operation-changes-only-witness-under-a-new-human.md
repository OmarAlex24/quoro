# ADR 0201: The roles operation changes only witness, under a new human-only scope node:roles; witness-only machines run no engine

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0201
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0170 defines POST /nodes/{id}/roles and spec.witness but names no scope, no request shape beyond add|remove, and no behaviour of a witness-only machine.

## Decision

The roles operation changes only witness, under a new human-only scope node:roles; witness-only machines run no engine.

Scope node:roles, human-only (operators and owners hold it). NodeRolesRequest {add?: [witness], remove?: [witness], force?: bool}: exactly one of add/remove, else 422; a change that leaves no role is 422; a removed node is 409; adding a role the machine has or removing one it lacks answers 200 and changes nothing; force only lifts the fewer-than-three-voters guard. witness may be added to a node machine but adds no vote (one machine, one vote). Desired state: spec.witness = {} on a machine with the witness role, spec.votes {version, port, voters[{node_id, roles, mesh_address}]} on every voter. A witness-only machine runs the node agent with no engine and no KVM, gets empty vms/routes, no network, spec.mesh with its peers, and a mesh address at enrollment or when it gains the role. Operation id updateNodeRoles, audited; no new event.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0201 in the project decision ledger. Do not edit by hand; changes go through a new decision.
