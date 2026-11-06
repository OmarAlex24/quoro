# ADR 0205: Cluster.votes keeps exactly the fields of ADR 0170; a lost node stays a voter; ha_ready also needs majority support

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0205
- **Tags:** m5b, contracts, leases
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0170 lists the fields but not the voter order, what connected means, whether lost or disconnected machines count, or exactly when ha_ready is true.

## Decision

Cluster.votes keeps exactly the fields of ADR 0170; a lost node stays a voter; ha_ready also needs majority support.

Cluster.votes (required) = ClusterVotes {voters: [ClusterVoter {node_id, roles, connected}], count, required, ha_ready}. Voters: every machine with role node or witness whose status is not removed, oldest enrolled first; connected = its node-agent session is up; a node declared lost stays a voter until removed. required is 3 in M5b (described, not a const). ha_ready = count >= required and the primary holds majority support right now; a primary that does not track support yet (before the lease work ships) reports false. No MCP tool is added: agents read votes through get_cluster and the lease through get_app. Audit: lease.grant and lease.revoke system records extend to expiring grants with a new token and to declared_lost revokes; support and votes write no record.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0205 in the project decision ledger. Do not edit by hand; changes go through a new decision.
