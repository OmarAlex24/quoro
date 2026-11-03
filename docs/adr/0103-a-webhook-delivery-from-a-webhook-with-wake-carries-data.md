# ADR 0103: A webhook delivery from a webhook with wake carries data.wake = {agent_id, grant_id} in the incident.opened payload (additive, schema_version 1)

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0103
- **Tags:** contracts, agents, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-03. Accepted option: the runner learns which agent to act as and which incident grant to read from the delivery itself.

## Decision

A webhook delivery from a webhook with wake carries data.wake = {agent_id, grant_id} in the incident.opened payload (additive, schema_version 1).

events.schema.json data_incident_opened gains optional wake {agent_id (agt_), grant_id (grt_)}, present only on deliveries of a webhook with wake. Standalone contract PR, then the dispatcher fills it and the reference runner prefers data.wake.agent_id, falling back to its configured default agent.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0103 in the project decision ledger. Do not edit by hand; changes go through a new decision.
