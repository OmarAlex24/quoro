# ADR 0049: Agent runner (D-0032 wake): reference runner in examples/agent-runner/, owned by the mcp workstream, outside the quoro binary

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0049
- **Tags:** agents, mcp, events
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0032 says a webhook starts an agent (e.g. Claude Agent SDK) with Quoro's MCP and incident context, but no workstream owns the receiver and no document specifies it; the M3 demo cannot be built without it.

## Decision

Agent runner (D-0032 wake): reference runner in examples/agent-runner/, owned by the mcp workstream, outside the quoro binary.

Options: (a) reference runner shipped as an example (examples/agent-runner/), not in the quoro binary; owned by the mcp workstream; one M3 DoD box. (b) built into the quoro binary: Quoro would hold LLM-provider credentials and an agent runtime on infrastructure nodes, a larger attack surface and more dependencies (D-0028 'every dependency justified'). (c) leave it to users: the M3 demo has no runner. Runner contract for (a): it receives a webhook delivery and verifies Quoro-Signature (contracts README, Request headers); reads the event and WebhookWake.agent_id plus incident_id from the payload; holds that agent's own agent key (akey_), configured by the operator at the runner, because Quoro never sends credentials in a webhook; mints a 5-minute access token via POST /api/v1/auth/agent-token; acts through Quoro's MCP endpoint within the incident grant the webhook created, passing incident_id and justification on every mutating tool. Accepted option: (a).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0049 in the project decision ledger. Do not edit by hand; changes go through a new decision.
