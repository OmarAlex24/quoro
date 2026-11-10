# ADR 0319: Sandboxes: the baseline choices for the first sandbox milestone (S1)

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0319
- **Tags:** sandboxes, s1, design, agents, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The sandbox design left fifteen questions for the maintainer, who answered them on 2026-11-10. They set the base of the design; each contract or security change still gets its own decision and standalone contract PR when the milestone opens.

## Decision

Sandboxes: the baseline choices for the first sandbox milestone (S1).

Users and order. Agents first, people right after. A CI runner stays deferred, as the roadmap says.

Resource. A sandbox is a standalone VM with expiry fields, not a new resource. Defaults: it lives 24 hours, can be extended to 7 days, and stops after 30 minutes idle keeping its disk; at expiry the system stops and destroys it and records that in the audit log.

Start. A reflink clone of a prepared workspace disk plus a cold boot is the only start path in S1. The workspace disk lives in the volume pool so it can be cloned; that is new engine work.

Profiles. Setup commands are written by hand in S1; detection comes later. The checkout is always at /workspace/`<repo>`. S1 handles public repositories only; private ones follow the credential references planned for M7.

Cache. Inside Quoro, S1 ships only the golden disk per repository, built from the default branch, keeping the current and the previous one. A hosted ci-cache over normal egress is allowed next to it, with its branch-binding gap stated as open. ci-cache as a catalog app comes in a later phase, after ci-cache derives the tier from a token that Quoro injects, so the agent never holds a key.

Security. An agent runs commands and moves files without approval only on sandboxes it created, bounded by quotas per identity and with every call audited. Exec into apps and other VMs keeps the approval rule. Egress is open by default with a rate cap and an outbound port policy; an allow-list is an option per profile. A sandbox reaches neither the host nor any other VM.

Surface. The MCP budget rises from 40 to 41 tools: reuse create and delete of VMs, add exec and file transfer.

Placement. Sandboxes run only on nodes labelled for them, with a cap on sandbox memory per node; a one-node install labels its only node.

Timing. S1 is its own milestone. It starts when M5b closes and runs in parallel with M6.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0319 in the project decision ledger. Do not edit by hand; changes go through a new decision.
