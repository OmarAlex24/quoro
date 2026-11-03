# ADR 0102: M3 demo agent runner uses the built-in deterministic policy; an LLM backend (Claude, Anthropic API) stays optional, configured at the runner with the operator's own key, never required by the DoD

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0102
- **Tags:** agents, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-03. The deterministic policy already passed the runner end to end on the test lab, needs no key and costs nothing; an LLM backend is a later choice.

## Decision

M3 demo agent runner uses the built-in deterministic policy; an LLM backend (Claude, Anthropic API) stays optional, configured at the runner with the operator's own key, never required by the DoD.

The reference runner keeps the pluggable decision step from #118. The M3 demo and DoD run on the deterministic policy. A Claude backend may ship as an option behind the interface; Quoro never stores the LLM key; the operator configures it at the runner.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0102 in the project decision ledger. Do not edit by hand; changes go through a new decision.
