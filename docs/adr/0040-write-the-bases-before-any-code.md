# ADR 0040: Write the bases before any code

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0040
- **Tags:** process, docs
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

So no point is lost before parallel work starts.

## Decision

Write the bases before any code.

Documents: decision ledger (written from the design conversation; the maintainer reviews), PRD v2, one ADR per decision, architecture (roles, control vs data plane, flows), contracts (OpenAPI, event schema, node-agent protocol, audit-log format, permission model, Engine trait), threat model, roadmap with verifiable DoD per milestone, one brief per workstream (owned paths, contracts consumed, DoD, forbidden actions), glossary, OSS files (LICENSE AGPL, CONTRIBUTING + CLA, SECURITY.md, CoC), repo AGENTS.md/CLAUDE.md. Every 'to verify' claim is researched before entering a document; unconfirmed ones are marked as hypotheses. Pipeline: verification -> parallel drafts -> consistency and completeness reviews -> maintainer review -> first docs PR.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0040 in the project decision ledger. Do not edit by hand; changes go through a new decision.
