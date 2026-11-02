# ADR 0042: Repo AGENTS.md must carry the maintainer's working rules, because non-Claude agents never see the maintainer's CLAUDE.md

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0042
- **Tags:** process, agents-md
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Non-Claude implementers do not receive the maintainer's CLAUDE.md; only Claude agents do.

## Decision

Repo AGENTS.md must carry the maintainer's working rules, because non-Claude agents never see the maintainer's CLAUDE.md.

Include at least: verify by running it before calling anything done; search before creating files/components; commit format `<type>: <description>`; only checked boxes in PR test plans; print target environment before any DB write; visual work avoids the generic look (no cream backgrounds, no italic words in headlines, no 01/02/03 or monospace labels, no pill buttons) and is checked rendered; ambiguous or destructive actions are confirmed first.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0042 in the project decision ledger. Do not edit by hand; changes go through a new decision.
