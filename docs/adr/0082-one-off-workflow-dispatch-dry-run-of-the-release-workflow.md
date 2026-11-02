# ADR 0082: One-off workflow_dispatch dry run of the release workflow: it attests and uploads workflow artifacts only, with no tag and no release

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0082
- **Tags:** installer, security, release
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-02. The keyless verify path (ADR 0081) cannot be exercised end to end before a tagged run; a dry run proves install-time verification before the first tag.

## Decision

One-off workflow_dispatch dry run of the release workflow: it attests and uploads workflow artifacts only, with no tag and no release.

Run once from main after the M1 integration epic lands. It creates a public Rekor transparency-log entry bound to the workflow identity; that is expected. Tags and releases still need the maintainer's explicit OK.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0082 in the project decision ledger. Do not edit by hand; changes go through a new decision.
