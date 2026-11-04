# ADR 0137: v0.1.0 ships without the trademark search, from main after the M4 demo approval, still labeled an experimental preview (it now contains M4); the first general release is a later tag after the M4 close-out

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0137
- **Tags:** release, product
- **Supersedes:** [ADR 0064](0064-first-public-release-is-an-m3-preview-v0-1-experimental-not.md)
- **Superseded by:** none
- **Related:** none

## Context

The maintainer waived the trademark search on 2026-11-04, after approving the v0.1.0 tag. The tag was planned at the M3 commit only because main carried unproven M4 code; M4 is now proven on the lab and approved, and issue #149 tags main.

## Decision

v0.1.0 ships without the trademark search, from main after the M4 demo approval, still labeled an experimental preview (it now contains M4); the first general release is a later tag after the M4 close-out.

Supersedes D-0064 only in its precondition (domain and trademark checks before the M3 tag) and its target (M3 end): the domain is registered (quoro.dev); the trademark search is waived by the maintainer, with the residual risk that another software product uses the name (quoro.tech) and a rename might be needed later. Unchanged: v0.1.0 is experimental, not for production, with signed builds (ADR 0027, D-0081); the first general release and the broad announcement come with M4 complete, as a later tag whose number is the maintainer's call.

_Some wording was changed for the public repository._

## Consequences

- Replaces ADR 0064.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0137 in the project decision ledger. Do not edit by hand; changes go through a new decision.
