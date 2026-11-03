# ADR 0109: Docs site: mdBook built from the repo's Markdown, published to GitHub Pages together with the v0.1.0 tag

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0109
- **Tags:** docs, m3
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-03. D-0065 asked to pick the tooling during M3. mdBook is Rust-native, static and needs no runtime service.

## Decision

Docs site: mdBook built from the repo's Markdown, published to GitHub Pages together with the v0.1.0 tag.

Build the book in CI from docs/ without moving the Markdown; publishing the site happens only with the v0.1.0 release approval.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0109 in the project decision ledger. Do not edit by hand; changes go through a new decision.
