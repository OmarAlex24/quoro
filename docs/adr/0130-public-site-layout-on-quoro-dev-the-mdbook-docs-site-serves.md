# ADR 0130: Public site layout on quoro.dev: the mdBook docs site serves quoro.dev from GitHub Pages at the v0.1.0 release; the installer gets a short URL quoro.dev/install.sh later

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0130
- **Tags:** docs, release
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0109 publishes the docs site with v0.1.0; the domain now exists. One site keeps it simple until M4 adds a landing page.

## Decision

Public site layout on quoro.dev: the mdBook docs site serves quoro.dev from GitHub Pages at the v0.1.0 release; the installer gets a short URL quoro.dev/install.sh later.

At v0.1.0: a Pages deploy job runs on the published release (never on push), builds docs/ with the pinned mdBook, and serves it at quoro.dev (CNAME, Cloudflare DNS record pointing at GitHub Pages, HTTPS enforced). The install one-liner keeps the raw GitHub URL until a stable path exists on the site. A landing page and docs.quoro.dev split can come with the M4 general release.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0130 in the project decision ledger. Do not edit by hand; changes go through a new decision.
