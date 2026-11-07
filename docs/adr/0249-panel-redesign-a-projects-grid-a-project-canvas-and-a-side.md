# ADR 0249: Panel redesign: a projects grid, a project canvas and a side sheet, drawn with a two-tone dither visual system

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0249
- **Tags:** panel, ux, process
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Approved on 2026-11-06 after a second version of the mockup. The open points were settled the same day: the panel draws its own charts, the theme follows the system, the cobalt accent and the fonts stay, and the work runs as a milestone parallel to the main roadmap, like the CLI track.

## Decision

Panel redesign: a projects grid, a project canvas and a side sheet, drawn with a two-tone dither visual system.

Shape: three levels (Projects grid, project canvas with apps and standalone VMs as cards, side sheet with tabs Deployments, Variables, Metrics, Logs, Storage, Security, Settings). Sidebar: Projects, Nodes, Agents, Approvals, Activity, Settings. Every old URL redirects to its new place.

Visual system: dark and light tokens designed separately; the theme follows the system, with a selector in the avatar menu stored per browser. Cobalt accent stays, cyan (ion) added as the second dither tone; kind colours for Git, image, template and VM, never used for state. Archivo for text, Martian Mono (self-hosted, CSP font-src 'self') for code values only. Ordered Bayer 8x8 dither (two-tone) for quantities, a dithered aura on what needs a person, motion only for deploy sweep and traffic. Humans round avatars, agents square dithered ones.

Data: the frozen contract only (ADR 0048). The canvas draws no connection lines until two standalone contract changes land: environment variable references between apps (#502) and observed traffic between apps on a node (#503). Card positions in localStorage.

Delivery: the GitHub milestone 'Panel — Railway-style redesign', one issue per PR, eight stacked PRs. Evidence per PR: mock-mode screenshots in both themes and the unit tests; the milestone closes with a demo against a real cluster. Charts: the panel's own canvas charts (ADR 0253).

_Some wording was changed for the public repository._ _Some operational details were redacted for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0249 in the project decision ledger. Do not edit by hand; changes go through a new decision.
