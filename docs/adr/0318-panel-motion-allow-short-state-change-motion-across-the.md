# ADR 0318: Panel motion: allow short state-change motion across the panel, superseding the motion rule of the panel redesign decision

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0318
- **Tags:** panel, design, motion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0249](0249-panel-redesign-a-projects-grid-a-project-canvas-and-a-side.md)

## Context

The maintainer said on 2026-11-10 that the panel feels simple and empty: solid colors, no animation, components that do not look refined, and unused areas. The panel redesign decision (ADR 0249) allows motion only for the deploy sweep and for traffic, so a refinement that adds any other motion contradicts an accepted ADR and needs a superseding decision first.

## Decision

Panel motion: allow short state-change motion across the panel, superseding the motion rule of the panel redesign decision.

Keep ADR 0249's direction and replace only its motion rule. Motion is allowed where it shows a change of state: hover and press feedback on controls, a side sheet or panel entering and leaving, a status changing (deploying, healthy, failed), a list row arriving or leaving, and a skeleton while data loads. Limits: 200 ms or less for feedback and 300 ms or less for entering and leaving, one easing curve for the whole panel, no decorative loops, nothing that moves while the screen is idle except the deploy sweep and traffic that ADR 0249 already allows, and everything off under prefers-reduced-motion.

Evidence: an audit at 1440 px found most screens mostly empty (canvas 93%, Agents 90%, Nodes 85%, Projects 82%, Approvals 79%).

The maintainer saw the rendered current-against-refined screens (canvas, Projects, Nodes) on 2026-11-10 and approved the direction. The motion itself is reviewed again on the rendered result of the PR that adds it.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0249](0249-panel-redesign-a-projects-grid-a-project-canvas-and-a-side.md): the motion rule: motion is allowed where it shows a change of state, within stated limits. The rest of [ADR 0249](0249-panel-redesign-a-projects-grid-a-project-canvas-and-a-side.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0318 in the project decision ledger. Do not edit by hand; changes go through a new decision.
