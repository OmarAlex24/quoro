# ADR 0277: Warm turns on at the end of its chain: until then updateApp refuses warm outside dev builds, and the dev-warm-policy feature stays until the last warm PR

- **Status:** Accepted
- **Date:** 2026-11-07
- **Ledger id:** D-0277
- **Tags:** m5b, warm, leases, lab
- **Supersedes:** none
- **Superseded by:** none
- **Related:** [ADR 0223](0223-expiring-leases-in-dev-builds-before-the-warm-policy-ships.md)

## Context

Decided on 2026-11-07. Once the warm policy PR merges, updateApp would accept warm while no stream flows, domains do not move and the lease stays local (#555). A release cut in that window would ship a policy that does nothing. ADR 0223 says the dev feature is removed when the warm policy merges; this decision says when that is.

## Decision

Warm turns on at the end of its chain: until then updateApp refuses warm outside dev builds, and the dev-warm-policy feature stays until the last warm PR.

Refines D-0223 (ADR 0223). The warm PRs (the warm policy, the replication stream, the domain move and the primary side of #555) merge one by one as each is green. Until the last of them merges, a build without the dev-warm-policy feature refuses ha_policy warm: ha_cost.warm reads available: false with reason not_in_this_release, and updateApp refuses it, as on main today. A build with the feature accepts warm and runs everything merged so far, replication included, so the test lab can exercise each part. This widens what D-0223's feature unlocks, which was only accepting warm so the lease switches to expiring. The last PR of the chain turns warm on for every build and removes the feature, its build variable, and the CI and release checks. That PR may touch ci.yml, release.yml, packaging/* and crates/quoro/src/main.rs. The release build never enables the feature, and CI keeps asserting it.

_Some wording was changed for the public repository._

## Consequences

- Refines [ADR 0223](0223-expiring-leases-in-dev-builds-before-the-warm-policy-ships.md): when the dev-warm-policy feature is removed: with the last PR of the warm chain, which turns warm on for every build. The rest of [ADR 0223](0223-expiring-leases-in-dev-builds-before-the-warm-policy-ships.md) stands.
- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0277 in the project decision ledger. Do not edit by hand; changes go through a new decision.
