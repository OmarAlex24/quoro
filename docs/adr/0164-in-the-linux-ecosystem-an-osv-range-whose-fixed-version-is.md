# ADR 0164: In the Linux ecosystem, an OSV range whose fixed version is below its introduced version is ignored when matching

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0164
- **Tags:** m5a, vulns
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

After the per-series rule (D-0162), the guest kernel 7.2.15 still shows CVE-2023-52920 and CVE-2024-36968: their OSV Linux records carry a range like introduced 6.7.0 / fixed 6.6.70, which the plain OSV reading turns into 'every version from 6.7.0 on is affected'. The maintainer chose the small fix on 2026-11-05.

## Decision

In the Linux ecosystem, an OSV range whose fixed version is below its introduced version is ignored when matching.

Applies only to ecosystem Linux (package Kernel), alongside the per-series rule. A range is ignored when every fixed event in it is below its introduced event; other ranges of the record still decide. The fix is in crates/control-plane/src/vulns/version, with tests built from those two records; host findings keep upstream_version_only.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0164 in the project decision ledger. Do not edit by hand; changes go through a new decision.
