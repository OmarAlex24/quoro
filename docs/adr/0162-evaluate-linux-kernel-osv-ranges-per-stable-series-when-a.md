# ADR 0162: Evaluate Linux/Kernel OSV ranges per stable series: when a record lists a fix in the version's own major.minor series, that fix alone decides

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0162
- **Tags:** m5a, vulns
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

On the test lab, host kernel 6.12.115 shows 671 findings (60 critical) and the guest kernel 7.2.15 five, mostly false positives: OSV's Linux records encode one stable series per range with overlapping bounds, e.g. CVE-2025-37750 has [6.2.0,6.12.24) and [6.12.0,6.13.12), so plain OSV evaluation flags 6.12.115 although the 6.12 fix is 6.12.24.

## Decision

Evaluate Linux/Kernel OSV ranges per stable series: when a record lists a fix in the version's own major.minor series, that fix alone decides.

Measured on the OSV Linux data of 2026-11-05: series-aware evaluation cuts 6.12.115 from 706 matching records to 266 and the guest kernel 7.2.15 from 3 to 0 (CVE-2026-99239/99243/99244 all list 6.12.115 or 6.18.x fixes but nothing in 7.2). Rule, only for ecosystem Linux package Kernel: collect every fixed event of the record; if any is in the version's major.minor series, the version is affected only when it is below that fix (and at or above the matching introduced); otherwise fall back to plain OSV evaluation. CVE-2026-98432 (fixed 7.2.10) still resolves correctly. A fix PR in vulns/version (critical zone) with tests from these records; host findings keep upstream_version_only. The maintainer approved it on 2026-11-05, knowing it departs from the OSV schema algorithm for one ecosystem.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0162 in the project decision ledger. Do not edit by hand; changes go through a new decision.
