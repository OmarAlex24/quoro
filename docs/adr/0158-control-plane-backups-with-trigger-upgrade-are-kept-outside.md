# ADR 0158: Control-plane backups with trigger: upgrade are kept outside the scheduled retention: the last 3 upgrade backups are always kept, whatever their age

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0158
- **Tags:** m5a, storage, control-plane
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

With keep 7 every 6 h, a pre-upgrade backup (ADR 0157) would be pruned after about 42 h, exactly when a rollback may still need it. The maintainer chose a separate rotation (2026-11-05).

## Decision

Control-plane backups with trigger: upgrade are kept outside the scheduled retention: the last 3 upgrade backups are always kept, whatever their age.

control_plane_schedule.keep counts only scheduled backups. Backups with trigger: upgrade are pruned only when more than 3 exist, oldest first. GC never removes chunks referenced by a kept upgrade backup. The contract states this in backup-format.md next to ADR 0157's text.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0158 in the project decision ledger. Do not edit by hand; changes go through a new decision.
