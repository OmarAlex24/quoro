# ADR 0192: deleteApp turns every volume of the app into deleting whatever its state; the record stays readable with getVolume until the node reports deleted

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0192
- **Tags:** m5b, contracts, storage
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0165 says the app's volumes become deleting; it does not say whether detached and failed volumes are included, nor how a deleting volume of a deleted app is observed.

## Decision

deleteApp turns every volume of the app into deleting whatever its state; the record stays readable with getVolume until the node reports deleted.

All volumes of the app, including detached (pre-restore data) and failed ones, become deleting in the deleteApp transaction, since deleteApp deletes the app and its data. The node removes each file on the deleted entry; the existing sweep (the primary drops the row on a deleted report, as for deleteVolume) removes the rows. volumes.app_id loses its foreign key (migration 0009 rebuilds the table). A deleting volume of a deleted app is not listed under the app (listAppVolumes answers 404 for a deleted app) but getVolume reads it until the sweep. Until #333 ships, deleteApp still deletes the rows itself (the behaviour of option B in ADR 0165).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0192 in the project decision ledger. Do not edit by hand; changes go through a new decision.
