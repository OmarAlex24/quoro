# ADR 0157: Pre-upgrade control-plane backup: the new binary takes the VACUUM INTO backup (trigger: upgrade) at its first start, before it applies any migration; no new API operation

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0157
- **Tags:** m5a, storage, control-plane
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0148 requires a control-plane backup before upgrades, but install.sh replaces the binary without asking the running primary. The maintainer chose (2026-11-05) to back up at first start of the new version, which protects exactly what an upgrade can damage (the database before migrations) with no API change.

## Decision

Pre-upgrade control-plane backup: the new binary takes the VACUUM INTO backup (trigger: upgrade) at its first start, before it applies any migration; no new API operation.

At start-up the primary compares the stored schema version with the one the binary ships; if migrations are pending it first writes a VACUUM INTO backup with trigger: upgrade through the normal control-plane backup path, and only then migrates. If that backup fails, the primary refuses to migrate and exits with a clear error, unless the operator passes an explicit skip flag. The contract text in backup-format.md 7.2 is updated in a standalone contract PR. No new API operation.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0157 in the project decision ledger. Do not edit by hand; changes go through a new decision.
