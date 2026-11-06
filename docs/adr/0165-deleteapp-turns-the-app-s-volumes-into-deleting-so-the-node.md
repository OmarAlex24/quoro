# ADR 0165: deleteApp turns the app's volumes into deleting so the node removes their files (a contract change for M5b)

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0165
- **Tags:** m5a, storage, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

openapi deleteApp says it deletes the app's volumes, but volumes.app_id cascades and node-agent-protocol 6.7 says a node never removes a file by absence; deleting the rows leaves the file orphaned on the node forever (reported orphaned, never deleted).

## Decision

deleteApp turns the app's volumes into deleting so the node removes their files (a contract change for M5b).

The maintainer chose option A on 2026-11-05, as a contract change in M5b: deleteApp keeps the volume rows in state deleting (the app_id cascade replaced by an explicit sweep that drops the row after the node reports deleted), so spec.volumes carries state deleted and the node removes the file. The contract change covers the db-schema volumes.app_id foreign key and the openapi deleteApp wording. Option B, not chosen: leave the file orphaned and document that an operator removes it. Until the contract change ships, M5a behaves like option B.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0165 in the project decision ledger. Do not edit by hand; changes go through a new decision.
