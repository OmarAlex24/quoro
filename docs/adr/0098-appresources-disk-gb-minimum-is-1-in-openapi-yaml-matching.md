# ADR 0098: AppResources.disk_gb minimum is 1 in openapi.yaml, matching vms CHECK (disk_gb >= 1)

- **Status:** Accepted
- **Date:** 2026-11-03
- **Ledger id:** D-0098
- **Tags:** contracts, control-plane, apps
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

Same mismatch D-0083 fixed for Vm/CreateVmRequest: the apps API spec allows disk_gb 0 but every app instance is a vms row whose CHECK refuses 0; the M2 apps handlers refuse 0 with validation_failed meanwhile.

## Decision

AppResources.disk_gb minimum is 1 in openapi.yaml, matching vms CHECK (disk_gb >= 1).

Standalone contract PR: set AppResources.disk_gb minimum: 1 in docs/contracts/openapi.yaml. Until then createApp/updateApp answer validation_failed for disk_gb 0 and the openapi diff keeps minimum 0 in the generated schema.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0098 in the project decision ledger. Do not edit by hand; changes go through a new decision.
