# ADR 0080: Dependency license policy for the AGPL-3.0-only core: an allowlist of permissive and AGPL-compatible licenses enforced in CI with cargo-deny, starting in M1

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0080
- **Tags:** governance, security
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer accepted the recommendation on 2026-11-02. Nothing checks dependency licenses today; rmcp is moving from MIT to Apache-2.0 and the Cloud Hypervisor reading is still a hypothesis (it is a separate binary, not linked).

## Decision

Dependency license policy for the AGPL-3.0-only core: an allowlist of permissive and AGPL-compatible licenses enforced in CI with cargo-deny, starting in M1.

Initial allowlist: MIT, Apache-2.0, Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, Zlib, Unicode-3.0, MPL-2.0 (file-level copyleft, allowed), AGPL-3.0-only for our own crates. cargo-deny also checks advisories and duplicate versions. Exceptions need a recorded decision.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0080 in the project decision ledger. Do not edit by hand; changes go through a new decision.
