# ADR 0308: Custody seals use the hpke crate (rust-hpke) for RFC 9180, added only if it passes cargo-deny under the dependency license policy

- **Status:** Accepted
- **Date:** 2026-11-10
- **Ledger id:** D-0308
- **Tags:** custody, governance, m5b
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The maintainer, 2026-11-10. The contract fixes the HPKE suite for custody seals (ADR 0231: base mode, DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, ChaCha20-Poly1305). A maintained implementation keeps hand-written cryptography out of the code base; building HPKE from the existing X25519, HKDF and ChaCha20-Poly1305 primitives would be new cryptographic code that needs its own review.

## Decision

Custody seals use the hpke crate (rust-hpke) for RFC 9180, added only if it passes cargo-deny under the dependency license policy.

The crate is dual-licensed MIT or Apache-2.0, inside the allowlist of the license policy (ADR 0080). It is added with only the features the fixed suite needs, and it must pass cargo-deny on every shipped target (ADR 0086). If it fails cargo-deny, the work stops and raises it rather than building HPKE by hand. Tests check the seal against the RFC 9180 test vectors for this suite.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0308 in the project decision ledger. Do not edit by hand; changes go through a new decision.
