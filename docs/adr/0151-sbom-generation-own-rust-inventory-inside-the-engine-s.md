# ADR 0151: SBOM generation: own Rust inventory inside the engine's image path (dpkg, apk, rpm sqlite, os-release, npm lockfiles, Python dist-info, Go build info, cargo-auditable), not a pinned syft binary

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0151
- **Tags:** m5a, security, engine
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The engine already unpacks every layer to a directory before mkfs.ext4 -d; syft v1.56.0 (Apache-2.0, 31 MB) would add a Go binary parsing untrusted images as root with unverified dependency licenses.

## Decision

SBOM generation: own Rust inventory inside the engine's image path (dpkg, apk, rpm sqlite, os-release, npm lockfiles, Python dist-info, Go build info, cargo-auditable), not a pinned syft binary.

Inventory records source package names (dpkg Source:, apk o:) because OSV keys on them. Version comparison: debversion (Apache-2.0), pep440_rs (Apache-2.0 OR BSD-2-Clause), own Alpine and RPM comparators (rpmvercmp is BSL-1.0, not allowed). Uncovered ecosystems are shown as 'not inventoried'. Recommendation: Rust inventory; revisit syft only if coverage proves too thin.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0151 in the project decision ledger. Do not edit by hand; changes go through a new decision.
