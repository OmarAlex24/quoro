# ADR 0161: M5a Quoro-component findings match crates (crates.io), guest kernel (Linux/Kernel upstream version), Cloud Hypervisor (crates.io), cloudflared and BuildKit (Go); npm packages in the SBOM are listed but not matched

- **Status:** Accepted
- **Date:** 2026-11-05
- **Ledger id:** D-0161
- **Tags:** m5a, vulns, contracts
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

openapi.yaml VulnerabilityFinding.component names 'a crate name, or one of guest-kernel, cloud-hypervisor, cloudflared, buildkit', while packaging/README.md says the primary matches 'that list' (which includes the panel's npm packages). An npm package has no legal component value.

## Decision

M5a Quoro-component findings match crates (crates.io), guest kernel (Linux/Kernel upstream version), Cloud Hypervisor (crates.io), cloudflared and BuildKit (Go); npm packages in the SBOM are listed but not matched.

Part 4 of E3 matches only crates with source crates.io and the four pins; npm components ship in the SBOM and the embedded list but raise no finding. Path and git crates are not matched. Alternatives: (a) match npm with component = npm package name, a description change in openapi.yaml (contract PR); (b) keep as is until M5b. The maintainer chose (b) for M5a on 2026-11-05; (a) can follow later as a contract change.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0161 in the project decision ledger. Do not edit by hand; changes go through a new decision.
