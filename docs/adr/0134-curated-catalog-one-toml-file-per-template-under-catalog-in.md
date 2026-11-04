# ADR 0134: Curated catalog: one TOML file per template under catalog/ in the public repo, embedded in the signed release binary; images pinned by digest; upstream cosign signatures verified in CI when a template is added or bumped, not on nodes

- **Status:** Accepted
- **Date:** 2026-11-04
- **Ledger id:** D-0134
- **Tags:** catalog, security, m4
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0070 requires the catalog format, owner, location and signature scheme to be decided at M4 planning, before the first template ships; threat-model items T-SUP-04 and T-VM-05 rely on it. D-0026 binds templates to pinned upstream images with verifiable signatures.

## Decision

Curated catalog: one TOML file per template under catalog/ in the public repo, embedded in the signed release binary; images pinned by digest; upstream cosign signatures verified in CI when a template is added or bumped, not on nodes.

Format: catalog/`<name>`/template.toml, one per template: name, description, upstream homepage, image as repo@sha256:`<digest>` (a tag alone is rejected), default resources (vcpus, memory_mb, disk_gb), port, env vars (name, default, required, secret flag), health check path, and a signer block: cosign keyless (certificate identity regexp + OIDC issuer) or a cosign public key in catalog/`<name>`/cosign.pub. Notation is not supported; an upstream that does not sign its images cannot be in the catalog (D-0026 requires verifiable signatures). Location and trust: the catalog/ directory in the public repo (D-0026), compiled into the quoro binary at release, so its integrity is the release's integrity (ADR 0027, D-0081 attestations); no runtime download, no registry of our own. A later refresh channel would need its own decision. Signature verification: a CI job runs the cosign CLI (pinned version, digest-pinned in the workflow) against each new or changed template, verifying the image digest's signature against the declared signer; the PR cannot merge otherwise. Nodes do not verify signatures at runtime: the digest pin already fixes the bytes (T-SUP-04: a replaced upstream tag cannot change a pinned digest), and the only maintained Rust verifier (sigstore 0.14) calls itself experimental and would add a large dependency tree under the ADR 0080 license policy. Residual risk stated in the threat model: verification happens at admission time; a signer that signs a malicious image passes (already in T-SUP-04). Bumps: Renovate-style digest bumps are PRs that rerun the check. Owner: a new catalog workstream owning catalog/ and its CI check; the control plane reads the embedded catalog (an API listing templates and a createApp-from-template path are a contract change, standalone PR) and the panel shows it. Milestone: M4 only decides (roadmap); the format, the CI check and the first templates ship as a later milestone item, scheduled when the maintainer picks it (proposed: M5a, alongside the edge). Threat model update: T-SUP-04 and T-VM-05 move from mitigation pending to this scheme when it ships.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0134 in the project decision ledger. Do not edit by hand; changes go through a new decision.
