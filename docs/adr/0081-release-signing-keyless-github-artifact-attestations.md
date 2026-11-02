# ADR 0081: Release signing: keyless GitHub artifact attestations (Sigstore, SLSA build provenance) verified by a sha256-pinned gh in install.sh

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0081
- **Tags:** installer, security, release
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

D-0027 requires signed binaries and the M1 DoD says the release verifies against the published key; the scheme was left to the installer workstream (architecture section 17). Keyless needs no secret from the maintainer and removes the long-lived key that T-SUP-03 says can be stolen.

## Decision

Release signing: keyless GitHub artifact attestations (Sigstore, SLSA build provenance) verified by a sha256-pinned gh in install.sh.

Compared: (A) Ed25519 key (minisign or raw openssl pkeyutl) held as a GitHub Actions secret: install.sh embeds the public key and verifies with openssl, which every Debian host with ca-certificates has; zero extra download, works offline; costs: the maintainer must generate and upload the secret, key custody, rotation and revocation are undefined (T-SUP-03 residual), no transparency log. (B) Keyless: the tag-triggered release workflow runs actions/attest-build-provenance (id-token: write, attestations: write); Sigstore public-good Fulcio issues a short-lived cert bound to the workflow identity (repo, .github/workflows/release.yml, refs/tags/vX), the signature is logged in Rekor, and the SLSA provenance covers the binary, the tar.gz and SHA256SUMS. The bundle (.sigstore.jsonl) is a release asset. Verification: gh attestation verify <file> --bundle <bundle> --repo OWNER/quoro --signer-workflow OWNER/quoro/.github/workflows/release.yml --source-ref refs/tags/vX. Checked on 2026-11-02: gh 2.103.3 verifies a bundle with no GitHub auth (fetches the Sigstore TUF root), and fails on a tampered file. cosign was rejected as the installer verifier: its linux-amd64 binary is 146 MB vs 15 MB for the gh tarball. install.sh downloads gh pinned by version and sha256 embedded in the script, verifies, then deletes it; trust anchor is install.sh fetched over TLS from GitHub, the same strength as an embedded public key. Costs of B: 15 MB extra download and network access to Sigstore at install; the trust roots are GitHub OIDC + Sigstore instead of a key the maintainer holds; signatures can only be minted by the workflow, so the verify path cannot be exercised end to end before a tagged run (proposal: allow a one-off workflow_dispatch dry run that attests and uploads workflow artifacts only, no tag, no release, if the maintainer agrees). The M1 DoD wording 'verifies against the published key' becomes 'verifies against the published signer identity and the Sigstore trust root'. Recommendation: B (keyless); it does not contradict D-0027. Implemented in #29/#30 as B.

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0081 in the project decision ledger. Do not edit by hand; changes go through a new decision.
