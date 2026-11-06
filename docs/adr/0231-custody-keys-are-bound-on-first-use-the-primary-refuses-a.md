# ADR 0231: Custody keys are bound on first use: the primary refuses a changed key with custody_key_changed; seals use HPKE with info binding cluster, node, kind and key

- **Status:** Accepted
- **Date:** 2026-11-06
- **Ledger id:** D-0231
- **Tags:** m5b, contracts, promotion
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

ADR 0180 fixes the X25519 custody key, the HPKE suite and the node_key_custody table, but not what happens when a machine reports a different key, what each seal holds, or how a seal is bound to its context.

## Decision

Custody keys are bound on first use: the primary refuses a changed key with custody_key_changed; seals use HPKE with info binding cluster, node, kind and key.

The primary stores the first custody key a machine reports (enroll request or hello) and never replaces it from a report. A different key gets error custody_key_changed (non-fatal), sets nodes.custody_key_changed_at, writes the audit record node.custody_key_changed once, and gets no seal; recovery is removing and re-enrolling the machine, since losing the key means losing the data directory and its credential too. HPKE base mode, KEM 0x0020, KDF 0x0001, AEAD 0x0003; info is 'quoro custody v1', cluster_id, node_id, key_kind and key_id, each followed by a zero byte; empty AAD; sealed is enc followed by the ciphertext. Plaintexts: signing, the 32-byte seed (key_id the hex SHA-256 of the public key); dek, the 32-byte key, one seal per DEK key id (key_id its 32 hex characters); tls, the JCS JSON {cert_pem, key_pem} (key_id the pinned leaf fingerprint). Node.custody reports sealed, pending, none or key_changed, null without the node role; only sealed machines are promotable. Rejected: accepting a changed key, which would let anyone holding a node credential redirect the next seals.

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0231 in the project decision ledger. Do not edit by hand; changes go through a new decision.
