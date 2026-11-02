# ADR 0050: Cluster keys: separate Ed25519 signing key and XChaCha20-Poly1305 data-encryption key, generated at setup, stored as 0600 files outside SQLite, plus a node-local key

- **Status:** Accepted
- **Date:** 2026-11-02
- **Ledger id:** D-0050
- **Tags:** security, control-plane, node-agent
- **Supersedes:** none
- **Superseded by:** none
- **Related:** none

## Context

The contracts used 'cluster key' both to sign desired_state (Ed25519) and to encrypt app_env.value_enc and webhooks.secret_enc; one Ed25519 key cannot encrypt, and no document fixes algorithms, generation, storage, backup, rotation or custody on promotion. M2 needs both (signed desired_state, encrypted env).

## Decision

Cluster keys: separate Ed25519 signing key and XChaCha20-Poly1305 data-encryption key, generated at setup, stored as 0600 files outside SQLite, plus a node-local key.

(1) cluster signing key, Ed25519, signs desired_state and lease messages; nodes pin its public key at enrollment. (2) cluster data-encryption key (DEK), 256-bit, AEAD XChaCha20-Poly1305, encrypts *_enc columns; the column stores nonce||ciphertext and a key id so rotation can re-encrypt lazily. (3) node-local key per node, encrypts the node's local copy of desired-state secrets at rest. Generation: at first start of the primary (before /setup), never derived from a password. Storage: files mode 0600 owned by the quoro user in the data directory, outside the SQLite file, so a database copy alone does not reveal secrets. Backup: the operator must back up the key files separately from the database; losing the DEK loses every secret (state it in the installer output and docs). Rotation: signing-key rotation is a key-change message signed by the old key that nodes accept; DEK rotation re-encrypts rows under a new key id. Custody on promotion (ties to Q-0024): (a) replicate both keys to replicas, wrapped to each replica's node key, so a promoted replica keeps signing with the same key; (b) re-key on promotion and have nodes accept the new key through an operator-confirmed re-pin. Algorithms, generation, storage, backup and rotation are decided as stated. Custody on promotion stays open and is settled together with Q-0024 (recommended: (a)).

_Some wording was changed for the public repository._

## Consequences

- This decision is binding for all contributors and agents. Changing it requires a new ADR approved by the maintainer (see ADR 0036).

---
Generated from decision D-0050 in the project decision ledger. Do not edit by hand; changes go through a new decision.
