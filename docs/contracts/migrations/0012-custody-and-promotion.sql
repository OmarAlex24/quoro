-- Migration 0012: change-log follow, key custody, promotion and signing-key
-- rotation (ADR 0154, ADR 0179 to ADR 0182, ADR 0226 to ADR 0236;
-- node-agent-protocol.md section 7).
--
-- nodes.custody_public_key is the base64 X25519 public key (32 bytes) of the
-- custody key a machine with the `node` role generates (section 7.2), or
-- NULL before the machine reports one and for edge-only and witness-only
-- machines. The primary stores the first key a machine reports, at
-- enrollment or in hello, and never replaces it from a later report.
--
-- nodes.custody_key_changed_at is when the machine first reported a
-- different custody key, which the primary refused, or NULL. It stays set
-- until the node is removed; the machine then enrolls again.
--
-- nodes.signing_key_id is the key_id of the cluster signing key the machine
-- pins, from hello.signing_key_id and from its answer to signing_key_rotate,
-- or NULL before its first M5b session. While a rotation runs, the primary
-- signs each machine's messages with the key that machine pins (section 7.5).
--
-- nodes.log_seq is unchanged. From M5b the primary writes it only together
-- with the writes it already makes to the node's row when a session starts
-- and ends; between them the newest acknowledged seq is kept in memory,
-- because each write to nodes is itself a change-log entry every follower
-- must acknowledge (section 7.1).
--
-- node_key_custody is replicated, a projection of the change log. One row per
-- key sealed to one machine (section 7.2): key_kind 'signing' (a cluster
-- signing key's 32-byte seed), 'dek' (a 32-byte data-encryption key) or 'tls'
-- (the primary's TLS certificate and private key). key_id is the lowercase
-- hex SHA-256 of the signing key's public key, the 32 hex characters of the
-- DEK's key id, or the lowercase hex SHA-256 of the TLS leaf certificate's
-- DER. sealed is the HPKE base-mode output: the 32-byte encapsulated key
-- followed by the ciphertext and its tag; in a change-log payload it is
-- base64. Every follower holds every row and can open only its own. Its
-- change-log entries have entity_type 'node_key_custody' and entity_id
-- node_id/key_kind/key_id.
--
-- signing_keys is replicated. One row per cluster signing key the primary
-- holds: the 'current' one and, while a rotation runs, the 'next' one
-- (section 7.5). The rotation's last write deletes the old key's row and its
-- seals and marks the next key current. The private keys stay files outside
-- the database, as before; the rows hold only public keys. Its change-log
-- entries have entity_type 'signing_key' and entity_id key_id.
--
-- node_credentials (migration 0002) is no longer local: from schema 13 every
-- insert and update of a row is a change-log entry, entity_type
-- 'node_credential', carrying the whole row, so a promoted primary accepts
-- every node's session (section 7.3). It holds only SHA-256 hashes of 32
-- random bytes, like api_tokens and agent_keys.
--
-- A migration cannot write change-log entries, whose hashes are computed
-- outside SQL. So at its first start on schema 13, before it serves, the
-- primary writes one change-log entry per existing node_credentials row,
-- inserts its current signing key into signing_keys with its change-log
-- entry, and seals its keys to every machine that has reported a custody
-- key. A promoted primary rebuilds these tables from the log like any other.
--
-- meta gains one key, written by the binary and never replicated:
-- demoted_by_term, the higher term a primary learned of when it demoted
-- itself (section 7.4). While it is set the primary answers every request
-- with 503 not_primary. term (ADR 0169) is written by a promotion to t + 1.
-- Neither exceeds 2^53 - 1, the term ceiling (section 8.8).
--
-- A promotion rebuilds the database by applying every followed entry to a
-- schema of the promoting binary (section 7.3). A payload written before a
-- column existed lacks that column, and the row takes the column's default:
-- this and every later migration gives a new column a default or allows
-- NULL.

ALTER TABLE nodes ADD COLUMN custody_public_key TEXT;
ALTER TABLE nodes ADD COLUMN custody_key_changed_at TEXT;
ALTER TABLE nodes ADD COLUMN signing_key_id TEXT
    CHECK (signing_key_id IS NULL OR (length(signing_key_id) = 64 AND signing_key_id NOT GLOB '*[^0-9a-f]*'));

CREATE TABLE node_key_custody (
    node_id    TEXT NOT NULL REFERENCES nodes (id) ON DELETE CASCADE,
    key_kind   TEXT NOT NULL CHECK (key_kind IN ('signing', 'dek', 'tls')),
    key_id     TEXT NOT NULL CHECK (length(key_id) IN (32, 64) AND key_id NOT GLOB '*[^0-9a-f]*'),
    sealed     BLOB NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (node_id, key_kind, key_id)
) STRICT;

CREATE TABLE signing_keys (
    key_id     TEXT PRIMARY KEY CHECK (length(key_id) = 64 AND key_id NOT GLOB '*[^0-9a-f]*'),
    public_key TEXT NOT NULL,                                          -- base64 Ed25519
    state      TEXT NOT NULL CHECK (state IN ('current', 'next')),
    created_at TEXT NOT NULL
) STRICT;

CREATE UNIQUE INDEX idx_signing_keys_state ON signing_keys (state);

UPDATE meta SET value = '13' WHERE key = 'schema_version';
