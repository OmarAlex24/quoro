-- Migration 0002: the hash of each node's session credential
-- (node-agent-protocol.md sections 2.4 and 11).
--
-- Enrollment returns a node credential once: 32 random bytes, base64-encoded.
-- The primary keeps only the SHA-256 hex of that string, exactly like the other
-- credential hashes listed at the top of db-schema.sql, so a database read does
-- not yield a usable credential. GET /api/v1/nodes/session hashes the bearer
-- credential and looks it up here.
--
-- LOCAL: not replicated. The hash is not a column of nodes, because every
-- nodes change travels as a full row in log_entries to every following node
-- (node-agent-protocol.md section 7); a credential hash has no business on
-- other nodes. Whether a promoted replica receives these rows is decided with
-- manual promotion in M5b, together with the custody of the cluster keys.
--
-- revoked_at is set when the node is removed (section 10); a revoked
-- credential never opens a session again. A node has at most one live
-- credential: re-enrolling a removed node creates a new node id.

CREATE TABLE node_credentials (
    node_id         TEXT PRIMARY KEY REFERENCES nodes (id) ON DELETE CASCADE,
    credential_hash TEXT NOT NULL UNIQUE CHECK (length(credential_hash) = 64 AND credential_hash NOT GLOB '*[^0-9a-f]*'),
    created_at      TEXT NOT NULL,
    revoked_at      TEXT
) STRICT;

UPDATE meta SET value = '3' WHERE key = 'schema_version';
