-- Migration 0014: the digest an audit record keeps instead of its parameters
-- (ADR 0306, which refines ADR 0031 and ADR 0302).
--
-- An audited call is always audited. A record reaches the nodes as a
-- change-log entry, and the primary writes no entry that a node could not
-- receive alone and verify (node-agent-protocol.md section 7.1, ADR 0302).
-- When a call's redacted parameters would make the record's entry one of
-- those, the record keeps them as a digest instead (audit-log-format.md
-- section 4.1):
--
-- - audit_log.params is '{}';
-- - audit_log.params_sha256 is the lowercase hex SHA-256 of the parameters'
--   text: their RFC 8785 form, with an integer beyond 2^53 that the primary
--   read exactly written as its exact decimal integer;
-- - audit_log.params_bytes is the length in bytes of that text.
--
-- Both are NULL on every other record, so they are set together or not at
-- all. Both are part of the hashed record when set; a record without them is
-- hashed as before, so existing records keep their hash and start with NULL.
-- The table stays append-only.

ALTER TABLE audit_log ADD COLUMN params_sha256 TEXT
    CHECK (params_sha256 IS NULL OR (params = '{}' AND length(params_sha256) = 64 AND params_sha256 NOT GLOB '*[^0-9a-f]*'));

ALTER TABLE audit_log ADD COLUMN params_bytes INTEGER
    CHECK ((params_bytes IS NULL) = (params_sha256 IS NULL) AND (params_bytes IS NULL OR params_bytes >= 0));

UPDATE meta SET value = '15' WHERE key = 'schema_version';
