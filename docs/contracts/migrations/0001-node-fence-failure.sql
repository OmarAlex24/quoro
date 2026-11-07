-- Migration 0001: the primary's record of failed fences and their
-- acknowledgement (D-0074, D-0079).
--
-- db-schema.sql is the frozen M0 schema (schema_version 1). After the freeze a
-- schema change is a numbered migration in this directory, applied in order on
-- top of db-schema.sql; the base file is never edited in place. Each migration
-- bumps meta.schema_version by one.
--
-- nodes.fence_failure: the failed fences of this node that no operator has
-- acknowledged yet, or NULL. JSON object
--   {"detected_at": RFC 3339, "lease_ids": [lse_...], "app_ids": [app_...],
--    "vm_ids": [vm_...]}
-- Written when the primary receives node.fence_failed or a lease_revoke
-- answered with the protocol error code fence_failed; lease, app and VM ids
-- accumulate across failures until acknowledged. While a lease id is listed,
-- the primary grants that lease to no node (node-agent-protocol.md section 8.5).
--
-- nodes.fence_ack_pending: an acknowledgement recorded by
-- POST /api/v1/nodes/{node_id}/fence-ack that the node has not yet confirmed,
-- or NULL. JSON object
--   {"acked_at": RFC 3339, "lease_ids": [lse_...],
--    "fencing_tokens": {lse_...: integer}}
-- From M5b, fencing_tokens holds the acknowledged token of each listed lease
-- (node-agent-protocol.md section 8.5 step 3, ADR 0245, ADR 0247, ADR 0262).
-- It is absent on a row a primary before ADR 0245 wrote; such a row is
-- delivered without fencing_tokens. The ids leave the object when the node
-- answers the fence_ack command with status ok, and the column is NULL once
-- none is left.
--
-- Both columns are replicated state: they change through the change log like
-- every other column of nodes.

ALTER TABLE nodes ADD COLUMN fence_failure TEXT
    CHECK (fence_failure IS NULL OR json_valid(fence_failure));

ALTER TABLE nodes ADD COLUMN fence_ack_pending TEXT
    CHECK (fence_ack_pending IS NULL OR json_valid(fence_ack_pending));

UPDATE meta SET value = '2' WHERE key = 'schema_version';
