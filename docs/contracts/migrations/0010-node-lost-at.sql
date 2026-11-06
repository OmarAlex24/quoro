-- Migration 0010: declaring a node lost (ADR 0183) and the M5b lease authority
-- (ADR 0169, ADR 0170, ADR 0171).
--
-- nodes.lost_at is the time an operator declared the node lost through
-- POST /api/v1/nodes/{node_id}/declare-lost (openapi.yaml `Node.lost_at`), or
-- NULL. In the same transaction the primary increments the fencing token of
-- every lease the node holds and sets leases.revoked_at, keeping the node as
-- the last holder (node-agent-protocol.md section 8.3). The node stays
-- enrolled and stays a voter. The primary clears lost_at once the returning
-- node has answered the revokes of those leases (section 9). Existing nodes
-- start with NULL.
--
-- Nothing else is stored for M5b's voters. Support is kept in the primary's
-- memory and in each voter's local store (section 8.8). The voter-set version
-- is the change-log seq of the entry that last changed the voter set (section
-- 8.10), so it needs no column. Renewals of expiring leases write no row:
-- leases.renewed_at and leases.expires_at stay as the grant wrote them, and
-- leases.duration_ms holds the longest duration the primary may have granted
-- the holder, raised before a longer grant (a maintenance window, section
-- 8.11) and lowered only once that grant's gate has passed (section 8.4).

ALTER TABLE nodes ADD COLUMN lost_at TEXT;

UPDATE meta SET value = '11' WHERE key = 'schema_version';
