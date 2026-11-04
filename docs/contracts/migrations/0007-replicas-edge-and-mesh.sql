-- Migration 0007: replicas of stateless apps (ADR 0142), the edge role
-- (ADR 0143) and the node mesh with one guest subnet per node (ADR 0144).
--
-- apps.replicas is the app's instance count (openapi.yaml `App.replicas`),
-- at most one instance per node. Existing apps keep 1. Only a stateless app
-- may hold more than 1; the service enforces it, because apps.stateful is
-- derived from volumes (migration 0006).
--
-- nodes.subnet is the node's guest subnet in CIDR form, a /24 of 10.42.0.0/16,
-- sent to the node as spec.network (node-agent-protocol.md section 5.11). It
-- is assigned when a node with the `node` role enrolls and is NULL for an
-- edge-only or witness-only machine. A subnet is never given to another node
-- while a VM addressed from it is recorded (vms.ip). Nodes enrolled before
-- M5a start with NULL: at its first start on this schema the primary gives the
-- oldest node with the `node` role the subnet every node used so far (the
-- primary's configured guest network, which may have any prefix from /8 to
-- /30 and lie outside 10.42.0.0/16), and every other node the next free /24
-- of 10.42.0.0/16 that does not overlap it; those nodes move their VMs to the
-- new subnet on the VMs' next deploy (ADR 0144, as ADR 0136 did for the
-- terminal). The CHECK therefore accepts any IPv4 CIDR shape; no two rows
-- overlap, which the primary enforces because SQLite cannot.
--
-- nodes.mesh_public_key is the base64 WireGuard public key the machine sent in
-- `hello` (node-agent-protocol.md section 5.1); NULL until its first M5a
-- hello. The private key never leaves the machine. nodes.mesh_address is the
-- machine's address on the mesh, a host address of 10.43.0.0/16, assigned by
-- the primary with the subnet (or, for an edge, at enrollment) and never
-- reused while the node exists. Neither column is exposed by the API in M5a.
--
-- Edge backend health is not stored: the edge reports it in every
-- status_report and the primary keeps the latest report in memory, as it does
-- for metrics. What it concludes is recorded as app.degraded, app.down and
-- app.up events.

ALTER TABLE apps ADD COLUMN replicas INTEGER NOT NULL DEFAULT 1 CHECK (replicas BETWEEN 1 AND 64);

ALTER TABLE nodes ADD COLUMN subnet TEXT CHECK (subnet IS NULL OR subnet GLOB '[0-9]*.[0-9]*.[0-9]*.[0-9]*/[0-9]*');
ALTER TABLE nodes ADD COLUMN mesh_public_key TEXT CHECK (mesh_public_key IS NULL OR length(mesh_public_key) = 44);
ALTER TABLE nodes ADD COLUMN mesh_address TEXT CHECK (mesh_address IS NULL OR mesh_address GLOB '10.43.[0-9]*.[0-9]*');

CREATE UNIQUE INDEX idx_nodes_subnet ON nodes (subnet) WHERE subnet IS NOT NULL;
CREATE UNIQUE INDEX idx_nodes_mesh_address ON nodes (mesh_address) WHERE mesh_address IS NOT NULL;

UPDATE meta SET value = '8' WHERE key = 'schema_version';
