-- Migration 0015: the mesh rules each machine reports.
--
-- nodes.mesh_rules is the version of the mesh firewall rules that the
-- machine's node agent reported in its last `hello` (node-agent-protocol.md
-- section 5.1), NULL when that `hello` carried none. The primary writes it with
-- the rest of the row at every `hello`, so it travels under its own name in the
-- node's change-log entry, and a promoted primary rebuilds it (section 7.3).
--
-- The primary sets spec.votes.listen on every voter once every machine with
-- the node role that is not removed has 2 or above here (section 6.12).

ALTER TABLE nodes ADD COLUMN mesh_rules INTEGER CHECK (mesh_rules IS NULL OR mesh_rules >= 1);

UPDATE meta SET value = '16' WHERE key = 'schema_version';
