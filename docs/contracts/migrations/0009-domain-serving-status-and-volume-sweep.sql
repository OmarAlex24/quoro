-- Migration 0009: per-domain serving status (ADR 0167) and volumes that
-- outlive their app until the node removes their files (ADR 0165).
--
-- domains.serving_status says whether a machine can serve the hostname
-- (openapi.yaml `Domain.serving_status`), independently of cert_status:
--   * `serving`: the hostname's route is in the desired state of a machine
--     that can serve it (for `tunnel`, a machine whose tunnel carries it);
--   * `unserved`: the app has an instance meant to run, but no machine can
--     serve the hostname; last_error says why;
--   * `pending`: neither yet, because the domain was just added or the app has
--     no instance meant to run.
-- The primary recomputes it whenever an app's placement, replicas or routes
-- change, and emits `domain.unserved` and `domain.served` on each change
-- between `serving` and `unserved` (events.schema.json). last_error is set
-- only while the status is `unserved`. Domains added before this migration
-- start as `pending` and are set at the primary's next recompute.
--
-- volumes.app_id loses its foreign key. deleteApp no longer deletes the app's
-- volume rows: in the same transaction that deletes the app it turns every
-- volume of the app, whatever its state, into `deleting`, so spec.volumes
-- carries it with state `deleted` and the node removes its file
-- (node-agent-protocol.md section 6.7). The row is dropped by the primary when
-- the node reports the volume `deleted`, as for deleteVolume; that sweep, not
-- a cascade, is what removes the rows of a deleted app. Until then the row
-- names an app that no longer exists, which the foreign key's ON DELETE
-- CASCADE could not allow. Deleting the rows together with the app, as before
-- this migration, left the files on the node for good, because a node never
-- removes a volume file by absence. SQLite cannot drop a foreign key in place,
-- so the table is rebuilt with the same columns, rows and indexes.

ALTER TABLE domains ADD COLUMN serving_status TEXT NOT NULL DEFAULT 'pending' CHECK (serving_status IN ('serving', 'unserved', 'pending'));
ALTER TABLE domains ADD COLUMN last_error TEXT;

CREATE TABLE volumes_new (
    id                      TEXT PRIMARY KEY,                          -- vol_
    app_id                  TEXT NOT NULL,                             -- no foreign key: a `deleting` row outlives its app
    node_id                 TEXT NOT NULL REFERENCES nodes (id),
    size_gb                 INTEGER NOT NULL CHECK (size_gb BETWEEN 1 AND 1024),
    mount_path              TEXT NOT NULL CHECK (substr(mount_path, 1, 1) = '/' AND length(mount_path) BETWEEN 2 AND 255),
    state                   TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'restoring', 'failed', 'detached', 'deleting')),
    allocated_bytes         INTEGER CHECK (allocated_bytes IS NULL OR allocated_bytes >= 0),
    restored_from_backup_id TEXT REFERENCES backups (id),
    error                   TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
) STRICT;

INSERT INTO volumes_new (id, app_id, node_id, size_gb, mount_path, state, allocated_bytes,
                         restored_from_backup_id, error, created_at, updated_at)
SELECT id, app_id, node_id, size_gb, mount_path, state, allocated_bytes,
       restored_from_backup_id, error, created_at, updated_at
FROM volumes;

DROP TABLE volumes;
ALTER TABLE volumes_new RENAME TO volumes;

CREATE INDEX idx_volumes_app ON volumes (app_id, created_at);
CREATE INDEX idx_volumes_node ON volumes (node_id, state);
CREATE UNIQUE INDEX idx_volumes_app_attached ON volumes (app_id) WHERE state IN ('creating', 'ready', 'restoring');

UPDATE meta SET value = '10' WHERE key = 'schema_version';
