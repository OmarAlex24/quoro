-- Migration 0016: a control-plane backup's node_id may be null
-- (backup-format.md section 8.2, "While no machine holds the primary status").
--
-- While no machine holds the primary status, as in a new cluster before its
-- primary's own agent enrolls, after a `quoro restore --accept-loss` first
-- write until the new host's agent enrolls, and for good on a host whose agent
-- never enrolls locally, the primary still takes its control-plane backups,
-- and their row names no node: backups.node_id is null
-- (openapi.yaml `Backup.node_id`). A volume backup always names its node, which
-- the new CHECK keeps.
--
-- SQLite cannot drop NOT NULL in place, so the table is rebuilt with the same
-- columns, rows and indexes, as migration 0009 rebuilt volumes. Migrations run
-- in one transaction with foreign keys on, where PRAGMA foreign_keys has no
-- effect, and volumes.restored_from_backup_id references backups: dropping the
-- old table while a volume names one of its rows would fail, even with
-- deferred foreign keys, since renaming the new table does not resolve the
-- violation. So the volumes' references are set aside first, cleared, and
-- written back once the new table holds every row; no volume row is otherwise
-- changed. Nothing here writes the change log: the rows keep their values.

CREATE TEMP TABLE volume_backup_refs AS
    SELECT id, restored_from_backup_id FROM volumes WHERE restored_from_backup_id IS NOT NULL;
UPDATE volumes SET restored_from_backup_id = NULL WHERE restored_from_backup_id IS NOT NULL;

CREATE TABLE backups_new (
    id              TEXT PRIMARY KEY,                                  -- bkp_
    repository_id   TEXT NOT NULL REFERENCES backup_repositories (id),
    source_kind     TEXT NOT NULL CHECK (source_kind IN ('volume', 'control_plane')),
    app_id          TEXT,                                              -- no foreign key: outlives the app
    volume_id       TEXT,                                              -- no foreign key: outlives the volume
    node_id         TEXT REFERENCES nodes (id),                        -- null: a control-plane backup while no machine holds the primary status
    status          TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    "trigger"       TEXT NOT NULL CHECK ("trigger" IN ('schedule', 'api', 'upgrade')),
    fencing_token   INTEGER CHECK (fencing_token IS NULL OR fencing_token >= 1),
    snapshot_method TEXT CHECK (snapshot_method IN ('reflink', 'pause_copy') OR snapshot_method IS NULL),
    pause_ms        INTEGER CHECK (pause_ms IS NULL OR pause_ms >= 0),
    logical_bytes   INTEGER CHECK (logical_bytes IS NULL OR logical_bytes >= 0),
    uploaded_bytes  INTEGER CHECK (uploaded_bytes IS NULL OR uploaded_bytes >= 0),
    chunk_count     INTEGER CHECK (chunk_count IS NULL OR chunk_count >= 0),
    manifest_key    TEXT,
    manifest_digest TEXT CHECK (manifest_digest IS NULL OR (length(manifest_digest) = 64 AND manifest_digest NOT GLOB '*[^0-9a-f]*')),
    error           TEXT,
    created_by_type TEXT NOT NULL CHECK (created_by_type IN ('user', 'agent', 'system')),
    created_by_id   TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    pruned_at       TEXT,
    CHECK ((source_kind = 'volume') = (app_id IS NOT NULL AND volume_id IS NOT NULL AND fencing_token IS NOT NULL)),
    CHECK (source_kind = 'control_plane' OR node_id IS NOT NULL),
    CHECK ((status = 'failed') = (error IS NOT NULL)),
    CHECK (status = 'succeeded' OR (manifest_key IS NULL AND manifest_digest IS NULL)),
    CHECK (pruned_at IS NULL OR status = 'succeeded')
) STRICT;

INSERT INTO backups_new (id, repository_id, source_kind, app_id, volume_id, node_id, status, "trigger",
                         fencing_token, snapshot_method, pause_ms, logical_bytes, uploaded_bytes,
                         chunk_count, manifest_key, manifest_digest, error, created_by_type,
                         created_by_id, created_at, started_at, finished_at, pruned_at)
SELECT id, repository_id, source_kind, app_id, volume_id, node_id, status, "trigger",
       fencing_token, snapshot_method, pause_ms, logical_bytes, uploaded_bytes,
       chunk_count, manifest_key, manifest_digest, error, created_by_type,
       created_by_id, created_at, started_at, finished_at, pruned_at
FROM backups;

DROP TABLE backups;
ALTER TABLE backups_new RENAME TO backups;

CREATE INDEX idx_backups_app ON backups (app_id, created_at);
CREATE INDEX idx_backups_source ON backups (repository_id, source_kind, status);
CREATE INDEX idx_backups_node_status ON backups (node_id, status);
CREATE UNIQUE INDEX idx_backups_one_active_volume ON backups (volume_id) WHERE status IN ('queued', 'running') AND volume_id IS NOT NULL;
CREATE UNIQUE INDEX idx_backups_one_active_control_plane ON backups (repository_id) WHERE status IN ('queued', 'running') AND source_kind = 'control_plane';

UPDATE volumes
SET restored_from_backup_id = (SELECT r.restored_from_backup_id FROM volume_backup_refs r WHERE r.id = volumes.id)
WHERE id IN (SELECT id FROM volume_backup_refs);
DROP TABLE volume_backup_refs;

UPDATE meta SET value = '17' WHERE key = 'schema_version';
