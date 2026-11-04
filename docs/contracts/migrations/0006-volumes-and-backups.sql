-- Migration 0006: local volumes (ADR 0145, ADR 0146), backups and backup
-- repositories (ADR 0147, ADR 0148), and the lock strength of the off-site
-- audit copy (ADR 0149).
--
-- volumes is replicated. It was shape only and empty before M5a (no endpoint
-- wrote it), so it is dropped and created again with its M5a columns rather
-- than altered. state follows openapi.yaml `Volume.state`. An app has at most
-- one volume in `creating`, `ready` or `restoring`. A restore, in the same
-- transaction that creates the new volume in `restoring`, turns the volume it
-- replaces `detached`: its file stays untouched on its node until an operator
-- deletes it. A volume left `failed` by a restore does not block another one. allocated_bytes is the sparse file's use on the node's
-- disk as last reported in status_report (node-agent-protocol.md section 5.12).
-- restored_from_backup_id names the backup a restore wrote the volume from.
--
-- apps.backup_schedule is the app's `cold` schedule as JSON (openapi.yaml
-- `BackupSchedule`): {interval_h, keep}. It is stored for every app and used
-- only while ha_policy is `cold`. apps.stateful stays a stored column, set by
-- the primary when it creates the app's first volume and cleared when the app
-- has no volume left other than `detached` or `failed` ones.
--
-- nodes.volume_snapshot is the snapshot method the node reported in `hello`
-- (ADR 0146): `reflink` when its data directory supports FICLONE, otherwise
-- `pause_copy`; NULL before the node's first M5a hello.
--
-- backup_repositories is replicated. repo_key_enc is the repository key wrapped
-- under the cluster data-encryption key, in the encrypted-column layout of
-- db-schema.sql; it lets the primary run backups unattended. The same key
-- wrapped under the owner's recovery key lives only in the repository's config
-- object in the bucket (backup-format.md section 4). The recovery key itself is
-- never stored: recovery_key_fingerprint identifies it without revealing it.
-- control_plane_schedule is the schedule of the control-plane database backups
-- (ADR 0148), same JSON shape as apps.backup_schedule.
--
-- backups is replicated, a projection of the change log like builds. One row
-- per backup of one source: a volume (`volume`) or the control-plane database
-- (`control_plane`). app_id and volume_id carry no foreign key, so the record
-- of a backup outlives a deleted app or volume until retention prunes it.
-- fencing_token is the token of the app's `local` lease the node held when the
-- backup was dispatched; the node refuses a job whose token is not its own
-- (node-agent-protocol.md section 5.11). manifest_key and manifest_digest name
-- the backup's manifest object and the BLAKE3 digest of its encrypted bytes,
-- which the seal repeats (backup-format.md section 5). pruned_at is set when
-- retention removed the seal and the manifest.
--
-- audit_exports: object_lock (0 or 1) is replaced by lock_strength
-- (none | bucket_rule | compliance), the strength the probe observed
-- (audit-log-format.md section 8.2). probed_at and probe_detail record the last
-- probe. last_exported_hash is the hash of the record at last_exported_seq, as
-- written in its head object, so the verifier compares heads without reading
-- the bucket. last_error keeps the last export or verification error. target
-- holds the key prefix of the copy inside the provider's bucket
-- (openapi.yaml `AuditExport.prefix`).

DROP TABLE volumes;

CREATE TABLE volumes (
    id                      TEXT PRIMARY KEY,                          -- vol_
    app_id                  TEXT NOT NULL REFERENCES apps (id) ON DELETE CASCADE,
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

CREATE INDEX idx_volumes_app ON volumes (app_id, created_at);
CREATE INDEX idx_volumes_node ON volumes (node_id, state);
CREATE UNIQUE INDEX idx_volumes_app_attached ON volumes (app_id) WHERE state IN ('creating', 'ready', 'restoring');

ALTER TABLE apps ADD COLUMN backup_schedule TEXT NOT NULL DEFAULT '{"interval_h":24,"keep":7}' CHECK (json_valid(backup_schedule));

ALTER TABLE nodes ADD COLUMN volume_snapshot TEXT CHECK (volume_snapshot IN ('reflink', 'pause_copy') OR volume_snapshot IS NULL);

CREATE TABLE backup_repositories (
    id                       TEXT PRIMARY KEY,                         -- brp_
    name                     TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    provider_id              TEXT NOT NULL REFERENCES providers (id),
    prefix                   TEXT NOT NULL,
    repo_key_enc             BLOB NOT NULL,
    recovery_key_fingerprint TEXT NOT NULL CHECK (length(recovery_key_fingerprint) = 16 AND recovery_key_fingerprint NOT GLOB '*[^0-9a-f]*'),
    recovery_key_rotated_at  TEXT,
    control_plane_schedule   TEXT NOT NULL DEFAULT '{"interval_h":6,"keep":7}' CHECK (json_valid(control_plane_schedule)),
    created_by               TEXT NOT NULL REFERENCES users (id),
    created_at               TEXT NOT NULL,
    UNIQUE (provider_id, prefix)
) STRICT;

CREATE TABLE backups (
    id              TEXT PRIMARY KEY,                                  -- bkp_
    repository_id   TEXT NOT NULL REFERENCES backup_repositories (id),
    source_kind     TEXT NOT NULL CHECK (source_kind IN ('volume', 'control_plane')),
    app_id          TEXT,                                              -- no foreign key: outlives the app
    volume_id       TEXT,                                              -- no foreign key: outlives the volume
    node_id         TEXT NOT NULL REFERENCES nodes (id),
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
    CHECK ((status = 'failed') = (error IS NOT NULL)),
    CHECK (status = 'succeeded' OR (manifest_key IS NULL AND manifest_digest IS NULL)),
    CHECK (pruned_at IS NULL OR status = 'succeeded')
) STRICT;

CREATE INDEX idx_backups_app ON backups (app_id, created_at);
CREATE INDEX idx_backups_source ON backups (repository_id, source_kind, status);
CREATE INDEX idx_backups_node_status ON backups (node_id, status);
CREATE UNIQUE INDEX idx_backups_one_active_volume ON backups (volume_id) WHERE status IN ('queued', 'running') AND volume_id IS NOT NULL;
CREATE UNIQUE INDEX idx_backups_one_active_control_plane ON backups (repository_id) WHERE status IN ('queued', 'running') AND source_kind = 'control_plane';

ALTER TABLE audit_exports DROP COLUMN object_lock;
ALTER TABLE audit_exports ADD COLUMN lock_strength TEXT NOT NULL DEFAULT 'none' CHECK (lock_strength IN ('none', 'bucket_rule', 'compliance'));
ALTER TABLE audit_exports ADD COLUMN probed_at TEXT;
ALTER TABLE audit_exports ADD COLUMN probe_detail TEXT;
ALTER TABLE audit_exports ADD COLUMN last_exported_hash TEXT CHECK (last_exported_hash IS NULL OR (length(last_exported_hash) = 64 AND last_exported_hash NOT GLOB '*[^0-9a-f]*'));
ALTER TABLE audit_exports ADD COLUMN last_error TEXT;

UPDATE meta SET value = '7' WHERE key = 'schema_version';
