-- Migration 0011: warm copies (ADR 0172 to ADR 0174) and the quarantined
-- copies a failover leaves behind (ADR 0176).
--
-- apps.warm_node_id is the warm node of a `warm` app (openapi.yaml
-- `WarmCopy.node_id`): the node that keeps the second copy of the app's volume,
-- its prepared image and its standby VM (node-agent-protocol.md section 6.13).
-- NULL while no node is eligible and for any other HA policy. The scheduler
-- sets it unless an operator pinned it (apps.warm_node_pinned = 1, set by
-- `UpdateAppRequest.warm.node_id`). Only a reported `pause_copy` counts as no
-- longer snapshotting with reflink: a warm node whose nodes.volume_snapshot
-- is still NULL keeps its place, while picking a new one still needs
-- `reflink`. A released pin (`warm.node_id` null) keeps the warm node while
-- it is still eligible; otherwise the scheduler picks among the connected
-- eligible nodes (`WarmCopy.node_id`, ADR 0287). A failover or a switchover
-- swaps it with volumes.node_id in the transaction that grants the lease to
-- the warm node, so the old source becomes the warm node. A policy change away from `warm`
-- clears it and the pin.
--
-- apps.warm_interval_s is the number of seconds between two snapshots on the
-- source (`WarmCopy.interval_s`): 30 by default, 5 to 3600 (ADR 0173, measured
-- in docs/research/m5b-warm-and-leases.md). It is kept when the policy changes,
-- so an app that goes back to `warm` keeps its interval.
--
-- `App` has no field of its own for these three columns, so an app's
-- change-log payload carries them as `warm_node_id`, `warm_node_pinned` and
-- `warm_interval_s`, beside the `App` fields, and its `warm` is null
-- (db-schema.sql header, ADR 0286). A promoted primary that rebuilds apps from
-- the log keeps the warm node, the pin and the interval.
--
-- Nothing about the progress of a warm copy is stored: the primary keeps each
-- node's newest `status_report.warm` entries in memory (section 5.3), as it
-- does for edge backends, so a snapshot shipped every interval appends no
-- change-log entry.
--
-- volume_quarantines records the quarantined copies of a volume
-- (`Volume.quarantines`): the live file of a node that ran a `warm` app until
-- a failover moved its lease, kept on that node because it holds writes the
-- warm node never received (section 9). The node generates the `qrn_` id; the
-- primary writes the row the first time a status report lists the copy with
-- its `sha256`, in the transaction that writes the system audit record
-- `volume.quarantine` (audit-log-format.md section 3.8) and emits
-- `volume.quarantined`. deleteVolumeQuarantine sets deleting_at, so the copy
-- reads `deleting` and goes into `spec.quarantine_deletes`; the primary drops
-- the row once the node no longer reports the copy. removeNode drops the rows
-- of the node it removes in the same transaction, so a node that is gone for
-- good does not keep them forever; the files stay on its disk. deleteVolume
-- and deleteApp are refused with `has_quarantines` while a row names the
-- volume, so the foreign key to volumes never blocks the volume sweep of
-- migration 0009.
-- app_id has no foreign key, like volumes.app_id: it names the app the copy
-- belongs to for listing and for the event.

ALTER TABLE apps ADD COLUMN warm_node_id TEXT REFERENCES nodes (id);
ALTER TABLE apps ADD COLUMN warm_node_pinned INTEGER NOT NULL DEFAULT 0 CHECK (warm_node_pinned IN (0, 1));
ALTER TABLE apps ADD COLUMN warm_interval_s INTEGER NOT NULL DEFAULT 30 CHECK (warm_interval_s BETWEEN 5 AND 3600);

CREATE TABLE volume_quarantines (
    id          TEXT PRIMARY KEY,                              -- qrn_
    volume_id   TEXT NOT NULL REFERENCES volumes (id),
    app_id      TEXT NOT NULL,                                 -- no foreign key, as volumes.app_id
    node_id     TEXT NOT NULL REFERENCES nodes (id),           -- the node that keeps the copy
    size_bytes  INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256      TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    created_at  TEXT NOT NULL,                                 -- when the node created the copy
    deleting_at TEXT                                           -- set by deleteVolumeQuarantine
) STRICT;

CREATE INDEX idx_volume_quarantines_volume ON volume_quarantines (volume_id, created_at);
CREATE INDEX idx_volume_quarantines_node ON volume_quarantines (node_id);

UPDATE meta SET value = '12' WHERE key = 'schema_version';
