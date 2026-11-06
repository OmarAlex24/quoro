// SPDX-License-Identifier: AGPL-3.0-only
//! Snapshots the node keeps across an agent restart (engine-trait.md section
//! 3.16, `node-agent-protocol.md` sections 6.13, 9 and 12).
//!
//! From M5b some volume snapshots must outlive the agent process that took
//! them: a warm source's base and the snapshot it is shipping, and later a
//! returning source's candidate copies and its quarantined copies. The engine
//! removes the snapshots left in its volume directory when it starts, so the
//! node agent records the ones it keeps in its store and hands their ids to
//! the engine before the engine starts ([`sweep_rule`]).
//!
//! The engine names a snapshot when it creates it, so a record cannot exist
//! before its file does. What keeps the sweep safe is the order around the
//! record instead:
//!
//! - **Keep before relying.** A snapshot is recorded ([`keep`]) right after
//!   the engine created it and before anything relies on it: before a base
//!   moves to it, before a live volume file it copies is replaced, before a
//!   stream of it is answered `complete`. A snapshot file with no record is
//!   therefore one whose creation was cut short, and the live volume still
//!   holds everything it holds, so the sweep may remove it.
//! - **Delete before forgetting.** A snapshot is deleted before its record
//!   ([`release`]). A crash in between leaves a record whose file is gone;
//!   [`forget_missing`] drops it at the next start, and whatever relied on it
//!   treats it as gone (a warm source without a base ships a full copy).

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use quoro_engine::{Engine, SnapshotSweep, VolumeSnapshot};
use sqlx::Connection as _;
use sqlx::Row as _;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode};
use tracing::{info, warn};

use crate::AgentError;
use crate::store::{KeptSnapshotRecord, NodeStore};

/// The file name of the node store inside the data directory.
const STORE_FILE: &str = "node.db";

/// Why the node keeps a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepPurpose {
    /// A warm source's base: the newest copy the target confirmed (section
    /// 6.13).
    WarmBase,
    /// The snapshot a warm source is shipping (section 6.13).
    WarmShipping,
}

impl KeepPurpose {
    /// The value of the store's `purpose` column.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WarmBase => "warm_base",
            Self::WarmShipping => "warm_shipping",
        }
    }
}

/// Which snapshots the engine's start-up sweep keeps: the ids the store
/// records (engine-trait.md section 3.16).
///
/// The engine starts before the node agent opens its store, so this reads
/// the store file directly, without the node-local key (the column it reads
/// is not sealed). Every doubt keeps more, never less:
///
/// - no store file: [`SnapshotSweep::KeepAll`], because a node that lost its
///   store cannot tell a quarantined copy from a leftover;
/// - a store from before this table existed: nothing was ever recorded, so
///   [`SnapshotSweep::Keep`] of an empty set, which removes the leftovers as
///   before;
/// - a store that cannot be opened or read: [`SnapshotSweep::KeepAll`].
pub async fn sweep_rule(data_dir: &Path) -> SnapshotSweep {
    let path = data_dir.join(STORE_FILE);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!(path = %path.display(), "no node store yet; the engine keeps every snapshot");
            return SnapshotSweep::KeepAll;
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot read the node store; the engine keeps every snapshot");
            return SnapshotSweep::KeepAll;
        }
    }
    match recorded_ids(&path).await {
        Ok(ids) => SnapshotSweep::Keep(ids),
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot read the kept snapshots; the engine keeps every snapshot");
            SnapshotSweep::KeepAll
        }
    }
}

/// The ids of `kept_snapshots`, or an empty set when the table does not
/// exist yet.
async fn recorded_ids(path: &Path) -> Result<BTreeSet<String>, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let table = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'kept_snapshots'",
    )
    .fetch_optional(&mut connection)
    .await?;
    let ids = if table.is_some() {
        let rows = sqlx::query("SELECT snapshot_id FROM kept_snapshots")
            .fetch_all(&mut connection)
            .await?;
        rows.into_iter()
            .map(|row| row.try_get::<String, _>("snapshot_id"))
            .collect::<Result<BTreeSet<_>, _>>()?
    } else {
        BTreeSet::new()
    };
    connection.close().await?;
    Ok(ids)
}

/// Records a snapshot the engine just created, so the next start-up sweep
/// keeps it. The caller relies on the snapshot only once this returned `Ok`.
///
/// # Errors
///
/// [`AgentError::Store`] when the record cannot be written; the caller then
/// must not rely on the snapshot, and should delete it.
pub async fn keep(
    store: &NodeStore,
    app_id: &str,
    snapshot: &VolumeSnapshot,
    purpose: KeepPurpose,
    seq: Option<u64>,
    cells: Option<Vec<u8>>,
) -> Result<(), AgentError> {
    store
        .save_kept_snapshot(&KeptSnapshotRecord {
            app_id: app_id.to_owned(),
            snapshot: snapshot.clone(),
            purpose: purpose.as_str().to_owned(),
            seq,
            cells,
        })
        .await
}

/// Deletes a kept snapshot: the file first, then the record.
///
/// # Errors
///
/// The engine's error when the file cannot be deleted, and the record stays;
/// [`AgentError::Store`] when the record cannot be removed after the file
/// was, which [`forget_missing`] cleans up at the next start.
pub async fn release<E: Engine>(
    engine: &E,
    store: &NodeStore,
    snapshot: &VolumeSnapshot,
) -> Result<(), AgentError> {
    engine
        .delete_volume_snapshot(snapshot)
        .await
        .map_err(|e| AgentError::Engine(e.to_string()))?;
    store.forget_kept_snapshot(&snapshot.snapshot_id).await
}

/// Forgets every record whose snapshot file is gone, and returns them.
///
/// Called at agent start, after the engine's start-up sweep. A record whose
/// file is missing comes from a crash between the two steps of [`release`],
/// or from a file someone removed by hand; either way the copy is gone. A
/// file whose state cannot be read keeps its record.
///
/// # Errors
///
/// [`AgentError::Store`] when the records cannot be read or one cannot be
/// removed.
pub async fn forget_missing(store: &NodeStore) -> Result<Vec<KeptSnapshotRecord>, AgentError> {
    let mut forgotten = Vec::new();
    for record in store.kept_snapshots().await? {
        let location = &record.snapshot.location;
        match std::fs::symlink_metadata(location) {
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                warn!(
                    snapshot_id = %record.snapshot.snapshot_id,
                    error = %e,
                    "cannot check a kept snapshot's file; keeping its record"
                );
                continue;
            }
        }
        warn!(
            snapshot_id = %record.snapshot.snapshot_id,
            volume_id = %record.snapshot.volume_id.0,
            purpose = %record.purpose,
            "a kept snapshot's file is gone; forgetting it"
        );
        store
            .forget_kept_snapshot(&record.snapshot.snapshot_id)
            .await?;
        forgotten.push(record);
    }
    Ok(forgotten)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use quoro_engine::{Engine, FakeEngine, SnapshotSweep, VolumeId};

    use super::{KeepPurpose, forget_missing, keep, release, sweep_rule};
    use crate::store::NodeStore;

    /// A scratch directory under the workspace `target/`, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target")
                .join("kept-tests")
                .join(format!("{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("volumes")).expect("scratch dir");
            Self(dir)
        }

        fn volumes(&self) -> PathBuf {
            self.0.join("volumes")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A fake engine with a 1 GiB volume holding `content` at offset 0.
    async fn engine_with_volume(scratch: &Scratch, content: &[u8]) -> (FakeEngine, VolumeId) {
        use std::io::Write as _;
        let engine = FakeEngine::new();
        engine.set_volume_dir(scratch.volumes());
        let volume = VolumeId("vol_01JD0KEPT00000000000000000".to_string());
        let info = engine.ensure_volume(&volume, 1).await.expect("volume");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&info.path)
            .expect("open");
        file.write_all(content).expect("write");
        (engine, volume)
    }

    fn head(path: &std::path::Path, len: usize) -> Vec<u8> {
        use std::io::Read as _;
        let mut buf = vec![0_u8; len];
        std::fs::File::open(path)
            .expect("open")
            .read_exact(&mut buf)
            .expect("read");
        buf
    }

    #[tokio::test]
    async fn without_a_store_file_the_engine_keeps_every_snapshot() {
        let scratch = Scratch::new("no-store");
        assert_eq!(sweep_rule(&scratch.0).await, SnapshotSweep::KeepAll);
    }

    #[tokio::test]
    async fn an_unreadable_store_keeps_every_snapshot() {
        let scratch = Scratch::new("garbage");
        std::fs::write(scratch.0.join("node.db"), b"not a sqlite file at all").expect("write");
        assert_eq!(sweep_rule(&scratch.0).await, SnapshotSweep::KeepAll);
    }

    #[tokio::test]
    async fn a_store_from_before_the_table_keeps_nothing() {
        use sqlx::Connection as _;
        let scratch = Scratch::new("old-store");
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(scratch.0.join("node.db"))
            .create_if_missing(true);
        let mut connection = sqlx::sqlite::SqliteConnection::connect_with(&options)
            .await
            .expect("connect");
        sqlx::query("CREATE TABLE node_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            .execute(&mut connection)
            .await
            .expect("create");
        connection.close().await.expect("close");
        assert_eq!(
            sweep_rule(&scratch.0).await,
            SnapshotSweep::Keep(std::collections::BTreeSet::new())
        );
    }

    #[tokio::test]
    async fn the_rule_names_the_recorded_snapshots() {
        let scratch = Scratch::new("recorded");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let base = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &base,
            KeepPurpose::WarmBase,
            Some(3),
            Some(vec![7; 32]),
        )
        .await
        .expect("keep");
        drop(store);

        assert_eq!(
            sweep_rule(&scratch.0).await,
            SnapshotSweep::Keep([base.snapshot_id.clone()].into_iter().collect())
        );
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let records = store.kept_snapshots().await.expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].snapshot.snapshot_id, base.snapshot_id);
        assert_eq!(records[0].snapshot.location, base.location);
        assert_eq!(records[0].snapshot.size_bytes, base.size_bytes);
        assert_eq!(records[0].purpose, "warm_base");
        assert_eq!(records[0].seq, Some(3));
        assert_eq!(records[0].cells.as_deref(), Some(&[7_u8; 32][..]));
    }

    /// Crash window A: the engine created a snapshot and the agent stopped
    /// before it recorded it. The next start sweeps the file away, the live
    /// volume is unchanged, and nothing relied on the lost snapshot.
    #[tokio::test]
    async fn a_snapshot_cut_short_before_its_record_is_swept_and_nothing_is_lost() {
        let scratch = Scratch::new("window-a");
        let (engine, volume) = engine_with_volume(&scratch, b"the live data").await;
        let live = engine.ensure_volume(&volume, 1).await.expect("volume").path;
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let base = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &base,
            KeepPurpose::WarmBase,
            Some(1),
            None,
        )
        .await
        .expect("keep");
        // The agent stops here: the snapshot exists, its record does not.
        let cut_short = engine.snapshot_volume(&volume).await.expect("snapshot");
        drop(store);

        // Agent start: the engine sweeps with the recorded ids.
        engine
            .start_up_sweep(&sweep_rule(&scratch.0).await)
            .expect("sweep");
        assert!(base.location.exists(), "the recorded base is kept");
        assert!(
            !cut_short.location.exists(),
            "the unrecorded snapshot is swept"
        );
        assert_eq!(
            head(&live, 13),
            b"the live data",
            "the live volume still holds the data"
        );
        let store = NodeStore::open(&scratch.0).await.expect("store");
        assert!(forget_missing(&store).await.expect("forget").is_empty());
        assert_eq!(store.kept_snapshots().await.expect("records").len(), 1);
    }

    /// Crash window B: the engine deleted a kept snapshot and the agent
    /// stopped before it forgot the record. The next start forgets the
    /// record, so nothing relies on a copy that is gone.
    #[tokio::test]
    async fn a_record_whose_file_is_gone_is_forgotten_at_start() {
        let scratch = Scratch::new("window-b");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let old = engine.snapshot_volume(&volume).await.expect("snapshot");
        let new = engine.snapshot_volume(&volume).await.expect("snapshot");
        for (snapshot, seq) in [(&old, 1), (&new, 2)] {
            keep(
                &store,
                "app_01",
                snapshot,
                KeepPurpose::WarmBase,
                Some(seq),
                None,
            )
            .await
            .expect("keep");
        }
        // `release` of the old base, cut after its first step.
        engine
            .delete_volume_snapshot(&old)
            .await
            .expect("delete the file");
        drop(store);

        engine
            .start_up_sweep(&sweep_rule(&scratch.0).await)
            .expect("sweep");
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let forgotten = forget_missing(&store).await.expect("forget");
        assert_eq!(forgotten.len(), 1);
        assert_eq!(forgotten[0].snapshot.snapshot_id, old.snapshot_id);
        let records = store.kept_snapshots().await.expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].snapshot.snapshot_id, new.snapshot_id);
        assert!(new.location.exists(), "the newer base is untouched");
    }

    #[tokio::test]
    async fn release_deletes_the_file_before_the_record() {
        let scratch = Scratch::new("release");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let snapshot = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &snapshot,
            KeepPurpose::WarmShipping,
            Some(4),
            None,
        )
        .await
        .expect("keep");
        release(&engine, &store, &snapshot).await.expect("release");
        assert!(!snapshot.location.exists());
        assert!(store.kept_snapshots().await.expect("records").is_empty());
        // Releasing again is harmless: a missing file is Ok for the engine.
        release(&engine, &store, &snapshot).await.expect("release");
    }
}
