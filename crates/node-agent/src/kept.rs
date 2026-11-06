// SPDX-License-Identifier: AGPL-3.0-only
//! Snapshots the node keeps across an agent restart (engine-trait.md section
//! 3.16, `node-agent-protocol.md` sections 6.13, 9 and 12).
//!
//! From M5b some volume snapshots must outlive the agent process that took
//! them: a warm source's base and the snapshot it is shipping, and later a
//! returning source's candidate copies and its quarantined copies. The engine
//! removes the snapshots left in its volume directory when it starts, so the
//! node agent records the ones it keeps in its store and hands their file
//! names to the engine before the engine starts ([`sweep_rule`]).
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
//! - **Delete before forgetting.** A snapshot is deleted through the engine
//!   before its record ([`release`]), and only an engine that answered `Ok`
//!   lets the record go. A crash in between leaves a record whose file is
//!   gone; the next [`release`] of it finishes the job, because the engine
//!   answers `Ok` for a snapshot that is already gone. Nothing forgets a
//!   record because a file lookup came back empty: a volume pool that did not
//!   mount, or a moved state directory, looks exactly like that.
//!
//! The first rule only holds for snapshots this store saw being created. A
//! store that is new, older than the table, or lost and created again cannot
//! tell its leftovers from a quarantined copy it never recorded. So the
//! sweep keeps every snapshot until the store **tracks** them: a mark in
//! `node_meta`, written once at an agent start where every snapshot file the
//! engine's sweep left is recorded ([`start_tracking`]).

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

/// The `node_meta` key that marks a store as tracking every snapshot the
/// node keeps; its value is `1`.
pub const TRACKED_KEY: &str = "kept_snapshots_tracked";

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

/// The file name the engine's sweep sees for a snapshot.
fn file_name(location: &Path) -> Option<String> {
    location
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
}

/// Which snapshots the engine's start-up sweep keeps: the file names the
/// store records (engine-trait.md section 3.16).
///
/// The engine starts before the node agent opens its store, so this reads
/// the store file directly, without the node-local key (the columns it reads
/// are not sealed). It returns [`SnapshotSweep::Keep`] only for a store that
/// tracks every snapshot ([`start_tracking`]), and every doubt keeps more,
/// never less: no store file, a store without the table or the mark, and a
/// store that cannot be opened or read all give [`SnapshotSweep::KeepAll`].
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
    match recorded_names(&path).await {
        Ok(Some(names)) => SnapshotSweep::Keep(names),
        Ok(None) => {
            info!(path = %path.display(), "the node store does not track every snapshot yet; the engine keeps every snapshot");
            SnapshotSweep::KeepAll
        }
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot read the kept snapshots; the engine keeps every snapshot");
            SnapshotSweep::KeepAll
        }
    }
}

/// The file names `kept_snapshots` records, or `None` when the store does
/// not track every snapshot: a table or the mark is missing, or a recorded
/// location has no file name the sweep could match.
async fn recorded_names(path: &Path) -> Result<Option<BTreeSet<String>>, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let mut connection = SqliteConnection::connect_with(&options).await?;
    let names = read_names(&mut connection).await;
    connection.close().await?;
    names
}

async fn read_names(
    connection: &mut SqliteConnection,
) -> Result<Option<BTreeSet<String>>, sqlx::Error> {
    let tables = sqlx::query(
        "SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' \
         AND name IN ('node_meta', 'kept_snapshots')",
    )
    .fetch_one(&mut *connection)
    .await?
    .try_get::<i64, _>("n")?;
    if tables != 2 {
        return Ok(None);
    }
    let tracked = sqlx::query("SELECT value FROM node_meta WHERE key = ?")
        .bind(TRACKED_KEY)
        .fetch_optional(&mut *connection)
        .await?
        .map(|row| row.try_get::<String, _>("value"))
        .transpose()?;
    if tracked.as_deref() != Some("1") {
        return Ok(None);
    }
    let rows = sqlx::query("SELECT location FROM kept_snapshots")
        .fetch_all(&mut *connection)
        .await?;
    let mut names = BTreeSet::new();
    for row in rows {
        let location: String = row.try_get("location")?;
        let Some(name) = file_name(Path::new(&location)) else {
            return Ok(None);
        };
        names.insert(name);
    }
    Ok(Some(names))
}

/// Marks the store as tracking every snapshot the node keeps, once no
/// snapshot file is left that it does not record, and returns whether it
/// tracks them now.
///
/// `at_start` is what the engine's start-up sweep left in the volume
/// directory (`ChEngine::snapshots_at_start`), or `None` when the engine has
/// no sweep or skipped it. Called at agent start, right after the store is
/// opened and before anything takes a snapshot. A file the store does not
/// record may be a quarantined copy from before the store was lost, so while
/// one is left the mark is not written and the sweep keeps every snapshot;
/// the files stay until an operator removes them.
///
/// # Errors
///
/// [`AgentError::Store`] when the records or the mark cannot be read or the
/// mark cannot be written.
pub async fn start_tracking(
    store: &NodeStore,
    at_start: Option<&BTreeSet<String>>,
) -> Result<bool, AgentError> {
    if store.meta(TRACKED_KEY).await?.as_deref() == Some("1") {
        return Ok(true);
    }
    let Some(at_start) = at_start else {
        info!("the engine did not report its snapshots; the start-up sweep keeps every snapshot");
        return Ok(false);
    };
    let recorded: BTreeSet<String> = store
        .kept_snapshots()
        .await?
        .iter()
        .filter_map(|record| file_name(&record.snapshot.location))
        .collect();
    let unknown: Vec<&String> = at_start.difference(&recorded).collect();
    if unknown.is_empty() {
        store.set_meta(TRACKED_KEY, "1").await?;
        info!("the node store tracks every snapshot; the start-up sweep removes the others");
        return Ok(true);
    }
    warn!(
        count = unknown.len(),
        first = %unknown[0],
        "snapshot files the node store does not record; the start-up sweep keeps every snapshot until they are removed"
    );
    Ok(false)
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

/// Deletes a kept snapshot: the file first, through the engine, then the
/// record.
///
/// The record goes only when the engine answered `Ok`. The engine answers
/// `Ok` for a snapshot that is already gone (section 3.16), so a release cut
/// short between its two steps is finished by releasing again; it answers an
/// error for a volume pool that did not mount or a file outside its volume
/// directory, and then the record stays.
///
/// # Errors
///
/// [`AgentError::Engine`] when the engine did not delete the file, and the
/// record stays; [`AgentError::Store`] when the record cannot be removed after
/// the file was.
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    use quoro_engine::{
        Engine, EngineError, FakeEngine, FakeOp, SnapshotSweep, VolumeId, VolumeSnapshot,
    };

    use super::{KeepPurpose, keep, release, start_tracking, sweep_rule};
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

    fn name(snapshot: &VolumeSnapshot) -> String {
        super::file_name(&snapshot.location).expect("a file name")
    }

    fn names<'a>(snapshots: impl IntoIterator<Item = &'a VolumeSnapshot>) -> BTreeSet<String> {
        snapshots.into_iter().map(name).collect()
    }

    /// An agent start: the engine sweeps with the store's rule, then the
    /// agent opens its store and marks it tracked when it can. Returns the
    /// rule the sweep used, the store and whether it tracks now.
    async fn agent_start(
        scratch: &Scratch,
        engine: &FakeEngine,
    ) -> (SnapshotSweep, NodeStore, bool) {
        let rule = sweep_rule(&scratch.0).await;
        let at_start = engine.start_up_sweep(&rule).expect("sweep");
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let tracked = start_tracking(&store, Some(&at_start))
            .await
            .expect("tracking");
        (rule, store, tracked)
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
    async fn an_empty_store_file_keeps_every_snapshot() {
        let scratch = Scratch::new("empty-file");
        std::fs::write(scratch.0.join("node.db"), b"").expect("write");
        assert_eq!(sweep_rule(&scratch.0).await, SnapshotSweep::KeepAll);
    }

    #[tokio::test]
    async fn a_store_from_before_the_table_keeps_every_snapshot() {
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
        assert_eq!(sweep_rule(&scratch.0).await, SnapshotSweep::KeepAll);

        // The upgrade's first start: no snapshot is left that the store does
        // not record, so it tracks from now on and the next sweep removes
        // what it does not record.
        let store = NodeStore::open(&scratch.0).await.expect("store");
        assert!(
            start_tracking(&store, Some(&BTreeSet::new()))
                .await
                .expect("tracking")
        );
        drop(store);
        assert_eq!(
            sweep_rule(&scratch.0).await,
            SnapshotSweep::Keep(BTreeSet::new())
        );
    }

    #[tokio::test]
    async fn without_the_engine_s_report_the_store_does_not_track() {
        let scratch = Scratch::new("no-report");
        let store = NodeStore::open(&scratch.0).await.expect("store");
        assert!(!start_tracking(&store, None).await.expect("tracking"));
        drop(store);
        assert_eq!(sweep_rule(&scratch.0).await, SnapshotSweep::KeepAll);
    }

    #[tokio::test]
    async fn the_rule_names_the_recorded_snapshots() {
        let scratch = Scratch::new("recorded");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let (rule, store, tracked) = agent_start(&scratch, &engine).await;
        assert_eq!(rule, SnapshotSweep::KeepAll, "a new store");
        assert!(tracked, "nothing unrecorded was left");
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
            SnapshotSweep::Keep(names([&base]))
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

    /// A record never moves to another file, because the sweep keeps it by
    /// its file name.
    #[tokio::test]
    async fn a_record_never_moves_to_another_file() {
        let scratch = Scratch::new("no-move");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let store = NodeStore::open(&scratch.0).await.expect("store");
        let base = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &base,
            KeepPurpose::WarmShipping,
            Some(1),
            None,
        )
        .await
        .expect("keep");
        // The same record with a new purpose and seq is an update.
        keep(
            &store,
            "app_01",
            &base,
            KeepPurpose::WarmBase,
            Some(1),
            None,
        )
        .await
        .expect("keep again");
        let mut moved = base.clone();
        moved.location = scratch.volumes().join("elsewhere");
        assert!(
            keep(
                &store,
                "app_01",
                &moved,
                KeepPurpose::WarmBase,
                Some(1),
                None
            )
            .await
            .is_err()
        );
        let records = store.kept_snapshots().await.expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].snapshot.location, base.location);
        assert_eq!(records[0].purpose, "warm_base");
    }

    /// Crash window A: the engine created a snapshot and the agent stopped
    /// before it recorded it. The next start sweeps the file away, the live
    /// volume is unchanged, and nothing relied on the lost snapshot.
    #[tokio::test]
    async fn a_snapshot_cut_short_before_its_record_is_swept_and_nothing_is_lost() {
        let scratch = Scratch::new("window-a");
        let (engine, volume) = engine_with_volume(&scratch, b"the live data").await;
        let live = engine.ensure_volume(&volume, 1).await.expect("volume").path;
        let (_, store, tracked) = agent_start(&scratch, &engine).await;
        assert!(tracked);
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

        let (rule, store, tracked) = agent_start(&scratch, &engine).await;
        assert_eq!(rule, SnapshotSweep::Keep(names([&base])));
        assert!(tracked);
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
        assert_eq!(store.kept_snapshots().await.expect("records").len(), 1);
    }

    /// Crash window B: the engine deleted a kept snapshot and the agent
    /// stopped before it forgot the record. Nothing forgets the record at
    /// start; the next release of it does, because the engine answers `Ok`
    /// for a snapshot that is already gone.
    #[tokio::test]
    async fn a_release_cut_short_is_finished_by_the_next_release() {
        let scratch = Scratch::new("window-b");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let (_, store, _) = agent_start(&scratch, &engine).await;
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

        let (rule, store, tracked) = agent_start(&scratch, &engine).await;
        assert!(tracked);
        assert_eq!(rule, SnapshotSweep::Keep(names([&old, &new])));
        assert_eq!(
            store.kept_snapshots().await.expect("records").len(),
            2,
            "a start forgets no record"
        );
        assert!(new.location.exists(), "the newer base is untouched");

        release(&engine, &store, &old).await.expect("release");
        let records = store.kept_snapshots().await.expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].snapshot.snapshot_id, new.snapshot_id);
    }

    /// An engine that cannot delete the file (a volume pool that did not
    /// mount, a file outside its volume directory) keeps the record, so the
    /// next sweep still keeps the file.
    #[tokio::test]
    async fn a_release_the_engine_refuses_keeps_the_record() {
        let scratch = Scratch::new("refused");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let (_, store, _) = agent_start(&scratch, &engine).await;
        let copy = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &copy,
            KeepPurpose::WarmBase,
            Some(1),
            None,
        )
        .await
        .expect("keep");
        engine.fail_next(
            FakeOp::DeleteVolumeSnapshot,
            EngineError::Unsupported("the volume pool did not mount".into()),
        );
        assert!(release(&engine, &store, &copy).await.is_err());
        assert!(copy.location.exists());
        assert_eq!(store.kept_snapshots().await.expect("records").len(), 1);
        drop(store);
        assert_eq!(
            sweep_rule(&scratch.0).await,
            SnapshotSweep::Keep(names([&copy]))
        );
    }

    /// A store lost and created again cannot tell its leftovers from a
    /// quarantined copy it never recorded: the sweep keeps every snapshot on
    /// every start while such a file is left, not only on the first one, and
    /// tracks again once it is gone.
    #[tokio::test]
    async fn after_the_store_is_lost_every_snapshot_is_kept_until_the_unknown_ones_are_gone() {
        let scratch = Scratch::new("store-lost");
        let (engine, volume) = engine_with_volume(&scratch, b"data").await;
        let (_, store, tracked) = agent_start(&scratch, &engine).await;
        assert!(tracked);
        let quarantine = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &quarantine,
            KeepPurpose::WarmBase,
            Some(1),
            None,
        )
        .await
        .expect("keep");
        drop(store);
        for file in ["node.db", "node.db-wal", "node.db-shm"] {
            let _ = std::fs::remove_file(scratch.0.join(file));
        }

        // Start 1: no store at all.
        let (rule, store, tracked) = agent_start(&scratch, &engine).await;
        assert_eq!(rule, SnapshotSweep::KeepAll);
        assert!(!tracked, "a snapshot the new store does not record is left");
        // The node keeps a new base after the loss.
        let base = engine.snapshot_volume(&volume).await.expect("snapshot");
        keep(
            &store,
            "app_01",
            &base,
            KeepPurpose::WarmBase,
            Some(2),
            None,
        )
        .await
        .expect("keep");
        drop(store);

        // Start 2: the store exists now, and still keeps everything.
        let (rule, store, tracked) = agent_start(&scratch, &engine).await;
        assert_eq!(rule, SnapshotSweep::KeepAll);
        assert!(!tracked);
        assert!(
            quarantine.location.exists(),
            "the unknown copy survives start 2"
        );
        assert!(base.location.exists());

        // An operator removes the unknown copy; the next start tracks again.
        engine
            .delete_volume_snapshot(&quarantine)
            .await
            .expect("remove the unknown copy");
        drop(store);
        let (_, store, tracked) = agent_start(&scratch, &engine).await;
        assert!(tracked, "every snapshot left is recorded");
        drop(store);
        assert_eq!(
            sweep_rule(&scratch.0).await,
            SnapshotSweep::Keep(names([&base]))
        );
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
