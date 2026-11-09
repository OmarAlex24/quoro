// SPDX-License-Identifier: AGPL-3.0-only
//! [`BackupManager`]: runs `spec.backups` and `spec.volumes[].restore` on this
//! node (`node-agent-protocol.md` sections 5.3, 6.7 and 12,
//! `backup-format.md` sections 7.1 and 8.1).
//!
//! The manager is a second loop beside the reconciler, like the build manager.
//! It never blocks a VM: each backup job and each restore runs in its own task,
//! and the apply loop only reads the rows this loop writes.
//!
//! What the contract fixes here:
//!
//! - One task per backup job, at most one per volume, never restarted once
//!   running or terminal (section 6.7).
//! - A job whose token is not the app's held token fails with
//!   `lease_mismatch` and writes nothing (`backup-format.md` section 7.1).
//! - A restore is never resumed: at agent start the staging directory is
//!   emptied and a restore starts from the first chunk again (section 6.7).
//! - Every status survives a reconnect in the local `backups` table
//!   (section 12), and a job the agent left non-terminal fails with
//!   `node agent restarted`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use quoro_backup::layout;
use quoro_backup::{BackupWriter, ChunkId, Manifest, Restorer, SourceKeys, chunks, open_manifest};
use quoro_engine::{Engine, SnapshotMethod, VolumeId, VolumeInfo, VolumeSnapshot};
use quoro_proto::wire::{
    BackupState, BackupStatus, Spec, SpecBackup, SpecBackupRepository, SpecRestore, SpecVolumeFile,
    SpecVolumeState, VolumeState,
};
use quoro_providers::object_store::{ObjectStore, ObjectStoreError};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::health::PausedVms;
use crate::lease::LeaseView;
use crate::reconcile::Snapshot;
use crate::store::{BackupRecord, NodeStore, VolumeRecord};
use crate::util;

use super::{
    GIB, RETRY_BASE, Repository, STAGING_DIR, TRANSPORT_RETRIES, object_key, one_line,
    retry_transport, source_keys,
};

/// How often a pass runs when nothing else wakes the manager (section 6.2 step
/// 5: on each snapshot and every two seconds).
const DEFAULT_TICK: Duration = Duration::from_secs(2);

/// The timings a pass and a job use; tests shorten them.
#[derive(Clone, Debug)]
pub struct BackupOptions {
    /// How often a pass runs when nothing else wakes the manager.
    pub tick: Duration,
    /// The first wait before a failed transport request is retried.
    pub retry_base: Duration,
    /// How many times a transport failure is retried.
    pub retries: u32,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            tick: DEFAULT_TICK,
            retry_base: RETRY_BASE,
            retries: TRANSPORT_RETRIES,
        }
    }
}

impl BackupOptions {
    /// The options a test uses: a short tick and no real back-off.
    #[cfg(test)]
    #[must_use]
    pub fn quick() -> Self {
        Self {
            tick: Duration::from_millis(10),
            retry_base: Duration::ZERO,
            retries: TRANSPORT_RETRIES,
        }
    }
}

/// Runs the node's backups and restores. See the module documentation.
pub struct BackupManager<E: Engine, R: Repository> {
    engine: Arc<E>,
    store: NodeStore,
    repository: Arc<R>,
    lease: LeaseView,
    /// `cls_` id of the cluster, the manifest's `cluster_id` (section 6.1).
    cluster_id: String,
    statuses: watch::Sender<Vec<BackupStatus>>,
    /// The rows the manager has loaded or written, by backup id.
    rows: HashMap<String, BackupRecord>,
    /// The snapshot the current pass acts on.
    current: Option<Snapshot>,
    /// Backup ids with a task that has not finished.
    running: HashSet<String>,
    /// Volume id to the backup id whose restore was started or finished here,
    /// so a failed restore is not retried inside one agent run (section 6.7).
    restores: HashMap<String, String>,
    /// The tasks a pass spawned; aborted when the manager stops.
    tasks: Vec<JoinHandle<()>>,
    options: BackupOptions,
    /// Task completions; taken by [`BackupManager::run`].
    events: Option<mpsc::UnboundedReceiver<BackupRecord>>,
    done: mpsc::UnboundedSender<BackupRecord>,
    /// The pause marks shared with the reconciler: a `pause_copy` snapshot
    /// marks the VM it pauses (D-0259).
    paused: PausedVms,
}

impl<E: Engine + 'static, R: Repository> BackupManager<E, R> {
    /// Builds the manager and the channel `status_report.backups` reads.
    #[must_use]
    pub fn new(
        engine: Arc<E>,
        store: NodeStore,
        repository: Arc<R>,
        lease: LeaseView,
        cluster_id: String,
    ) -> (Self, watch::Receiver<Vec<BackupStatus>>) {
        let (statuses, statuses_rx) = watch::channel(Vec::new());
        let (done, events) = mpsc::unbounded_channel();
        (
            Self {
                engine,
                store,
                repository,
                lease,
                cluster_id,
                statuses,
                rows: HashMap::new(),
                current: None,
                running: HashSet::new(),
                restores: HashMap::new(),
                tasks: Vec::new(),
                options: BackupOptions::default(),
                events: Some(events),
                done,
                paused: PausedVms::default(),
            },
            statuses_rx,
        )
    }

    /// Replaces the timings; tests shorten the tick and the back-off.
    #[must_use]
    pub fn with_options(mut self, options: BackupOptions) -> Self {
        self.options = options;
        self
    }

    /// Shares the pause marks with the reconciler, so the VM a `pause_copy`
    /// snapshot pauses is neither counted failing nor restarted for its
    /// health during the pause (D-0259).
    #[must_use]
    pub fn with_paused_vms(mut self, paused: PausedVms) -> Self {
        self.paused = paused;
        self
    }

    /// Fails what a previous agent left non-terminal, deletes leftover
    /// snapshots, then runs passes until `shutdown` resolves.
    pub async fn run(
        mut self,
        mut snapshots: watch::Receiver<Option<Snapshot>>,
        shutdown: impl Future<Output = ()> + Send,
    ) {
        let mut shutdown = std::pin::pin!(shutdown);
        self.startup().await;
        self.current = snapshots.borrow_and_update().clone();
        let Some(mut events) = self.events.take() else {
            return;
        };
        let mut ticker = tokio::time::interval(self.options.tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        self.reconcile().await;
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                changed = snapshots.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.current = snapshots.borrow_and_update().clone();
                    self.reconcile().await;
                }
                _ = ticker.tick() => {
                    self.reconcile().await;
                }
                event = events.recv() => {
                    match event {
                        Some(record) => self.on_event(record).await,
                        // Every task holds the sender, so this only happens when
                        // none is left and the manager holds the last one.
                        None => break,
                    }
                }
            }
        }
        for task in std::mem::take(&mut self.tasks) {
            task.abort();
        }
        info!("backup manager stopped");
    }

    /// Section 12: a job the agent left non-terminal fails with
    /// `node agent restarted`, and its snapshot file is deleted. A restore is
    /// never resumed, so the staging directory is emptied (section 6.7).
    pub async fn startup(&mut self) {
        if let Err(error) = empty_staging(&self.store.data_dir().join(STAGING_DIR)) {
            warn!(error = %error, "could not empty the restore staging directory");
        }
        let rows = match self.store.backups().await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "could not read the backup rows at start-up");
                Vec::new()
            }
        };
        for mut row in rows {
            if is_terminal(row.status) {
                self.rows.insert(row.backup_id.clone(), row);
                continue;
            }
            if let Some(path) = row.snapshot_path.take() {
                remove_file(Path::new(&path));
            }
            row.status = BackupState::Failed;
            row.error = Some("node agent restarted".to_owned());
            row.finished_at = Some(util::now_rfc3339());
            if let Err(e) = self.store.save_backup(&row).await {
                warn!(backup_id = %row.backup_id, error = %e, "could not fail an interrupted backup");
            }
            self.rows.insert(row.backup_id.clone(), row);
        }
    }

    /// One pass: start the jobs that are neither running nor terminal, drop
    /// the rows of jobs that left the spec, start pending restores, and
    /// publish the status report (section 6.2 step 5).
    pub async fn reconcile(&mut self) {
        let Some(snapshot) = self.current.clone() else {
            self.refresh_statuses();
            return;
        };
        let spec = &snapshot.state.spec;
        let desired: BTreeSet<String> = spec
            .backups
            .iter()
            .map(|job| job.backup_id.clone())
            .collect();

        self.reap();

        // A job the primary stopped naming leaves the table with its row
        // (section 6.7); a task still uploading keeps its row until it ends.
        for row in self.store.backups().await.unwrap_or_default() {
            if desired.contains(&row.backup_id) || self.running.contains(&row.backup_id) {
                continue;
            }
            if let Err(e) = self.store.delete_backup(&row.backup_id).await {
                warn!(backup_id = %row.backup_id, error = %e, "could not forget a backup row");
            }
            self.rows.remove(&row.backup_id);
        }

        for job in &spec.backups {
            if self.running.contains(&job.backup_id) {
                continue;
            }
            // One backup per volume at a time (section 7.1 step 1): the engine
            // allows one snapshot per volume, so a second job waits a pass.
            let busy = self
                .rows
                .values()
                .any(|row| row.volume_id == job.volume_id && self.running.contains(&row.backup_id));
            if busy {
                continue;
            }
            let known = match self.rows.get(&job.backup_id) {
                Some(row) => Some(row.clone()),
                None => self.store.backup(&job.backup_id).await.ok().flatten(),
            };
            if let Some(row) = known {
                let terminal = is_terminal(row.status);
                self.rows.insert(row.backup_id.clone(), row);
                if terminal {
                    continue;
                }
            }
            self.start_job(job, spec.backup_repository.as_ref()).await;
        }

        self.reconcile_restores(spec).await;
        self.refresh_statuses();
    }

    /// The `status_report.backups[]` entries of the current spec (section 5.3).
    fn refresh_statuses(&self) {
        let mut statuses: Vec<BackupStatus> = match &self.current {
            Some(snapshot) => snapshot
                .state
                .spec
                .backups
                .iter()
                .filter_map(|job| self.rows.get(&job.backup_id))
                .map(BackupRecord::to_status)
                .collect(),
            None => Vec::new(),
        };
        statuses.sort_by(|a, b| a.backup_id.cmp(&b.backup_id));
        let _ = self.statuses.send_replace(statuses);
    }

    /// Records a job's change and publishes it when the spec still names it.
    async fn on_event(&mut self, record: BackupRecord) {
        if is_terminal(record.status) {
            self.running.remove(&record.backup_id);
        }
        let named = self.current.as_ref().is_some_and(|snapshot| {
            snapshot
                .state
                .spec
                .backups
                .iter()
                .any(|job| job.backup_id == record.backup_id)
        });
        if named {
            self.rows.insert(record.backup_id.clone(), record);
        }
        self.refresh_statuses();
    }

    /// Drops the handles of finished tasks.
    fn reap(&mut self) {
        self.tasks.retain(|task| !task.is_finished());
    }

    /// Writes the `queued` row and spawns the job's task (section 6.7).
    async fn start_job(&mut self, job: &SpecBackup, repository: Option<&SpecBackupRepository>) {
        let record = BackupRecord {
            backup_id: job.backup_id.clone(),
            app_id: job.app_id.clone(),
            volume_id: job.volume_id.clone(),
            status: BackupState::Queued,
            snapshot_method: None,
            pause_ms: None,
            manifest_key: None,
            manifest_digest: None,
            logical_bytes: None,
            uploaded_bytes: None,
            chunk_count: None,
            error: None,
            started_at: None,
            finished_at: None,
            snapshot_path: None,
        };
        if let Err(e) = self.store.save_backup(&record).await {
            warn!(backup_id = %job.backup_id, error = %e, "could not record a queued backup");
        }
        self.rows.insert(job.backup_id.clone(), record);
        self.running.insert(job.backup_id.clone());
        let task = tokio::spawn(run_job(
            JobContext {
                engine: Arc::clone(&self.engine),
                store: self.store.clone(),
                repository: Arc::clone(&self.repository),
                cluster_id: self.cluster_id.clone(),
                options: self.options.clone(),
                done: self.done.clone(),
                paused: self.paused.clone(),
            },
            self.lease.clone(),
            job.clone(),
            repository.cloned(),
        ));
        self.tasks.push(task);
    }

    /// Starts every restore the spec asks for that has not started in this run
    /// (`backup-format.md` section 8.1).
    async fn reconcile_restores(&mut self, spec: &Spec) {
        // Forget the volumes the spec no longer names.
        let pending: HashSet<&str> = spec
            .volumes
            .iter()
            .filter(|entry| entry.restore.is_some())
            .map(|entry| entry.volume_id.as_str())
            .collect();
        self.restores
            .retain(|volume_id, _| pending.contains(volume_id.as_str()));

        for entry in &spec.volumes {
            if entry.state != SpecVolumeState::Present {
                continue;
            }
            let Some(restore) = &entry.restore else {
                continue;
            };
            if self
                .restores
                .get(&entry.volume_id)
                .is_some_and(|backup_id| *backup_id == restore.backup_id)
            {
                continue;
            }
            let row = self.store.volume(&entry.volume_id).await.ok().flatten();
            if row
                .as_ref()
                .is_some_and(|row| row.state == VolumeState::Ready && row.size_gb == entry.size_gb)
            {
                // The file is already complete; never write over it again.
                self.restores
                    .insert(entry.volume_id.clone(), restore.backup_id.clone());
                continue;
            }
            self.restores
                .insert(entry.volume_id.clone(), restore.backup_id.clone());
            let Some(repository) = spec.backup_repository.as_ref() else {
                record_volume(
                    &self.store,
                    entry,
                    VolumeState::Failed,
                    Some("no backup repository in the desired state"),
                    None,
                )
                .await;
                continue;
            };
            let task = tokio::spawn(run_restore(
                Arc::clone(&self.engine),
                self.store.clone(),
                Arc::clone(&self.repository),
                entry.clone(),
                restore.clone(),
                repository.clone(),
                self.options.clone(),
            ));
            self.tasks.push(task);
        }
    }
}

/// Whether a status is terminal, so the job is never started again
/// (section 6.7).
fn is_terminal(status: BackupState) -> bool {
    matches!(status, BackupState::Succeeded | BackupState::Failed)
}

/// The wire form of an engine snapshot method (section 5.3).
fn wire_method(method: SnapshotMethod) -> quoro_proto::wire::SnapshotMethod {
    match method {
        SnapshotMethod::Reflink => quoro_proto::wire::SnapshotMethod::Reflink,
        SnapshotMethod::PauseCopy => quoro_proto::wire::SnapshotMethod::PauseCopy,
    }
}

/// What one backup uploaded and wrote.
struct Written {
    /// The chunks the manifest lists.
    manifest: Manifest,
    /// BLAKE3 of the encrypted manifest object.
    digest: String,
    /// Bytes of chunk objects this backup created.
    uploaded: u64,
}

/// What one job task shares with the manager: the engine, the store, how to
/// open the bucket, and where to report.
struct JobContext<E: Engine, R: Repository> {
    engine: Arc<E>,
    store: NodeStore,
    repository: Arc<R>,
    /// `cls_` id of the cluster, the manifest's `cluster_id` (section 6.1).
    cluster_id: String,
    options: BackupOptions,
    done: mpsc::UnboundedSender<BackupRecord>,
    /// The pause marks shared with the reconciler (D-0259).
    paused: PausedVms,
}

/// One backup job: check the lease, snapshot, chunk, upload, write the
/// manifest, delete the snapshot (`backup-format.md` section 7.1).
async fn run_job<E: Engine, R: Repository>(
    context: JobContext<E, R>,
    lease: LeaseView,
    job: SpecBackup,
    repo: Option<SpecBackupRepository>,
) {
    let JobContext {
        engine,
        store,
        repository,
        cluster_id,
        options,
        done,
        paused,
    } = context;
    let mut record = BackupRecord {
        backup_id: job.backup_id.clone(),
        app_id: job.app_id.clone(),
        volume_id: job.volume_id.clone(),
        status: BackupState::Queued,
        snapshot_method: None,
        pause_ms: None,
        manifest_key: None,
        manifest_digest: None,
        logical_bytes: None,
        uploaded_bytes: None,
        chunk_count: None,
        error: None,
        started_at: Some(util::now_rfc3339()),
        finished_at: None,
        snapshot_path: None,
    };

    // Section 7.1 step 2: the job's token must be the app's held token, or the
    // node does nothing else.
    if !lease.holds_app_lease(&job.app_id, job.fencing_token) {
        finish_failed(&store, &done, record, "lease_mismatch").await;
        return;
    }
    let volume_ready = store
        .volume(&job.volume_id)
        .await
        .ok()
        .flatten()
        .is_some_and(|row| row.state == VolumeState::Ready);
    if !volume_ready {
        finish_failed(
            &store,
            &done,
            record,
            "the volume is not ready on this node",
        )
        .await;
        return;
    }
    let Some(repo) = repo else {
        finish_failed(
            &store,
            &done,
            record,
            "no backup repository in the desired state",
        )
        .await;
        return;
    };
    let keys = match source_keys(&job.id_key, &job.data_key) {
        Ok(keys) => keys,
        Err(reason) => {
            finish_failed(&store, &done, record, &reason).await;
            return;
        }
    };
    let client = match repository.open(&repo) {
        Ok(client) => client,
        Err(reason) => {
            finish_failed(&store, &done, record, &reason).await;
            return;
        }
    };

    // Section 7.1 step 3: take the snapshot, then report `running` with its
    // method and, for `pause_copy`, the pause. A `pause_copy` snapshot pauses
    // the VM that attaches the volume for the whole copy while the engine
    // still reports it running, so the volume is marked for that time: the
    // health check of the VM that attaches it neither counts the pause nor
    // restarts it (D-0259). The volume is marked, not the job's `vm_id`: the
    // engine pauses whichever VM attaches the volume when it looks, which a
    // redeploy or a stale VM row can make another one (#524).
    let mark = (engine.capabilities().await.volume_snapshot_method
        == Some(SnapshotMethod::PauseCopy))
    .then(|| paused.mark(&job.volume_id));
    let snapshot = engine
        .snapshot_volume(&VolumeId(job.volume_id.clone()))
        .await;
    drop(mark);
    let snapshot = match snapshot {
        Ok(snapshot) => snapshot,
        Err(e) => {
            finish_failed(
                &store,
                &done,
                record,
                &format!("could not snapshot the volume: {e}"),
            )
            .await;
            return;
        }
    };
    record.status = BackupState::Running;
    record.snapshot_method = Some(wire_method(snapshot.method));
    record.pause_ms = (snapshot.method == SnapshotMethod::PauseCopy)
        .then(|| u64::try_from(snapshot.pause.as_millis()).unwrap_or(u64::MAX));
    record.snapshot_path = Some(snapshot.location.display().to_string());
    save(&store, &done, record.clone()).await;

    let header = Manifest {
        format: quoro_backup::FORMAT,
        backup_id: job.backup_id.clone(),
        cluster_id,
        repository_id: repo.repository_id.clone(),
        source_kind: quoro_backup::SourceKind::Volume,
        source_id: job.app_id.clone(),
        volume_id: Some(job.volume_id.clone()),
        fencing_token: Some(job.fencing_token),
        snapshot_method: Some(match snapshot.method {
            SnapshotMethod::Reflink => "reflink".to_owned(),
            SnapshotMethod::PauseCopy => "pause_copy".to_owned(),
        }),
        logical_bytes: 0,
        chunker: quoro_backup::Chunker::fixed(),
        chunks: Vec::new(),
        quoro_version: env!("CARGO_PKG_VERSION").to_owned(),
        created_at: util::now_rfc3339(),
    };
    let outcome = upload_snapshot(&snapshot, &keys, &*client, &repo.root, header, &options).await;

    // Section 7.1 step 4: the snapshot file is deleted whatever happened.
    if let Err(e) = engine.delete_volume_snapshot(&snapshot).await {
        warn!(backup_id = %job.backup_id, error = ?e, "could not delete the volume snapshot");
    }
    record.snapshot_path = None;

    match outcome {
        Ok(written) => {
            record.status = BackupState::Succeeded;
            record.manifest_key = Some(layout::manifest(&job.app_id, &job.backup_id));
            record.manifest_digest = Some(written.digest);
            record.logical_bytes = Some(snapshot.size_bytes);
            record.uploaded_bytes = Some(written.uploaded);
            record.chunk_count = Some(written.manifest.chunks.len() as u64);
            record.finished_at = Some(util::now_rfc3339());
            save(&store, &done, record).await;
        }
        Err(reason) => finish_failed(&store, &done, record, &reason).await,
    }
}

/// Chunks and uploads one snapshot and writes its manifest (section 7.1 step
/// 4).
async fn upload_snapshot<S: ObjectStore>(
    snapshot: &VolumeSnapshot,
    keys: &SourceKeys,
    client: &S,
    root: &str,
    header: Manifest,
    options: &BackupOptions,
) -> Result<Written, String> {
    let file = std::fs::File::open(&snapshot.location)
        .map_err(|e| format!("could not open the snapshot: {e}"))?;
    let mut writer = BackupWriter::new(keys.clone(), header);
    let mut uploaded: u64 = 0;

    // One chunk at a time, so the whole volume is never held in memory
    // (section 5).
    for chunk in chunks(file) {
        let chunk = chunk.map_err(|e| format!("could not read the snapshot: {e}"))?;
        let (id, object, _zero) = writer.push(chunk);
        let size = object.len() as u64;
        let key = object_key(root, &layout::chunk(&id));
        match retry_transport(options.retries, options.retry_base, || {
            client.put_new(&key, object.clone(), None)
        })
        .await
        {
            // The size is counted only for a chunk this backup created.
            Ok(_) => uploaded = uploaded.saturating_add(size),
            // `412` means the chunk is already in the repository and counts as
            // present, not uploaded (section 5).
            Err(ObjectStoreError::AlreadyExists) => {}
            Err(e) => return Err(format!("could not upload a chunk: {e}")),
        }
    }

    let (manifest, object, digest) = writer
        .finish()
        .map_err(|e| format!("could not write the manifest: {e}"))?;
    if manifest.logical_bytes != snapshot.size_bytes {
        return Err(format!(
            "the snapshot is {} bytes, not the {} the chunks cover",
            snapshot.size_bytes, manifest.logical_bytes
        ));
    }
    let key = object_key(
        root,
        &layout::manifest(&manifest.source_id, &manifest.backup_id),
    );
    match retry_transport(options.retries, options.retry_base, || {
        client.put_new(&key, object.clone(), None)
    })
    .await
    {
        Ok(_) => {}
        Err(ObjectStoreError::AlreadyExists) => {
            return Err("the manifest object already exists".to_owned());
        }
        Err(e) => return Err(format!("could not upload the manifest: {e}")),
    }
    Ok(Written {
        manifest,
        digest,
        uploaded,
    })
}

/// Writes a backup row and tells the manager (section 12).
async fn save(store: &NodeStore, done: &mpsc::UnboundedSender<BackupRecord>, record: BackupRecord) {
    if let Err(e) = store.save_backup(&record).await {
        warn!(backup_id = %record.backup_id, error = %e, "could not record a backup");
    }
    let _ = done.send(record);
}

/// Marks a job failed with a one-line reason and records it (section 5.3).
async fn finish_failed(
    store: &NodeStore,
    done: &mpsc::UnboundedSender<BackupRecord>,
    mut record: BackupRecord,
    reason: &str,
) {
    record.status = BackupState::Failed;
    record.error = Some(one_line(reason));
    record.finished_at = Some(util::now_rfc3339());
    record.manifest_key = None;
    record.manifest_digest = None;
    record.logical_bytes = None;
    record.uploaded_bytes = None;
    record.chunk_count = None;
    save(store, done, record).await;
}

/// One restore: write the staged file, check every object, import it
/// (`backup-format.md` section 8.1).
async fn run_restore<E: Engine, R: Repository>(
    engine: Arc<E>,
    store: NodeStore,
    repository: Arc<R>,
    entry: SpecVolumeFile,
    restore: SpecRestore,
    repo: SpecBackupRepository,
    options: BackupOptions,
) {
    let staging = store.data_dir().join(STAGING_DIR);
    if let Err(error) = create_staging(&staging) {
        record_volume(
            &store,
            &entry,
            VolumeState::Failed,
            Some(&one_line(&error.to_string())),
            None,
        )
        .await;
        return;
    }
    let temp = staging.join(format!("{}.restore", entry.volume_id));
    record_volume(&store, &entry, VolumeState::Restoring, None, None).await;

    let outcome = restore_into(
        &*engine,
        &*repository,
        &repo,
        &restore,
        &entry,
        &temp,
        &options,
    )
    .await;
    match outcome {
        Ok(info) => {
            record_volume(
                &store,
                &entry,
                VolumeState::Ready,
                None,
                Some(info.allocated_bytes),
            )
            .await;
        }
        Err(reason) => {
            // Section 8.1: a failed check stops the restore and the partial
            // file is deleted.
            remove_file(&temp);
            record_volume(
                &store,
                &entry,
                VolumeState::Failed,
                Some(&one_line(&reason)),
                None,
            )
            .await;
        }
    }
}

/// The work of one restore, up to `import_volume`.
async fn restore_into<E: Engine, R: Repository>(
    engine: &E,
    repository: &R,
    repo: &SpecBackupRepository,
    restore: &SpecRestore,
    entry: &SpecVolumeFile,
    temp: &Path,
    options: &BackupOptions,
) -> Result<VolumeInfo, String> {
    let size = u64::from(entry.size_gb) * GIB;
    // Section 8.1: the chunks must cover the volume exactly, and a snapshot is
    // the whole file, so every length is the volume's size.
    if restore.logical_bytes != size {
        return Err("the restore's logical_bytes do not match the volume size".to_owned());
    }
    let keys = source_keys(&restore.id_key, &restore.data_key)?;
    let client = repository.open(repo)?;

    let key = object_key(&repo.root, &restore.manifest_key);
    let object = retry_transport(options.retries, options.retry_base, || client.get(&key))
        .await
        .map_err(|e| format!("could not read the manifest: {e}"))?;
    let manifest = open_manifest(
        &keys,
        &restore.backup_id,
        &entry.app_id,
        &restore.manifest_digest,
        &object,
    )
    .map_err(|e| format!("the manifest is not this backup: {e}"))?;
    if manifest.logical_bytes != restore.logical_bytes {
        return Err("the manifest's logical_bytes do not match the restore".to_owned());
    }

    let mut restorer = Restorer::create(keys, temp, size)
        .map_err(|e| format!("could not create the restore file: {e}"))?;
    for chunk in &manifest.chunks {
        // The manifest's layout was checked by `open_manifest`, so the id
        // parses; every chunk is read back, zero or not (section 8.1).
        let id = ChunkId::from_hex(&chunk.id)
            .map_err(|e| format!("a manifest chunk id is not valid: {e}"))?;
        let key = object_key(&repo.root, &layout::chunk(&id));
        let object = retry_transport(options.retries, options.retry_base, || client.get(&key))
            .await
            .map_err(|e| format!("could not read a chunk: {e}"))?;
        restorer
            .apply(chunk, &object)
            .map_err(|e| format!("a chunk does not match the backup: {e}"))?;
    }
    restorer
        .finish()
        .map_err(|e| format!("the chunks do not cover the volume: {e}"))?;

    engine
        .import_volume(&VolumeId(entry.volume_id.clone()), entry.size_gb, temp)
        .await
        .map_err(|e| format!("could not import the restored file: {e}"))
}

/// Records one volume row (`node-agent-protocol.md` section 12 `volumes`).
async fn record_volume(
    store: &NodeStore,
    entry: &SpecVolumeFile,
    state: VolumeState,
    error: Option<&str>,
    allocated_bytes: Option<u64>,
) {
    let record = VolumeRecord {
        volume_id: entry.volume_id.clone(),
        app_id: entry.app_id.clone(),
        size_gb: entry.size_gb,
        state,
        error: error.map(one_line),
        allocated_bytes,
    };
    if let Err(e) = store.save_volume(&record).await {
        warn!(volume_id = %entry.volume_id, error = %e, "could not record a restored volume");
    }
}

/// Creates `dir` `0700` when it does not exist.
fn create_staging(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Empties the staging directory, so a restore a previous agent started never
/// resumes (section 6.7).
fn empty_staging(dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Removes a file, ignoring one that is already gone.
fn remove_file(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(path = %path.display(), error = %error, "could not remove a file"),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use quoro_backup::{
        Chunker, Manifest, ManifestChunk, RepoKey, SourceKind, encrypt_manifest, manifest_digest,
    };
    use quoro_engine::{FakeEngine, VolumeId};
    use quoro_proto::wire::{DesiredState, Spec, SpecNetwork, SpecVolumeState};
    use quoro_proto::{LeaseGrant, LeaseId, LeaseMode, ResourceType};
    use quoro_providers::object_store::{
        ComplianceLock, MemoryStore, ObjectHead, ObjectInfo, ObjectStore, ObjectStoreError,
        ObjectVersion,
    };
    use tokio::sync::watch;
    use tokio::time::{Duration, sleep, timeout};

    use super::*;
    use crate::health::PauseMark;
    use crate::lease::LeaseTable;

    /// One gibibyte, the size a `vol_` file has in these tests.
    const VOLUME_BYTES: u64 = GIB;

    /// A scratch directory under the checkout's `target/`, removed on drop.
    struct TestDir(PathBuf);

    impl TestDir {
        fn new(name: &str) -> Self {
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/backup-agent-tests")
                .join(format!("{name}-{}", util::hex(&util::random_bytes::<8>())));
            std::fs::create_dir_all(&path).expect("create the test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A repository that hands every job the same in-memory bucket.
    struct SharedStore(Arc<MemoryStore>);

    impl Repository for SharedStore {
        type Store = MemoryStore;

        fn open(&self, _repository: &SpecBackupRepository) -> Result<Arc<MemoryStore>, String> {
            Ok(Arc::clone(&self.0))
        }
    }

    /// A bucket whose `put_new` can be made to fail, for the cleanup test.
    struct FaultyStore {
        inner: Arc<MemoryStore>,
        fail_puts: AtomicBool,
    }

    impl ObjectStore for FaultyStore {
        async fn put_new(
            &self,
            key: &str,
            body: Vec<u8>,
            lock: Option<&ComplianceLock>,
        ) -> Result<Option<String>, ObjectStoreError> {
            if self.fail_puts.load(Ordering::Relaxed) {
                return Err(ObjectStoreError::Denied(
                    "the bucket refused the write".to_owned(),
                ));
            }
            self.inner.put_new(key, body, lock).await
        }

        async fn put_overwrite(
            &self,
            key: &str,
            body: Vec<u8>,
        ) -> Result<Option<String>, ObjectStoreError> {
            self.inner.put_overwrite(key, body).await
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
            self.inner.get(key).await
        }

        async fn get_version(
            &self,
            key: &str,
            version_id: &str,
        ) -> Result<Vec<u8>, ObjectStoreError> {
            self.inner.get_version(key, version_id).await
        }

        async fn head(&self, key: &str) -> Result<ObjectHead, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectStoreError> {
            self.inner.list(prefix).await
        }

        async fn list_versions(
            &self,
            prefix: &str,
        ) -> Result<Vec<ObjectVersion>, ObjectStoreError> {
            self.inner.list_versions(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        async fn delete_version(
            &self,
            key: &str,
            version_id: &str,
        ) -> Result<(), ObjectStoreError> {
            self.inner.delete_version(key, version_id).await
        }
    }

    struct FaultyRepository {
        store: Arc<FaultyStore>,
    }

    impl Repository for FaultyRepository {
        type Store = FaultyStore;

        fn open(&self, _repository: &SpecBackupRepository) -> Result<Arc<FaultyStore>, String> {
            Ok(Arc::clone(&self.store))
        }
    }

    fn repository(root: &str) -> SpecBackupRepository {
        SpecBackupRepository {
            repository_id: "brp_1".to_owned(),
            endpoint: "https://s3.example.test".to_owned(),
            region: "auto".to_owned(),
            bucket: "quoro-backups".to_owned(),
            root: root.to_owned(),
            access_key_id: "access".to_owned(),
            secret_access_key: "secret".to_owned(),
        }
    }

    fn keys_for(app_id: &str) -> SourceKeys {
        RepoKey::from_bytes([0x42; 32]).source_keys(app_id)
    }

    fn base64_key(key: &quoro_backup::Key) -> String {
        STANDARD.encode(key.as_bytes())
    }

    fn lease_view(token: u64) -> LeaseView {
        let mut table = LeaseTable::new();
        table
            .on_grant(
                &LeaseGrant {
                    lease_id: LeaseId("lse_1".to_owned()),
                    resource_type: ResourceType::App,
                    resource_id: "app_1".to_owned(),
                    mode: LeaseMode::Local,
                    fencing_token: token,
                    duration_ms: None,
                    granted_at: "2026-11-02T10:15:33Z".to_owned(),
                },
                None,
                tokio::time::Instant::now(),
            )
            .expect("a valid local grant");
        LeaseView::new(table, false)
    }

    fn present(volume_id: &str, restore: Option<SpecRestore>) -> SpecVolumeFile {
        SpecVolumeFile {
            volume_id: volume_id.to_owned(),
            app_id: "app_1".to_owned(),
            size_gb: 1,
            state: SpecVolumeState::Present,
            restore,
            warm: None,
        }
    }

    fn job(backup_id: &str, volume_id: &str, keys: &SourceKeys, token: u64) -> SpecBackup {
        SpecBackup {
            backup_id: backup_id.to_owned(),
            app_id: "app_1".to_owned(),
            volume_id: volume_id.to_owned(),
            vm_id: None,
            fencing_token: token,
            id_key: base64_key(&keys.id_key),
            data_key: base64_key(&keys.data_key),
        }
    }

    fn snapshot(
        volumes: Vec<SpecVolumeFile>,
        backups: Vec<SpecBackup>,
        repository: Option<SpecBackupRepository>,
    ) -> Snapshot {
        Snapshot {
            rev: 1,
            state: DesiredState {
                rev: 1,
                issued_at: "2026-11-02T10:15:31Z".to_owned(),
                term: 1,
                spec: Spec {
                    vms: Vec::new(),
                    routes: Vec::new(),
                    network: Some(SpecNetwork {
                        subnet: "10.42.1.0/24".to_owned(),
                        gateway: "10.42.1.1".to_owned(),
                    }),
                    volumes,
                    backups,
                    backup_repository: repository,
                    ..Spec::default()
                },
                signature: String::new(),
            },
        }
    }

    /// Deterministic, non-zero bytes for a volume region.
    fn pattern(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    /// Creates the volume file and writes the given `(offset, length)` regions
    /// of non-zero data; the rest stays a hole.
    fn seed_volume(path: &Path, regions: &[(u64, usize)]) {
        use std::io::{Seek as _, SeekFrom, Write as _};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open the volume file");
        file.set_len(VOLUME_BYTES).expect("size the volume file");
        for (offset, len) in regions {
            file.seek(SeekFrom::Start(*offset)).expect("seek");
            file.write_all(&pattern(*len, *offset))
                .expect("write the region");
        }
    }

    async fn ready_volume(store: &NodeStore, volume_id: &str) {
        store
            .save_volume(&VolumeRecord {
                volume_id: volume_id.to_owned(),
                app_id: "app_1".to_owned(),
                size_gb: 1,
                state: VolumeState::Ready,
                error: None,
                allocated_bytes: None,
            })
            .await
            .unwrap();
    }

    /// Waits for `volume_id`'s row to reach `state`.
    async fn wait_volume(store: &NodeStore, volume_id: &str, state: VolumeState) -> VolumeRecord {
        timeout(Duration::from_secs(180), async {
            loop {
                if let Some(row) = store.volume(volume_id).await.unwrap()
                    && row.state == state
                {
                    return row;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("volume {volume_id} did not reach {state:?}"))
    }

    /// Waits for one backup's terminal entry (section 5.3).
    async fn wait_backup(
        statuses: &mut watch::Receiver<Vec<BackupStatus>>,
        backup_id: &str,
    ) -> BackupStatus {
        timeout(Duration::from_secs(180), async {
            loop {
                let found = statuses
                    .borrow()
                    .iter()
                    .find(|status| status.backup_id == backup_id)
                    .cloned();
                if let Some(status) = found
                    && is_terminal(status.status)
                {
                    return status;
                }
                statuses.changed().await.expect("the manager is running");
            }
        })
        .await
        .expect("the backup reaches a terminal status")
    }

    /// Whether the volume directory holds any snapshot file.
    fn snapshot_files(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .expect("read the volume directory")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".snap"))
            })
            .collect()
    }

    fn assert_same_bytes(original: &Path, restored: &Path) {
        use std::io::Read as _;
        let len = std::fs::metadata(original).expect("the original").len();
        assert_eq!(
            std::fs::metadata(restored)
                .expect("the restored file")
                .len(),
            len,
            "the restored file has the volume's length"
        );
        let mut left = std::fs::File::open(original).expect("open the original");
        let mut right = std::fs::File::open(restored).expect("open the restored file");
        let mut a = vec![0u8; 1 << 20];
        let mut b = vec![0u8; 1 << 20];
        let mut offset = 0u64;
        while offset < len {
            let n = usize::try_from((len - offset).min(a.len() as u64)).unwrap();
            left.read_exact(&mut a[..n]).expect("read the original");
            right
                .read_exact(&mut b[..n])
                .expect("read the restored file");
            assert!(
                a[..n] == b[..n],
                "the restored file differs at offset {offset}"
            );
            offset += n as u64;
        }
    }

    async fn backup_once(
        name: &str,
        regions: &[(u64, usize)],
    ) -> (
        TestDir,
        SourceKeys,
        Arc<MemoryStore>,
        SpecBackupRepository,
        BackupStatus,
    ) {
        let dir = TestDir::new(name);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        engine
            .ensure_volume(&VolumeId("vol_1".to_owned()), 1)
            .await
            .unwrap();
        seed_volume(&volumes.join("vol_1"), regions);
        ready_volume(&store, "vol_1").await;

        let keys = keys_for("app_1");
        let memory = Arc::new(MemoryStore::new());
        let repo = repository(&format!("tests/{name}"));
        let (manager, mut statuses) = BackupManager::new(
            Arc::clone(&engine),
            store,
            Arc::new(SharedStore(Arc::clone(&memory))),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", None)],
            vec![job("bkp_1", "vol_1", &keys, 7)],
            Some(repo.clone()),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));
        let status = wait_backup(&mut statuses, "bkp_1").await;
        handle.abort();
        (dir, keys, memory, repo, status)
    }

    /// The fake engine, which also records, at the moment the backup manager
    /// takes a snapshot, the pause marks of a VM that attaches `vol_1`. The
    /// manager calls nothing else of the engine for a backup; the guest flush
    /// is passed through so the fake's outcome reaches the snapshot.
    struct MarkSpy {
        fake: Arc<FakeEngine>,
        paused: PausedVms,
        at_snapshot: std::sync::Mutex<Option<PauseMark>>,
    }

    impl Engine for MarkSpy {
        async fn capabilities(&self) -> quoro_engine::Capabilities {
            self.fake.capabilities().await
        }

        async fn snapshot_volume(
            &self,
            id: &VolumeId,
        ) -> Result<VolumeSnapshot, quoro_engine::EngineError> {
            *self.at_snapshot.lock().unwrap() = Some(self.paused.mark_of(["vol_1"]));
            self.fake.snapshot_volume(id).await
        }

        async fn delete_volume_snapshot(
            &self,
            snap: &VolumeSnapshot,
        ) -> Result<(), quoro_engine::EngineError> {
            self.fake.delete_volume_snapshot(snap).await
        }

        async fn flush_volume(
            &self,
            id: &VolumeId,
            within: Duration,
        ) -> Result<quoro_engine::GuestFlush, quoro_engine::EngineError> {
            self.fake.flush_volume(id, within).await
        }

        async fn probe_host(&self) -> Result<quoro_engine::HostProbe, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn ensure_network(
            &self,
            _net: &quoro_engine::NodeNetwork,
        ) -> Result<(), quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn prepare_image(
            &self,
            _image: &quoro_engine::ImageRef,
        ) -> Result<quoro_engine::PreparedImage, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn remove_image(
            &self,
            _digest: &quoro_engine::ImageDigest,
        ) -> Result<(), quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn create(
            &self,
            _spec: &quoro_engine::VmSpec,
        ) -> Result<quoro_engine::VmInfo, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn start(
            &self,
            _id: &quoro_engine::VmId,
            _within: Duration,
        ) -> Result<quoro_engine::VmInfo, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn stop(
            &self,
            _id: &quoro_engine::VmId,
            _mode: quoro_engine::StopMode,
        ) -> Result<quoro_engine::VmInfo, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn delete(&self, _id: &quoro_engine::VmId) -> Result<(), quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn inspect(
            &self,
            _id: &quoro_engine::VmId,
        ) -> Result<quoro_engine::VmInfo, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn list(&self) -> Result<Vec<quoro_engine::VmInfo>, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        fn events(&self) -> quoro_engine::EventStream {
            unimplemented!("not used by a backup")
        }

        fn output(
            &self,
            _id: &quoro_engine::VmId,
        ) -> Result<quoro_engine::LineStream, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        fn console(
            &self,
            _id: &quoro_engine::VmId,
        ) -> Result<quoro_engine::ConsoleStream, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }

        async fn counters(
            &self,
            _id: &quoro_engine::VmId,
        ) -> Result<quoro_engine::VmCounters, quoro_engine::EngineError> {
            unimplemented!("not used by a backup")
        }
    }

    /// How one backup of `vol_1` went for the pause marks of `vm_1`.
    #[derive(Debug)]
    struct MarkRun {
        status: BackupState,
        at_snapshot: Option<PauseMark>,
        after: PauseMark,
    }

    /// Runs one backup of `vol_1` whose job names the VM `vm_id`, with
    /// `method`, its snapshot failing when `fail` is set.
    async fn marks_of_a_backup(
        name: &str,
        method: SnapshotMethod,
        vm_id: Option<&str>,
        fail: bool,
    ) -> MarkRun {
        let dir = TestDir::new(name);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let fake = Arc::new(FakeEngine::new());
        fake.set_volume_dir(&volumes);
        fake.set_snapshot_method(method, Duration::from_millis(50));
        fake.ensure_volume(&VolumeId("vol_1".to_owned()), 1)
            .await
            .unwrap();
        seed_volume(&volumes.join("vol_1"), &[(0, 1 << 20)]);
        ready_volume(&store, "vol_1").await;
        if fail {
            fake.fail_next(
                quoro_engine::fake::FakeOp::SnapshotVolume,
                quoro_engine::EngineError::Io("the copy failed".to_owned()),
            );
        }

        let keys = keys_for("app_1");
        let paused = PausedVms::default();
        let engine = Arc::new(MarkSpy {
            fake,
            paused: paused.clone(),
            at_snapshot: std::sync::Mutex::new(None),
        });
        let (manager, mut statuses) = BackupManager::new(
            Arc::clone(&engine),
            store,
            Arc::new(SharedStore(Arc::new(MemoryStore::new()))),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager
            .with_options(BackupOptions::quick())
            .with_paused_vms(paused.clone());
        let backup = SpecBackup {
            vm_id: vm_id.map(str::to_owned),
            ..job("bkp_1", "vol_1", &keys, 7)
        };
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", None)],
            vec![backup],
            Some(repository(&format!("tests/{name}"))),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));
        let status = wait_backup(&mut statuses, "bkp_1").await;
        handle.abort();
        let at_snapshot = *engine.at_snapshot.lock().unwrap();
        MarkRun {
            status: status.status,
            at_snapshot,
            after: paused.mark_of(["vol_1"]),
        }
    }

    /// D-0259: a `pause_copy` snapshot marks the volume, and so the VM that
    /// attaches it, which the engine pauses, for the snapshot and no longer;
    /// a reflink snapshot pauses nothing and marks nothing.
    #[tokio::test]
    async fn a_pause_copy_snapshot_marks_the_vm_it_pauses_for_the_snapshot_only() {
        let run = marks_of_a_backup(
            "backup-pause-mark",
            SnapshotMethod::PauseCopy,
            Some("vm_1"),
            false,
        )
        .await;
        assert_eq!(run.status, BackupState::Succeeded, "{run:?}");
        assert_eq!(run.at_snapshot, Some(PauseMark { held: 1, latest: 1 }));
        assert_eq!(run.after, PauseMark { held: 0, latest: 1 });

        let run = marks_of_a_backup(
            "backup-reflink-mark",
            SnapshotMethod::Reflink,
            Some("vm_1"),
            false,
        )
        .await;
        assert_eq!(run.status, BackupState::Succeeded, "{run:?}");
        assert_eq!(run.at_snapshot, Some(PauseMark::default()));
        assert_eq!(run.after, PauseMark::default());
    }

    /// D-0259: a snapshot that fails releases the mark like one that
    /// succeeds, so a failed backup leaves no volume marked.
    #[tokio::test]
    async fn a_failed_pause_copy_snapshot_releases_the_mark() {
        let run = marks_of_a_backup(
            "backup-failed-pause-mark",
            SnapshotMethod::PauseCopy,
            Some("vm_1"),
            true,
        )
        .await;
        assert_eq!(run.status, BackupState::Failed, "{run:?}");
        assert_eq!(run.at_snapshot, Some(PauseMark { held: 1, latest: 1 }));
        assert_eq!(run.after, PauseMark { held: 0, latest: 1 });
    }

    /// #524: the engine pauses whichever VM attaches the volume, so the
    /// volume is marked also when the job names another VM, as after a
    /// redeploy or with a stale VM row on the primary, or no VM at all.
    #[tokio::test]
    async fn a_pause_copy_snapshot_marks_the_volume_whatever_vm_the_job_names() {
        for (name, vm_id) in [
            ("backup-other-vm-mark", Some("vm_old")),
            ("backup-no-vm-mark", None),
        ] {
            let run = marks_of_a_backup(name, SnapshotMethod::PauseCopy, vm_id, false).await;
            assert_eq!(run.status, BackupState::Succeeded, "{vm_id:?}: {run:?}");
            assert_eq!(
                run.at_snapshot,
                Some(PauseMark { held: 1, latest: 1 }),
                "{vm_id:?}"
            );
            assert_eq!(run.after, PauseMark { held: 0, latest: 1 }, "{vm_id:?}");
        }
    }

    #[tokio::test]
    async fn a_backup_uploads_chunks_and_a_second_of_unchanged_data_uploads_none() {
        let dir = TestDir::new("backup-happy");
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        engine
            .ensure_volume(&VolumeId("vol_1".to_owned()), 1)
            .await
            .unwrap();
        seed_volume(&volumes.join("vol_1"), &[(0, 2 << 20), (4 << 20, 2 << 20)]);
        ready_volume(&store, "vol_1").await;

        let keys = keys_for("app_1");
        let memory = Arc::new(MemoryStore::new());
        let repo = repository("tests/happy");
        let (manager, mut statuses) = BackupManager::new(
            Arc::clone(&engine),
            store.clone(),
            Arc::new(SharedStore(Arc::clone(&memory))),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", None)],
            vec![job("bkp_1", "vol_1", &keys, 7)],
            Some(repo.clone()),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));

        let first = wait_backup(&mut statuses, "bkp_1").await;
        assert_eq!(first.status, BackupState::Succeeded, "{first:?}");
        assert_eq!(
            first.snapshot_method,
            Some(quoro_proto::wire::SnapshotMethod::Reflink)
        );
        assert_eq!(first.manifest_key.as_deref(), Some("manifests/app_1/bkp_1"));
        assert_eq!(first.logical_bytes, Some(VOLUME_BYTES));
        assert!(first.chunk_count.unwrap() > 0);
        assert!(first.uploaded_bytes.unwrap() > 0);
        assert!(first.started_at.is_some() && first.finished_at.is_some());
        assert!(
            snapshot_files(&volumes).is_empty(),
            "the snapshot is deleted"
        );

        // The bucket holds the manifest and the chunks, and `open_manifest`
        // with the same keys decrypts it.
        let manifest_object = memory
            .get("tests/happy/manifests/app_1/bkp_1")
            .await
            .unwrap();
        let manifest = open_manifest(
            &keys,
            "bkp_1",
            "app_1",
            first.manifest_digest.as_deref().unwrap(),
            &manifest_object,
        )
        .unwrap();
        assert_eq!(manifest.logical_bytes, VOLUME_BYTES);
        assert_eq!(manifest.chunks.len() as u64, first.chunk_count.unwrap());
        // Every chunk the manifest lists is in the bucket, zero chunks
        // included (section 5).
        for entry in [
            &manifest.chunks[0],
            &manifest.chunks[manifest.chunks.len() - 1],
        ] {
            let id = ChunkId::from_hex(&entry.id).unwrap();
            assert!(
                memory
                    .get(&object_key("tests/happy", &layout::chunk(&id)))
                    .await
                    .is_ok(),
                "chunk {} is in the bucket",
                entry.id
            );
        }

        // A second backup of unchanged data finds every chunk present and
        // counts no new bytes (section 5).
        tx.send(Some(snapshot(
            vec![present("vol_1", None)],
            vec![job("bkp_2", "vol_1", &keys, 7)],
            Some(repo),
        )))
        .unwrap();
        let second = wait_backup(&mut statuses, "bkp_2").await;
        assert_eq!(second.status, BackupState::Succeeded, "{second:?}");
        assert_eq!(second.uploaded_bytes, Some(0));
        assert_eq!(second.chunk_count, first.chunk_count);
        handle.abort();
    }

    #[tokio::test]
    async fn a_wrong_token_fails_with_lease_mismatch_and_uploads_nothing() {
        let dir = TestDir::new("backup-lease");
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        engine
            .ensure_volume(&VolumeId("vol_1".to_owned()), 1)
            .await
            .unwrap();
        ready_volume(&store, "vol_1").await;

        let keys = keys_for("app_1");
        let memory = Arc::new(MemoryStore::new());
        let (manager, mut statuses) = BackupManager::new(
            Arc::clone(&engine),
            store,
            Arc::new(SharedStore(Arc::clone(&memory))),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", None)],
            vec![job("bkp_1", "vol_1", &keys, 8)],
            Some(repository("tests/lease")),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));

        let status = wait_backup(&mut statuses, "bkp_1").await;
        assert_eq!(status.status, BackupState::Failed);
        assert_eq!(status.error.as_deref(), Some("lease_mismatch"));
        assert!(
            memory.list("").await.unwrap().is_empty(),
            "nothing was uploaded"
        );
        assert!(snapshot_files(&volumes).is_empty(), "no snapshot was taken");
        handle.abort();
    }

    #[tokio::test]
    async fn a_failed_upload_deletes_the_snapshot() {
        let dir = TestDir::new("backup-failed-upload");
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        engine
            .ensure_volume(&VolumeId("vol_1".to_owned()), 1)
            .await
            .unwrap();
        ready_volume(&store, "vol_1").await;

        let keys = keys_for("app_1");
        let faulty = Arc::new(FaultyStore {
            inner: Arc::new(MemoryStore::new()),
            fail_puts: AtomicBool::new(true),
        });
        let (manager, mut statuses) = BackupManager::new(
            Arc::clone(&engine),
            store.clone(),
            Arc::new(FaultyRepository {
                store: Arc::clone(&faulty),
            }),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", None)],
            vec![job("bkp_1", "vol_1", &keys, 7)],
            Some(repository("tests/failed")),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));

        let status = wait_backup(&mut statuses, "bkp_1").await;
        assert_eq!(status.status, BackupState::Failed);
        let error = status.error.unwrap_or_default();
        assert!(error.contains("could not upload a chunk"), "{error}");
        assert!(!error.contains('\n'), "the error is one line");
        assert!(
            snapshot_files(&volumes).is_empty(),
            "the snapshot file is deleted on failure"
        );
        let row = store.backup("bkp_1").await.unwrap().expect("a backup row");
        assert_eq!(row.status, BackupState::Failed);
        handle.abort();
    }

    #[tokio::test]
    async fn a_restart_fails_a_running_job_and_deletes_its_snapshot() {
        let dir = TestDir::new("backup-restart");
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let leftover = volumes.join("vol_1.snap-7");
        std::fs::write(&leftover, b"half a snapshot").unwrap();
        let keys = keys_for("app_1");
        store
            .save_backup(&BackupRecord {
                backup_id: "bkp_1".to_owned(),
                app_id: "app_1".to_owned(),
                volume_id: "vol_1".to_owned(),
                status: BackupState::Running,
                snapshot_method: Some(quoro_proto::wire::SnapshotMethod::Reflink),
                pause_ms: None,
                manifest_key: None,
                manifest_digest: None,
                logical_bytes: None,
                uploaded_bytes: None,
                chunk_count: None,
                error: None,
                started_at: Some("2026-11-05T03:00:00Z".to_owned()),
                finished_at: None,
                snapshot_path: Some(leftover.display().to_string()),
            })
            .await
            .unwrap();

        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        let (manager, mut statuses) = BackupManager::new(
            Arc::clone(&engine),
            store.clone(),
            Arc::new(SharedStore(Arc::new(MemoryStore::new()))),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", None)],
            vec![job("bkp_1", "vol_1", &keys, 7)],
            Some(repository("tests/restart")),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));

        let status = wait_backup(&mut statuses, "bkp_1").await;
        assert_eq!(status.status, BackupState::Failed);
        assert_eq!(status.error.as_deref(), Some("node agent restarted"));
        assert!(!leftover.exists(), "the leftover snapshot is deleted");
        let row = store.backup("bkp_1").await.unwrap().expect("a backup row");
        assert_eq!(row.status, BackupState::Failed);
        assert_eq!(row.error.as_deref(), Some("node agent restarted"));
        handle.abort();
    }

    #[tokio::test]
    async fn a_restore_writes_the_original_bytes_and_keeps_the_hole() {
        let (source, keys, memory, repo, status) =
            backup_once("restore-round-trip", &[(0, 8 << 20), (600 << 20, 8 << 20)]).await;
        assert_eq!(status.status, BackupState::Succeeded, "{status:?}");
        let original = source.path().join("volumes").join("vol_1");

        // A second node with its own data directory and volume directory.
        let target = TestDir::new("restore-target");
        let store = NodeStore::open(target.path()).await.unwrap();
        let volumes = target.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        let (manager, _statuses) = BackupManager::new(
            Arc::clone(&engine),
            store.clone(),
            Arc::new(SharedStore(Arc::clone(&memory))),
            LeaseView::default(),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let restore = SpecRestore {
            backup_id: "bkp_1".to_owned(),
            manifest_key: status.manifest_key.unwrap(),
            manifest_digest: status.manifest_digest.unwrap(),
            logical_bytes: VOLUME_BYTES,
            id_key: base64_key(&keys.id_key),
            data_key: base64_key(&keys.data_key),
        };
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", Some(restore))],
            Vec::new(),
            Some(repo),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));

        let row = wait_volume(&store, "vol_1", VolumeState::Ready).await;
        assert_eq!(row.size_gb, 1);
        let restored = volumes.join("vol_1");
        assert_same_bytes(&original, &restored);
        // The zero regions stay holes: the sparse file uses less than its size.
        use std::os::unix::fs::MetadataExt as _;
        let allocated = std::fs::metadata(&restored).unwrap().blocks() * 512;
        assert!(
            allocated < VOLUME_BYTES,
            "the restored file is {allocated} bytes on disk"
        );
        assert!(
            !target
                .path()
                .join(STAGING_DIR)
                .join("vol_1.restore")
                .exists(),
            "the staged file was renamed into place"
        );
        handle.abort();
    }

    /// A restore manager over one volume, for the refusal tests.
    async fn run_refused_restore(
        name: &str,
        memory: Arc<MemoryStore>,
        repo: &SpecBackupRepository,
        restore: SpecRestore,
    ) -> (TestDir, VolumeRecord) {
        let dir = TestDir::new(name);
        let store = NodeStore::open(dir.path()).await.unwrap();
        let volumes = dir.path().join("volumes");
        std::fs::create_dir_all(&volumes).unwrap();
        let engine = Arc::new(FakeEngine::new());
        engine.set_volume_dir(&volumes);
        let (manager, _statuses) = BackupManager::new(
            Arc::clone(&engine),
            store.clone(),
            Arc::new(SharedStore(memory)),
            LeaseView::default(),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (_tx, rx) = watch::channel(Some(snapshot(
            vec![present("vol_1", Some(restore))],
            Vec::new(),
            Some(repo.clone()),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));
        let row = wait_volume(&store, "vol_1", VolumeState::Failed).await;
        assert!(
            !dir.path().join(STAGING_DIR).join("vol_1.restore").exists(),
            "no partial file is left"
        );
        assert!(
            !volumes.join("vol_1").exists(),
            "no volume file is imported"
        );
        handle.abort();
        (dir, row)
    }

    #[tokio::test]
    async fn a_restore_refuses_a_manifest_whose_digest_does_not_match() {
        let keys = keys_for("app_1");
        let memory = Arc::new(MemoryStore::new());
        let repo = repository("tests/wrong-digest");
        memory
            .put_new(
                &object_key(&repo.root, &layout::manifest("app_1", "bkp_1")),
                b"not the manifest".to_vec(),
                None,
            )
            .await
            .unwrap();
        let restore = SpecRestore {
            backup_id: "bkp_1".to_owned(),
            manifest_key: layout::manifest("app_1", "bkp_1"),
            manifest_digest: "0".repeat(64),
            logical_bytes: VOLUME_BYTES,
            id_key: base64_key(&keys.id_key),
            data_key: base64_key(&keys.data_key),
        };
        let (_dir, row) = run_refused_restore("restore-wrong-digest", memory, &repo, restore).await;
        let error = row.error.unwrap_or_default();
        assert!(error.contains("not this backup"), "{error}");
    }

    #[tokio::test]
    async fn a_restore_refuses_an_altered_chunk_and_leaves_no_file() {
        let keys = keys_for("app_1");
        let memory = Arc::new(MemoryStore::new());
        let repo = repository("tests/altered-chunk");
        // A valid manifest whose chunks cover exactly one gibibyte, with the
        // first chunk object present but not the one it names.
        let manifest = Manifest {
            format: quoro_backup::FORMAT,
            backup_id: "bkp_1".to_owned(),
            cluster_id: "cls_1".to_owned(),
            repository_id: "brp_1".to_owned(),
            source_kind: SourceKind::Volume,
            source_id: "app_1".to_owned(),
            volume_id: Some("vol_1".to_owned()),
            fencing_token: Some(7),
            snapshot_method: Some("reflink".to_owned()),
            logical_bytes: VOLUME_BYTES,
            chunker: Chunker::fixed(),
            chunks: (0..256)
                .map(|index| ManifestChunk {
                    id: "ab".repeat(32),
                    offset: index * (4 << 20),
                    length: 4 << 20,
                })
                .collect(),
            quoro_version: "0.6.0".to_owned(),
            created_at: "2026-11-05T03:00:41Z".to_owned(),
        };
        let object = encrypt_manifest(&keys, &manifest).unwrap();
        let digest = manifest_digest(&object);
        memory
            .put_new(
                &object_key(&repo.root, &layout::manifest("app_1", "bkp_1")),
                object,
                None,
            )
            .await
            .unwrap();
        let id = ChunkId::from_hex(&"ab".repeat(32)).unwrap();
        memory
            .put_new(
                &object_key(&repo.root, &layout::chunk(&id)),
                b"an altered chunk".to_vec(),
                None,
            )
            .await
            .unwrap();
        let restore = SpecRestore {
            backup_id: "bkp_1".to_owned(),
            manifest_key: layout::manifest("app_1", "bkp_1"),
            manifest_digest: digest,
            logical_bytes: VOLUME_BYTES,
            id_key: base64_key(&keys.id_key),
            data_key: base64_key(&keys.data_key),
        };
        let (_dir, row) =
            run_refused_restore("restore-altered-chunk", memory, &repo, restore).await;
        let error = row.error.unwrap_or_default();
        assert!(error.contains("does not match the backup"), "{error}");
    }

    #[tokio::test]
    async fn a_restore_refuses_a_logical_size_that_is_not_the_volume_size() {
        let keys = keys_for("app_1");
        let memory = Arc::new(MemoryStore::new());
        let repo = repository("tests/wrong-size");
        let restore = SpecRestore {
            backup_id: "bkp_1".to_owned(),
            manifest_key: layout::manifest("app_1", "bkp_1"),
            manifest_digest: "0".repeat(64),
            logical_bytes: VOLUME_BYTES + 1,
            id_key: base64_key(&keys.id_key),
            data_key: base64_key(&keys.data_key),
        };
        let (_dir, row) = run_refused_restore("restore-wrong-size", memory, &repo, restore).await;
        let error = row.error.unwrap_or_default();
        assert!(
            error.contains("logical_bytes do not match the volume size"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn the_status_report_carries_every_job_of_the_current_spec() {
        let dir = TestDir::new("backup-status");
        let store = NodeStore::open(dir.path()).await.unwrap();
        let keys = keys_for("app_1");
        let (manager, mut statuses) = BackupManager::new(
            Arc::new(FakeEngine::new()),
            store,
            Arc::new(SharedStore(Arc::new(MemoryStore::new()))),
            lease_view(7),
            "cls_1".to_owned(),
        );
        let manager = manager.with_options(BackupOptions::quick());
        let (_tx, rx) = watch::channel(Some(snapshot(
            Vec::new(),
            vec![
                job("bkp_1", "vol_1", &keys, 7),
                job("bkp_2", "vol_2", &keys, 7),
            ],
            Some(repository("tests/status")),
        )));
        let handle = tokio::spawn(manager.run(rx, std::future::pending::<()>()));

        let entries = timeout(Duration::from_secs(10), async {
            loop {
                let current: Vec<BackupStatus> = statuses.borrow().clone();
                if current.len() == 2 && current.iter().all(|entry| is_terminal(entry.status)) {
                    return current;
                }
                statuses.changed().await.expect("the manager is running");
            }
        })
        .await
        .expect("both jobs report");
        let ids: Vec<&str> = entries
            .iter()
            .map(|entry| entry.backup_id.as_str())
            .collect();
        assert_eq!(ids, vec!["bkp_1", "bkp_2"]);
        // Their volumes are not ready on this node, so both fail with a reason.
        for entry in &entries {
            assert_eq!(entry.status, BackupState::Failed);
            assert_eq!(
                entry.error.as_deref(),
                Some("the volume is not ready on this node")
            );
        }
        handle.abort();
    }
}
