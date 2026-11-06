// SPDX-License-Identifier: AGPL-3.0-only
//! Wiring: load the identity, verify the stored snapshot, run the reconciler
//! and the session until shutdown.
//!
//! The order matters. The store's newest snapshot is verified again against the
//! pinned key before it is handed to the reconciler, so a tampered file cannot
//! drive the engine; and the reconciler is spawned before the first dial, so
//! running VMs keep being reconciled while the primary is unreachable
//! (section 6.5).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use quoro_engine::Engine;
use quoro_ingress::proxy::BackendObserver;
use quoro_proto::sign;
use quoro_proto::wire::DesiredState;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::backup::S3Repository;
use crate::backup::manager::BackupManager;
use crate::build::buildkit::{
    BuilderConfig, DEFAULT_FETCH_TIMEOUT, DockerfileBuilder, find_git, pinned_image,
};
use crate::build::manager::BuildManager;
use crate::edge::EdgeManager;
use crate::identity::IdentityHandle;
use crate::ingress::{CertificateEvents, DnsBridge, IngressSupervisor};
use crate::interactive::Interactive;
use crate::lease::{FenceTimers, LeaseConfig};
use crate::mesh::{MeshKey, MeshManager, PlatformMeshDriver};
use crate::metrics::{MetricsRing, MetricsSampler};
use crate::reconcile::{ReconcileConfig, ReconcileOptions, Reconciler, Snapshot};
use crate::session::{Session, SessionSetup, wait_stop};
use crate::store::NodeStore;
use crate::transcript::{self, TranscriptLimits};
use crate::{AgentConfig, AgentError, NodeFacts, util};

/// Runs the node until `shutdown` resolves.
///
/// The reconciler keeps the engine matching the last verified snapshot, and the
/// session dials the primary, redialing until a fatal end says not to.
///
/// # Errors
///
/// [`AgentError::Identity`] when the node is not enrolled,
/// [`AgentError::Store`] when the database cannot be opened, and
/// [`AgentError::Fatal`] when the primary rejected the credential or removed
/// the node.
pub async fn run<E: Engine + 'static>(
    config: AgentConfig,
    engine: E,
    facts: NodeFacts,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), AgentError> {
    let identity = IdentityHandle::load(&config.data_dir)?;
    let store = NodeStore::open(&config.data_dir).await?;
    // The engine's start-up sweep already ran. Once every snapshot it left
    // is recorded, the store tracks them and later sweeps remove the others
    // (section 12); until then the sweep keeps every snapshot.
    if let Err(e) = crate::kept::start_tracking(&store, config.snapshots_at_start.as_ref()).await {
        warn!(error = %e, "could not check the kept snapshots");
    }
    // The mesh key pair is generated on the first start and reused after that;
    // only its public key leaves the machine (section 6.9).
    let mesh_key = MeshKey::load_or_create(&config.data_dir)?;

    let current = load_verified_snapshot(&identity, &store).await;

    // The ingress supervisor serves the routes of the snapshot. It outlives a
    // session drop by design: the last applied table keeps serving while the
    // primary is unreachable (section 6.5).
    let dns = Arc::new(DnsBridge::new());
    let ingress = Arc::new(IngressSupervisor::new(
        config.ingress_config(),
        Arc::clone(&dns),
        CertificateEvents::new(store.clone(), Some(identity.get().node_id)),
    ));
    // The edge manager runs on every machine; it does nothing while the
    // snapshot has no `spec.edge`. It is also the proxy's backend observer, so
    // a connection failure takes a backend out of rotation at once.
    let edge = Arc::new(EdgeManager::new(Arc::clone(&ingress)));
    ingress.set_observer(Arc::clone(&edge) as Arc<dyn BackendObserver>);

    // One engine handle serves every loop: the reconciler runs the app VMs,
    // the builder runs one builder VM per build (ADR 0117, ADR 0118), and the
    // sampler and the interactive sessions read the running VMs.
    let engine = Arc::new(engine);
    let capabilities = engine.capabilities().await;
    // One inventory hub serves the reconciler, the build manager and the
    // session (sections 5.26, 6.11).
    let inventory = crate::inventory::InventoryHub::new(Arc::clone(&engine));
    // Images cached by a release before M5a have no inventory: the engine
    // reads them from their root disks once, in the background, and the hub
    // sends them like freshly prepared ones (section 6.11). Nothing waits on
    // it, and an image that fails is retried at the next start.
    let backfill_task = crate::inventory::spawn_backfill(Arc::clone(&engine), inventory.clone());
    let node_id = identity.get().node_id;
    let cluster_id = identity.get().cluster_id;
    let (desired, desired_rx) = watch::channel(current.clone());

    // Section 8.3: `local` leases are held on this node alone, so they survive
    // a lost session. `expiring` leases renew through the session and fence
    // at their deadline (section 8.4); the service also fences the VMs of a
    // revoked lease.
    let leases = crate::lease::start_local_leases(
        Arc::clone(&engine),
        store.clone(),
        LeaseConfig::new(
            node_id.clone(),
            FenceTimers {
                grace: Duration::from_secs(10),
                force: Duration::from_secs(5),
            },
        ),
        desired.subscribe(),
    )
    .await?;

    let (reconciler, observed) = Reconciler::new(
        Arc::clone(&engine),
        store.clone(),
        ReconcileConfig {
            bridge: config.bridge.clone(),
            dns: config.dns.clone(),
        },
        ReconcileOptions::default(),
    );
    let reconciler = reconciler
        .with_node_id(node_id)
        .with_lease_view(leases.view.clone())
        .with_ingress(Arc::clone(&ingress))
        .with_edge(Arc::clone(&edge))
        .with_inventory(inventory.clone());
    let volume_statuses = reconciler.volume_statuses();

    // Metrics stay in memory on this node (ADR 0014): the sampler fills a ring
    // the session answers `metrics_query` and `metrics` streams from.
    let metrics = MetricsRing::new();
    let sampler = MetricsSampler::new(
        Arc::clone(&engine),
        observed.clone(),
        desired.subscribe(),
        metrics.clone(),
    );
    let builder = Arc::new(DockerfileBuilder::new(
        Arc::clone(&engine),
        BuilderConfig {
            builds_dir: config.data_dir.join("builds"),
            git: find_git(),
            image: pinned_image(),
            fetch_timeout: DEFAULT_FETCH_TIMEOUT,
        },
    ));
    let (manager, builds) = BuildManager::new(Arc::clone(&engine), builder, store.clone());
    let manager = manager.with_inventory(inventory.clone());

    // Backups and restores run beside the apply loop, one task per job
    // (section 6.7, ADR 0147). The repository is opened per job, so the
    // bucket credential lives no longer than the snapshot that carries it.
    let (backup_manager, backup_statuses) = BackupManager::new(
        Arc::clone(&engine),
        store.clone(),
        Arc::new(S3Repository),
        leases.view.clone(),
        cluster_id,
    );

    let (stop, stop_rx) = watch::channel(false);
    let shutdown_stop = stop.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_stop.send(true);
    });
    let metrics_task = tokio::spawn(sampler.run(wait_stop(stop_rx.clone())));
    let reconcile_task =
        tokio::spawn(reconciler.run(desired_rx.clone(), wait_stop(stop_rx.clone())));
    let build_task = tokio::spawn(manager.run(desired_rx, wait_stop(stop_rx.clone())));
    let mesh_public_key = mesh_key.public_key_base64();
    let mesh_task = tokio::spawn(
        MeshManager::new(PlatformMeshDriver::new(), mesh_key)
            .run(desired.subscribe(), wait_stop(stop_rx.clone())),
    );
    let edge_task =
        tokio::spawn(Arc::clone(&edge).run(desired.subscribe(), wait_stop(stop_rx.clone())));
    let backup_task =
        tokio::spawn(backup_manager.run(desired.subscribe(), wait_stop(stop_rx.clone())));

    // Terminal transcripts are pruned at agent start and then hourly; each
    // terminal session also prunes once it ends (section 3.3).
    let limits = TranscriptLimits::default();
    if let Err(e) = transcript::sweep(&store, limits, util::now_secs()).await {
        warn!(error = %e, "could not prune transcripts");
    }
    let retention_store = store.clone();
    let retention_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60 * 60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(e) = transcript::sweep(&retention_store, limits, util::now_secs()).await {
                warn!(error = %e, "could not prune transcripts");
            }
        }
    });

    let interactive = Interactive::new(Arc::clone(&engine), store.clone());
    let mut session = Session::new(SessionSetup {
        identity,
        store,
        facts,
        desired,
        observed,
        current,
        builds: Some(builds),
    })
    .with_leases(leases.link.clone())
    .with_volume_snapshot(
        capabilities
            .volume_snapshot_method
            .map(wire_snapshot_method),
    )
    .with_volumes(volume_statuses)
    .with_disk_free_path(config.data_dir.clone())
    .with_backups(backup_statuses)
    .with_ingress(Arc::clone(&ingress), dns)
    .with_metrics(metrics)
    .with_edge(Arc::clone(&edge))
    .with_mesh_key(mesh_public_key)
    .with_interactive(interactive)
    .with_inventory(inventory);
    let outcome = session.run(stop_rx).await;

    leases.abort();
    let _ = stop.send(true);
    retention_task.abort();
    backfill_task.abort();
    let _ = reconcile_task.await;
    let _ = metrics_task.await;
    let _ = build_task.await;
    let _ = mesh_task.await;
    let _ = edge_task.await;
    let _ = backup_task.await;
    ingress.shutdown().await;
    shutdown_task.abort();
    outcome
}

/// The wire form of the engine's volume snapshot method (section 5.1).
fn wire_snapshot_method(method: quoro_engine::SnapshotMethod) -> quoro_proto::wire::SnapshotMethod {
    match method {
        quoro_engine::SnapshotMethod::Reflink => quoro_proto::wire::SnapshotMethod::Reflink,
        quoro_engine::SnapshotMethod::PauseCopy => quoro_proto::wire::SnapshotMethod::PauseCopy,
    }
}

/// The newest stored snapshot, re-verified against the current pin.
///
/// A body that does not verify is logged and ignored: the agent then runs with
/// no snapshot, which means it changes nothing rather than acting on tampered
/// intent.
async fn load_verified_snapshot(identity: &IdentityHandle, store: &NodeStore) -> Option<Snapshot> {
    let (rev, body) = match store.latest_desired().await {
        Ok(Some(latest)) => latest,
        Ok(None) => return None,
        Err(e) => {
            warn!(error = %e, "could not read the stored desired state");
            return None;
        }
    };
    let key = match identity.verifying_key() {
        Ok(key) => key,
        Err(e) => {
            warn!(error = %e, "could not read the pinned cluster key");
            return None;
        }
    };
    if let Err(e) = sign::verify_body(&key, &body) {
        warn!(
            rev,
            error = %e,
            "the stored desired state does not verify against the pinned key; ignoring it"
        );
        return None;
    }
    match DesiredState::from_value(&body) {
        Ok(state) => {
            info!(rev, "loaded the stored desired state");
            Some(Snapshot { rev, state })
        }
        Err(e) => {
            warn!(rev, error = %e, "the stored desired state is malformed; ignoring it");
            None
        }
    }
}
