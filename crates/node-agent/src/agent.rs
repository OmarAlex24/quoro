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

use crate::build::buildkit::{
    BuilderConfig, DEFAULT_FETCH_TIMEOUT, DockerfileBuilder, find_git, pinned_image,
};
use crate::build::manager::BuildManager;
use crate::edge::EdgeManager;
use crate::identity::IdentityHandle;
use crate::ingress::{CertificateEvents, DnsBridge, IngressSupervisor};
use crate::interactive::Interactive;
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
    let (desired, desired_rx) = watch::channel(current.clone());
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
        .with_node_id(identity.get().node_id)
        .with_ingress(Arc::clone(&ingress));

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
    .with_ingress(Arc::clone(&ingress), dns)
    .with_metrics(metrics)
    .with_edge(Arc::clone(&edge))
    .with_mesh_key(mesh_public_key)
    .with_interactive(interactive);
    let outcome = session.run(stop_rx).await;

    let _ = stop.send(true);
    retention_task.abort();
    let _ = reconcile_task.await;
    let _ = metrics_task.await;
    let _ = build_task.await;
    let _ = mesh_task.await;
    let _ = edge_task.await;
    ingress.shutdown().await;
    shutdown_task.abort();
    outcome
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
