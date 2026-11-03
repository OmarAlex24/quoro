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

use quoro_engine::Engine;
use quoro_proto::sign;
use quoro_proto::wire::DesiredState;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::identity::IdentityHandle;
use crate::ingress::{CertificateEvents, DnsBridge, IngressSupervisor};
use crate::reconcile::{ReconcileConfig, ReconcileOptions, Reconciler, Snapshot};
use crate::session::{Session, SessionSetup, wait_stop};
use crate::store::NodeStore;
use crate::{AgentConfig, AgentError, NodeFacts};

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

    let (desired, desired_rx) = watch::channel(current.clone());
    let (reconciler, observed) = Reconciler::new(
        Arc::new(engine),
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

    let (stop, stop_rx) = watch::channel(false);
    let shutdown_stop = stop.clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_stop.send(true);
    });
    let reconcile_task = tokio::spawn(reconciler.run(desired_rx, wait_stop(stop_rx.clone())));

    let mut session = Session::new(SessionSetup {
        identity,
        store,
        facts,
        desired,
        observed,
        current,
    })
    .with_ingress(Arc::clone(&ingress), dns);
    let outcome = session.run(stop_rx).await;

    let _ = stop.send(true);
    let _ = reconcile_task.await;
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
