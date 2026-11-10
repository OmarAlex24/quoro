// SPDX-License-Identifier: AGPL-3.0-only
//! The primary-side tunnel manager (ADR 0094, 0095;
//! `node-agent-protocol.md` section 5.11).
//!
//! One `node_tunnels` row maps a node to the `tunnel` provider whose account
//! holds its Cloudflare Tunnel. This service is the only place that uses the
//! provider's account credential for a tunnel: it creates or looks up the
//! tunnel, stores its id and its run token encrypted with the cluster
//! data-encryption key, and reconciles the tunnel's ingress rules and proxied
//! CNAMEs with the node's `tunnel` routes.
//!
//! A hostname that moves to another machine's tunnel keeps its ingress rule
//! on the tunnel it left for a while after the new tunnel repointed its CNAME
//! (ADR 0194, [`crate::session::drain`]). The manager starts such a drain when
//! a hostname leaves the set its last sync served, records when a tunnel takes
//! a CNAME over, and ends every drain when its window is over.
//!
//! The node itself never sees the account credential: desired state carries
//! only its own tunnel id, its run token and the loopback origin port.
//! Tokens and provider secrets are never logged; errors are logged with the
//! node id and a message only.

use crate::api::providers::provider_secret_in;
use crate::changelog::{self, Actor, Op};
use crate::crypto::{CryptoError, DataKey, Secret};
use crate::error::ApiError;
use crate::session::NodeHub;
use crate::session::drain;
use crate::store::{Store, now_rfc3339};
use quoro_ingress::tunnel::TunnelController;
use quoro_providers::Token;
use quoro_providers::cloudflare::{API_BASE, Cloudflare, DOH_BASE};
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::Notify;
use tokio::time::Duration;

/// The AEAD context of `node_tunnels.token_enc`; the row identity is the node
/// id. One constant, so the writer here and the reader in
/// [`crate::session::desired`] can never disagree.
pub const TOKEN_CONTEXT: &str = "node_tunnels.token_enc";

/// How often every mapping is reconciled on its own.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(60);

/// Encrypts a tunnel run token for `node_id`.
#[must_use]
pub fn encrypt_token(key: &DataKey, node_id: &str, token: &str) -> Vec<u8> {
    key.encrypt(TOKEN_CONTEXT, node_id, token.as_bytes())
}

/// Decrypts a token written by [`encrypt_token`] for the same node.
///
/// # Errors
///
/// [`CryptoError`] when the blob was written under another key, for another
/// row, or was tampered with.
pub fn decrypt_token(key: &DataKey, node_id: &str, blob: &[u8]) -> Result<Secret, CryptoError> {
    key.decrypt(TOKEN_CONTEXT, node_id, blob)
}

/// Where the Cloudflare API and its DNS-over-HTTPS resolver live. Production
/// uses [`quoro_providers::cloudflare::API_BASE`]; the tests point at a local
/// mock through [`crate::AppState::with_cloudflare_endpoints`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudflareEndpoints {
    pub api_base: String,
    pub doh_base: String,
}

impl CloudflareEndpoints {
    /// The public Cloudflare endpoints.
    #[must_use]
    pub fn production() -> Self {
        Self {
            api_base: API_BASE.to_string(),
            doh_base: DOH_BASE.to_string(),
        }
    }
}

/// What this process last did for one node's tunnel.
#[derive(Clone, Debug)]
struct NodeSync {
    provider_id: String,
    tunnel_id: Option<String>,
    origin_port: u16,
    /// The hostname set of the last successful sync: the ingress rules,
    /// draining hostnames included.
    hostnames: BTreeSet<String>,
    /// The hostnames of the last successful sync that the tunnel serves as
    /// usual, without the draining ones. A hostname that leaves this set for
    /// another machine's tunnel starts draining here.
    normal: BTreeSet<String>,
    /// The CNAMEs this manager created and has not removed yet.
    published: BTreeSet<String>,
    /// Whether `hostnames`/`origin_port` describe a completed sync.
    synced: bool,
}

/// Nodes a wake-up asked for, or every mapping when `all` is set.
#[derive(Default)]
struct Pending {
    all: bool,
    nodes: HashSet<String>,
}

impl Pending {
    /// Takes the queued work, leaving an empty set behind.
    fn take(&mut self) -> (bool, HashSet<String>) {
        (
            std::mem::replace(&mut self.all, false),
            std::mem::take(&mut self.nodes),
        )
    }
}

struct TunnelInner {
    store: Store,
    data_key: Arc<DataKey>,
    hub: NodeHub,
    wake: Notify,
    pending: Mutex<Pending>,
    nodes: Mutex<HashMap<String, NodeSync>>,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    started: Mutex<bool>,
}

/// The tunnel manager owned by the control plane.
#[derive(Clone)]
pub struct TunnelManager {
    inner: Arc<TunnelInner>,
}

impl std::fmt::Debug for TunnelManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TunnelManager").finish_non_exhaustive()
    }
}

/// The handle the session hub keeps, so a session never owns the manager
/// (the manager already owns the hub).
#[derive(Clone)]
pub(crate) struct TunnelKick {
    inner: Weak<TunnelInner>,
}

impl TunnelKick {
    /// Wakes the manager for `node_id`.
    pub(crate) fn kick(&self, node_id: &str) {
        if let Some(inner) = self.inner.upgrade() {
            TunnelManager { inner }.kick(node_id);
        }
    }

    /// Wakes the manager for every mapped machine.
    pub(crate) fn kick_all(&self) {
        if let Some(inner) = self.inner.upgrade() {
            TunnelManager { inner }.kick_all();
        }
    }
}

impl TunnelManager {
    /// Builds the manager around the hub and the state it needs. Call
    /// [`Self::start`] once to run its background loop.
    pub(crate) fn new(store: Store, data_key: Arc<DataKey>, hub: NodeHub) -> Self {
        Self {
            inner: Arc::new(TunnelInner {
                store,
                data_key,
                hub,
                wake: Notify::new(),
                pending: Mutex::new(Pending::default()),
                nodes: Mutex::new(HashMap::new()),
                locks: Mutex::new(HashMap::new()),
                started: Mutex::new(false),
            }),
        }
    }

    /// A weak handle for the session hub.
    pub(crate) fn kick_handle(&self) -> TunnelKick {
        TunnelKick {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Starts the background loop: a full reconcile at startup and then every
    /// [`RECONCILE_INTERVAL`], plus one per [`Self::kick`] and one for each
    /// machine whose draining hostnames reached the end of their window.
    pub(crate) fn start(&self) {
        {
            let mut started = self.inner.started.lock().expect("tunnel start lock");
            if *started {
                return;
            }
            *started = true;
        }
        let manager = self.clone();
        tokio::spawn(async move { manager.run().await });
    }

    /// Asks the manager to reconcile `node_id` soon.
    pub fn kick(&self, node_id: &str) {
        self.inner
            .pending
            .lock()
            .expect("tunnel pending lock")
            .nodes
            .insert(node_id.to_string());
        self.inner.wake.notify_one();
    }

    /// Asks the manager to reconcile every mapped machine soon, as its
    /// periodic pass does. A change to a domain, an app's replica count or a
    /// running VM can move a hostname between a node's tunnel and the edge's,
    /// whether or not those machines have a session.
    pub fn kick_all(&self) {
        self.inner.pending.lock().expect("tunnel pending lock").all = true;
        self.inner.wake.notify_one();
    }

    /// Reconciles one node now. The per-node lock serializes it with the
    /// background loop, so two syncs never race on `published`.
    pub async fn sync_now(&self, node_id: &str) {
        let lock = self.lock_for(node_id);
        let _guard = lock.lock().await;
        if let Err(e) = self.reconcile(node_id).await
            && !self.inner.store.is_demoted()
        {
            tracing::warn!(node_id, error = %e, "cannot reconcile the node's tunnel");
        }
    }

    /// Whether the primary is demoted, checked before each step of a
    /// [`Self::sync_now`] pass that writes to Cloudflare: the demotion may
    /// land in the middle of one, and a demoted primary changes no tunnel and
    /// no DNS record (`node-agent-protocol.md` section 7.4). Within a step,
    /// the controller checks again before each of its calls
    /// ([`TunnelController::stop_when`]).
    fn stops_for_demotion(&self) -> bool {
        self.inner.store.stops_for_demotion("a tunnel sync")
    }

    /// The per-node reconcile lock.
    fn lock_for(&self, node_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.inner.locks.lock().expect("tunnel lock map");
        Arc::clone(
            locks
                .entry(node_id.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Seeds `published` and the last synced state from the database, so a
    /// restart never deletes CNAMEs it created before, then runs the loop.
    async fn run(self) {
        if let Err(e) = self.seed().await {
            tracing::warn!(error = %e, "cannot seed the tunnel manager");
        }
        self.inner.pending.lock().expect("tunnel pending lock").all = true;
        self.inner.wake.notify_one();
        // The periodic pass keeps its own deadline, so the wake-ups of kicks
        // and drains never postpone it.
        let mut next_pass = tokio::time::Instant::now() + RECONCILE_INTERVAL;
        loop {
            // A demoted primary changes no tunnel and no DNS record
            // (`node-agent-protocol.md` section 7.4).
            if self.inner.store.stops_for_demotion("the tunnel manager") {
                return;
            }
            let drains = self.inner.hub.drains();
            let next_end = drains.next_end().map(|end| {
                Duration::try_from(end - time::OffsetDateTime::now_utc()).unwrap_or(Duration::ZERO)
            });
            tokio::select! {
                () = self.inner.wake.notified() => {}
                () = drains.changed() => {}
                () = tokio::time::sleep(next_end.unwrap_or(RECONCILE_INTERVAL)),
                    if next_end.is_some() => {}
                () = tokio::time::sleep_until(next_pass) => {
                    next_pass = tokio::time::Instant::now() + RECONCILE_INTERVAL;
                    self.inner.pending.lock().expect("tunnel pending lock").all = true;
                }
            }
            if self.inner.store.stops_for_demotion("the tunnel manager") {
                return;
            }
            // A drain whose window is over leaves its machine's snapshot and
            // its tunnel's ingress rules now (ADR 0194).
            let ended = drain::sweep(
                self.inner.store.pool(),
                drains,
                time::OffsetDateTime::now_utc(),
            )
            .await;
            for node_id in &ended {
                self.inner.hub.node_changed(node_id);
            }
            let (all, mut nodes) = {
                let mut pending = self.inner.pending.lock().expect("tunnel pending lock");
                pending.take()
            };
            nodes.extend(ended);
            let targets: Vec<String> = if all {
                let mut targets: BTreeSet<String> = nodes.into_iter().collect();
                if let Ok(rows) = sqlx::query("SELECT node_id FROM node_tunnels")
                    .fetch_all(self.inner.store.pool())
                    .await
                {
                    for row in rows {
                        if let Ok(node_id) = row.try_get::<String, _>("node_id") {
                            targets.insert(node_id);
                        }
                    }
                }
                targets.extend(
                    self.inner
                        .nodes
                        .lock()
                        .expect("tunnel state lock")
                        .keys()
                        .cloned(),
                );
                targets.into_iter().collect()
            } else {
                nodes.into_iter().collect()
            };
            for node_id in targets {
                self.sync_now(&node_id).await;
            }
        }
    }

    /// Loads every mapping into memory without calling the provider.
    async fn seed(&self) -> Result<(), String> {
        let rows = sqlx::query(
            "SELECT node_id, provider_id, tunnel_id, origin_port FROM node_tunnels ORDER BY node_id",
        )
        .fetch_all(self.inner.store.pool())
        .await
        .map_err(|e| format!("cannot read node_tunnels: {e}"))?;
        let mut loaded = Vec::new();
        for row in &rows {
            let node_id: String = row.try_get("node_id").map_err(|e| e.to_string())?;
            let provider_id: String = row.try_get("provider_id").map_err(|e| e.to_string())?;
            let tunnel_id: Option<String> = row.try_get("tunnel_id").map_err(|e| e.to_string())?;
            let origin_port: i64 = row.try_get("origin_port").map_err(|e| e.to_string())?;
            let hostnames = match tunnel_id {
                Some(_) => {
                    crate::session::desired::tunnel_hostnames(self.inner.store.pool(), &node_id)
                        .await
                        .map_err(|e| format!("cannot read the tunnel routes of {node_id}: {e}"))?
                }
                None => BTreeSet::new(),
            };
            loaded.push((node_id, provider_id, tunnel_id, origin_port, hostnames));
        }
        let mut nodes = self.inner.nodes.lock().expect("tunnel state lock");
        for (node_id, provider_id, tunnel_id, origin_port, hostnames) in loaded {
            nodes.insert(
                node_id,
                NodeSync {
                    provider_id,
                    tunnel_id,
                    origin_port: u16::try_from(origin_port).unwrap_or(8081),
                    published: hostnames.clone(),
                    normal: hostnames.clone(),
                    hostnames,
                    synced: true,
                },
            );
        }
        Ok(())
    }

    /// Reconciles one node's row with the provider.
    async fn reconcile(&self, node_id: &str) -> Result<(), String> {
        let row = sqlx::query(
            "SELECT provider_id, tunnel_id, origin_port FROM node_tunnels WHERE node_id = ?",
        )
        .bind(node_id)
        .fetch_optional(self.inner.store.pool())
        .await
        .map_err(|e| format!("cannot read node_tunnels: {e}"))?;
        let Some(row) = row else {
            return self.remove(node_id).await;
        };
        let provider_id: String = row.try_get("provider_id").map_err(|e| e.to_string())?;
        let tunnel_id: Option<String> = row.try_get("tunnel_id").map_err(|e| e.to_string())?;
        let origin_port: i64 = row.try_get("origin_port").map_err(|e| e.to_string())?;
        let origin_port = u16::try_from(origin_port)
            .map_err(|e| format!("node_tunnels.origin_port of {node_id}: {e}"))?;

        let controller = match self.controller(&provider_id).await {
            Ok(controller) => controller,
            Err(e) => {
                if is_not_found(&e) {
                    tracing::warn!(
                        node_id,
                        provider_id,
                        "the tunnel provider is gone; dropping the mapping state"
                    );
                    self.inner
                        .nodes
                        .lock()
                        .expect("tunnel state lock")
                        .remove(node_id);
                    self.inner.hub.drains().forget(node_id);
                    return Ok(());
                }
                return Err(format!(
                    "cannot load the tunnel provider: {}",
                    e.code.as_str()
                ));
            }
        };

        // A provider change retires the old tunnel at the old provider first.
        let previous = self
            .inner
            .nodes
            .lock()
            .expect("tunnel state lock")
            .get(node_id)
            .cloned();
        if let Some(previous) = previous
            && previous.provider_id != provider_id
            && let Some(old_id) = previous.tunnel_id.clone()
        {
            self.retire(node_id, &previous, &old_id).await;
            self.inner.hub.drains().forget(node_id);
        }

        let tunnel_id = match tunnel_id {
            Some(id) => id,
            None => {
                let name: Option<String> =
                    sqlx::query_scalar("SELECT name FROM nodes WHERE id = ?")
                        .bind(node_id)
                        .fetch_optional(self.inner.store.pool())
                        .await
                        .map_err(|e| format!("cannot read the node name: {e}"))?;
                let Some(name) = name else {
                    return Ok(());
                };
                if self.stops_for_demotion() {
                    return Ok(());
                }
                let (tunnel, token) = controller
                    .ensure_tunnel(&name)
                    .await
                    .map_err(|e| format!("cannot ensure the tunnel: {e}"))?;
                self.store_tunnel(node_id, &provider_id, &tunnel.id, token.expose())
                    .await?;
                self.inner.hub.node_changed(node_id);
                tunnel.id
            }
        };

        // Taken before the hostnames are read: a drain that starts after it
        // may come from a newer change than the set below reflects.
        let read_at = std::time::Instant::now();
        let normal = crate::session::desired::tunnel_hostnames(self.inner.store.pool(), node_id)
            .await
            .map_err(|e| format!("cannot read the tunnel routes: {e}"))?;
        let drains = self.inner.hub.drains();
        // A hostname the last sync served as usual that moved to another
        // machine's tunnel keeps its ingress rule here for a while (ADR 0194),
        // whether or not the machine has a session to notice it.
        let last_normal = self
            .inner
            .nodes
            .lock()
            .expect("tunnel state lock")
            .get(node_id)
            .filter(|state| state.provider_id == provider_id)
            .map(|state| state.normal.clone());
        if let Some(last_normal) = last_normal {
            for hostname in last_normal.difference(&normal) {
                let departed = drain::departed(self.inner.store.pool(), node_id, hostname)
                    .await
                    .map_err(|e| format!("cannot check a departed hostname: {e}"))?;
                if let Some(app_id) = departed
                    && drains.start(node_id, hostname, &app_id, time::OffsetDateTime::now_utc())
                {
                    self.inner.hub.node_changed(node_id);
                }
            }
        }
        drains.clear_served(node_id, &normal, read_at);
        let draining = drain::draining_hostnames(self.inner.store.pool(), drains, node_id, &normal)
            .await
            .map_err(|e| format!("cannot read the draining hostnames: {e}"))?;
        let hostnames: BTreeSet<String> = normal.union(&draining).cloned().collect();
        let unchanged = {
            let nodes = self.inner.nodes.lock().expect("tunnel state lock");
            nodes.get(node_id).is_some_and(|state| {
                state.synced
                    && state.provider_id == provider_id
                    && state.tunnel_id.as_deref() == Some(tunnel_id.as_str())
                    && state.origin_port == origin_port
                    && state.hostnames == hostnames
                    && state.normal == normal
            })
        };
        if unchanged {
            return Ok(());
        }

        let mut published = self
            .inner
            .nodes
            .lock()
            .expect("tunnel state lock")
            .get(node_id)
            .map(|state| state.published.clone())
            .unwrap_or_default();
        // A hostname this tunnel drops but another tunnel now serves keeps
        // its CNAME: the new tunnel repoints that record instead of this one
        // deleting it and the new one creating another (issue #344). A
        // draining hostname is one of them: it keeps only its ingress rule.
        let handoff = self
            .handoff(node_id, &provider_id, &published, &normal)
            .await?;
        for hostname in handoff.keys() {
            published.remove(hostname);
        }
        if self.stops_for_demotion() {
            return Ok(());
        }
        match controller
            .sync_routes_draining(&tunnel_id, origin_port, &normal, &draining, &mut published)
            .await
        {
            Ok(failures) => {
                for (hostname, reason) in &failures {
                    tracing::warn!(
                        node_id,
                        hostname,
                        error = %reason,
                        "cannot publish a tunnel hostname"
                    );
                }
                // Until the new tunnel has written the CNAME, this one still
                // answers for the record: it stays in `published`, so it is
                // deleted here if the hostname ends up served by no tunnel,
                // and the next pass looks again.
                let mut waiting = false;
                for (hostname, repointed) in handoff {
                    if !repointed {
                        published.insert(hostname);
                        waiting = true;
                    }
                }
                // A CNAME this tunnel now holds for a hostname that drains on
                // another machine starts that drain's window (ADR 0194).
                // A hostname whose upsert failed may hold an older entry
                // in `published`: it took nothing over.
                let failed: BTreeSet<&str> = failures
                    .iter()
                    .map(|(hostname, _)| hostname.as_str())
                    .collect();
                let written: BTreeSet<String> = normal
                    .intersection(&published)
                    .filter(|hostname| !failed.contains(hostname.as_str()))
                    .cloned()
                    .collect();
                let repointed = drains.record_cnames(
                    node_id,
                    &written,
                    &published,
                    time::OffsetDateTime::now_utc(),
                );
                for machine in &repointed {
                    self.inner.hub.node_changed(machine);
                }
                let synced = failures.is_empty() && !waiting;
                self.inner.nodes.lock().expect("tunnel state lock").insert(
                    node_id.to_string(),
                    NodeSync {
                        provider_id,
                        tunnel_id: Some(tunnel_id),
                        origin_port,
                        hostnames,
                        normal,
                        published,
                        synced,
                    },
                );
                Ok(())
            }
            Err(e) => Err(format!("cannot sync the tunnel routes: {e}")),
        }
    }

    /// The hostnames `node_id`'s tunnel published and no longer serves that
    /// another machine's tunnel of the same provider serves now, each with
    /// whether that tunnel already wrote its CNAME (it is in its `published`).
    ///
    /// Such a hostname moved between tunnels, typically between a node and
    /// the edge when an app's replica count crossed 1. Its CNAME is the new
    /// tunnel's to repoint, never this tunnel's to delete.
    async fn handoff(
        &self,
        node_id: &str,
        provider_id: &str,
        published: &BTreeSet<String>,
        hostnames: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, bool>, String> {
        let stale: BTreeSet<&String> = published.difference(hostnames).collect();
        if stale.is_empty() {
            return Ok(BTreeMap::new());
        }
        let others: Vec<String> = sqlx::query_scalar(
            "SELECT node_id FROM node_tunnels \
             WHERE node_id <> ? AND provider_id = ? AND tunnel_id IS NOT NULL \
             ORDER BY node_id",
        )
        .bind(node_id)
        .bind(provider_id)
        .fetch_all(self.inner.store.pool())
        .await
        .map_err(|e| format!("cannot read node_tunnels: {e}"))?;
        let mut handoff = BTreeMap::new();
        for other in others {
            let served = crate::session::desired::tunnel_hostnames(self.inner.store.pool(), &other)
                .await
                .map_err(|e| format!("cannot read the tunnel routes of {other}: {e}"))?;
            let repointed: BTreeSet<String> = self
                .inner
                .nodes
                .lock()
                .expect("tunnel state lock")
                .get(&other)
                .map(|state| state.published.clone())
                .unwrap_or_default();
            for hostname in stale.iter().filter(|hostname| served.contains(**hostname)) {
                let entry = handoff.entry((*hostname).clone()).or_insert(false);
                *entry |= repointed.contains(*hostname);
            }
        }
        Ok(handoff)
    }

    /// Retires a tunnel whose node no longer has a mapping.
    async fn remove(&self, node_id: &str) -> Result<(), String> {
        let Some(state) = self
            .inner
            .nodes
            .lock()
            .expect("tunnel state lock")
            .get(node_id)
            .cloned()
        else {
            return Ok(());
        };
        let Some(tunnel_id) = state.tunnel_id.clone() else {
            self.inner
                .nodes
                .lock()
                .expect("tunnel state lock")
                .remove(node_id);
            return Ok(());
        };
        // Keep the state when the provider is unreachable, so the removal is
        // retried on the next wake-up.
        if self.retire(node_id, &state, &tunnel_id).await {
            self.inner
                .nodes
                .lock()
                .expect("tunnel state lock")
                .remove(node_id);
            self.inner.hub.drains().forget(node_id);
        }
        Ok(())
    }

    /// Removes a tunnel's CNAMEs and then the tunnel itself, best effort.
    /// Returns whether the tunnel is gone at the provider.
    async fn retire(&self, node_id: &str, state: &NodeSync, tunnel_id: &str) -> bool {
        let controller = match self.controller(&state.provider_id).await {
            Ok(controller) => controller,
            Err(e) => {
                if is_not_found(&e) {
                    tracing::warn!(
                        node_id,
                        provider_id = %state.provider_id,
                        "the tunnel provider is gone; nothing to remove at the provider"
                    );
                    return true;
                }
                tracing::warn!(
                    node_id,
                    code = e.code.as_str(),
                    "cannot load the tunnel provider to remove a tunnel"
                );
                return false;
            }
        };
        let mut published = state.published.clone();
        let empty = BTreeSet::new();
        if self.stops_for_demotion() {
            return false;
        }
        if let Err(e) = controller
            .sync_routes(tunnel_id, state.origin_port, &empty, &mut published)
            .await
        {
            if !self.inner.store.is_demoted() {
                tracing::warn!(node_id, error = %e, "cannot remove the tunnel hostnames");
            }
            return false;
        }
        if self.stops_for_demotion() {
            return false;
        }
        if let Err(e) = controller.delete_tunnel(tunnel_id).await {
            if !self.inner.store.is_demoted() {
                tracing::warn!(node_id, error = %e, "cannot delete the tunnel");
            }
            return false;
        }
        true
    }

    /// Builds the tunnel controller of `provider_id` from its decrypted
    /// credential. Never logs the secret.
    async fn controller(&self, provider_id: &str) -> Result<TunnelController, ApiError> {
        let (record, secret) =
            provider_secret_in(&self.inner.store, &self.inner.data_key, provider_id).await?;
        if record.kind != "tunnel" {
            return Err(ApiError::internal(&format!(
                "provider {provider_id} is not a tunnel provider"
            )));
        }
        let account_id = record
            .config
            .get("account_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApiError::internal(&format!("tunnel provider {provider_id} has no account_id"))
            })?;
        let zone_id = record
            .config
            .get("zone_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ApiError::internal(&format!("tunnel provider {provider_id} has no zone_id"))
            })?;
        let endpoints = self.inner.hub.cloudflare_endpoints();
        let cf = Cloudflare::with_endpoints(
            Token::new(secret.expose().to_string()),
            &endpoints.api_base,
            &endpoints.doh_base,
        )
        .map_err(|e| ApiError::internal(&e))?;
        // A demoted primary changes no tunnel and no DNS record: the
        // controller checks the flag before each of its Cloudflare writes, so
        // a demotion in the middle of a sync stops the rest of it
        // (`node-agent-protocol.md` section 7.4).
        let store = self.inner.store.clone();
        Ok(
            TunnelController::new(cf, account_id.to_string(), zone_id.to_string())
                .stop_when(move || store.is_demoted()),
        )
    }

    /// Stores the tunnel id and the encrypted run token, with its change-log
    /// entry, in one transaction.
    async fn store_tunnel(
        &self,
        node_id: &str,
        provider_id: &str,
        tunnel_id: &str,
        token: &str,
    ) -> Result<(), String> {
        let token_enc = encrypt_token(&self.inner.data_key, node_id, token);
        let now = now_rfc3339();
        let mut w = self
            .inner
            .store
            .begin_write()
            .await
            .map_err(|e| e.to_string())?;
        let updated = sqlx::query(
            "UPDATE node_tunnels SET tunnel_id = ?, token_enc = ?, updated_at = ? \
             WHERE node_id = ? AND provider_id = ? AND tunnel_id IS NULL",
        )
        .bind(tunnel_id)
        .bind(&token_enc)
        .bind(&now)
        .bind(node_id)
        .bind(provider_id)
        .execute(&mut *w.tx)
        .await
        .map_err(|e| e.to_string())?
        .rows_affected();
        if updated == 0 {
            // The row changed under us (another provider); the next reconcile
            // starts over from the new row.
            return Ok(());
        }
        changelog::append(
            &mut w.tx,
            &Actor::system("tunnel"),
            "node_tunnel",
            node_id,
            Op::Update,
            serde_json::json!({
                "node_id": node_id,
                "provider_id": provider_id,
                "tunnel_id": tunnel_id,
                "token_enc": hex(&token_enc),
                "updated_at": now,
            }),
        )
        .await
        .map_err(|e| e.to_string())?;
        w.commit().await.map_err(|e| e.to_string())
    }
}

/// Whether an API error is a `not_found`.
fn is_not_found(error: &ApiError) -> bool {
    error.code == crate::error::Code::NotFound
}

/// Lowercase hex, for the ciphertext the change log may carry.
pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0F)]));
    }
    out
}
