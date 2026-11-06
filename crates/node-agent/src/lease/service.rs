// SPDX-License-Identifier: AGPL-3.0-only
//! The `local` lease service the session and the reconciler share
//! (`node-agent-protocol.md` sections 5.12, 5.13, 8.3, 8.5).
//!
//! It adapts the M0 state machine ([`LeaseTable`]) to the running agent: the
//! session forwards every `lease_grant` and `lease_revoke` here, the reconciler
//! asks [`LeaseView`] whether a stateful VM may start, and both see the same
//! table. Only `local` leases exist in M5a (section 8.7): they never expire and
//! send no `lease_renew`, so the timer half of [`crate::lease::LeaseAgent`] is
//! not needed here.
//!
//! A revoke is handled completely, including its fence, before its answer is
//! returned, so the session answers it before it processes the next message
//! (section 4.2 rule 3).
//!
//! Fences run concurrently (section 8.5 "Concurrency", D-0075): each one is a
//! task of its own, so the service keeps taking grants and revokes for other
//! leases while one is stopping its VMs, and the fences of different leases
//! never wait for each other. A second trigger for a lease whose fence is
//! running joins that fence instead of stopping the same VMs again. A fence
//! holds its lease's start gate exclusively, and the reconciler starts a
//! stateful VM only under that gate's shared side ([`LeaseView::start_permit`]),
//! so a start and a fence of the same lease never overlap.
//!
//! A fence chooses its VMs only once it holds the gate, so a start that ran
//! before it is seen. It stops every VM any desired snapshot bound to the
//! lease since the agent started, and every app VM the engine runs that the
//! current snapshot no longer names: such a VM is on its way out anyway, and
//! it may have been bound to the lease in a snapshot this process never saw
//! (a snapshot that dropped it, or one from before an agent restart). With no
//! snapshot at all, that is every app VM on the node. A fence that cannot list
//! the engine's VMs cannot confirm that every bound VM stopped, so it fails
//! like a fence whose forced stop could not be confirmed (sections 5.13 and
//! 8.5, D-0074): it stops what it knows, records the failure, emits
//! `node.fence_failed`, marks the node failed and answers `fence_failed`.
//!
//! A revoke of a lease that is already `revoked` or `expired` at the same
//! token, with a complete fence recorded for it, is answered `ok` at once:
//! nothing could have started for the lease since, and the primary sends such
//! a revoke at every session start for a lease the node still reports.
//!
//! A fence is recorded only once its stop finished, in one transaction with
//! the lease's row, its event and, for a failed fence, the node's failed mark.
//! A lease that is `revoked` or `expired` in the store without a record for its
//! token was therefore interrupted by an agent crash, and the restore fences it
//! again ([`LeaseTable::restore`]). A revoke is answered with success only once
//! that transaction committed.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use quoro_engine::{Engine, VmId, VmState};
use quoro_proto::wire::HelloLease;
use quoro_proto::{
    CommandResult, CommandStatus, Envelope, ErrorBody, ErrorCode, FenceReason, LeaseGrant, LeaseId,
    LeaseMode, LeaseRevoke, MsgId, ProtocolError, ResourceType,
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::AgentError;
use crate::lease::clock::{HostNow, SystemHostClock};
use crate::lease::fence::{FenceReport, FenceTimers, VmFenceOutcome, budget_for, stop_vms};
use crate::lease::machine::{LeaseState, LeaseTable, Refusal, RevokeOutcome};
use crate::lease::store::{FenceRecord, StoredLease};
use crate::reconcile::Snapshot;
use crate::store::{HOST_BOOT_ID, NodeStore};
use crate::util;

/// What the service needs that is not the engine or the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseConfig {
    /// The `nod_` id `node.fenced` carries (section 8.5 step 3).
    pub node_id: String,
    /// The stop timeouts a fence uses.
    pub timers: FenceTimers,
}

/// The read side of the lease table, shared with the reconciler.
#[derive(Clone, Debug)]
pub struct LeaseView {
    table: Arc<Mutex<LeaseTable>>,
    failed: Arc<AtomicBool>,
    gates: Arc<FenceGates>,
}

/// What keeps a stateful start and a fence of the same lease apart.
#[derive(Debug, Default)]
struct FenceGates {
    /// Leases whose fence is running or not yet recorded: nothing stateful
    /// starts for them, even after a newer grant made them `held` again.
    fencing: Mutex<HashSet<LeaseId>>,
    /// One gate per lease: a start holds it shared, a fence exclusively.
    gates: Mutex<HashMap<LeaseId, Arc<RwLock<()>>>>,
}

impl FenceGates {
    fn fencing(&self) -> MutexGuard<'_, HashSet<LeaseId>> {
        self.fencing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn gate(&self, lease_id: &LeaseId) -> Arc<RwLock<()>> {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(gates.entry(lease_id.clone()).or_default())
    }
}

/// Held across the start of a stateful VM (section 6.4): while it lives, no
/// fence of the VM's lease can begin, and a fence that is waiting stops the VM
/// as soon as the start is done.
#[derive(Debug)]
pub struct StartPermit {
    _shared: OwnedRwLockReadGuard<()>,
}

impl Default for LeaseView {
    fn default() -> Self {
        Self::new(LeaseTable::new(), false)
    }
}

impl LeaseView {
    /// A view over an existing table; a test uses this to place a lease
    /// without running the service.
    #[must_use]
    pub fn new(table: LeaseTable, failed: bool) -> Self {
        Self {
            table: Arc::new(Mutex::new(table)),
            failed: Arc::new(AtomicBool::new(failed)),
            gates: Arc::new(FenceGates::default()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, LeaseTable> {
        self.table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Section 6.4: may a stateful VM with this spec token start now?
    ///
    /// Not while the node is marked failed, and not while a fence of the lease
    /// is running.
    #[must_use]
    pub fn may_start_stateful(&self, lease_id: &str, spec_token: u64) -> bool {
        let lease_id = LeaseId(lease_id.to_owned());
        !self.failed.load(Ordering::SeqCst)
            && !self.gates.fencing().contains(&lease_id)
            && self.lock().may_start_stateful(&lease_id, spec_token)
    }

    /// Section 8.4: may a stateful VM with this spec token that already runs
    /// keep running? True while the lease is `held` or `uncertain` with that
    /// token: an `uncertain` lease keeps its VMs and starts nothing new. Not
    /// while the node is marked failed or a fence of the lease is running.
    #[must_use]
    pub fn may_keep_running(&self, lease_id: &str, spec_token: u64) -> bool {
        let lease_id = LeaseId(lease_id.to_owned());
        !self.failed.load(Ordering::SeqCst)
            && !self.gates.fencing().contains(&lease_id)
            && self.lock().may_keep_running(&lease_id, spec_token)
    }

    /// Section 6.4 at the moment of the start: waits until no fence of the
    /// lease is stopping VMs, then answers [`Self::may_start_stateful`] while
    /// holding the lease's start gate.
    ///
    /// The caller keeps the permit until the engine's start returned. A fence
    /// triggered meanwhile waits for it and then stops the VM, so a VM can
    /// never be started after the fence that should stop it.
    pub async fn start_permit(&self, lease_id: &str, spec_token: u64) -> Option<StartPermit> {
        let gate = self.gates.gate(&LeaseId(lease_id.to_owned()));
        let shared = gate.read_owned().await;
        self.may_start_stateful(lease_id, spec_token)
            .then_some(StartPermit { _shared: shared })
    }

    /// Whether the node is marked failed (section 12 `node_meta`).
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    /// Whether the node holds the app's lease with exactly `token`, so a backup
    /// job of that token may run (section 6.7, `backup-format.md` section 7.1
    /// step 2).
    ///
    /// Only `held` counts: an `uncertain`, `expired` or `revoked` lease means
    /// the node may no longer act for the app, and the job fails with
    /// `lease_mismatch`.
    #[must_use]
    pub fn holds_app_lease(&self, app_id: &str, token: u64) -> bool {
        self.lock().leases().any(|lease| {
            lease.resource_type == ResourceType::App
                && lease.resource_id == app_id
                && lease.state == LeaseState::Held
                && lease.token == token
        })
    }

    /// The node-side state of one lease, for a test.
    #[must_use]
    pub fn state(&self, lease_id: &str) -> Option<LeaseState> {
        self.lock()
            .get(&LeaseId(lease_id.to_owned()))
            .map(|lease| lease.state)
    }
}

/// What the session sends the service and awaits.
enum LeaseCommand {
    Grant {
        envelope: Envelope<LeaseGrant>,
        reply: oneshot::Sender<Result<(), ProtocolError>>,
    },
    Revoke {
        envelope: Envelope<LeaseRevoke>,
        reply: oneshot::Sender<Result<CommandResult, ProtocolError>>,
    },
    FenceAck {
        lease_ids: Vec<String>,
        reply: oneshot::Sender<CommandResult>,
    },
}

/// The session's handle to the lease service.
#[derive(Clone)]
pub struct LeaseLink {
    commands: mpsc::UnboundedSender<LeaseCommand>,
    hello: watch::Receiver<Vec<HelloLease>>,
}

impl LeaseLink {
    /// Every lease the node knows, as `hello` reports them (section 5.1).
    #[must_use]
    pub fn hello(&self) -> Vec<HelloLease> {
        self.hello.borrow().clone()
    }

    /// Applies a `lease_grant` (section 5.12).
    ///
    /// # Errors
    ///
    /// [`ProtocolError`] code `bad_message` when the grant is refused.
    pub async fn grant(
        &self,
        id: MsgId,
        re: Option<MsgId>,
        body: LeaseGrant,
    ) -> Result<(), ProtocolError> {
        let (reply, answer) = oneshot::channel();
        if self
            .commands
            .send(LeaseCommand::Grant {
                envelope: Envelope { id, re, body },
                reply,
            })
            .is_err()
        {
            return Err(service_gone());
        }
        answer.await.unwrap_or_else(|_| Err(service_gone()))
    }

    /// Applies a `lease_revoke` and fences the VMs bound to it (section 5.13).
    ///
    /// A service that is not running answers `command_result` status `error`:
    /// nothing was stopped or recorded, and a refusal (`bad_message`) would
    /// claim the revoke was stale.
    ///
    /// # Errors
    ///
    /// [`ProtocolError`] code `bad_message` when the revoke is refused.
    pub async fn revoke(
        &self,
        id: MsgId,
        re: Option<MsgId>,
        body: LeaseRevoke,
    ) -> Result<CommandResult, ProtocolError> {
        let (reply, answer) = oneshot::channel();
        if self
            .commands
            .send(LeaseCommand::Revoke {
                envelope: Envelope { id, re, body },
                reply,
            })
            .is_err()
        {
            return Ok(not_fenced("the lease service is not running"));
        }
        answer
            .await
            .unwrap_or_else(|_| Ok(not_fenced("the lease service stopped")))
    }
}

impl LeaseLink {
    /// Applies a `fence_ack` (section 5.17, section 8.5): clears the failed
    /// fence of each listed lease, and the node's failed mark once none
    /// remains. The listed leases stay `revoked` or `expired`: nothing starts
    /// for them before a grant with a higher token.
    pub async fn fence_ack(&self, lease_ids: Vec<String>) -> CommandResult {
        let (reply, answer) = oneshot::channel();
        if self
            .commands
            .send(LeaseCommand::FenceAck { lease_ids, reply })
            .is_err()
        {
            return not_fenced("the lease service is not running");
        }
        answer
            .await
            .unwrap_or_else(|_| not_fenced("the lease service stopped"))
    }
}

/// A running lease service plus the handles the rest of the agent uses.
pub struct LocalLeases {
    /// The session's handle.
    pub link: LeaseLink,
    /// The reconciler's read side.
    pub view: LeaseView,
    task: JoinHandle<()>,
}

impl LocalLeases {
    /// Stops the service task.
    pub fn abort(self) {
        self.task.abort();
    }
}

/// Restores the lease table from the store and starts the service.
///
/// # Errors
///
/// [`AgentError`] when the stored lease rows cannot be read or the restored
/// state cannot be written back.
pub async fn start_local_leases<E: Engine + 'static>(
    engine: Arc<E>,
    store: NodeStore,
    config: LeaseConfig,
    desired: watch::Receiver<Option<Snapshot>>,
) -> Result<LocalLeases, AgentError> {
    let records = store.fences().await?;
    let unreported = unreported_failures(&store, &records).await;
    let recorded: HashSet<(LeaseId, u64)> = records
        .iter()
        .filter(|record| record.complete)
        .map(|record| (record.lease_id.clone(), record.token))
        .collect();
    let fenced: HashSet<(LeaseId, u64)> = records
        .into_iter()
        .map(|record| (record.lease_id, record.token))
        .collect();
    let host = HostNow::read(&SystemHostClock);
    // This service has no renewal loop yet, so nothing could renew a restored
    // `expiring` lease or fence it at its deadline: the restore keeps fencing
    // every such lease, the conservative M0 rule, until the loop arrives
    // (section 8.4 "Agent restart", #75).
    let now = HostNow::unknown(host.instant);
    let (table, pending) = LeaseTable::restore(
        store.load_leases().await?,
        store.load_highest_tokens().await?,
        &fenced,
        &now,
    );
    // An `expiring` lease the restore could not prove is written back
    // `expired` before its fence begins, so the store never keeps a
    // validity the table no longer stands behind.
    // Both writes are best effort: a store that takes no writes must not
    // keep the agent from starting the fences the restore found, and a
    // validity that failed to restore once fails again on this boot.
    for lease in table.leases() {
        if lease.mode == LeaseMode::Expiring {
            let highest = table.highest_for(lease.resource_type, &lease.resource_id);
            if let Err(e) = store
                .save_lease(&StoredLease::from_lease(lease, &now), highest)
                .await
            {
                warn!(error = %e, lease_id = %lease.id.0, "could not write back a restored lease");
            }
        }
    }
    if let Some(boot_id) = &host.boot_id
        && let Err(e) = store.set_meta(HOST_BOOT_ID, boot_id).await
    {
        warn!(error = %e, "could not store the host boot id");
    }
    let failed = store.node_failed().await?;
    if let Some(failures) = unreported {
        report_unreported(&store, &config, &table, failures).await;
    }
    if failed
        && store
            .failed_fences()
            .await
            .is_ok_and(|listed| listed.is_empty())
    {
        warn!(
            "the node is marked failed but names no failed fence, so the primary lists nothing \
             to acknowledge; the next fence_ack this node takes clears the mark, or clear it by \
             hand once its VMs are checked"
        );
    }
    let view = LeaseView::new(table, failed);
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (hello_tx, hello_rx) = watch::channel(hello_leases(&view.table));

    let mut service = LocalLeaseService {
        engine,
        store,
        config,
        view: view.clone(),
        desired,
        bound: HashMap::new(),
        commands: command_rx,
        answered: HashMap::new(),
        hello: hello_tx,
        pending,
        in_flight: HashMap::new(),
        tasks: JoinSet::new(),
        task_leases: HashMap::new(),
        recorded,
        unstored_failures: BTreeSet::new(),
    };
    service.note_bindings();
    let task = tokio::spawn(service.run());
    Ok(LocalLeases {
        link: LeaseLink {
            commands,
            hello: hello_rx,
        },
        view,
        task,
    })
}

/// The latest failed fence record of each lease an upgrade left to report,
/// or `None` when there is nothing to report (`node_meta`
/// `failed_fences_unreported`).
async fn unreported_failures(
    store: &NodeStore,
    records: &[FenceRecord],
) -> Option<Vec<FenceRecord>> {
    match store.failed_fences_unreported().await {
        Ok(true) => {}
        Ok(false) => return None,
        Err(e) => {
            warn!(error = %e, "could not read whether failed fences wait to be reported");
            return None;
        }
    }
    let listed = match store.failed_fences().await {
        Ok(listed) => listed,
        Err(e) => {
            // The flag stays set, so the next start reports them.
            warn!(error = %e, "could not read the failed fences to report");
            return None;
        }
    };
    let mut latest: HashMap<String, FenceRecord> = HashMap::new();
    for record in records.iter().filter(|record| !record.complete) {
        if listed.contains(&record.lease_id.0) {
            latest.insert(record.lease_id.0.clone(), record.clone());
        }
    }
    let mut failures: Vec<FenceRecord> = latest.into_values().collect();
    failures.sort_by(|a, b| a.lease_id.0.cmp(&b.lease_id.0));
    Some(failures)
}

/// Reports, once, the failed fences an older agent recorded: one
/// `node.fence_failed` per lease, naming the VMs its record names, so the
/// primary lists them for an acknowledgement (section 8.5). Best effort: a
/// store that takes no writes reports them at the next start.
async fn report_unreported(
    store: &NodeStore,
    config: &LeaseConfig,
    table: &LeaseTable,
    failures: Vec<FenceRecord>,
) {
    let mut events = Vec::with_capacity(failures.len());
    for record in failures {
        let app_ids: Vec<String> = table
            .get(&record.lease_id)
            .filter(|lease| lease.resource_type == ResourceType::App)
            .map(|lease| vec![lease.resource_id.clone()])
            .unwrap_or_default();
        let vm_ids: Vec<String> = record.vm_ids.iter().map(|vm| vm.0.clone()).collect();
        let evt_id = util::event_id();
        let event = serde_json::json!({
            "schema_version": 1,
            "id": evt_id,
            "type": "node.fence_failed",
            "time": util::rfc3339_nanos(store.next_log_time()),
            "source": {"kind": "node", "node_id": config.node_id},
            "subject": {"type": "lease", "id": record.lease_id.0},
            "actor": {"type": "node", "id": config.node_id},
            "data": {
                "node_id": config.node_id,
                "app_ids": app_ids,
                "lease_ids": [record.lease_id.0],
                "vm_ids": vm_ids,
                "reason": match record.reason {
                    FenceReason::LeaseExpired => "lease_expired",
                    FenceReason::LeaseRevoked => "lease_revoked",
                },
                "error": "reported again after an agent upgrade: this fence failed before the agent kept a list of failed fences",
            },
        });
        events.push((evt_id, event));
    }
    if let Err(e) = store.report_failed_fences(&events).await {
        warn!(error = %e, "could not report the failed fences an older agent recorded");
    }
}

/// Who waits for a fence to finish: the revoke it answers.
struct Waiter {
    msg: MsgId,
    reply: oneshot::Sender<Result<CommandResult, ProtocolError>>,
}

/// A fence whose stop is running.
struct InFlight {
    /// The token the lease had when the fence began.
    token: u64,
    /// Why it began; a trigger that joins later does not change it.
    reason: FenceReason,
    /// The VMs known to be bound when it began; the task chooses the VMs it
    /// stops itself, and these only name the VMs of a task that died.
    vms: Vec<VmId>,
    /// The revokes it answers once it is recorded.
    waiters: Vec<Waiter>,
}

/// What a fence task reports: the stop of each VM it chose, and why its
/// choice may have missed a VM.
struct FenceOutcome {
    report: FenceReport,
    /// Set when the engine's VMs could not be listed, so a VM bound to the
    /// lease may have been missed and still run.
    unlisted: Option<String>,
    /// Set when the fence task itself died, so no stop is confirmed.
    died: Option<String>,
}

impl FenceOutcome {
    /// Every VM the lease may hold is confirmed stopped; otherwise a VM may
    /// still run and the fence failed (sections 5.13 and 8.5, D-0074).
    fn complete(&self) -> bool {
        self.died.is_none() && self.unlisted.is_none() && self.report.complete()
    }
}

/// The async driver behind [`LocalLeases`].
struct LocalLeaseService<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    config: LeaseConfig,
    view: LeaseView,
    /// The desired state the reconciler follows.
    desired: watch::Receiver<Option<Snapshot>>,
    /// Every VM a desired snapshot bound to each lease since the agent
    /// started; a snapshot that drops a VM does not drop it here.
    bound: HashMap<LeaseId, Vec<VmId>>,
    commands: mpsc::UnboundedReceiver<LeaseCommand>,
    answered: HashMap<MsgId, CommandResult>,
    hello: watch::Sender<Vec<HelloLease>>,
    pending: Vec<crate::lease::Action>,
    /// Running fences, at most one per lease.
    in_flight: HashMap<LeaseId, InFlight>,
    /// The stops of the running fences. Dropping the service aborts them, as
    /// an agent crash would; the restore then runs them again.
    tasks: JoinSet<FenceOutcome>,
    /// Which lease each stop task fences.
    task_leases: HashMap<tokio::task::Id, LeaseId>,
    /// The complete fences recorded in the store, by lease and token.
    recorded: HashSet<(LeaseId, u64)>,
    /// Leases whose failed fence the store did not take: the failed mark
    /// holds for them in memory, and only a `fence_ack` naming them clears
    /// them (section 5.17).
    unstored_failures: BTreeSet<String>,
}

impl<E: Engine + 'static> LocalLeaseService<E> {
    fn lock(&self) -> MutexGuard<'_, LeaseTable> {
        self.view.lock()
    }

    async fn run(mut self) {
        // Fences the restore found begin before any message is taken, and
        // run side by side (section 8.5).
        for action in std::mem::take(&mut self.pending) {
            if let crate::lease::Action::Fence { lease_id, reason } = action {
                self.begin_fence(lease_id, reason, None);
            }
        }
        loop {
            tokio::select! {
                changed = self.desired.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.note_bindings();
                }
                command = self.commands.recv() => {
                    match command {
                        Some(command) => self.handle(command).await,
                        None => break,
                    }
                }
                Some(joined) = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    self.finish_fence(joined).await;
                }
            }
        }
        // The agent is shutting down. A fence still running is cut off with
        // the service, as by a crash: its lease is `revoked` or `expired`
        // without a record, so the restore fences it again.
    }

    /// Adds the bindings of the current desired snapshot to [`Self::bound`].
    fn note_bindings(&mut self) {
        let current = bindings_from(self.desired.borrow_and_update().as_ref());
        for (lease_id, vms) in current {
            let known = self.bound.entry(lease_id).or_default();
            for vm in vms {
                if !known.contains(&vm) {
                    known.push(vm);
                }
            }
        }
    }

    async fn handle(&mut self, command: LeaseCommand) {
        match command {
            LeaseCommand::Grant { envelope, reply } => {
                let _ = reply.send(self.grant(envelope).await);
            }
            LeaseCommand::Revoke { envelope, reply } => {
                self.revoke(envelope, reply).await;
            }
            LeaseCommand::FenceAck { lease_ids, reply } => {
                let _ = reply.send(self.fence_ack(&lease_ids).await);
            }
        }
    }

    /// Section 5.17 `fence_ack`: the store clears the listed failed fences
    /// first, and the failed mark in memory follows only once the store says
    /// none remains, so a crash never leaves the node startable with a failed
    /// fence still recorded.
    async fn fence_ack(&mut self, lease_ids: &[String]) -> CommandResult {
        // A fence of a listed lease that is still running may fail after the
        // operator checked; its outcome must not be cleared unseen.
        if let Some(busy) = lease_ids
            .iter()
            .find(|lease_id| self.in_flight.contains_key(&LeaseId((*lease_id).clone())))
        {
            return not_fenced(&format!(
                "a fence of lease {busy} is running; the acknowledgement is not applied"
            ));
        }
        match self.store.ack_fences(lease_ids).await {
            Ok(mut remaining) => {
                for lease_id in lease_ids {
                    self.unstored_failures.remove(lease_id);
                }
                for lease_id in &self.unstored_failures {
                    if !remaining.contains(lease_id) {
                        remaining.push(lease_id.clone());
                    }
                }
                if remaining.is_empty() {
                    self.view.failed.store(false, Ordering::SeqCst);
                    CommandResult {
                        status: CommandStatus::Ok,
                        detail: Some("no failed fence remains".to_owned()),
                        error: None,
                        lines: None,
                        metrics: None,
                    }
                } else {
                    CommandResult {
                        status: CommandStatus::Ok,
                        detail: Some(format!(
                            "the node stays failed: failed fences remain for {}",
                            remaining.join(", ")
                        )),
                        error: None,
                        lines: None,
                        metrics: None,
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "could not clear acknowledged fences");
                not_fenced(&format!("the acknowledgement could not be stored: {e}"))
            }
        }
    }

    async fn grant(&mut self, envelope: Envelope<LeaseGrant>) -> Result<(), ProtocolError> {
        // M5a places `local` leases only (section 8.7); an expiring lease needs
        // the renewal loop M5b adds, and accepting one here would let it run
        // without ever being renewed.
        if envelope.body.mode != LeaseMode::Local {
            return Err(refused(
                "expiring leases are not supported by this agent yet",
            ));
        }
        let now = Instant::now();
        let (lease, highest) = {
            let mut table = self.lock();
            match table.on_grant(&envelope.body, envelope.re.as_ref(), now) {
                Ok(()) => {
                    let highest =
                        table.highest_for(envelope.body.resource_type, &envelope.body.resource_id);
                    (table.get(&envelope.body.lease_id).cloned(), highest)
                }
                Err(Refusal::BadMessage(message)) => return Err(refused(&message)),
            }
        };
        if let Some(lease) = lease
            && let Err(e) = self
                .store
                .save_lease(
                    &StoredLease::from_lease(&lease, &HostNow::read(&SystemHostClock)),
                    highest,
                )
                .await
        {
            warn!(error = %e, "could not persist a lease grant");
        }
        self.publish_hello();
        Ok(())
    }

    /// Applies a `lease_revoke` and answers it once the lease's fence is
    /// recorded; the service takes other messages meanwhile.
    async fn revoke(
        &mut self,
        envelope: Envelope<LeaseRevoke>,
        reply: oneshot::Sender<Result<CommandResult, ProtocolError>>,
    ) {
        // A redelivered revoke gets the same answer and fences nothing again
        // (section 12 `cmd_results`), also while its fence still runs.
        if let Some(answer) = self.answered.get(&envelope.id) {
            let _ = reply.send(Ok(answer.clone()));
            return;
        }
        if let Some(flight) = self
            .in_flight
            .values_mut()
            .find(|flight| flight.waiters.iter().any(|w| w.msg == envelope.id))
        {
            flight.waiters.push(Waiter {
                msg: envelope.id,
                reply,
            });
            return;
        }
        if self.already_fenced(&envelope.body) {
            let result = CommandResult {
                status: CommandStatus::Ok,
                detail: Some("already fenced".to_owned()),
                error: None,
                lines: None,
                metrics: None,
            };
            self.answered.insert(envelope.id, result.clone());
            let _ = reply.send(Ok(result));
            return;
        }
        let outcome = {
            let mut table = self.lock();
            table.on_revoke(&envelope.body)
        };
        match outcome {
            Err(Refusal::BadMessage(message)) => {
                let _ = reply.send(Err(refused(&message)));
            }
            Ok(RevokeOutcome::UnknownLease) => {
                let result = CommandResult {
                    status: CommandStatus::Ok,
                    detail: Some("unknown lease".to_owned()),
                    error: None,
                    lines: None,
                    metrics: None,
                };
                self.answered.insert(envelope.id, result.clone());
                let _ = reply.send(Ok(result));
            }
            Ok(RevokeOutcome::Fence { lease_id }) => {
                // Persist the revoked state before stopping anything, so an
                // agent that dies mid-fence restores the lease as revoked,
                // never as held (section 8.5 step 2).
                self.persist(&lease_id).await;
                self.publish_hello();
                self.begin_fence(
                    lease_id,
                    FenceReason::LeaseRevoked,
                    Some(Waiter {
                        msg: envelope.id,
                        reply,
                    }),
                );
            }
        }
    }

    /// Whether `revoke` names a lease that is already `revoked` or `expired`
    /// at its token, with a complete fence recorded and none running: then no
    /// VM of the lease can have started since (section 6.4), and the revoke
    /// changes nothing.
    fn already_fenced(&self, revoke: &LeaseRevoke) -> bool {
        let key = (revoke.lease_id.clone(), revoke.fencing_token);
        let fenced_at_token = self.lock().get(&revoke.lease_id).is_some_and(|lease| {
            matches!(lease.state, LeaseState::Revoked | LeaseState::Expired)
                && lease.token == revoke.fencing_token
        });
        fenced_at_token
            && self.recorded.contains(&key)
            && !self.in_flight.contains_key(&revoke.lease_id)
            && !self.view.gates.fencing().contains(&revoke.lease_id)
    }

    async fn persist(&self, lease_id: &LeaseId) {
        let (lease, highest) = {
            let table = self.lock();
            let Some(lease) = table.get(lease_id).cloned() else {
                return;
            };
            let highest = table.highest_for(lease.resource_type, &lease.resource_id);
            (lease, highest)
        };
        if let Err(e) = self
            .store
            .save_lease(
                &StoredLease::from_lease(&lease, &HostNow::read(&SystemHostClock)),
                highest,
            )
            .await
        {
            warn!(error = %e, "could not persist a lease");
        }
    }

    /// Section 8.5 step 1: starts stopping every VM bound to the lease, in a
    /// task of its own. A lease whose fence is already running gets no second
    /// stop: the trigger joins that fence and is answered with its result.
    fn begin_fence(&mut self, lease_id: LeaseId, reason: FenceReason, waiter: Option<Waiter>) {
        if let Some(flight) = self.in_flight.get_mut(&lease_id) {
            flight.waiters.extend(waiter);
            return;
        }
        let Some(lease) = self.lock().get(&lease_id).cloned() else {
            // Only a known lease reaches a fence; answer as for an unknown one.
            if let Some(waiter) = waiter {
                let _ = waiter.reply.send(Ok(CommandResult {
                    status: CommandStatus::Ok,
                    detail: Some("unknown lease".to_owned()),
                    error: None,
                    lines: None,
                    metrics: None,
                }));
            }
            return;
        };
        // The newest snapshot may not have been noted yet when a revoke is
        // taken before the change that came first.
        self.note_bindings();
        let vms = self.bound.get(&lease_id).cloned().unwrap_or_default();
        let budget = budget_for(&lease, self.config.timers);
        let bounded = lease.mode == LeaseMode::Expiring;
        // Nothing stateful starts for this lease until the fence is recorded.
        self.view.gates.fencing().insert(lease_id.clone());
        let gate = self.view.gates.gate(&lease_id);
        let engine = Arc::clone(&self.engine);
        let desired = self.desired.clone();
        let known = vms.clone();
        let fenced = lease_id.clone();
        let begun = Instant::now();
        let handle = self.tasks.spawn(async move {
            // A start that holds the gate finishes first; its VM is then
            // among those stopped here.
            let _exclusive = gate.write_owned().await;
            let (stopping, unlisted) = fence_targets(&*engine, &desired, &fenced, known).await;
            // An `expiring` lease must be stopped within `M` of the trigger
            // (section 8.4), so time spent waiting for a start comes out of
            // the graceful stop.
            let budget = if bounded {
                FenceTimers {
                    grace: budget.grace.saturating_sub(begun.elapsed()),
                    force: budget.force,
                }
            } else {
                budget
            };
            FenceOutcome {
                report: stop_vms(&*engine, &stopping, budget).await,
                unlisted,
                died: None,
            }
        });
        self.task_leases.insert(handle.id(), lease_id.clone());
        debug!(lease_id = %lease_id.0, ?reason, vms = vms.len(), "fencing a lease");
        self.in_flight.insert(
            lease_id,
            InFlight {
                token: lease.token,
                reason,
                vms,
                waiters: waiter.into_iter().collect(),
            },
        );
    }

    /// Section 8.5 steps 2 and 3 for a fence whose stop finished: record it
    /// with its event, then answer the revokes that wait for it.
    async fn finish_fence(
        &mut self,
        joined: Result<(tokio::task::Id, FenceOutcome), tokio::task::JoinError>,
    ) {
        let (task, outcome) = match joined {
            Ok((task, outcome)) => (task, Ok(outcome)),
            Err(error) => (error.id(), Err(error.to_string())),
        };
        let Some(lease_id) = self.task_leases.remove(&task) else {
            return;
        };
        let Some(flight) = self.in_flight.remove(&lease_id) else {
            return;
        };
        let outcome = outcome.unwrap_or_else(|error| FenceOutcome {
            // A stop that never returned leaves the VMs' state unknown: they
            // may still run (D-0074).
            report: FenceReport {
                vms: flight
                    .vms
                    .iter()
                    .map(|vm| {
                        (
                            vm.clone(),
                            VmFenceOutcome::Failed(format!("the fence stopped early: {error}")),
                        )
                    })
                    .collect(),
            },
            unlisted: None,
            died: Some(format!("the fence stopped early: {error}")),
        });

        // A revoke that joined the fence may have raised the lease's token;
        // the fence covers it too, so the record carries the newest one.
        let host = HostNow::read(&SystemHostClock);
        let (lease, token, row) = {
            let table = self.lock();
            let lease = table.get(&lease_id).cloned();
            let token = lease
                .as_ref()
                .filter(|lease| matches!(lease.state, LeaseState::Revoked | LeaseState::Expired))
                .map_or(flight.token, |lease| lease.token.max(flight.token));
            // The lease's row is written with the record, so a lease whose
            // `revoked` state could not be saved before the stop is saved now
            // or not at all, and a fenced lease is never restored `held`.
            let row = lease.as_ref().map(|lease| {
                (
                    StoredLease::from_lease(lease, &host),
                    table.highest_for(lease.resource_type, &lease.resource_id),
                )
            });
            (lease, token, row)
        };
        let result = self
            .record_fence(&lease_id, &flight, lease.as_ref(), token, row, &outcome)
            .await;
        for waiter in flight.waiters {
            // Only a final answer is kept for a redelivered revoke; one whose
            // fence was not recorded fences again when it comes back.
            if result.status == CommandStatus::Ok
                || result.error.as_ref().map(|error| error.code) == Some(ErrorCode::FenceFailed)
            {
                self.answered.insert(waiter.msg.clone(), result.clone());
            }
            let _ = waiter.reply.send(Ok(result.clone()));
        }
    }

    /// Section 8.5 steps 2 and 3: records a complete or failed fence with the
    /// lease's row, its event and, for a failed one, the node's failed mark,
    /// and returns the revoke's answer.
    async fn record_fence(
        &mut self,
        lease_id: &LeaseId,
        flight: &InFlight,
        lease: Option<&crate::lease::Lease>,
        token: u64,
        row: Option<(StoredLease, u64)>,
        outcome: &FenceOutcome,
    ) -> CommandResult {
        let complete = outcome.complete();
        if !complete {
            // ADR 0074: the forced stop could not be confirmed, so a VM may
            // still run; nothing stateful starts until an operator clears the
            // mark. The mark is set in memory before it is stored.
            self.view.failed.store(true, Ordering::SeqCst);
        }
        let record = FenceRecord {
            lease_id: lease_id.clone(),
            token,
            vm_ids: outcome
                .report
                .vms
                .iter()
                .map(|(vm, _)| vm.clone())
                .collect(),
            at: SystemTime::now(),
            reason: flight.reason,
            complete,
        };
        let resource = lease.map_or((ResourceType::App, String::new()), |lease| {
            (lease.resource_type, lease.resource_id.clone())
        });
        let (evt_id, event) = self.fence_event(lease_id, resource, flight.reason, outcome);
        let recorded = self
            .store
            .finish_fence(
                &record,
                row.as_ref().map(|(lease, highest)| (lease, *highest)),
                &evt_id,
                &event,
            )
            .await;
        match recorded {
            Ok(()) => {
                self.view.gates.fencing().remove(lease_id);
                if complete {
                    self.recorded.insert((lease_id.clone(), token));
                }
                revoke_result(lease_id, outcome)
            }
            Err(e) => {
                // The stop is not recorded, so a restart may not know the
                // lease is fenced: nothing stateful starts for it in this
                // process, and the revoke is not answered with success, so
                // the primary does not take the VMs for stopped and recorded
                // (section 5.9). A failed fence keeps its `fence_failed`
                // answer: the failed mark holds in memory either way.
                warn!(error = %e, lease_id = %lease_id.0, "could not record a self-fence");
                // Best effort, one write at a time: a restart should still
                // find the lease fenced and, after a failed fence, the node
                // failed.
                if let Some((lease, highest)) = &row
                    && let Err(e) = self.store.save_lease(lease, *highest).await
                {
                    warn!(error = %e, lease_id = %lease_id.0, "could not persist a lease");
                }
                if !complete
                    && let Err(e) = self
                        .store
                        .mark_fence_failed(&lease_id.0, &evt_id, &event)
                        .await
                {
                    warn!(error = %e, "could not store the node's failed mark");
                    // An acknowledgement of other leases must not clear the
                    // mark this failure holds in memory.
                    self.unstored_failures.insert(lease_id.0.clone());
                }
                if complete {
                    not_fenced(&format!(
                        "the fence of lease {} stopped its VMs but could not be recorded: {e}",
                        lease_id.0
                    ))
                } else {
                    revoke_result(lease_id, outcome)
                }
            }
        }
    }

    /// The `node.fenced` or `node.fence_failed` event of one fence (section
    /// 8.5 step 3, D-0074).
    fn fence_event(
        &self,
        lease_id: &LeaseId,
        (resource_type, resource_id): (ResourceType, String),
        reason: FenceReason,
        outcome: &FenceOutcome,
    ) -> (String, serde_json::Value) {
        let app_ids = match resource_type {
            ResourceType::App if !resource_id.is_empty() => vec![resource_id],
            ResourceType::App | ResourceType::Primary => Vec::new(),
        };
        let mut data = serde_json::json!({
            "node_id": self.config.node_id,
            "app_ids": app_ids,
            "lease_ids": [lease_id.0],
            "reason": match reason {
                FenceReason::LeaseExpired => "lease_expired",
                FenceReason::LeaseRevoked => "lease_revoked",
            },
        });
        let kind = if outcome.complete() {
            "node.fenced"
        } else {
            let (vm_ids, error) = fence_failures(outcome);
            data["vm_ids"] = serde_json::json!(vm_ids);
            data["error"] = serde_json::json!(error);
            "node.fence_failed"
        };
        let evt_id = util::event_id();
        let body = serde_json::json!({
            "schema_version": 1,
            "id": evt_id,
            "type": kind,
            "time": util::rfc3339_nanos(self.store.next_log_time()),
            "source": {"kind": "node", "node_id": self.config.node_id},
            "subject": {"type": "lease", "id": lease_id.0},
            "actor": {"type": "node", "id": self.config.node_id},
            "data": data,
        });
        (evt_id, body)
    }

    fn publish_hello(&self) {
        let _ = self.hello.send_replace(hello_leases(&self.view.table));
    }
}

/// The VMs whose lease binds them, from one desired snapshot (section 6.4).
fn bindings_from(snapshot: Option<&Snapshot>) -> HashMap<LeaseId, Vec<VmId>> {
    let mut bindings: HashMap<LeaseId, Vec<VmId>> = HashMap::new();
    let Some(snapshot) = snapshot else {
        return bindings;
    };
    for vm in &snapshot.state.spec.vms {
        if !vm.stateful {
            continue;
        }
        let Some(lease_id) = &vm.lease_id else {
            continue;
        };
        bindings
            .entry(LeaseId(lease_id.clone()))
            .or_default()
            .push(VmId(vm.vm_id.clone()));
    }
    bindings
}

fn hello_leases(table: &Mutex<LeaseTable>) -> Vec<HelloLease> {
    let table = table
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut leases: Vec<HelloLease> = table
        .leases()
        .map(|lease| HelloLease {
            lease_id: lease.id.0.clone(),
            resource_type: lease.resource_type,
            resource_id: lease.resource_id.clone(),
            mode: lease.mode,
            fencing_token: lease.token,
            state: match lease.state {
                LeaseState::Held => "held",
                LeaseState::Uncertain => "uncertain",
                LeaseState::Expired => "expired",
                LeaseState::Revoked => "revoked",
            }
            .to_owned(),
        })
        .collect();
    leases.sort_by(|a, b| a.lease_id.cmp(&b.lease_id));
    leases
}

fn refused(message: &str) -> ProtocolError {
    ProtocolError {
        code: ErrorCode::BadMessage,
        message: message.to_owned(),
        fatal: false,
    }
}

/// A grant the service cannot take is refused: the node does not hold it.
fn service_gone() -> ProtocolError {
    ProtocolError {
        code: ErrorCode::BadMessage,
        message: "the lease service is not running".to_owned(),
        fatal: false,
    }
}

/// An error answer with no error code. For a revoke whose fence is neither
/// recorded nor failed, the VMs are not known to be stopped and recorded
/// (section 5.9), so the answer is not `ok`; `bad_message` would mean the
/// revoke was stale, `fence_failed` that a VM may still run. For a
/// `fence_ack`, nothing was cleared and the primary sends it again.
fn not_fenced(detail: &str) -> CommandResult {
    CommandResult {
        status: CommandStatus::Error,
        detail: Some(detail.to_owned()),
        error: None,
        lines: None,
        metrics: None,
    }
}

/// The VMs that may still run after a failed fence, and why.
fn fence_failures(outcome: &FenceOutcome) -> (Vec<String>, String) {
    let mut vm_ids = Vec::new();
    let mut errors = Vec::new();
    for (vm, result) in &outcome.report.vms {
        if let VmFenceOutcome::Failed(error) = result {
            vm_ids.push(vm.0.clone());
            errors.push(format!("{}: {error}", vm.0));
        }
    }
    for error in [&outcome.died, &outcome.unlisted].into_iter().flatten() {
        errors.push(error.clone());
    }
    (vm_ids, errors.join(", "))
}

/// The VMs a fence of `lease_id` stops, chosen while it holds the lease's
/// gate: every VM `known` names, every VM the current snapshot binds to the
/// lease, and every app VM the engine runs that the current snapshot does not
/// name. The second value is set when the engine's VMs could not be listed.
async fn fence_targets<E: Engine>(
    engine: &E,
    desired: &watch::Receiver<Option<Snapshot>>,
    lease_id: &LeaseId,
    known: Vec<VmId>,
) -> (Vec<VmId>, Option<String>) {
    let mut targets = known;
    let named: HashSet<String> = {
        let snapshot = desired.borrow();
        if let Some(bound) = bindings_from(snapshot.as_ref()).remove(lease_id) {
            for vm in bound {
                if !targets.contains(&vm) {
                    targets.push(vm);
                }
            }
        }
        snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .state
                    .spec
                    .vms
                    .iter()
                    .map(|vm| vm.vm_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    };
    match engine.list().await {
        Ok(infos) => {
            for info in infos {
                // Builder VMs have ids of their own and hold no lease; a
                // stopped VM without a process has nothing left to stop.
                let idle = matches!(
                    info.state,
                    VmState::Stopped | VmState::Crashed | VmState::Deleted
                ) && info.pid.is_none();
                if info.id.0.starts_with("vm_")
                    && !named.contains(&info.id.0)
                    && !idle
                    && !targets.contains(&info.id)
                {
                    targets.push(info.id);
                }
            }
            (targets, None)
        }
        Err(e) => (
            targets,
            Some(format!(
                "could not list the engine's VMs, so any app VM on this node may still run: {e}"
            )),
        ),
    }
}

/// The `command_result` for a revoke (section 5.9, section 5.13).
fn revoke_result(lease_id: &LeaseId, outcome: &FenceOutcome) -> CommandResult {
    if outcome.complete() {
        return CommandResult {
            status: CommandStatus::Ok,
            detail: None,
            error: None,
            lines: None,
            metrics: None,
        };
    }
    let (_, failed) = fence_failures(outcome);
    let detail = format!("fence incomplete for lease {}: {failed}", lease_id.0);
    CommandResult {
        status: CommandStatus::Error,
        detail: Some(detail.clone()),
        error: Some(ErrorBody {
            code: ErrorCode::FenceFailed,
            message: detail,
        }),
        lines: None,
        metrics: None,
    }
}
