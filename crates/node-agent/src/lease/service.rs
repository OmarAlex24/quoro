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
//! A fence is recorded only once its stop finished. A lease that is `revoked`
//! or `expired` in the store without a record for its token was therefore
//! interrupted by an agent crash, and the restore fences it again
//! ([`LeaseTable::restore`]).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use quoro_engine::{Engine, VmId};
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
use crate::lease::fence::{FenceReport, FenceTimers, VmFenceOutcome, budget_for, stop_vms};
use crate::lease::machine::{LeaseState, LeaseTable, Refusal, RevokeOutcome};
use crate::lease::store::{FenceRecord, StoredLease};
use crate::reconcile::Snapshot;
use crate::store::NodeStore;
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
            return Err(service_gone());
        }
        answer.await.unwrap_or_else(|_| Err(service_gone()))
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
/// [`AgentError`] when the stored lease rows cannot be read.
pub async fn start_local_leases<E: Engine + 'static>(
    engine: Arc<E>,
    store: NodeStore,
    config: LeaseConfig,
    desired: watch::Receiver<Option<Snapshot>>,
) -> Result<LocalLeases, AgentError> {
    let fenced: HashSet<(LeaseId, u64)> = store
        .fences()
        .await?
        .into_iter()
        .map(|record| (record.lease_id, record.token))
        .collect();
    let (table, pending) = LeaseTable::restore(
        store.load_leases().await?,
        store.load_highest_tokens().await?,
        &fenced,
    );
    let failed = store.node_failed().await?;
    let view = LeaseView::new(table, failed);
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (hello_tx, hello_rx) = watch::channel(hello_leases(&view.table));

    let service = LocalLeaseService {
        engine,
        store,
        config,
        view: view.clone(),
        bindings: bindings_from(desired.borrow().as_ref()),
        commands: command_rx,
        answered: HashMap::new(),
        hello: hello_tx,
        pending,
        in_flight: HashMap::new(),
        tasks: JoinSet::new(),
        task_leases: HashMap::new(),
    };
    let task = tokio::spawn(service.run(desired));
    Ok(LocalLeases {
        link: LeaseLink {
            commands,
            hello: hello_rx,
        },
        view,
        task,
    })
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
    /// The VMs it stops.
    vms: Vec<VmId>,
    /// The revokes it answers once it is recorded.
    waiters: Vec<Waiter>,
}

/// The async driver behind [`LocalLeases`].
struct LocalLeaseService<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    config: LeaseConfig,
    view: LeaseView,
    bindings: HashMap<LeaseId, Vec<VmId>>,
    commands: mpsc::UnboundedReceiver<LeaseCommand>,
    answered: HashMap<MsgId, CommandResult>,
    hello: watch::Sender<Vec<HelloLease>>,
    pending: Vec<crate::lease::Action>,
    /// Running fences, at most one per lease.
    in_flight: HashMap<LeaseId, InFlight>,
    /// The stops of the running fences. Dropping the service aborts them, as
    /// an agent crash would; the restore then runs them again.
    tasks: JoinSet<FenceReport>,
    /// Which lease each stop task fences.
    task_leases: HashMap<tokio::task::Id, LeaseId>,
}

impl<E: Engine + 'static> LocalLeaseService<E> {
    fn lock(&self) -> MutexGuard<'_, LeaseTable> {
        self.view.lock()
    }

    async fn run(mut self, mut desired: watch::Receiver<Option<Snapshot>>) {
        // Fences the restore found begin before any message is taken, and
        // run side by side (section 8.5).
        for action in std::mem::take(&mut self.pending) {
            if let crate::lease::Action::Fence { lease_id, reason } = action {
                self.begin_fence(lease_id, reason, None);
            }
        }
        loop {
            tokio::select! {
                changed = desired.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    self.bindings = bindings_from(desired.borrow_and_update().as_ref());
                }
                command = self.commands.recv() => {
                    match command {
                        Some(command) => self.handle(command).await,
                        None => return,
                    }
                }
                Some(joined) = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                    self.finish_fence(joined).await;
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
                .save_lease(&StoredLease::from_lease(&lease), highest)
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
            .save_lease(&StoredLease::from_lease(&lease), highest)
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
        let vms = self.bindings.get(&lease_id).cloned().unwrap_or_default();
        let budget = budget_for(&lease, self.config.timers);
        let bounded = lease.mode == LeaseMode::Expiring;
        // Nothing stateful starts for this lease until the fence is recorded.
        self.view.gates.fencing().insert(lease_id.clone());
        let gate = self.view.gates.gate(&lease_id);
        let engine = Arc::clone(&self.engine);
        let stopping = vms.clone();
        let begun = Instant::now();
        let handle = self.tasks.spawn(async move {
            // A start that holds the gate finishes first; its VM is then
            // among those stopped here.
            let _exclusive = gate.write_owned().await;
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
            stop_vms(&*engine, &stopping, budget).await
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
        joined: Result<(tokio::task::Id, FenceReport), tokio::task::JoinError>,
    ) {
        let (task, report) = match joined {
            Ok((task, report)) => (task, Ok(report)),
            Err(error) => (error.id(), Err(error.to_string())),
        };
        let Some(lease_id) = self.task_leases.remove(&task) else {
            return;
        };
        let Some(flight) = self.in_flight.remove(&lease_id) else {
            return;
        };
        let report = report.unwrap_or_else(|error| FenceReport {
            // A stop that never returned leaves the VMs' state unknown: they
            // may still run (D-0074).
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
        });

        // A revoke that joined the fence may have raised the lease's token;
        // the fence covers it too, so the record carries the newest one.
        let (lease, token) = {
            let table = self.lock();
            let lease = table.get(&lease_id).cloned();
            let token = lease
                .as_ref()
                .filter(|lease| matches!(lease.state, LeaseState::Revoked | LeaseState::Expired))
                .map_or(flight.token, |lease| lease.token.max(flight.token));
            (lease, token)
        };
        let complete = report.complete();
        if !complete {
            // ADR 0074: the forced stop could not be confirmed, so a VM may
            // still run; nothing stateful starts until an operator clears the
            // mark. The mark is set in memory before it is stored.
            self.view.failed.store(true, Ordering::SeqCst);
        }
        let record = FenceRecord {
            lease_id: lease_id.clone(),
            token,
            vm_ids: flight.vms,
            at: SystemTime::now(),
            reason: flight.reason,
            complete,
        };
        let resource = lease
            .as_ref()
            .map_or((ResourceType::App, String::new()), |lease| {
                (lease.resource_type, lease.resource_id.clone())
            });
        let (evt_id, event) = self.fence_event(&lease_id, resource, flight.reason, &report);
        if let Err(e) = self.store.finish_fence(&record, &evt_id, &event).await {
            warn!(error = %e, lease_id = %lease_id.0, "could not record a self-fence");
        }
        self.view.gates.fencing().remove(&lease_id);

        let result = revoke_result(&lease_id, &report);
        for waiter in flight.waiters {
            self.answered.insert(waiter.msg, result.clone());
            let _ = waiter.reply.send(Ok(result.clone()));
        }
    }

    /// The `node.fenced` or `node.fence_failed` event of one fence (section
    /// 8.5 step 3, D-0074).
    fn fence_event(
        &self,
        lease_id: &LeaseId,
        (resource_type, resource_id): (ResourceType, String),
        reason: FenceReason,
        report: &FenceReport,
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
        let kind = if report.complete() {
            "node.fenced"
        } else {
            let mut vm_ids = Vec::new();
            let mut errors = Vec::new();
            for (vm, outcome) in &report.vms {
                if let VmFenceOutcome::Failed(error) = outcome {
                    vm_ids.push(vm.0.clone());
                    errors.push(format!("{}: {error}", vm.0));
                }
            }
            data["vm_ids"] = serde_json::json!(vm_ids);
            data["error"] = serde_json::json!(errors.join(", "));
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

fn service_gone() -> ProtocolError {
    ProtocolError {
        code: ErrorCode::BadMessage,
        message: "the lease service is not running".to_owned(),
        fatal: false,
    }
}

/// The `command_result` for a revoke (section 5.9, section 5.13).
fn revoke_result(lease_id: &LeaseId, report: &FenceReport) -> CommandResult {
    if report.complete() {
        return CommandResult {
            status: CommandStatus::Ok,
            detail: None,
            error: None,
            lines: None,
            metrics: None,
        };
    }
    let failed: Vec<String> = report
        .vms
        .iter()
        .filter_map(|(vm, outcome)| match outcome {
            VmFenceOutcome::Failed(reason) => Some(format!("{}: {reason}", vm.0)),
            _ => None,
        })
        .collect();
    let detail = format!(
        "fence incomplete for lease {}: {}",
        lease_id.0,
        failed.join(", ")
    );
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
