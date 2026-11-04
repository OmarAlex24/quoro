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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::SystemTime;

use quoro_engine::{Engine, VmId};
use quoro_proto::wire::HelloLease;
use quoro_proto::{
    CommandResult, CommandStatus, Envelope, ErrorBody, ErrorCode, FenceReason, LeaseGrant, LeaseId,
    LeaseMode, LeaseRevoke, MsgId, ProtocolError, ResourceType,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::AgentError;
use crate::lease::fence::{FenceReport, FenceTimers, budget_for, stop_vms};
use crate::lease::machine::{Lease, LeaseState, LeaseTable, Refusal, RevokeOutcome};
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
}

impl Default for LeaseView {
    fn default() -> Self {
        Self {
            table: Arc::new(Mutex::new(LeaseTable::new())),
            failed: Arc::new(AtomicBool::new(false)),
        }
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
        }
    }

    fn lock(&self) -> MutexGuard<'_, LeaseTable> {
        self.table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Section 6.4: may a stateful VM with this spec token start now?
    #[must_use]
    pub fn may_start_stateful(&self, lease_id: &str, spec_token: u64) -> bool {
        !self.failed.load(Ordering::Relaxed)
            && self
                .lock()
                .may_start_stateful(&LeaseId(lease_id.to_owned()), spec_token)
    }

    /// Whether the node is marked failed (section 12 `node_meta`).
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
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
    let (table, pending) = LeaseTable::restore(
        store.load_leases().await?,
        store.load_highest_tokens().await?,
    );
    let failed = store.node_failed().await?;
    let table = Arc::new(Mutex::new(table));
    let failed = Arc::new(AtomicBool::new(failed));
    let (commands, command_rx) = mpsc::unbounded_channel();
    let (hello_tx, hello_rx) = watch::channel(hello_leases(&table));

    let service = LocalLeaseService {
        engine,
        store,
        config,
        table: Arc::clone(&table),
        failed: Arc::clone(&failed),
        bindings: bindings_from(desired.borrow().as_ref()),
        commands: command_rx,
        answered: HashMap::new(),
        hello: hello_tx,
        pending,
    };
    let task = tokio::spawn(service.run(desired));
    Ok(LocalLeases {
        link: LeaseLink {
            commands,
            hello: hello_rx,
        },
        view: LeaseView { table, failed },
        task,
    })
}

/// The async driver behind [`LocalLeases`].
struct LocalLeaseService<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    config: LeaseConfig,
    table: Arc<Mutex<LeaseTable>>,
    failed: Arc<AtomicBool>,
    bindings: HashMap<LeaseId, Vec<VmId>>,
    commands: mpsc::UnboundedReceiver<LeaseCommand>,
    answered: HashMap<MsgId, CommandResult>,
    hello: watch::Sender<Vec<HelloLease>>,
    pending: Vec<crate::lease::Action>,
}

impl<E: Engine> LocalLeaseService<E> {
    fn lock(&self) -> MutexGuard<'_, LeaseTable> {
        self.table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn run(mut self, mut desired: watch::Receiver<Option<Snapshot>>) {
        for action in std::mem::take(&mut self.pending) {
            if let crate::lease::Action::Fence { lease_id, reason } = action {
                self.fence(&lease_id, reason).await;
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
            }
        }
    }

    async fn handle(&mut self, command: LeaseCommand) {
        match command {
            LeaseCommand::Grant { envelope, reply } => {
                let _ = reply.send(self.grant(envelope).await);
            }
            LeaseCommand::Revoke { envelope, reply } => {
                let _ = reply.send(self.revoke(envelope).await);
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
        let now = tokio::time::Instant::now();
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

    async fn revoke(
        &mut self,
        envelope: Envelope<LeaseRevoke>,
    ) -> Result<CommandResult, ProtocolError> {
        // A redelivered revoke gets the same answer and fences nothing again
        // (section 12 `cmd_results`).
        if let Some(answer) = self.answered.get(&envelope.id) {
            return Ok(answer.clone());
        }
        let outcome = {
            let mut table = self.lock();
            table.on_revoke(&envelope.body)
        };
        let result = match outcome {
            Err(Refusal::BadMessage(message)) => return Err(refused(&message)),
            Ok(RevokeOutcome::UnknownLease) => CommandResult {
                status: CommandStatus::Ok,
                detail: Some("unknown lease".to_owned()),
                error: None,
                lines: None,
                metrics: None,
            },
            Ok(RevokeOutcome::Fence { lease_id }) => {
                // Persist the revoked state before stopping anything, so an
                // agent that dies mid-fence restores the lease as revoked,
                // never as held (section 8.5 step 2).
                self.persist(&lease_id).await;
                let report = self.fence(&lease_id, FenceReason::LeaseRevoked).await;
                revoke_result(&lease_id, &report)
            }
        };
        self.answered.insert(envelope.id.clone(), result.clone());
        self.publish_hello();
        Ok(result)
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

    /// Section 8.5: stop every VM bound to the lease, record the stop, and
    /// report it. Returns the stop report.
    async fn fence(&mut self, lease_id: &LeaseId, reason: FenceReason) -> FenceReport {
        let Some(lease) = self.lock().get(lease_id).cloned() else {
            return FenceReport { vms: Vec::new() };
        };
        let budget = budget_for(&lease, self.config.timers);
        let vms = self.bindings.get(lease_id).cloned().unwrap_or_default();
        let report = stop_vms(&*self.engine, &vms, budget).await;
        let complete = report.complete();
        if let Err(e) = self
            .store
            .record_fence(&FenceRecord {
                lease_id: lease_id.clone(),
                token: lease.token,
                vm_ids: vms,
                at: SystemTime::now(),
                reason,
                complete,
            })
            .await
        {
            warn!(error = %e, "could not record a self-fence");
        }
        self.persist(lease_id).await;

        if complete {
            self.emit("node.fenced", lease_id, &lease, reason, &[], None)
                .await;
        } else {
            // ADR 0074: the forced stop could not be confirmed, so the VM may
            // still run; nothing stateful starts until an operator clears the
            // mark.
            let mut vm_ids = Vec::new();
            let mut errors = Vec::new();
            for (vm, outcome) in &report.vms {
                if let crate::lease::VmFenceOutcome::Failed(error) = outcome {
                    vm_ids.push(vm.0.clone());
                    errors.push(format!("{}: {error}", vm.0));
                }
            }
            let failed = errors.join(", ");
            self.failed.store(true, Ordering::Relaxed);
            if let Err(e) = self.store.set_node_failed(true).await {
                warn!(error = %e, "could not record the node's failed mark");
            }
            self.emit(
                "node.fence_failed",
                lease_id,
                &lease,
                reason,
                &vm_ids,
                Some(&failed),
            )
            .await;
        }
        report
    }

    async fn emit(
        &self,
        kind: &str,
        lease_id: &LeaseId,
        lease: &Lease,
        reason: FenceReason,
        vm_ids: &[String],
        error: Option<&str>,
    ) {
        let app_ids = match lease.resource_type {
            ResourceType::App => vec![lease.resource_id.clone()],
            ResourceType::Primary => Vec::new(),
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
        if kind == "node.fence_failed" {
            data["vm_ids"] = serde_json::json!(vm_ids);
            if let Some(error) = error {
                data["error"] = serde_json::json!(error);
            }
        }
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
        if let Err(e) = self.store.push_event(&evt_id, &body).await {
            warn!(error = %e, "could not buffer a fence event");
        }
    }

    fn publish_hello(&self) {
        let _ = self.hello.send_replace(hello_leases(&self.table));
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
            crate::lease::VmFenceOutcome::Failed(reason) => Some(format!("{}: {reason}", vm.0)),
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
