// SPDX-License-Identifier: AGPL-3.0-only
//! The async lease driver: feeds the pure state machine, sends renewals,
//! executes self-fences and answers revokes (sections 4.2, 8.4, 8.5).

use std::collections::HashMap;
use std::time::SystemTime;

use quoro_engine::{Engine, VmId};
use quoro_proto::{
    CommandResult, CommandStatus, Envelope, ErrorBody, ErrorCode, FenceReason, FromNode, LeaseId,
    LeaseRenew, MsgId, NodeEvent, NodeEventData, NodeFenceFailed, NodeFenced, ProtocolError,
    ResourceType, ToNode,
};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::lease::fence::{FenceReport, FenceTimers, VmFenceOutcome, budget_for, stop_vms};
use crate::lease::machine::{Action, Lease, LeaseTable, Refusal, RevokeOutcome};
use crate::lease::store::{FenceRecord, LeaseStore, StoredLease};

/// What the agent needs that is not the engine or the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseAgentConfig {
    /// The node id reported in `node.fenced` (section 8.5 step 3).
    pub node_id: String,
    /// The configured fence timeouts, clamped per lease by [`budget_for`].
    pub timers: FenceTimers,
}

/// Drives leases for one node.
pub struct LeaseAgent<'e, E: Engine, S: LeaseStore> {
    engine: &'e E,
    store: S,
    table: LeaseTable,
    /// The stateful VMs bound to each lease, from desired state.
    bindings: HashMap<LeaseId, Vec<VmId>>,
    config: LeaseAgentConfig,
    outbound: mpsc::UnboundedSender<FromNode>,
    next_msg: u64,
    next_evt: u64,
    /// Answers already given to intrusive messages, by the message they answer,
    /// so a redelivered `lease_revoke` is idempotent (section 12 `cmd_results`).
    answered: HashMap<MsgId, CommandResult>,
    /// Fences that the restore found and that must run before anything else.
    pending_fences: Vec<Action>,
    /// The node's failed mark (section 12 `node_meta`): a fence could not
    /// confirm that every VM stopped, so nothing stateful may start.
    failed: bool,
}

impl<'e, E: Engine, S: LeaseStore> LeaseAgent<'e, E, S> {
    /// Builds an agent, restoring the lease table from the local store.
    ///
    /// Fences that the restore requires (section 8.5 step 2) are kept and run
    /// first by [`Self::run`]. The node's failed mark is restored too, so a
    /// restart stays failed until an operator clears it (section 12
    /// `node_meta`).
    pub fn new(
        engine: &'e E,
        store: S,
        config: LeaseAgentConfig,
        outbound: mpsc::UnboundedSender<FromNode>,
    ) -> Self {
        let (table, pending_fences) =
            LeaseTable::restore(store.load_leases(), store.load_highest_tokens());
        let failed = store.node_failed();
        Self {
            engine,
            store,
            table,
            bindings: HashMap::new(),
            config,
            outbound,
            next_msg: 0,
            next_evt: 0,
            answered: HashMap::new(),
            pending_fences,
            failed,
        }
    }

    /// Binds a stateful VM to a lease; fencing that lease stops it.
    pub fn bind_vm(&mut self, lease_id: LeaseId, vm: VmId) {
        let bound = self.bindings.entry(lease_id).or_default();
        if !bound.contains(&vm) {
            bound.push(vm);
        }
    }

    /// The current lease table.
    #[must_use]
    pub fn table(&self) -> &LeaseTable {
        &self.table
    }

    /// Section 6.4: whether a stateful VM with this spec token may start.
    ///
    /// Nothing stateful starts on a node that is marked failed: a VM whose
    /// lease it lost may still be running somewhere (D-0074).
    #[must_use]
    pub fn may_start_stateful(&self, lease_id: &LeaseId, spec_token: u64) -> bool {
        !self.failed && self.table.may_start_stateful(lease_id, spec_token)
    }

    /// Whether the node is marked failed (section 12 `node_meta`).
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.failed
    }

    /// Runs until the stop flag is set.
    ///
    /// Messages are handled completely, including any fence, before the next
    /// one is taken (section 4.2 rule 3).
    ///
    /// Known limitation: fences run one at a time. While one lease is being
    /// fenced, another `expiring` lease that reaches `valid_until` starts its
    /// own fence only afterwards, which can push it past `t_send + D`. M0 has
    /// no `expiring` lease outside tests; running fences concurrently is
    /// required before `expiring` leases are used (M5b). Concurrent fences and
    /// a mid-fence crash test are mandatory before `expiring` leases ship
    /// (D-0075).
    pub async fn run(
        &mut self,
        inbound: &mut mpsc::UnboundedReceiver<ToNode>,
        mut stop: watch::Receiver<bool>,
    ) {
        let restored = std::mem::take(&mut self.pending_fences);
        for action in restored {
            self.execute(action, Instant::now()).await;
        }

        let mut inbound_open = true;
        loop {
            if *stop.borrow() {
                return;
            }
            // Everything the clock has already reached runs before we wait,
            // so the sleep is always on a strictly future instant.
            self.advance(Instant::now()).await;
            let deadline = self.table.next_deadline(Instant::now());
            tokio::select! {
                biased;
                changed = stop.changed() => {
                    // An error means the owner of the stop flag is gone.
                    if changed.is_err() || *stop.borrow() {
                        return;
                    }
                }
                received = inbound.recv(), if inbound_open => {
                    match received {
                        Some(message) => self.handle(message, Instant::now()).await,
                        None => {
                            // The session is gone. Local leases are untouched;
                            // expiring ones become uncertain (section 8.3, 8.4).
                            inbound_open = false;
                            self.table.authority_unreachable(Instant::now());
                        }
                    }
                }
                // The loop top polls, so the timer only needs to wake us.
                () = wait_until(deadline) => {}
            }
        }
    }

    /// Polls the machine and executes what it asks for.
    async fn advance(&mut self, now: Instant) {
        for action in self.table.poll(now) {
            self.execute(action, now).await;
        }
    }

    async fn execute(&mut self, action: Action, now: Instant) {
        match action {
            Action::SendRenew {
                lease_id,
                fencing_token,
            } => {
                let id = self.next_message_id();
                self.send(FromNode::LeaseRenew(Envelope {
                    id: id.clone(),
                    re: None,
                    body: LeaseRenew {
                        lease_id: lease_id.clone(),
                        fencing_token,
                    },
                }));
                self.table.renew_sent(&lease_id, id, now);
            }
            Action::Fence { lease_id, reason } => {
                self.fence(&lease_id, reason).await;
            }
        }
    }

    async fn handle(&mut self, message: ToNode, now: Instant) {
        match message {
            ToNode::LeaseGrant(envelope) => {
                match self
                    .table
                    .on_grant(&envelope.body, envelope.re.as_ref(), now)
                {
                    Ok(()) => {
                        if let Some(lease) = self.table.get(&envelope.body.lease_id) {
                            self.save(lease);
                        }
                    }
                    Err(Refusal::BadMessage(message)) => {
                        self.send_error(envelope.id, message);
                    }
                }
            }
            ToNode::LeaseRevoke(envelope) => {
                // A redelivered revoke gets the same answer and fences nothing
                // again (section 12 `cmd_results`).
                if let Some(answer) = self.answered.get(&envelope.id).cloned() {
                    self.send_command_result(envelope.id, answer);
                    return;
                }
                match self.table.on_revoke(&envelope.body) {
                    Ok(RevokeOutcome::Fence { lease_id }) => {
                        let report = self.fence(&lease_id, FenceReason::LeaseRevoked).await;
                        let result = revoke_result(&lease_id, &report);
                        self.remember(envelope.id, result);
                    }
                    Ok(RevokeOutcome::UnknownLease) => {
                        self.remember(
                            envelope.id,
                            CommandResult {
                                status: CommandStatus::Ok,
                                detail: Some("unknown lease".to_string()),
                                error: None,
                            },
                        );
                    }
                    Err(Refusal::BadMessage(message)) => {
                        self.send_error(envelope.id, message);
                    }
                }
            }
        }
    }

    /// The self-fence procedure of section 8.5 for one lease.
    async fn fence(&mut self, lease_id: &LeaseId, reason: FenceReason) -> FenceReport {
        let Some(lease) = self.table.get(lease_id) else {
            return FenceReport { vms: Vec::new() };
        };
        let token = lease.token;
        let resource_type = lease.resource_type;
        let resource_id = lease.resource_id.clone();
        let budget = budget_for(lease, self.config.timers);
        let vms = self.bindings.get(lease_id).cloned().unwrap_or_default();

        // Persist the fenced state before stopping anything, so an agent
        // that dies mid-fence restores the lease as fenced, never as held.
        if let Some(lease) = self.table.get(lease_id) {
            self.save(lease);
        }

        // Step 1: stop every bound VM, concurrently and inside one budget.
        let report = stop_vms(self.engine, &vms, budget).await;

        // Step 2: record the stop so it survives an agent restart. An
        // incomplete fence is recorded too: it must not be forgotten.
        let complete = report.complete();
        self.store.record_fence(FenceRecord {
            lease_id: lease_id.clone(),
            token,
            vm_ids: vms,
            at: SystemTime::now(),
            reason,
            complete,
        });
        if let Some(lease) = self.table.get(lease_id) {
            self.save(lease);
        }

        let app_ids = match resource_type {
            ResourceType::App => vec![resource_id],
            ResourceType::Primary => Vec::new(),
        };

        if !complete {
            // D-0074: the forced stop could not be confirmed, so the VM may
            // still run. Emit `node.fence_failed` instead of `node.fenced` and
            // mark the node failed: nothing stateful starts until an operator
            // clears the mark (section 12 `node_meta`).
            let mut vm_ids: Vec<String> = Vec::new();
            let mut errors: Vec<String> = Vec::new();
            for (vm, outcome) in &report.vms {
                if let VmFenceOutcome::Failed(error) = outcome {
                    vm_ids.push(vm.0.clone());
                    errors.push(format!("{}: {error}", vm.0));
                }
            }
            let event_id = self.next_event_id();
            let envelope_id = self.next_message_id();
            self.send(FromNode::Event(Envelope {
                id: envelope_id,
                re: None,
                body: NodeEvent {
                    id: event_id,
                    time: SystemTime::now(),
                    data: NodeEventData::NodeFenceFailed(NodeFenceFailed {
                        node_id: self.config.node_id.clone(),
                        app_ids,
                        lease_ids: vec![lease_id.clone()],
                        vm_ids,
                        reason,
                        error: Some(errors.join(", ")),
                    }),
                },
            }));
            self.failed = true;
            self.store.set_node_failed(true);
            return report;
        }

        // Step 3: emit `node.fenced` with the lease and the app.
        let event_id = self.next_event_id();
        let envelope_id = self.next_message_id();
        self.send(FromNode::Event(Envelope {
            id: envelope_id,
            re: None,
            body: NodeEvent {
                id: event_id,
                time: SystemTime::now(),
                data: NodeEventData::NodeFenced(NodeFenced {
                    node_id: self.config.node_id.clone(),
                    app_ids,
                    lease_ids: vec![lease_id.clone()],
                    reason,
                }),
            },
        }));

        // Step 4 is implicit: `may_start_stateful` is false for every state
        // but `held`, and only a grant with a higher token sets `held`.
        report
    }

    fn save(&self, lease: &Lease) {
        let highest = self
            .table
            .highest_for(lease.resource_type, &lease.resource_id)
            .max(lease.token);
        self.store
            .save_lease(&StoredLease::from_lease(lease), highest);
    }

    fn remember(&mut self, id: MsgId, result: CommandResult) {
        self.send_command_result(id.clone(), result.clone());
        self.answered.insert(id, result);
    }

    fn send_command_result(&mut self, id: MsgId, result: CommandResult) {
        let envelope_id = self.next_message_id();
        self.send(FromNode::CommandResult(Envelope {
            id: envelope_id,
            re: Some(id),
            body: result,
        }));
    }

    fn send_error(&mut self, id: MsgId, message: String) {
        let envelope_id = self.next_message_id();
        self.send(FromNode::Error(Envelope {
            id: envelope_id,
            re: Some(id),
            body: ProtocolError {
                code: ErrorCode::BadMessage,
                message,
                fatal: false,
            },
        }));
    }

    /// Sends one message. A dropped receiver means the session layer is gone;
    /// the message is dropped with it. Buffering events for a later session
    /// arrives with the session work (section 6.5).
    fn send(&self, message: FromNode) {
        let _ = self.outbound.send(message);
    }

    fn next_message_id(&mut self) -> MsgId {
        self.next_msg += 1;
        MsgId(format!("msg_{:026}", self.next_msg))
    }

    fn next_event_id(&mut self) -> String {
        self.next_evt += 1;
        format!("evt_{:026}", self.next_evt)
    }
}

/// The `command_result` for a revoke (section 5.9): `ok` means the bound VMs
/// are stopped and recorded.
fn revoke_result(lease_id: &LeaseId, report: &FenceReport) -> CommandResult {
    if report.complete() {
        return CommandResult {
            status: CommandStatus::Ok,
            detail: None,
            error: None,
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
        // Contract gap: section 5.18 has no code for "the node could not stop
        // a VM". `bad_message` is the only non-fatal, non-specific code, so it
        // is used and the detail carries the truth. The session stays usable.
        error: Some(ErrorBody {
            code: ErrorCode::BadMessage,
            message: detail,
        }),
    }
}

/// Waits for `deadline`; with no deadline the branch never fires.
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
