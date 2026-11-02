// SPDX-License-Identifier: AGPL-3.0-only
//! In-process primary and helpers for the lease tests.
//!
//! This is `tests/common/mod.rs`, so cargo does not build it as its own test
//! binary; each helper is used by at least one test.

#![allow(dead_code)]

use std::future::Future;
use std::net::Ipv4Addr;
use std::time::Duration;

use quoro_engine::{
    ConsoleMode, Engine, FakeEngine, ImageDigest, ImageRef, InitConfig, MacAddr, NetAttach,
    StopMode, VmId, VmSpec,
};
use quoro_node_agent::{FenceTimers, LeaseAgent, LeaseAgentConfig, LeaseState, MemoryLeaseStore};
use quoro_proto::{
    CommandResult, Envelope, FromNode, LeaseGrant, LeaseId, LeaseMode, LeaseRenew, LeaseRevoke,
    MsgId, NodeEvent, NodeEventData, NodeFenceFailed, NodeFenced, ProtocolError, ResourceType,
    RevokeReason, ToNode,
};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

/// The app id every test lease names.
pub const APP: &str = "app_test";
/// The node id every test agent reports.
pub const NODE: &str = "nod_test";
/// Shorthand for a duration in milliseconds.
#[must_use]
pub fn millis(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

/// The fence timeouts used by tests; `budget_for` clamps them per lease.
#[must_use]
pub fn config() -> LeaseAgentConfig {
    LeaseAgentConfig {
        node_id: NODE.to_string(),
        timers: FenceTimers {
            grace: millis(700),
            force: millis(300),
        },
    }
}

/// One side of an in-process session: the primary.
pub struct Primary {
    /// Messages this primary sends to the node agent.
    pub to_node: mpsc::UnboundedSender<ToNode>,
    /// Messages the node agent sends back.
    pub from_node: mpsc::UnboundedReceiver<FromNode>,
    next: u64,
}

/// Builds a primary, the node's inbound channel and the node's outbound
/// channel.
///
/// The two sides number their messages from different starts so a log of a
/// failing test shows which side sent what; message ids are session-local
/// (section 3.3), so this has no protocol meaning.
pub fn pair() -> (
    Primary,
    mpsc::UnboundedReceiver<ToNode>,
    mpsc::UnboundedSender<FromNode>,
) {
    let (to_node, inbound) = mpsc::unbounded_channel();
    let (outbound, from_node) = mpsc::unbounded_channel();
    (
        Primary {
            to_node,
            from_node,
            next: 1000,
        },
        inbound,
        outbound,
    )
}

impl Primary {
    fn id(&mut self) -> MsgId {
        self.next += 1;
        MsgId(format!("msg_{:026}", self.next))
    }

    /// Sends one message as is.
    pub fn send(&self, message: ToNode) {
        let _ = self.to_node.send(message);
    }

    /// Grants a `local` lease (section 5.12).
    pub fn grant_local(&mut self, lease: &str, app: &str, token: u64) -> MsgId {
        self.grant(
            LeaseGrant {
                lease_id: LeaseId(lease.to_string()),
                resource_type: ResourceType::App,
                resource_id: app.to_string(),
                mode: LeaseMode::Local,
                fencing_token: token,
                duration_ms: None,
                granted_at: "2026-11-02T10:15:33Z".to_string(),
            },
            None,
        )
    }

    /// Grants an `expiring` lease, optionally answering a renewal.
    pub fn grant_expiring(
        &mut self,
        lease: &str,
        app: &str,
        token: u64,
        duration_ms: u64,
        re: Option<MsgId>,
    ) -> MsgId {
        self.grant(
            LeaseGrant {
                lease_id: LeaseId(lease.to_string()),
                resource_type: ResourceType::App,
                resource_id: app.to_string(),
                mode: LeaseMode::Expiring,
                fencing_token: token,
                duration_ms: Some(duration_ms),
                granted_at: "2026-11-02T10:15:33Z".to_string(),
            },
            re,
        )
    }

    fn grant(&mut self, body: LeaseGrant, re: Option<MsgId>) -> MsgId {
        let id = self.id();
        self.send(ToNode::LeaseGrant(Envelope {
            id: id.clone(),
            re,
            body,
        }));
        id
    }

    /// Sends a `lease_grant` exactly as given, for the shapes the helpers
    /// above do not build (a `local` grant that carries a duration, for one).
    pub fn grant_body(&mut self, body: LeaseGrant) -> MsgId {
        self.grant(body, None)
    }

    /// Revokes a lease (section 5.13).
    pub fn revoke(&mut self, lease: &str, token: u64, reason: RevokeReason) -> MsgId {
        let id = self.id();
        self.send_revoke(&id, lease, token, reason);
        id
    }

    /// Sends a revoke that carries an id already used, as a redelivery does.
    pub fn resend_revoke(&mut self, id: &MsgId, lease: &str, token: u64, reason: RevokeReason) {
        self.send_revoke(id, lease, token, reason);
    }

    fn send_revoke(&mut self, id: &MsgId, lease: &str, token: u64, reason: RevokeReason) {
        self.send(ToNode::LeaseRevoke(Envelope {
            id: id.clone(),
            re: None,
            body: LeaseRevoke {
                lease_id: LeaseId(lease.to_string()),
                fencing_token: token,
                reason,
            },
        }));
    }

    /// Drops the session: the node's inbound channel closes, as it does when
    /// the WebSocket is gone.
    pub fn lose_session(&mut self) {
        let (dead, _) = mpsc::unbounded_channel();
        self.to_node = dead;
    }

    /// The next message from the node.
    ///
    /// # Panics
    ///
    /// Panics when the node's side is gone; every test sends a stop flag
    /// before that can happen.
    pub async fn recv(&mut self) -> FromNode {
        self.from_node.recv().await.expect("the node agent is gone")
    }

    /// The next message, if one is already queued.
    pub fn try_recv(&mut self) -> Option<FromNode> {
        self.from_node.try_recv().ok()
    }

    /// The next `lease_renew`, with the instant it was sent.
    pub async fn recv_renew(&mut self) -> (MsgId, Instant) {
        let message = self.recv().await;
        let at = Instant::now();
        match message {
            FromNode::LeaseRenew(envelope) => (envelope.id, at),
            other => panic!("expected lease_renew, got {other:?}"),
        }
    }

    /// Answers the next `lease_renew` with a grant that carries the same token
    /// (D-0073: only an ownership change raises the token) and answers the
    /// renewal with `re`.
    pub async fn answer_renewal(
        &mut self,
        lease: &str,
        app: &str,
        duration_ms: u64,
        token: u64,
    ) -> (MsgId, Instant) {
        let (id, at) = self.recv_renew().await;
        self.grant_expiring(lease, app, token, duration_ms, Some(id.clone()));
        (id, at)
    }

    /// Sends the first `expiring` grant and confirms it: the node asks for a
    /// renewal at once (D-0072), and answering it with the same token is what
    /// makes the lease `held` with `valid_until = t_send + D - M`.
    ///
    /// Returns the answered renewal's id and its send instant, which is the
    /// `t_send` the lease's validity is based on.
    pub async fn grant_and_confirm(
        &mut self,
        lease: &str,
        app: &str,
        token: u64,
        duration_ms: u64,
    ) -> (MsgId, Instant) {
        self.grant_expiring(lease, app, token, duration_ms, None);
        let message = self.recv().await;
        let at = Instant::now();
        let FromNode::LeaseRenew(envelope) = message else {
            panic!("expected the immediate lease_renew, got {message:?}");
        };
        assert_eq!(
            envelope.body.fencing_token, token,
            "the immediate renewal echoes the granted token"
        );
        let id = envelope.id;
        self.grant_expiring(lease, app, token, duration_ms, Some(id.clone()));
        (id, at)
    }

    /// Answers every renewal until `until`, and returns the last one answered.
    pub async fn serve_renewals(
        &mut self,
        lease: &str,
        app: &str,
        duration_ms: u64,
        token: u64,
        until: Instant,
    ) -> Option<(MsgId, Instant)> {
        let mut last = None;
        loop {
            let message = tokio::select! {
                biased;
                () = tokio::time::sleep_until(until) => break,
                message = self.from_node.recv() => message,
            };
            let Some(message) = message else {
                break;
            };
            match message {
                FromNode::LeaseRenew(envelope) => {
                    let at = Instant::now();
                    // D-0073: a renewal answer keeps the token.
                    self.grant_expiring(lease, app, token, duration_ms, Some(envelope.id.clone()));
                    last = Some((envelope.id, at));
                }
                other => panic!("expected lease_renew while serving renewals, got {other:?}"),
            }
        }
        last
    }
}

/// Runs one phase of the agent until the script is done, then sets the stop
/// flag so the agent's loop returns.
///
/// The agent and the script run in the same task, so both share the paused test
/// clock and the script can set the stop flag the agent is watching.
pub async fn run_phase<F>(
    agent: &mut LeaseAgent<'_, FakeEngine, MemoryLeaseStore>,
    inbound: &mut mpsc::UnboundedReceiver<ToNode>,
    script: F,
) where
    F: Future<Output = ()>,
{
    let (stop_tx, stop_rx) = watch::channel(false);
    let script = async move {
        script.await;
        let _ = stop_tx.send(true);
    };
    tokio::join!(agent.run(inbound, stop_rx), script);
}

/// Lets the agent handle the messages just sent before the stop flag is set.
///
/// The paused clock does not move; each yield repolls the joined futures, and
/// the agent is polled before the script on every pass.
pub async fn settle() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}

/// Creates a created-and-started VM on the fake engine.
pub async fn running_vm(engine: &FakeEngine, id: &str, index: u8) -> VmId {
    let digest = ImageDigest(format!("sha256:{}", "0".repeat(64)));
    let image = ImageRef {
        reference: "busybox".to_string(),
        digest: digest.clone(),
    };
    engine.prepare_image(&image).await.expect("prepare_image");
    let vm = VmId(id.to_string());
    let spec = VmSpec {
        id: vm.clone(),
        vcpus: 1,
        memory_mb: 128,
        rootfs: digest,
        disk_gb: 1,
        volumes: Vec::new(),
        net: NetAttach {
            ip: Ipv4Addr::new(10, 0, 0, 2 + index),
            gateway: Ipv4Addr::new(10, 0, 0, 1),
            mac: MacAddr([0x02, 0, 0, 0, 0, index]),
        },
        init: InitConfig {
            entrypoint: Vec::new(),
            cmd: Vec::new(),
            env: Vec::new(),
            user: None,
            workdir: None,
        },
        console: ConsoleMode::Off,
    };
    engine.create(&spec).await.expect("create");
    engine.start(&vm).await.expect("start");
    vm
}

/// The event in a `FromNode::Event`.
pub fn event_of(message: FromNode) -> NodeEvent {
    match message {
        FromNode::Event(envelope) => envelope.body,
        other => panic!("expected event, got {other:?}"),
    }
}

/// The `node.fenced` payload, or a panic naming the event that arrived.
pub fn fenced_of(event: NodeEvent) -> NodeFenced {
    match event.data {
        NodeEventData::NodeFenced(fenced) => fenced,
        other => panic!("expected node.fenced, got {other:?}"),
    }
}

/// The `node.fence_failed` payload, or a panic naming the event that arrived.
pub fn fence_failed_of(event: NodeEvent) -> NodeFenceFailed {
    match event.data {
        NodeEventData::NodeFenceFailed(failed) => failed,
        other => panic!("expected node.fence_failed, got {other:?}"),
    }
}

/// The answered id and body of a `command_result`.
pub fn command_result_of(message: FromNode) -> (MsgId, CommandResult) {
    match message {
        FromNode::CommandResult(envelope) => (
            envelope.re.expect("command_result answers a message"),
            envelope.body,
        ),
        other => panic!("expected command_result, got {other:?}"),
    }
}

/// The answered id and body of an `error`.
pub fn error_of(message: FromNode) -> (MsgId, ProtocolError) {
    match message {
        FromNode::Error(envelope) => (envelope.re.expect("error answers a message"), envelope.body),
        other => panic!("expected error, got {other:?}"),
    }
}

/// The state of a lease, for a readable assertion message.
pub fn lease_state(
    agent: &LeaseAgent<'_, FakeEngine, MemoryLeaseStore>,
    lease: &LeaseId,
) -> LeaseState {
    agent
        .table()
        .get(lease)
        .expect("the agent tracks the lease")
        .state
}

/// The number of graceful and forced stop calls in `stop_calls`.
#[must_use]
pub fn stop_counts(stop_calls: &[(VmId, StopMode)]) -> (usize, usize) {
    let graceful = stop_calls
        .iter()
        .filter(|(_, mode)| matches!(mode, StopMode::Graceful { .. }))
        .count();
    (graceful, stop_calls.len() - graceful)
}

/// A `lease_renew` body, for tests that inspect one directly.
pub fn renew_of(message: FromNode) -> (MsgId, LeaseRenew) {
    match message {
        FromNode::LeaseRenew(envelope) => (envelope.id, envelope.body),
        other => panic!("expected lease_renew, got {other:?}"),
    }
}
