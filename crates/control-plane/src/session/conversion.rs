// SPDX-License-Identifier: AGPL-3.0-only
//! A converted lease's first renewal, and the settle (`node-agent-protocol.md`
//! section 8.4, "Changing the mode" and "Settling a lease"; ADR 0241, ADR
//! 0244, ADR 0254, ADR 0257, ADR 0261; issue #555).
//!
//! - **A conversion in flight.** The hub keeps, for each session generation,
//!   the conversion grants it sent and whose first renewal it has not
//!   answered ([`InFlight`]). It dies with the generation: after a session
//!   cut or a restart, the holder's next `hello` decides (below). Nothing
//!   sends a conversion grant yet; [`NodeHub::note_conversion_sent`] is where
//!   its send records it.
//! - **The first answer.** A `lease_renew` of a lease in flight on its
//!   session, at the conversion's token, over a row that still reads `local`,
//!   is answered as the first one (ADR 0257: the conversion goes out only
//!   after the barrier `pong`, so such a renewal is one the node sent after
//!   it took the conversion). One single-writer transaction writes the row
//!   `expiring` and records the gate at `T_grant + D_1 + 2M(D_1)`, with `D_1`
//!   the longer of the conversion's and the answer's duration (ADR 0244, ADR
//!   0254). It commits before the answer is queued. Until then the row reads
//!   `local` and opens no gate, so the lease moves only as a `local` lease
//!   does: `failoverApp` answers `lease_held` (ADR 0241).
//! - **The settle.** A renewal of the holder answered on this session, then
//!   a `ping`; once its `pong` arrives, one write gives the row the holder's
//!   token, `mode` `expiring`, the longest duration granted on the session,
//!   and clears the unsettled mark. It starts in two ways: the holder's
//!   `hello` lists the lease `expiring` (`held` or `uncertain`) while the
//!   policy is `warm`, over a marked row or over a `local` row at its token
//!   (a conversion whose first renewal this primary never answered); or a
//!   renewal of a marked `expiring` row at its token while the policy is
//!   `warm`, the first renewal after an `expiring` grant with a new token,
//!   such as a failover's or a regrant's ("The mark").
//!
//! Not built here: sending the conversion grant, the holder's `local` report,
//! R and the same-token `local` grant with its `ping` (issue #555).

use super::{ControlMessage, NodeHub, Session, lease_token, new_msg_id};
use crate::authority::{DEFAULT_DURATION_MS, GrantKind, MIN_DURATION_MS};
use crate::changelog::{Actor, ActorType};
use crate::leases::{self, Lease};
use std::collections::HashMap;

/// A conversion grant a session sent, whose first renewal it has not
/// answered: the token it carried and its `duration_ms` (`D_conv`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InFlight {
    pub token: u64,
    pub duration_ms: u64,
}

/// What one session knows of the leases it settles.
#[derive(Debug, Default)]
pub(super) struct Settles {
    /// The holder's token its `hello` listed for each lease that this
    /// session settles from `hello`.
    reported: HashMap<String, u64>,
    /// The longest `duration_ms` this session granted for each lease.
    granted: HashMap<String, u64>,
    /// Each settling `ping` not answered yet: its message id, the lease and
    /// the holder's token.
    pings: HashMap<String, (String, u64)>,
}

impl Settles {
    /// The settle of `lease_id` from the holder's `hello`, at `token`.
    pub(super) fn report(&mut self, lease_id: &str, token: u64) {
        self.reported.insert(lease_id.to_string(), token);
    }
}

/// How a renewal is answered, decided from the row as read in the answer's
/// transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Plan {
    /// No answer; the reason is logged.
    Unanswered(&'static str),
    /// A conversion is in flight over a row that carries the mark, or while
    /// the app's policy is off `warm`: the policy change ended it. The
    /// renewal is answered as for an `expiring` lease, with no write, and the
    /// conversion is no longer in flight.
    Ended(InFlight),
    /// The first answer after the conversion.
    Converted(InFlight),
    /// A settle's answer, from the holder's `hello`, at the holder's token.
    Settle,
    /// The answer to an `expiring` row at its token. `settle` when the row
    /// carries the mark and the policy is `warm`: a `ping` follows.
    Ordinary { settle: bool },
}

/// How a renewal at `token` of `row` is answered, once the row names the node
/// unrevoked at a token no higher than `token` (`renewal_revoke` found
/// nothing to revoke). `in_flight` is this session's conversion of the
/// lease, `reported` the token this session's `hello` registered a settle at,
/// and `warm` whether the app's policy is `warm`.
pub(super) fn plan(
    row: &Lease,
    token: u64,
    in_flight: Option<InFlight>,
    reported: Option<u64>,
    warm: bool,
) -> Plan {
    let row_token = lease_token(row);
    let local = row.mode != "expiring";
    if let Some(in_flight) = in_flight.filter(|in_flight| in_flight.token == token) {
        // Section 8.4 "Changing the mode" (#555): a policy change off
        // `warm` (R) ends a conversion in flight; the primary answers the
        // holder's renewals as "From R until the holder has taken the grant"
        // says, and writes no `expiring` for them. R marks the row, so a
        // marked row is one R may have written; a policy off `warm` over an
        // unmarked row is the same change before R is built.
        if row.unsettled || !warm {
            return Plan::Ended(in_flight);
        }
        if local && row_token == token {
            return Plan::Converted(in_flight);
        }
    }
    if warm && reported == Some(token) && (row.unsettled || (local && row_token == token)) {
        return Plan::Settle;
    }
    if !local && row_token == token {
        return Plan::Ordinary {
            settle: row.unsettled && warm,
        };
    }
    Plan::Unanswered(if local {
        "a renewal of a local lease gets no answer"
    } else {
        "a renewal above the lease's token gets no answer"
    })
}

/// The duration of a first answer or a settle's answer: `D`, or the stored
/// duration when it is shorter. A grant longer than the stored duration
/// waits until its raise is replicated, and the fallback is the shorter
/// duration (ADR 0268); the conversion raised the stored duration to at
/// least its own before it went out (ADR 0254). `None` for a row that stores
/// no valid duration: there is no shorter one to fall back to (ADR 0289).
pub(super) fn answer_ms(row: &Lease) -> Option<u64> {
    let stored = row.duration_ms.and_then(|ms| u64::try_from(ms).ok())?;
    (stored >= MIN_DURATION_MS).then(|| stored.min(DEFAULT_DURATION_MS))
}

/// The kind the authority records a planned answer of `duration_ms` as, or
/// `None` when the row cannot be answered so. The first answer counts the
/// conversion's duration, and the stored duration must cover it (the raise
/// before the conversion, ADR 0254). An answer after the policy change ended
/// the conversion counts it too: the node's provisional deadline came from
/// it, and the side that grants less is a longer gate. A settle's answer from `hello` counts
/// the stored duration, which bounds every duration an earlier grant may have
/// carried, the conversion's included (section 8.4, "What the authority must
/// enforce").
pub(super) fn counted(plan: Plan, row: &Lease) -> Option<GrantKind> {
    let stored = row.duration_ms.and_then(|ms| u64::try_from(ms).ok());
    match plan {
        Plan::Converted(in_flight) => stored
            .filter(|stored| *stored >= in_flight.duration_ms)
            .map(|_| GrantKind::CountedRenewal {
                counted_ms: in_flight.duration_ms,
            }),
        Plan::Ended(in_flight) => Some(GrantKind::CountedRenewal {
            counted_ms: in_flight.duration_ms,
        }),
        Plan::Settle => stored.map(|counted_ms| GrantKind::CountedRenewal { counted_ms }),
        Plan::Ordinary { .. } => Some(GrantKind::Renewal),
        Plan::Unanswered(_) => None,
    }
}

impl NodeHub {
    /// Records that `generation` sent `node_id` the conversion grant of
    /// `lease_id` at `token` for `duration_ms`, so the next renewal of it on
    /// that session is answered as the first one. Called when the grant is
    /// queued, which follows the barrier `pong` (ADR 0257). Returns whether
    /// `generation` is still the node's session.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "nothing sends a conversion grant yet (#555)")
    )]
    pub(crate) fn note_conversion_sent(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        token: u64,
        duration_ms: u64,
    ) -> bool {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
        else {
            return false;
        };
        handle
            .conversions
            .insert(lease_id.to_string(), InFlight { token, duration_ms });
        true
    }

    /// `generation`'s conversion of `lease_id` in flight, if any.
    pub(super) fn conversion_in_flight(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
    ) -> Option<InFlight> {
        self.0
            .sessions
            .lock()
            .expect("session lock")
            .get(node_id)
            .filter(|handle| handle.generation == generation)
            .and_then(|handle| handle.conversions.get(lease_id).copied())
    }

    /// The first answer of `answered` committed, or a policy change ended
    /// it: the conversion is no longer in flight. A conversion recorded since
    /// stays.
    pub(super) fn conversion_answered(
        &self,
        node_id: &str,
        generation: u64,
        lease_id: &str,
        answered: InFlight,
    ) {
        let mut sessions = self.0.sessions.lock().expect("session lock");
        if let Some(handle) = sessions
            .get_mut(node_id)
            .filter(|handle| handle.generation == generation)
            && handle.conversions.get(lease_id) == Some(&answered)
        {
            handle.conversions.remove(lease_id);
        }
    }
}

impl Session {
    /// Notes that this session granted `lease_id` for `duration_ms`, for the
    /// settle's row write ("the longest `duration_ms` it granted on that
    /// session").
    pub(super) fn note_granted(&mut self, lease_id: &str, duration_ms: u64) {
        let longest = self
            .settles
            .granted
            .entry(lease_id.to_string())
            .or_insert(0);
        *longest = (*longest).max(duration_ms);
    }

    /// The token this session's `hello` registered a settle of `lease_id`
    /// at.
    pub(super) fn reported_settle(&self, lease_id: &str) -> Option<u64> {
        self.settles.reported.get(lease_id).copied()
    }

    /// Queues the settling `ping` after an answer of `lease_id` at `token`,
    /// unless one is already unanswered for it on this session: its `pong`
    /// proves the node received every answer sent before it (section 4.2),
    /// and the row write takes the longest duration granted on the session,
    /// which covers any answer sent after it.
    pub(super) fn ping_to_settle(&mut self, lease_id: &str, token: u64, outbox: &mut Vec<String>) {
        if self
            .settles
            .pings
            .values()
            .any(|(pinged, _)| pinged == lease_id)
        {
            return;
        }
        let id = new_msg_id();
        outbox.push(ControlMessage::Ping.to_frame(&id, None).to_text());
        self.settles.pings.insert(id, (lease_id.to_string(), token));
    }

    /// A `pong` (section 5.20): when it answers a settling `ping`, the row is
    /// settled ("Settling a lease", ADR 0261), in one single-writer
    /// transaction that finds, again:
    ///
    /// - this session still the node's;
    /// - the row naming the node, unrevoked, at a token no higher than the
    ///   holder's, and still to settle (marked, or reading `local`);
    /// - no failed fence of the lease, recorded or not (section 8.5);
    /// - the app's policy `warm`.
    ///
    /// The write takes the holder's token, `mode` `expiring`, the longest
    /// duration granted on this session but never less than the stored one,
    /// and clears the mark ([`leases::settle`]). Anything else writes nothing:
    /// the row keeps the mark and moves only as a `local` lease does.
    pub(super) async fn on_pong(&mut self, re: Option<&str>) {
        let Some((lease_id, token)) = re.and_then(|re| self.settles.pings.remove(re)) else {
            return;
        };
        let granted = self.settles.granted.get(&lease_id).copied().unwrap_or(0);
        match self.settle_row(&lease_id, token, granted).await {
            Ok(Some(lease)) => {
                self.settles.reported.remove(&lease_id);
                tracing::info!(
                    lease_id,
                    node_id = %self.node_id,
                    token = lease.fencing_token,
                    duration_ms = ?lease.duration_ms,
                    "the lease is settled: its row reads expiring, unmarked"
                );
                // Section 6.4: the desired state carries the lease's token.
                self.hub.node_changed(&self.node_id);
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(code = %e.code.as_str(), lease_id, "cannot settle a lease");
            }
        }
    }

    async fn settle_row(
        &self,
        lease_id: &str,
        token: u64,
        granted: u64,
    ) -> Result<Option<Lease>, crate::error::ApiError> {
        let mut w = self.hub.0.store.begin_write().await?;
        if !self.hub.is_current(&self.node_id, self.generation) {
            return Ok(None);
        }
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(None);
        };
        let to_settle = row.unsettled || row.mode != "expiring";
        if row.holder_node_id != self.node_id
            || row.is_revoked()
            || lease_token(&row) > token
            || !to_settle
        {
            tracing::info!(
                lease_id,
                "the row changed since its settling answer; no settle"
            );
            return Ok(None);
        }
        if crate::fences::has_failed_fence(&mut w.tx, &self.hub.0.unrecorded_failures, lease_id)
            .await?
            || !app_is_warm(&mut w.tx, &row.resource_id).await?
        {
            return Ok(None);
        }
        let stored = row
            .duration_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .unwrap_or(0);
        let duration_ms = i64::try_from(stored.max(granted)).unwrap_or(i64::MAX);
        let token = i64::try_from(token).unwrap_or(i64::MAX);
        let actor = Actor {
            kind: ActorType::Node,
            id: self.node_id.clone(),
        };
        let lease = leases::settle(&mut w.tx, row, token, duration_ms, &actor).await?;
        w.commit().await?;
        Ok(Some(lease))
    }
}

/// Whether `app_id`'s HA policy is `warm` (or `hot`, as `grant_app_lease`
/// reads it).
///
/// # Errors
///
/// A database error.
pub(super) async fn app_is_warm(
    conn: &mut sqlx::SqliteConnection,
    app_id: &str,
) -> Result<bool, crate::error::ApiError> {
    let policy: Option<String> = sqlx::query_scalar("SELECT ha_policy FROM apps WHERE id = ?")
        .bind(app_id)
        .fetch_optional(&mut *conn)
        .await?;
    Ok(matches!(policy.as_deref(), Some("warm" | "hot")))
}

#[cfg(test)]
mod tests {
    use super::super::tests::hub_with_node;
    use super::{InFlight, Plan, plan};
    use crate::AppState;
    use crate::authority::{ManualClock, MonotonicClock, VoterSupport};
    use crate::leases::{self, Lease};
    use crate::session::Session;
    use quoro_proto::wire::{self, NodeMessage};
    use quoro_proto::{LeaseId, LeaseMode, LeaseRenew};
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Duration;

    const LEASE: &str = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8D";
    const APP: &str = "app_01JC9Z7B3N5Q7S9Z1W3Y5A7C9E";
    const D: u64 = 15_000;

    #[derive(Debug)]
    struct EverySupport;

    impl VoterSupport for EverySupport {
        fn supports(&self, _duration_ms: u64) -> bool {
            true
        }
    }

    /// One node, one app with `policy`, and its lease row on the node: `mode`
    /// at token 3, storing `stored`, marked or not. The hub's authority runs
    /// on a manual clock and holds every support, so the tests move time
    /// only through the clock and never sleep.
    struct Fixture {
        state: AppState,
        node_id: String,
        data_dir: std::path::PathBuf,
        clock: Arc<ManualClock>,
    }

    async fn fixture(policy: &str, mode: &str, stored: Option<i64>, marked: bool) -> Fixture {
        let (state, node_id, data_dir) = hub_with_node().await;
        let pool = state.store.pool();
        sqlx::query("UPDATE nodes SET subnet = '10.42.1.0/24' WHERE id = ?")
            .bind(&node_id)
            .execute(pool)
            .await
            .unwrap();
        let now = crate::store::now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) \
             VALUES ('prj_c', 'c', 'c', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO apps (id, project_id, name, ha_policy, image, port, health_check, \
             resources, placement, status, created_at, updated_at) \
             VALUES (?, 'prj_c', 'c', ?, 'img', 80, '{}', '{}', '{\"node_ids\":[]}', \
             'stopped', ?, ?)",
        )
        .bind(APP)
        .bind(policy)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, \
             fencing_token, duration_ms, granted_at, unsettled) \
             VALUES (?, 'app', ?, ?, ?, 3, ?, ?, ?)",
        )
        .bind(LEASE)
        .bind(APP)
        .bind(&node_id)
        .bind(mode)
        .bind(stored)
        .bind(&now)
        .bind(i64::from(marked))
        .execute(pool)
        .await
        .unwrap();
        let clock = Arc::new(ManualClock::default());
        state
            .hub
            .authority()
            .set_test_clock(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        state.hub.set_voter_support(Arc::new(EverySupport));
        Fixture {
            state,
            node_id,
            data_dir,
            clock,
        }
    }

    impl Fixture {
        /// A session of the node whose `hello` lists the lease `state` in
        /// `mode` at `token`, or not at all, and the frames the session
        /// start queued on the outbox.
        async fn session(&self, listed: Option<(&str, LeaseMode, u64)>) -> (Session, Vec<Value>) {
            let hub = &self.state.hub;
            let (generation, _rx, _closed, _changed) = hub.attach(&self.node_id);
            let mut session = Session::new(hub.clone(), self.node_id.clone(), generation);
            let mut outbox = Vec::new();
            let hello = NodeMessage::Hello(hello(&self.node_id, listed))
                .to_frame("msg_hello", None)
                .to_text();
            assert!(!session.handle_text(&hello, &mut outbox).await);
            (session, frames(outbox))
        }

        async fn row(&self) -> Lease {
            let mut conn = self.state.store.pool().acquire().await.unwrap();
            leases::by_id(&mut conn, LEASE).await.unwrap().unwrap()
        }

        /// How many change-log entries the lease has.
        async fn writes(&self) -> i64 {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM change_log WHERE entity_type = 'lease' AND entity_id = ?",
            )
            .bind(LEASE)
            .fetch_one(self.state.store.pool())
            .await
            .unwrap()
        }

        /// What `failoverApp` finds for a source that is not connected
        /// ([`crate::failover::held_while_away`]).
        async fn held_while_away(&self) -> Option<&'static str> {
            let lease = self.row().await;
            let mut conn = self.state.store.pool().acquire().await.unwrap();
            crate::failover::held_while_away(&mut conn, self.state.hub.authority(), &lease)
                .await
                .unwrap()
        }

        fn passed(&self) -> bool {
            self.state.hub.authority().passed(LEASE)
        }

        fn done(self) {
            std::fs::remove_dir_all(&self.data_dir).unwrap();
        }
    }

    fn hello(node_id: &str, listed: Option<(&str, LeaseMode, u64)>) -> wire::Hello {
        wire::Hello {
            node_id: node_id.to_string(),
            agent_version: "agent".to_string(),
            engine_version: "engine".to_string(),
            boot_id: "boot-1".to_string(),
            stored_rev: 0,
            applied_rev: 0,
            log_seq: 0,
            log_hash: "0".repeat(64),
            leases: listed
                .into_iter()
                .map(|(state, mode, token)| wire::HelloLease {
                    lease_id: LEASE.to_string(),
                    resource_type: quoro_proto::ResourceType::App,
                    resource_id: APP.to_string(),
                    mode,
                    fencing_token: token,
                    state: state.to_string(),
                })
                .collect(),
            capacity: wire::Capacity {
                cpus: 2,
                memory_mb: 2_048,
                disk_gb: 10,
            },
            allocated: wire::Capacity::default(),
            kvm: wire::Kvm {
                available: true,
                kernel: "6.12.0".to_string(),
                nested: None,
            },
            dropped_events: 0,
            mesh_public_key: None,
            volume_snapshot: None,
            custody_public_key: None,
        }
    }

    fn frames(outbox: Vec<String>) -> Vec<Value> {
        outbox
            .iter()
            .map(|text| serde_json::from_str(text).unwrap())
            .collect()
    }

    fn of_type<'a>(frames: &'a [Value], kind: &str) -> Vec<&'a Value> {
        frames
            .iter()
            .filter(|frame| frame["type"] == kind)
            .collect()
    }

    /// The node renews the lease at `token` on `session`; the frames the
    /// session queued in answer.
    async fn renew(session: &mut Session, id: &str, token: u64) -> Vec<Value> {
        let mut outbox = Vec::new();
        let renew = NodeMessage::LeaseRenew(LeaseRenew {
            lease_id: LeaseId(LEASE.to_string()),
            fencing_token: token,
        })
        .to_frame(id, None)
        .to_text();
        session.handle_text(&renew, &mut outbox).await;
        frames(outbox)
    }

    async fn pong(session: &mut Session, re: &str) {
        let mut outbox = Vec::new();
        let pong = NodeMessage::Pong.to_frame("msg_pong", Some(re)).to_text();
        session.handle_text(&pong, &mut outbox).await;
        assert!(outbox.is_empty(), "a pong is not answered");
    }

    /// The one `lease_grant` among `frames`, which answers `re`, `expiring`
    /// at `token` for `duration_ms`.
    fn assert_answer(frames: &[Value], re: &str, token: u64, duration_ms: u64) {
        let grants = of_type(frames, "lease_grant");
        assert_eq!(grants.len(), 1, "{frames:?}");
        let grant = grants[0];
        assert_eq!(grant["re"], re);
        assert_eq!(grant["body"]["lease_id"], LEASE);
        assert_eq!(grant["body"]["mode"], "expiring");
        assert_eq!(grant["body"]["fencing_token"], token);
        assert_eq!(grant["body"]["duration_ms"], duration_ms);
    }

    /// Opens a second primary on the fixture's database, as a restart does:
    /// nothing in memory survives, and the start seeds the gates from the
    /// rows. Its authority then runs on a manual clock, seeded again from
    /// its start.
    async fn restart(fixture: &Fixture) -> (AppState, Arc<ManualClock>) {
        let store = crate::store::Store::open(&fixture.data_dir.join("quoro.db"))
            .await
            .unwrap();
        let state = AppState::open(
            store,
            crate::ServerConfig {
                data_dir: fixture.data_dir.clone(),
                secure_cookies: false,
                public_url: None,
                tls_fingerprint: String::new(),
                tls: crate::custody::test_tls(),
                node_network: crate::NodeNetwork {
                    subnet: "10.42.1.0/24".to_string(),
                    gateway: "10.42.1.1".to_string(),
                },
                heartbeat_interval_ms: 200,
                session_timeout_ms: 800,
            },
            Arc::new(crate::ReferenceAsDigest),
            Arc::new(crate::crypto::DataKey::ephemeral()),
        )
        .await
        .unwrap();
        let clock = Arc::new(ManualClock::default());
        state
            .hub
            .authority()
            .set_test_clock(Arc::clone(&clock) as Arc<dyn MonotonicClock>);
        state.hub.seed_lease_gates().await.unwrap();
        (state, clock)
    }

    /// ADR 0241, ADR 0244, ADR 0254, ADR 0257, the unsafe direction and the
    /// clocks. A renewal that reaches the primary before it sent the
    /// conversion (one that crossed it on the wire) gets no answer and
    /// writes nothing. The first renewal after the conversion is answered:
    /// the row reads `expiring` once the answer is queued, written in a
    /// transaction committed before it, and the gate counts the conversion's
    /// 30 s with its 3 s margin, on the primary's monotonic clock alone.
    /// Before that answer the lease has no gate and a failover finds it held.
    /// Later renewals are answered as any other.
    #[tokio::test]
    async fn the_first_renewal_after_a_conversion_writes_expiring_before_its_answer() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, start) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert_eq!(
            of_type(&start, "lease_grant").len(),
            1,
            "the held local lease is granted again at the start, as on main"
        );
        let writes = f.writes().await;

        // Crossed on the wire: sent before the node took the conversion.
        assert!(of_type(&renew(&mut session, "msg_r0", 3).await, "lease_grant").is_empty());
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        assert!(f.passed(), "the conversion opened no gate");
        assert!(f.held_while_away().await.is_some(), "lease_held");

        let in_flight = InFlight {
            token: 3,
            duration_ms: 30_000,
        };
        assert!(f.state.hub.note_conversion_sent(
            &f.node_id,
            session.generation,
            LEASE,
            in_flight.token,
            in_flight.duration_ms,
        ));
        assert!(f.passed(), "nor does the conversion's send");
        assert!(f.held_while_away().await.is_some(), "still lease_held");

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty(), "no ping: not a settle");
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("expiring", false));
        assert_eq!(
            row.duration_ms,
            Some(30_000),
            "the conversion's raise stays"
        );
        assert_eq!(f.writes().await, writes + 1, "one write, the mode");
        assert_eq!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE),
            None
        );

        // T_grant + 30 s + 2 x 3 s, not the answer's 15 s + 2 x 1.5 s.
        f.clock.advance(Duration::from_millis(35_999));
        assert!(!f.passed());
        assert!(f.held_while_away().await.is_some(), "the gate holds it");
        f.clock.advance(Duration::from_millis(1));
        assert!(f.passed());
        assert_eq!(f.held_while_away().await, None);

        let later = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&later, "msg_r2", 3, D);
        assert_eq!(f.writes().await, writes + 1, "a renewal writes nothing");
        f.done();
    }

    /// The lost session in each window. A conversion is in flight only on
    /// the session that sent it: once a newer session replaced it (the
    /// conversion not taken, taken, or its first renewal read but not
    /// answered), the old session answers nothing and writes nothing, the
    /// lease has no gate, and a failover finds it held. The newer session
    /// has no conversion in flight.
    #[tokio::test]
    async fn a_conversion_is_answered_only_on_the_session_that_sent_it() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let (mut old, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert!(
            f.state
                .hub
                .note_conversion_sent(&f.node_id, old.generation, LEASE, 3, D)
        );
        let writes = f.writes().await;
        let (_new, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        assert_eq!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, old.generation, LEASE),
            None
        );
        assert!(
            !f.state
                .hub
                .note_conversion_sent(&f.node_id, old.generation, LEASE, 3, D),
            "a replaced session records nothing"
        );
        assert!(of_type(&renew(&mut old, "msg_r1", 3).await, "lease_grant").is_empty());
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        assert!(f.passed());
        assert!(f.held_while_away().await.is_some());
        f.done();
    }

    /// The answer lost after its commit (the session lost between the
    /// commit and the send): the row reads `expiring` and the gate counts
    /// `D_1` = 30 s from the answer's `T_grant`. The node, which never got
    /// the answer, fences at its provisional deadline `t_send + 30 s - 3 s`,
    /// done by `t_send + 30 s`, and `t_send <= T_grant`: at least `2M` before
    /// the gate.
    #[tokio::test]
    async fn a_first_answer_lost_after_its_commit_leaves_the_conversions_gate() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);
        let _lost = renew(&mut session, "msg_r1", 3).await;
        assert_eq!(f.row().await.mode, "expiring");
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        f.done();
    }

    /// A primary restart between the conversion and its first answer: the
    /// row reads `local`, so the restarted primary seeds no gate, has no
    /// conversion in flight, and a failover finds the lease held (ADR 0244).
    /// A restart after the first answer committed (whether or not it was
    /// sent): the row reads `expiring` with the conversion's raised duration,
    /// so the start gate counts `start + 30 s + 2 x 3 s`, which covers the
    /// first answer's gate (ADR 0254).
    #[tokio::test]
    async fn a_restarted_primary_has_no_gate_before_the_first_answer_and_the_start_gate_after() {
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);

        let (before, _) = restart(&f).await;
        assert!(before.hub.authority().passed(LEASE), "no gate");
        assert_eq!(before.hub.authority().gate_remaining(LEASE), None);
        let lease = f.row().await;
        let mut conn = before.store.pool().acquire().await.unwrap();
        assert!(
            crate::failover::held_while_away(&mut conn, before.hub.authority(), &lease)
                .await
                .unwrap()
                .is_some()
        );
        drop(conn);

        assert_answer(&renew(&mut session, "msg_r1", 3).await, "msg_r1", 3, D);
        let (after, clock) = restart(&f).await;
        assert_eq!(
            after.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        clock.advance(Duration::from_secs(36));
        assert!(after.hub.authority().passed(LEASE));
        f.done();
    }

    /// Section 8.4 "Changing the mode" (#555): a policy change off `warm`
    /// (R, which marks the row) ends a conversion in flight. The holder's
    /// renewal is answered as for an `expiring` lease, with no write: the
    /// row stays `local` and marked, and the conversion is no longer in
    /// flight. Its gate counts the conversion's duration, the longer.
    #[tokio::test]
    async fn a_policy_change_off_warm_ends_a_conversion_in_flight() {
        let f = fixture("none", "local", Some(30_000), true).await;
        let (mut session, _) = f.session(None).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);
        let writes = f.writes().await;

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty());
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("local", true));
        assert_eq!(f.writes().await, writes, "no write");
        assert_eq!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE),
            None
        );
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        assert!(f.held_while_away().await.is_some(), "marked: lease_held");
        f.done();
    }

    /// "Settling a lease" from `hello` (section 8.4, ADR 0261), after a
    /// session cut that lost a conversion's first answer: the holder lists
    /// the lease `expiring` over a row that still reads `local`, and the
    /// policy is `warm`. The start sends no same-token `local` grant. Its
    /// next renewal is answered, the gate counting the stored duration, and
    /// a `ping` follows; the row still reads `local` until the `pong` that
    /// answers that `ping` writes it `expiring`. Another `pong` writes
    /// nothing.
    #[tokio::test]
    async fn a_holder_that_lists_a_converted_lease_expiring_is_settled_at_the_pong() {
        let f = fixture("warm", "local", Some(D as i64), false).await;
        let (mut session, start) = f.session(Some(("uncertain", LeaseMode::Expiring, 3))).await;
        assert!(
            of_type(&start, "lease_grant").is_empty(),
            "no local grant ends its renewals: {start:?}"
        );
        let writes = f.writes().await;

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let pings = of_type(&answer, "ping");
        assert_eq!(pings.len(), 1, "{answer:?}");
        let ping = pings[0]["id"].as_str().unwrap().to_string();
        assert_eq!(f.row().await.mode, "local", "not before the pong");
        assert_eq!(f.writes().await, writes);
        assert!(f.held_while_away().await.is_some());
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(18))
        );

        // A second renewal before the pong is answered with no second ping.
        let again = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&again, "msg_r2", 3, D);
        assert!(of_type(&again, "ping").is_empty());

        pong(&mut session, "msg_other").await;
        assert_eq!(f.row().await.mode, "local", "a pong to another ping");
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (
                row.mode.as_str(),
                row.fencing_token,
                row.duration_ms,
                row.unsettled
            ),
            ("expiring", 3, Some(D as i64), false)
        );
        assert_eq!(f.writes().await, writes + 1);
        pong(&mut session, &ping).await;
        assert_eq!(f.writes().await, writes + 1, "once");
        f.done();
    }

    /// The settle of a marked row, as after a failover's held `expiring`
    /// grant (ADR 0303), and what a second failover then finds. While the
    /// row is marked, a source that is not connected holds the lease whatever
    /// its gate (`lease_held`). The holder's renewal is answered and a
    /// `ping` follows; the `pong` clears the mark. From then on the lease
    /// moves on its gate: held until it passes, free after.
    #[tokio::test]
    async fn a_marked_rows_renewal_settles_it_and_the_lease_then_moves_on_its_gate() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let (mut session, _) = f.session(None).await;
        assert!(f.held_while_away().await.is_some(), "marked");

        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        let ping = of_type(&answer, "ping")[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        f.clock.advance(Duration::from_secs(18));
        assert!(f.passed());
        assert!(f.held_while_away().await.is_some(), "still marked");

        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!((row.mode.as_str(), row.unsettled), ("expiring", false));
        assert_eq!(f.held_while_away().await, None, "the gate has passed");

        let later = renew(&mut session, "msg_r2", 3).await;
        assert_answer(&later, "msg_r2", 3, D);
        assert!(of_type(&later, "ping").is_empty(), "settled");
        assert!(f.held_while_away().await.is_some(), "a new gate");
        f.done();
    }

    /// A first answer the authority refuses (no majority support, section
    /// 8.8) rolls back with its write: no grant, the row still `local`, no
    /// change-log entry, no gate, and the conversion still in flight. Never
    /// a row that reads `expiring` with no gate behind it. Once support
    /// returns, the next renewal is answered as the first one.
    #[tokio::test]
    async fn a_refused_first_answer_writes_nothing_and_keeps_the_conversion() {
        #[derive(Debug)]
        struct NoSupport;
        impl VoterSupport for NoSupport {
            fn supports(&self, _duration_ms: u64) -> bool {
                false
            }
        }
        let f = fixture("warm", "local", Some(30_000), false).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        f.state
            .hub
            .note_conversion_sent(&f.node_id, session.generation, LEASE, 3, 30_000);
        let writes = f.writes().await;
        f.state.hub.set_voter_support(Arc::new(NoSupport));
        assert!(of_type(&renew(&mut session, "msg_r1", 3).await, "lease_grant").is_empty());
        assert_eq!(f.row().await.mode, "local");
        assert_eq!(f.writes().await, writes);
        assert!(f.passed());
        assert!(f.held_while_away().await.is_some());
        assert!(
            f.state
                .hub
                .conversion_in_flight(&f.node_id, session.generation, LEASE)
                .is_some()
        );

        f.state.hub.set_voter_support(Arc::new(EverySupport));
        assert_answer(&renew(&mut session, "msg_r2", 3).await, "msg_r2", 3, D);
        assert_eq!(f.row().await.mode, "expiring");
        assert_eq!(f.writes().await, writes + 1);
        assert_eq!(
            f.state.hub.authority().gate_remaining(LEASE),
            Some(Duration::from_secs(36))
        );
        f.done();
    }

    /// "The holder's next `hello`" at a token above a marked row's, while the
    /// row names that holder: the settle answers the renewal at the holder's
    /// token, and the `pong` writes the row at that token (section 8.4).
    #[tokio::test]
    async fn a_holder_above_a_marked_rows_token_is_settled_at_its_own_token() {
        let f = fixture("warm", "expiring", Some(D as i64), true).await;
        let (mut session, start) = f.session(Some(("held", LeaseMode::Expiring, 5))).await;
        assert!(of_type(&start, "lease_revoke").is_empty(), "{start:?}");
        assert!(of_type(&start, "lease_grant").is_empty(), "{start:?}");
        let answer = renew(&mut session, "msg_r1", 5).await;
        assert_answer(&answer, "msg_r1", 5, D);
        let ping = of_type(&answer, "ping")[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(f.row().await.fencing_token, 3, "not before the pong");
        pong(&mut session, &ping).await;
        let row = f.row().await;
        assert_eq!(
            (row.mode.as_str(), row.fencing_token, row.unsettled),
            ("expiring", 5, false)
        );
        f.done();
    }

    /// No settle while the policy is off `warm`, nor once the session is
    /// replaced before the `pong`: the mark stays, and the lease moves only
    /// as a `local` lease does. A replaced session that still reads a
    /// renewal answers nothing: the newer session's start decided the lease.
    #[tokio::test]
    async fn no_settle_off_warm_or_after_the_session_is_replaced() {
        let f = fixture("none", "expiring", Some(D as i64), true).await;
        let (mut session, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        let answer = renew(&mut session, "msg_r1", 3).await;
        assert_answer(&answer, "msg_r1", 3, D);
        assert!(of_type(&answer, "ping").is_empty(), "off warm");
        assert!(f.row().await.unsettled);

        sqlx::query("UPDATE apps SET ha_policy = 'warm' WHERE id = ?")
            .bind(APP)
            .execute(f.state.store.pool())
            .await
            .unwrap();
        let answer = renew(&mut session, "msg_r2", 3).await;
        let ping = of_type(&answer, "ping")[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let writes = f.writes().await;
        let (_new, _) = f.session(Some(("held", LeaseMode::Expiring, 3))).await;
        pong(&mut session, &ping).await;
        assert!(f.row().await.unsettled, "the old session settles nothing");
        assert_eq!(f.writes().await, writes);
        assert!(
            of_type(&renew(&mut session, "msg_r3", 3).await, "lease_grant").is_empty(),
            "nor answers a renewal it read after the newer session started"
        );
        f.done();
    }

    fn row(mode: &str, token: i64, unsettled: bool) -> Lease {
        Lease {
            id: LEASE.to_string(),
            resource_type: "app".to_string(),
            resource_id: APP.to_string(),
            holder_node_id: "nod_a".to_string(),
            mode: mode.to_string(),
            fencing_token: token,
            duration_ms: Some(15_000),
            granted_at: "2026-11-10T10:00:00Z".to_string(),
            renewed_at: None,
            expires_at: None,
            revoked_at: None,
            unsettled,
        }
    }

    /// Every branch of the plan, the crossing renewal's included.
    #[test]
    fn the_plan_follows_the_row_the_conversion_and_the_settle() {
        let conv = InFlight {
            token: 3,
            duration_ms: 15_000,
        };
        let local = row("local", 3, false);
        let marked_local = row("local", 3, true);
        let expiring = row("expiring", 3, false);
        let marked = row("expiring", 3, true);
        let unanswered = |plan: Plan| matches!(plan, Plan::Unanswered(_));
        // The first answer, and only at the conversion's token.
        assert_eq!(
            plan(&local, 3, Some(conv), None, true),
            Plan::Converted(conv)
        );
        assert!(unanswered(plan(&local, 4, Some(conv), None, true)));
        assert!(
            unanswered(plan(&local, 3, None, None, true)),
            "no conversion"
        );
        // Ended by the policy change (section 8.4 "Changing the mode").
        assert_eq!(
            plan(&marked_local, 3, Some(conv), None, true),
            Plan::Ended(conv)
        );
        assert_eq!(plan(&local, 3, Some(conv), None, false), Plan::Ended(conv));
        // The settle from `hello`, at the holder's token.
        assert_eq!(plan(&local, 3, None, Some(3), true), Plan::Settle);
        assert_eq!(plan(&marked, 5, None, Some(5), true), Plan::Settle);
        assert!(
            unanswered(plan(&local, 3, None, Some(3), false)),
            "off warm"
        );
        assert!(unanswered(plan(&local, 4, None, Some(4), true)), "unmarked");
        assert!(unanswered(plan(&expiring, 5, None, Some(5), true)));
        // Ordinary answers, settling a marked row while `warm`.
        assert_eq!(
            plan(&expiring, 3, None, None, true),
            Plan::Ordinary { settle: false }
        );
        assert_eq!(
            plan(&marked, 3, None, None, true),
            Plan::Ordinary { settle: true }
        );
        assert_eq!(
            plan(&marked, 3, None, None, false),
            Plan::Ordinary { settle: false }
        );
        assert!(unanswered(plan(&expiring, 4, None, None, true)));
    }
}
