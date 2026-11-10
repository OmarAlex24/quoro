// SPDX-License-Identifier: AGPL-3.0-only
//! The holder's `local` report (`node-agent-protocol.md` section 8.4, "The
//! holder's next `hello`", "Writing the row `local`" and "The mark"; ADR
//! 0280, ADR 0285; issue #555).
//!
//! - **The report.** The holder's `hello` lists the lease `local`, `held` or
//!   `uncertain` (ADR 0285), at the row's token, or at a higher token while
//!   the row names that holder. `expired` and `revoked` are no report and
//!   keep their own paths (section 8.5, "Regrant from `hello`"; ADR 0281).
//! - **The write.** A row that reads `expiring`, marked or not, is written
//!   `local` at the holder's token ("Writing the row `local`": "whether or
//!   not the row is marked"); so is a row that reads `local` and carries the
//!   mark ("The mark", the `local` report). Either write clears the mark,
//!   keeps `duration_ms` ([`leases::write_local_report`]) and appends one
//!   change-log entry. A row that reads `local`, unmarked, is not written.
//! - **The checks.** One single-writer transaction finds, again, as the
//!   settle's does ([`super::conversion`]): this session still the node's;
//!   the row naming the node, unrevoked (a declaration of the node lost
//!   revokes it, section 8.3), at a token no higher than the holder's; and
//!   no failed fence of the lease, recorded or not (section 8.5).
//! - **A failed write.** A write that fails on a database error ends the
//!   session before its first desired state and its grants, as an
//!   incomplete pass over the leases does: the row may read `expiring`,
//!   unmarked, while the holder runs the lease `local`. The node dials
//!   again, and its next session writes the row
//!   ([`Session::record_local_reports`]).
//! - **The order.** It runs once the session start's pass over the leases
//!   ran to its end (`revoke_stale_leases`, with the `declared_lost`
//!   revokes and the step-4 decision, which drops any held grant of a row
//!   listed at its token or above), and before the first desired state and
//!   [`Session::send_grants`], so both follow the row as written. For a row
//!   now `local`, unmarked, at the holder's token, that grant is the
//!   same-token `local` repeat, which changes nothing on the node (section
//!   8.4, node state machine). Once a row is written, the node's desired
//!   state is rebuilt.
//!
//! The policy is not read: the report is what the holder holds, under any
//! policy.
//!
//! Reading: refusing the write while the lease has a failed fence is
//! stricter than "The mark"'s `local` report bullet, which names no such
//! check. It is harmless: while the fence stands, the row stays as it was,
//! marked or `expiring`, but no failover can move the lease on it:
//! `failoverApp` refuses with `fence_failed` until an operator acknowledges
//! the fence (section 8.5), and the holder's next report after that writes
//! the row.

use super::{Fault, Session, lease_token};
use crate::changelog::{Actor, ActorType};
use crate::error::ApiError;
use crate::leases::{self, Lease};

impl Session {
    /// Records every `local` report of this session's `hello`
    /// ([`Session::write_local_report`]), then rebuilds the node's desired
    /// state when a row was written. Returns whether the session start may
    /// go on.
    ///
    /// A report whose checks refuse it (`None`) leaves the row as it was,
    /// and the start goes on. A write that fails on a database error leaves
    /// the row as it was too, which may be a row that reads `expiring`,
    /// unmarked, while the holder runs the lease `local`: the start's
    /// grants send nothing for it, and once its gate passed a failover could
    /// move the lease while the holder still runs. So the start stops there,
    /// sends nothing the write would have unlocked, and the session ends
    /// ([`Session::finish_start`]); the node dials again, and its next
    /// session's `hello` reports the lease again.
    pub(super) async fn record_local_reports(&mut self) -> bool {
        let mut reports: Vec<(String, u64)> = self
            .hello_leases
            .iter()
            .filter(|(_, (state, _, expiring))| {
                !*expiring && (state == "held" || state == "uncertain")
            })
            .map(|(lease_id, (_, token, _))| (lease_id.clone(), *token))
            .collect();
        reports.sort();
        let mut written = false;
        for (lease_id, token) in reports {
            match self.write_local_report(&lease_id, token).await {
                Ok(Some(lease)) => {
                    written = true;
                    tracing::info!(
                        lease_id,
                        node_id = %self.node_id,
                        token = lease.fencing_token,
                        "the holder reports the lease local: its row reads local, unmarked"
                    );
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        code = %e.code.as_str(),
                        lease_id,
                        node_id = %self.node_id,
                        "cannot record a holder's local report; the session ends, and the node dials again"
                    );
                    return false;
                }
            }
        }
        if written {
            // Section 6.4: the desired state carries the lease's token.
            self.hub.node_changed(&self.node_id);
        }
        true
    }

    /// The write of one `local` report of `lease_id` at the holder's
    /// `token`, if every check holds again in its single-writer
    /// transaction (module docs). Returns the row as written, `None` when
    /// nothing is written.
    pub(super) async fn write_local_report(
        &self,
        lease_id: &str,
        token: u64,
    ) -> Result<Option<Lease>, ApiError> {
        let mut w = self.hub.0.store.begin_write().await?;
        if !self.hub.is_current(&self.node_id, self.generation) {
            return Ok(None);
        }
        let Some(row) = leases::by_id(&mut w.tx, lease_id).await? else {
            return Ok(None);
        };
        // Another holder, a revoked row (a declaration of the node lost
        // included, section 8.3), or a row above the holder's token is no
        // report of this holder's.
        if row.holder_node_id != self.node_id || row.is_revoked() || lease_token(&row) > token {
            return Ok(None);
        }
        // A row that reads `local`, unmarked, already says what the holder
        // holds.
        if row.mode != "expiring" && !row.unsettled {
            return Ok(None);
        }
        if crate::fences::has_failed_fence(&mut w.tx, &self.hub.0.unrecorded_failures, lease_id)
            .await?
        {
            return Ok(None);
        }
        // No primary issues a token above 2^53 - 1 (section 8.5): a node
        // that reports one holds a corrupted or forged store, and the change
        // log would refuse it.
        let Some(token) = i64::try_from(token)
            .ok()
            .filter(|token| *token <= leases::MAX_TOKEN)
        else {
            return Ok(None);
        };
        if self.hub.take_fault(&Fault::LocalReport {
            node_id: self.node_id.clone(),
        }) {
            return Err(ApiError::internal(&"a test seam failed the write"));
        }
        let actor = Actor {
            kind: ActorType::Node,
            id: self.node_id.clone(),
        };
        let lease = leases::write_local_report(&mut w.tx, row, token, &actor).await?;
        w.commit().await?;
        Ok(Some(lease))
    }
}

#[cfg(test)]
mod tests {
    use super::super::conversion::tests::{
        APP, Fixture, LEASE, actor, fixture, frames, hello, of_type, restart, write_row,
    };
    use crate::api::leases::Lease as LeaseView;
    use crate::leases;
    use crate::session::Session;
    use quoro_proto::LeaseMode;
    use quoro_proto::wire::NodeMessage;
    use serde_json::Value;
    use tokio::sync::mpsc;

    /// `Lease.local_pending` of the row as the API shows it.
    async fn local_pending(f: &Fixture) -> bool {
        LeaseView::from_row(f.row().await, false, Vec::new())
            .unwrap()
            .local_pending
    }

    /// `Lease.duration_ms` of the row as the API shows it: `expiring`
    /// leases only (`openapi.yaml`), whatever the row stores.
    async fn shown_duration(f: &Fixture) -> Option<i64> {
        LeaseView::from_row(f.row().await, false, Vec::new())
            .unwrap()
            .duration_ms
    }

    /// The row as (mode, token, `duration_ms`, unsettled, revoked).
    async fn state(f: &Fixture) -> (String, i64, Option<i64>, bool, bool) {
        let row = f.row().await;
        let revoked = row.is_revoked();
        (
            row.mode,
            row.fencing_token,
            row.duration_ms,
            row.unsettled,
            revoked,
        )
    }

    /// The newest change-log payload of the lease.
    async fn newest_payload(f: &Fixture) -> Value {
        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM change_log WHERE entity_type = 'lease' AND entity_id = ? \
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(LEASE)
        .fetch_one(f.state.store.pool())
        .await
        .unwrap();
        serde_json::from_str(&payload).unwrap()
    }

    /// The one `lease_grant` among `frames`: the same-token `local` repeat at
    /// `token`, which answers nothing and carries no `duration_ms` (section
    /// 5.12). The node agent takes it as a `local` grant repeated with the
    /// same token while `held`, which changes nothing (section 8.4, node
    /// state machine; `node-agent` `lease::machine`, `on_grant`): a `local`
    /// grant with `duration_ms` would be refused with `bad_message`.
    fn assert_local_repeat(frames: &[Value], token: u64) {
        let grants = of_type(frames, "lease_grant");
        assert_eq!(grants.len(), 1, "{frames:?}");
        let grant = grants[0];
        assert!(grant["re"].is_null(), "{grant}");
        assert_eq!(grant["body"]["lease_id"], LEASE);
        assert_eq!(grant["body"]["mode"], "local", "{grant}");
        assert_eq!(grant["body"]["fencing_token"], token, "{grant}");
        assert!(grant["body"].get("duration_ms").is_none(), "{grant}");
    }

    /// Test 2 of #555's local report, "Writing the row `local`": a row that
    /// reads `expiring`, unmarked or marked, under any policy, which the
    /// holder lists `local`, `held` or `uncertain`, at the row's token, is
    /// written `local` at that token, the mark cleared and `duration_ms`
    /// kept, in one change-log entry. The session start then sends the
    /// same-token `local` repeat with no `duration_ms`, and nothing else.
    #[tokio::test]
    async fn an_expiring_row_the_holder_reports_local_is_written_local() {
        for (policy, marked) in [("warm", false), ("warm", true), ("none", true)] {
            for listed in ["held", "uncertain"] {
                let case = format!("{policy}, marked {marked}, {listed}");
                let f = fixture(policy, "expiring", Some(30_000), marked).await;
                let writes = f.writes().await;
                let (_session, start) = f.session(Some((listed, LeaseMode::Local, 3))).await;
                assert_eq!(
                    state(&f).await,
                    ("local".to_string(), 3, Some(30_000), false, false),
                    "{case}"
                );
                assert_eq!(f.writes().await, writes + 1, "{case}");
                let payload = newest_payload(&f).await;
                assert_eq!(payload["mode"], "local", "{case}: {payload}");
                assert_eq!(payload["unsettled"], false, "{case}: {payload}");
                assert_eq!(payload["fencing_token"], 3, "{case}: {payload}");
                assert!(!local_pending(&f).await, "{case}");
                assert_eq!(shown_duration(&f).await, None, "{case}");
                assert_local_repeat(&start, 3);
                f.done();
            }
        }
    }

    /// Test 1's write, "The mark" (the `local` report bullet): a row that
    /// reads `local` and carries the mark, as a new-token `local` grant of a
    /// `warm` app's lease leaves it (ADR 0285, ADR 0288), and that its
    /// holder lists `local` `held` at the row's token, loses the mark in
    /// one change-log entry: `Lease.local_pending` turns false, and a
    /// failover with the holder away finds the lease held because it is
    /// `local`, not because of the mark.
    #[tokio::test]
    async fn a_marked_local_row_the_holder_reports_local_loses_its_mark() {
        let f = fixture("warm", "local", None, true).await;
        assert!(local_pending(&f).await);
        let marked = f.held_while_away().await.unwrap();
        assert!(marked.contains("unsettled mark"), "{marked}");
        let writes = f.writes().await;
        let (_session, start) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, None, false, false)
        );
        assert_eq!(f.writes().await, writes + 1);
        assert!(!local_pending(&f).await);
        let held = f.held_while_away().await.unwrap();
        assert!(held.contains("its lease is local"), "{held}");
        assert!(!held.contains("unsettled mark"), "{held}");
        assert_local_repeat(&start, 3);
        f.done();
    }

    /// Test 3: the holder lists the lease `local` at a token above the
    /// row's while the row names it. A row that reads `expiring`, marked or
    /// not, or `local` with the mark, is written `local` at the holder's
    /// token, unmarked, and the repeat carries that token.
    #[tokio::test]
    async fn a_holder_above_the_rows_token_is_recorded_at_its_own_token() {
        for (mode, stored, marked) in [
            ("expiring", Some(15_000), false),
            ("expiring", Some(15_000), true),
            ("local", None, true),
        ] {
            let case = format!("{mode}, marked {marked}");
            let f = fixture("warm", mode, stored, marked).await;
            let writes = f.writes().await;
            let (_session, start) = f.session(Some(("held", LeaseMode::Local, 5))).await;
            assert_eq!(
                state(&f).await,
                ("local".to_string(), 5, stored, false, false),
                "{case}"
            );
            assert_eq!(f.writes().await, writes + 1, "{case}");
            assert_local_repeat(&start, 5);
            f.done();
        }
    }

    /// A row that reads `local`, unmarked, already says what the holder
    /// holds: listed `local` at its token, it is not written, and the
    /// session start sends the same-token repeat with no `duration_ms`,
    /// although the row keeps one from an earlier `expiring` grant. Listed
    /// above its token, it is not written either (reading: no sentence of
    /// section 8.4 writes an unmarked `local` row for a report).
    #[tokio::test]
    async fn an_unmarked_local_row_is_not_written() {
        for listed in [3, 5] {
            let f = fixture("warm", "local", Some(30_000), false).await;
            let writes = f.writes().await;
            let (_session, start) = f.session(Some(("held", LeaseMode::Local, listed))).await;
            assert_eq!(
                state(&f).await,
                ("local".to_string(), 3, Some(30_000), false, false),
                "listed at {listed}"
            );
            assert_eq!(f.writes().await, writes, "listed at {listed}");
            if listed == 3 {
                assert_local_repeat(&start, 3);
            }
            f.done();
        }
    }

    /// Another node, enrolled as a copy of the fixture's, with no session.
    async fn other_node(f: &Fixture) -> String {
        let other = "nod_01JC9Z8K2M4P6R8T0V2X4Z6B9Q";
        let mut conn = f.state.store.pool().acquire().await.unwrap();
        sqlx::query("CREATE TEMP TABLE copied AS SELECT * FROM nodes WHERE id = ?")
            .bind(&f.node_id)
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE copied SET id = ?, name = 'node-b', cp_status = NULL, subnet = NULL, \
             status = 'disconnected'",
        )
        .bind(other)
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO nodes SELECT * FROM copied")
            .execute(&mut *conn)
            .await
            .unwrap();
        other.to_string()
    }

    /// Test 4, the rows a report never writes: one that names another node;
    /// one revoked, by a revoke or by a declaration of the node lost (in
    /// one transaction, as `declareNodeLost` writes it); one whose lease
    /// has a failed fence, recorded or not (section 8.5); and one above
    /// the holder's token. The row keeps its mode, its token and its mark.
    #[tokio::test]
    async fn a_report_never_writes_a_row_that_does_not_give_the_holder_the_lease() {
        for case in [
            "another node",
            "revoked",
            "declared lost",
            "recorded failed fence",
            "unrecorded failed fence",
            "below the row's token",
        ] {
            let f = fixture("warm", "expiring", Some(15_000), true).await;
            let mut listed = 3;
            match case {
                "another node" => {
                    let other = other_node(&f).await;
                    // Unmarked, so the ADR 0285 raise stays out of the way.
                    sqlx::query("UPDATE leases SET holder_node_id = ?, unsettled = 0 WHERE id = ?")
                        .bind(&other)
                        .bind(LEASE)
                        .execute(f.state.store.pool())
                        .await
                        .unwrap();
                }
                "revoked" => {
                    write_row(
                        &f,
                        "UPDATE leases SET revoked_at = '2026-11-10T10:00:00Z' WHERE id = ?",
                    )
                    .await;
                }
                "declared lost" => {
                    let mut w = f.state.store.begin_write().await.unwrap();
                    leases::bump_tokens(&mut w.tx, &[LEASE.to_string()], &f.node_id, &actor())
                        .await
                        .unwrap();
                    sqlx::query("UPDATE nodes SET lost_at = ? WHERE id = ?")
                        .bind(crate::store::now_rfc3339())
                        .bind(&f.node_id)
                        .execute(&mut *w.tx)
                        .await
                        .unwrap();
                    w.commit().await.unwrap();
                    // Listed at the token the declaration wrote, so that
                    // only the revoke keeps the row from being written; at
                    // the token it held, the row is above it.
                    listed = 4;
                }
                "recorded failed fence" => {
                    let mut w = f.state.store.begin_write().await.unwrap();
                    let failed = crate::fences::Failed {
                        lease_ids: &[LEASE.to_string()],
                        app_ids: &[APP.to_string()],
                        vm_ids: &[],
                    };
                    crate::fences::record_failure(&mut w.tx, &f.node_id, &failed, &actor())
                        .await
                        .unwrap();
                    w.commit().await.unwrap();
                }
                "unrecorded failed fence" => {
                    f.state.hub.note_failure_recorded(&f.node_id, LEASE, false);
                }
                "below the row's token" => listed = 2,
                _ => unreachable!(),
            }
            let before = state(&f).await;
            let writes = f.writes().await;
            let (_session, start) = f.session(Some(("held", LeaseMode::Local, listed))).await;
            assert_eq!(state(&f).await, before, "{case}");
            assert_eq!(f.writes().await, writes, "{case}");
            assert!(
                of_type(&start, "lease_grant").is_empty(),
                "{case}: {start:?}"
            );
            f.done();
        }
    }

    /// Test 4, what is no `local` report (ADR 0285): the holder lists the
    /// lease `expired` or `revoked`, or `expiring`. None clears the mark.
    /// `expired` keeps its own path, the regrant at a higher token, whose
    /// write keeps the mark (section 8.5); `revoked` and `expiring` leave
    /// the row as it was.
    #[tokio::test]
    async fn an_expired_revoked_or_expiring_listing_is_no_local_report() {
        for (listed, mode) in [
            ("expired", LeaseMode::Local),
            ("revoked", LeaseMode::Local),
            ("held", LeaseMode::Expiring),
            ("uncertain", LeaseMode::Expiring),
        ] {
            for policy in ["warm", "none"] {
                let case = format!("{listed} {mode:?}, {policy}");
                let f = fixture(policy, "expiring", Some(15_000), true).await;
                let before = state(&f).await;
                let writes = f.writes().await;
                let (_session, _start) = f.session(Some((listed, mode, 3))).await;
                let after = state(&f).await;
                assert!(after.3, "{case}: the mark stays");
                if listed == "expired" {
                    assert!(after.1 > 3, "{case}: the regrant's token: {after:?}");
                } else {
                    assert_eq!(after, before, "{case}");
                    assert_eq!(f.writes().await, writes, "{case}");
                    // Still `expiring`: the view shows the duration.
                    assert_eq!(shown_duration(&f).await, Some(15_000), "{case}");
                }
                f.done();
            }
        }
    }

    /// Test 4, the session replaced before the write: the write's own
    /// transaction finds the session no longer the node's, and writes
    /// nothing.
    #[tokio::test]
    async fn a_replaced_session_records_no_report() {
        let f = fixture("warm", "expiring", Some(15_000), true).await;
        let hub = &f.state.hub;
        let (generation, _rx, _closed, _changed) = hub.attach(&f.node_id);
        let mut session = Session::new(hub.clone(), f.node_id.clone(), generation);
        session
            .hello_leases
            .insert(LEASE.to_string(), ("held".to_string(), 3, false));
        let (_newer, _rx2, _closed2, _changed2) = hub.attach(&f.node_id);
        let before = state(&f).await;
        let writes = f.writes().await;
        assert!(
            session
                .write_local_report(LEASE, 3)
                .await
                .unwrap()
                .is_none()
        );
        // A refused report lets the session start go on.
        assert!(session.record_local_reports().await);
        assert_eq!(state(&f).await, before);
        assert_eq!(f.writes().await, writes);
        f.done();
    }

    /// Test 5: a primary restart after the write. The row stays `local`,
    /// unmarked, and the restarted primary seeds no start gate for it (a
    /// marked row gets one, whatever its mode, "The mark"); a failover finds
    /// the lease held as a `local` one.
    #[tokio::test]
    async fn a_restart_after_the_write_keeps_the_mark_cleared_and_seeds_no_gate() {
        let f = fixture("warm", "expiring", Some(30_000), true).await;
        let (_session, _start) = f.session(Some(("held", LeaseMode::Local, 3))).await;
        let (after, _clock) = restart(&f).await;
        assert_eq!(after.hub.authority().gate_remaining(LEASE), None);
        let lease = f.row().await;
        assert_eq!((lease.mode.as_str(), lease.unsettled), ("local", false));
        let mut conn = after.store.pool().acquire().await.unwrap();
        let held = crate::failover::held_while_away(&mut conn, after.hub.authority(), &lease)
            .await
            .unwrap()
            .unwrap();
        assert!(held.contains("its lease is local"), "{held}");
        drop(conn);
        f.done();
    }

    /// Every frame queued on the hub's channel of a session.
    fn queued(rx: &mut mpsc::UnboundedReceiver<String>) -> Vec<Value> {
        let mut texts = Vec::new();
        while let Ok(text) = rx.try_recv() {
            texts.push(text);
        }
        frames(texts)
    }

    /// Test 6: a step-4 row ("The holder fenced first" step 4: marked,
    /// unrevoked, its newest write gave it its token) whose grant the hub
    /// holds, and which the holder lists `local` at the row's token. The
    /// session start drops the held grant before the write
    /// (`held_listed`), the write clears the mark, and the session start
    /// sends only the same-token `local` repeat. Neither the start nor a
    /// later attempt at the held grants sends anything else, on the
    /// session's outbox or the hub's channel.
    #[tokio::test]
    async fn the_session_start_sends_only_the_local_repeat_and_no_held_grant() {
        let f = fixture("warm", "expiring", Some(15_000), true).await;
        // The row's first change-log entry: its newest write gave it its
        // token.
        write_row(&f, "UPDATE leases SET granted_at = granted_at WHERE id = ?").await;
        let row = f.row().await;
        let hub = &f.state.hub;
        let mut conn = f.state.store.pool().acquire().await.unwrap();
        assert!(
            leases::is_step4(&mut conn, hub.unrecorded_failures(), &row)
                .await
                .unwrap()
        );
        drop(conn);
        hub.hold_grant(&row);
        assert!(hub.grant_pending(LEASE));

        let (generation, mut rx, _closed, _changed) = hub.attach(&f.node_id);
        let mut session = Session::new(hub.clone(), f.node_id.clone(), generation);
        let mut outbox = Vec::new();
        let text = NodeMessage::Hello(hello(&f.node_id, Some(("held", LeaseMode::Local, 3))))
            .to_frame("msg_hello", None)
            .to_text();
        assert!(!session.handle_text(&text, &mut outbox).await);
        let start = frames(outbox);
        assert_eq!(
            state(&f).await,
            ("local".to_string(), 3, Some(15_000), false, false)
        );
        assert!(!hub.grant_pending(LEASE));
        assert_local_repeat(&start, 3);
        assert!(of_type(&start, "lease_revoke").is_empty(), "{start:?}");

        hub.attempt_held().await;
        let sent = queued(&mut rx);
        assert!(of_type(&sent, "lease_grant").is_empty(), "{sent:?}");
        assert!(of_type(&sent, "lease_revoke").is_empty(), "{sent:?}");
        f.done();
    }
}
