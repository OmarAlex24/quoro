// SPDX-License-Identifier: AGPL-3.0-only
//! Change-log follow, the primary's side (`node-agent-protocol.md` sections
//! 5.6, 5.14 and 7.1; ADR 0179, ADR 0227, ADR 0261).
//!
//! Every machine with the `node` role follows the change log, the primary's
//! own host included; edge-only and witness-only machines do not. Follow
//! starts once the session's first `desired_state` has been sent (section 4.2
//! rule 6). The primary first checks the `hello` position against its own
//! log and against the newest acknowledgement it holds for the node, and
//! answers `log_diverged` when the position differs from its log, is above
//! its newest `seq`, or is below that acknowledgement (section 7.1, "On
//! `hello`"). It then sends `log_entries` from the node's position: at most
//! 500 entries per message, within the 1 MiB message limit (section 3.2),
//! each message with `head_seq`, and never more than one message
//! unacknowledged. The next one goes out when the `log_ack` of the previous
//! one arrives, or when the store's head watch
//! ([`crate::store::Store::change_log_head`]) rises while nothing is
//! outstanding.
//!
//! The node's newest acknowledged `seq` is kept in memory in the hub: for
//! its live session ([`super::NodeHub::log_seq`]), and per node across
//! sessions ([`super::NodeHub::newest_log_ack`]), which each session's start
//! raises to `nodes.log_seq` as that start found it. `nodes.log_seq` is
//! written only with the writes a session's start and end already make, and
//! with the 0 the primary writes when it answers `log_diverged` (section
//! 7.1, "Lag", ADR 0261). A session whose follow never reached its check
//! writes back at its end the acknowledgement its start write replaced, so a
//! `hello` that was never checked cannot lower it
//! ([`super::NodeHub::end_log_seq`]).
//!
//! Every frame goes through the session's queue, the channel its
//! `desired_state` takes, so `log_diverged` and `log_entries` always follow
//! that `desired_state` on the wire.

use super::{Session, has_role, new_msg_id};
use crate::changelog::{self, StoredEntry};
use crate::store::{GENESIS_HASH, Store};
use quoro_proto::wire::{self, ControlMessage};
use serde_json::Value;
use tokio::time::{Duration, Instant};

/// Most entries one `log_entries` carries (section 5.14).
pub(super) const MAX_LOG_ENTRIES: usize = 500;

/// Bytes a `log_entries` frame leaves free below the 1 MiB message limit
/// (section 3.2).
const FRAME_MARGIN: usize = 16 * 1024;

/// The largest `log_entries` frame the primary builds.
const FRAME_BUDGET: usize = wire::MAX_MESSAGE_BYTES - FRAME_MARGIN;

/// How long follow waits before it sends again after the node refused a
/// `log_entries` with `bad_message`, so an entry the node cannot verify does
/// not cause a resend storm; also how long it waits after a failed read.
pub(super) const LOG_RETRY_DELAY: Duration = Duration::from_secs(3);

/// Where one session stands in following the change log.
#[derive(Debug)]
pub(super) enum Follow {
    /// No `hello` yet, a machine without the `node` role, or follow stopped
    /// on an entry the primary cannot send.
    Off,
    /// The `hello` position, until the session's first `desired_state` has
    /// been sent, with `nodes.log_seq` as it stood before the session's
    /// start write replaced it ([`super::connect_node`]): the stored half of
    /// the check for a log that went back (section 7.1, "On `hello`", ADR
    /// 0261).
    Waiting {
        log_seq: u64,
        log_hash: String,
        stored_log_seq: u64,
    },
    /// Following from `position`, the newest acknowledged `seq` (or the
    /// seed), with at most one `log_entries` unacknowledged.
    Active {
        position: u64,
        outstanding: Option<Outstanding>,
    },
}

/// The `log_entries` the node has not acknowledged yet.
#[derive(Debug)]
pub(super) struct Outstanding {
    msg_id: String,
    last_seq: u64,
    last_hash: String,
}

/// What the next `log_entries` holds.
#[derive(Debug)]
pub(super) enum Batch {
    /// Nothing after the position.
    Empty,
    /// Entries in `seq` order, with the `seq` and `hash` of the last one.
    Entries {
        entries: Vec<Value>,
        last_seq: u64,
        last_hash: String,
    },
    /// The first entry after the position cannot be sent: it does not fit in
    /// a message alone, or the node could not verify it.
    Blocked { seq: u64, reason: String },
}

/// Builds the next batch from `rows`, read in `seq` order after the
/// position: at most [`MAX_LOG_ENTRIES`] entries whose serialized forms, with
/// the commas between them, fit in `room` bytes. An entry that does not fit,
/// or that has no RFC 8785 form ([`StoredEntry::wire_entry`]), ends the batch
/// before it; when it is the first, the batch is blocked on it.
pub(super) fn build_batch(rows: &[StoredEntry], room: usize) -> Batch {
    let mut entries = Vec::new();
    let mut used = 0usize;
    let mut last: Option<&StoredEntry> = None;
    for row in rows.iter().take(MAX_LOG_ENTRIES) {
        let seq = u64::try_from(row.seq).unwrap_or(0);
        let entry = match row.wire_entry() {
            Ok(entry) => entry,
            Err(error) if entries.is_empty() => {
                return Batch::Blocked {
                    seq,
                    reason: format!("the node could not verify it: {error}"),
                };
            }
            Err(_) => break,
        };
        let size = serde_json::to_string(&entry).map_or(usize::MAX, |text| text.len())
            + usize::from(!entries.is_empty());
        if used.saturating_add(size) > room {
            if entries.is_empty() {
                return Batch::Blocked {
                    seq,
                    reason: format!(
                        "it takes {size} bytes alone, and a log_entries message has room for {room}"
                    ),
                };
            }
            break;
        }
        used += size;
        entries.push(entry);
        last = Some(row);
    }
    match last {
        Some(last) => Batch::Entries {
            entries,
            last_seq: u64::try_from(last.seq).unwrap_or(0),
            last_hash: last.hash.clone(),
        },
        None => Batch::Empty,
    }
}

/// The `log_entries` frame (section 5.14).
fn log_entries_frame(msg_id: &str, from_seq: u64, head_seq: u64, entries: Vec<Value>) -> String {
    ControlMessage::LogEntries(wire::LogEntries {
        from_seq,
        entries,
        head_seq: Some(head_seq),
    })
    .to_frame(msg_id, None)
    .to_text()
}

/// Whether a `hello` position differs from the primary's log (section 7.1,
/// "On `hello`"): `log_seq` 0 must come with 64 zeros, and any other
/// `log_seq` must name an entry the primary holds, with its `hash`. A
/// `log_seq` above the primary's newest `seq` names no entry, so it differs.
async fn diverged(store: &Store, log_seq: u64, log_hash: &str) -> Result<bool, sqlx::Error> {
    if log_seq == 0 {
        return Ok(log_hash != GENESIS_HASH);
    }
    let mut conn = store.pool().acquire().await?;
    let held = changelog::hash_at(&mut conn, i64::try_from(log_seq).unwrap_or(i64::MAX)).await?;
    Ok(held.as_deref() != Some(log_hash))
}

impl Session {
    /// Queues one follow frame on this session, behind its `desired_state`.
    /// Returns whether the session took it.
    fn queue_follow(&self, text: String) -> bool {
        self.hub.0.send_text(&self.node_id, self.generation, text)
    }

    /// Waits [`LOG_RETRY_DELAY`] before follow goes on.
    fn retry_follow_later(&mut self) {
        self.follow_retry = Some(Instant::now() + LOG_RETRY_DELAY);
    }

    /// Whether the head watch may send: following, with nothing outstanding
    /// and no retry waiting.
    pub(super) fn follow_idle(&self) -> bool {
        self.follow_retry.is_none()
            && matches!(
                self.follow,
                Follow::Active {
                    outstanding: None,
                    ..
                }
            )
    }

    /// Starts follow once the session's first `desired_state` has been sent
    /// (section 4.2 rule 6, section 7.1). A machine without the `node` role
    /// does not follow.
    ///
    /// The `hello` position draws `error` `log_diverged`, non-fatal, once and
    /// before any `log_entries`, when it is below the newest acknowledgement
    /// the primary holds for the node, in memory
    /// ([`super::NodeHub::newest_log_ack`]) or in `nodes.log_seq` as it stood
    /// before this session's start write (a log that went back, ADR 0261),
    /// or when it differs from the primary's log or is above its newest
    /// `seq` (section 7.1, "On `hello`", ADR 0227). The start write raised
    /// the memory to that stored value already, so for a session still
    /// current here the memory covers it; the stored value stays in the
    /// comparison as a second guard. The node then follows
    /// from `seq` 1. In the same step the primary first commits
    /// `nodes.log_seq = 0`, then sets its in-memory acknowledgement to 0, and
    /// only then queues the error, so `log_diverged` never goes out without
    /// the 0 written and the node's next `hello` does not read as a log that
    /// went back (ADR 0261). A failed write sends nothing and tries again
    /// after [`LOG_RETRY_DELAY`], as a failed read does.
    ///
    /// Otherwise the node follows from the `hello` position, which seeds the
    /// hub's in-memory position and acknowledgement (section 8.4,
    /// "Acknowledgements").
    pub(super) async fn start_follow(&mut self) {
        if self.follow_retry.is_some() {
            return;
        }
        let Follow::Waiting {
            log_seq,
            log_hash,
            stored_log_seq,
        } = &self.follow
        else {
            return;
        };
        let (log_seq, log_hash, stored_log_seq) = (*log_seq, log_hash.clone(), *stored_log_seq);
        match has_role(&self.hub, &self.node_id, "node").await {
            Ok(true) => {}
            Ok(false) => {
                self.follow = Follow::Off;
                return;
            }
            Err(e) => {
                tracing::warn!(%e, node_id = %self.node_id, "cannot read whether the node follows the change log; trying again");
                self.retry_follow_later();
                return;
            }
        }
        let newest_ack = self.hub.newest_log_ack(&self.node_id).max(stored_log_seq);
        let reason = if log_seq < newest_ack {
            tracing::warn!(
                node_id = %self.node_id,
                log_seq,
                newest_ack,
                "the node's change log is below its newest acknowledgement, so it went back; it follows again from seq 1"
            );
            Some(format!(
                "the change log at seq {log_seq} is below seq {newest_ack}, the newest the primary holds as acknowledged; follow again from seq 1"
            ))
        } else {
            match diverged(&self.hub.0.store, log_seq, &log_hash).await {
                Ok(false) => None,
                Ok(true) => {
                    tracing::warn!(
                        node_id = %self.node_id,
                        log_seq,
                        "the node's change log differs from the primary's; it follows again from seq 1"
                    );
                    Some(format!(
                        "the change log at seq {log_seq} does not match the primary's; follow again from seq 1"
                    ))
                }
                Err(e) => {
                    tracing::warn!(%e, node_id = %self.node_id, "cannot compare the node's change log with the primary's; trying again");
                    self.retry_follow_later();
                    return;
                }
            }
        };
        let position = if let Some(message) = reason {
            match super::reset_log_seq(&self.hub, &self.node_id, self.generation).await {
                Ok(true) => {}
                // A newer session of the node took over; it runs its own
                // check.
                Ok(false) => {
                    self.follow = Follow::Off;
                    return;
                }
                Err(e) => {
                    tracing::warn!(%e, node_id = %self.node_id, "cannot write nodes.log_seq = 0 before log_diverged; nothing is sent, trying again");
                    self.retry_follow_later();
                    return;
                }
            }
            self.hub
                .set_log_position(&self.node_id, self.generation, 0, false);
            let text = ControlMessage::Error(quoro_proto::ProtocolError {
                code: quoro_proto::ErrorCode::LogDiverged,
                message,
                fatal: false,
                term: None,
            })
            .to_frame(&new_msg_id(), None)
            .to_text();
            if !self.queue_follow(text) {
                return;
            }
            0
        } else {
            self.hub
                .set_log_position(&self.node_id, self.generation, log_seq, false);
            log_seq
        };
        self.follow = Follow::Active {
            position,
            outstanding: None,
        };
        self.send_log_entries().await;
    }

    /// Sends the next `log_entries` from the position + 1 (section 5.14),
    /// when following with nothing outstanding and no retry waiting, and the
    /// log holds more entries. An entry that cannot be sent stops follow on
    /// this session: the contract has no rule for it, and sending it would
    /// only draw `bad_message` again and again.
    pub(super) async fn send_log_entries(&mut self) {
        if self.follow_retry.is_some() {
            return;
        }
        let Follow::Active {
            position,
            outstanding: None,
        } = &self.follow
        else {
            return;
        };
        let position = *position;
        let read = async {
            let mut conn = self.hub.0.store.pool().acquire().await?;
            changelog::read_after(
                &mut conn,
                i64::try_from(position).unwrap_or(i64::MAX),
                MAX_LOG_ENTRIES,
            )
            .await
        }
        .await;
        let (head, rows) = match read {
            Ok(read) => read,
            Err(e) => {
                tracing::warn!(%e, node_id = %self.node_id, "cannot read the change log to send; trying again");
                self.retry_follow_later();
                return;
            }
        };
        let from_seq = position + 1;
        let head_seq = u64::try_from(head).unwrap_or(0);
        let msg_id = new_msg_id();
        let overhead = log_entries_frame(&msg_id, from_seq, head_seq, Vec::new()).len();
        match build_batch(&rows, FRAME_BUDGET.saturating_sub(overhead)) {
            Batch::Empty => {}
            Batch::Blocked { seq, reason } => {
                tracing::error!(
                    node_id = %self.node_id,
                    seq,
                    %reason,
                    "a change-log entry cannot be sent to the node; follow stops on this session"
                );
                self.follow = Follow::Off;
            }
            Batch::Entries {
                entries,
                last_seq,
                last_hash,
            } => {
                let text = log_entries_frame(&msg_id, from_seq, head_seq, entries);
                if self.queue_follow(text) {
                    self.follow = Follow::Active {
                        position,
                        outstanding: Some(Outstanding {
                            msg_id,
                            last_seq,
                            last_hash,
                        }),
                    };
                }
            }
        }
    }

    /// A `log_ack` (section 5.6). One whose `seq` and `hash` are those of the
    /// last entry of the outstanding `log_entries` moves the position there,
    /// in the session and in the hub, and lets the next message go out. Any
    /// other is ignored: the node verifies every entry in order, so an ack
    /// that skips or does not match proves nothing.
    pub(super) async fn on_log_ack(&mut self, ack: &wire::LogAck) {
        let Follow::Active {
            position,
            outstanding,
        } = &mut self.follow
        else {
            tracing::debug!(node_id = %self.node_id, seq = ack.seq, "log_ack without change-log follow; ignored");
            return;
        };
        let acknowledges = outstanding
            .as_ref()
            .is_some_and(|sent| sent.last_seq == ack.seq && sent.last_hash == ack.hash);
        if !acknowledges {
            tracing::debug!(
                node_id = %self.node_id,
                seq = ack.seq,
                "log_ack that does not acknowledge the outstanding log_entries; ignored"
            );
            return;
        }
        *position = ack.seq;
        *outstanding = None;
        self.hub
            .set_log_position(&self.node_id, self.generation, ack.seq, true);
        self.send_log_entries().await;
    }

    /// A node's `error` that answers message `re`. `bad_message` for the
    /// outstanding `log_entries` means its entries did not verify on the node
    /// (section 7.1, "Verification"): the message is no longer outstanding,
    /// and the entries go again from the node's last ack, after
    /// [`LOG_RETRY_DELAY`]. Returns whether the error answered that message.
    pub(super) fn on_log_refused(&mut self, re: &str, code: quoro_proto::ErrorCode) -> bool {
        if code != quoro_proto::ErrorCode::BadMessage {
            return false;
        }
        let Follow::Active {
            position,
            outstanding,
        } = &mut self.follow
        else {
            return false;
        };
        if !outstanding.as_ref().is_some_and(|sent| sent.msg_id == re) {
            return false;
        }
        *outstanding = None;
        tracing::warn!(
            node_id = %self.node_id,
            from_seq = *position + 1,
            delay_s = LOG_RETRY_DELAY.as_secs(),
            "the node refused log_entries; they are sent again from its last ack after a delay"
        );
        self.retry_follow_later();
        true
    }

    /// The retry delay ran out: follow starts, or the next `log_entries`
    /// goes out.
    pub(super) async fn follow_retry_due(&mut self) {
        self.follow_retry = None;
        if matches!(self.follow, Follow::Waiting { .. }) {
            self.start_follow().await;
        } else {
            self.send_log_entries().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(seq: i64, payload: &str) -> StoredEntry {
        StoredEntry {
            seq,
            term: 1,
            time: "2026-11-02T10:15:31.000Z".to_string(),
            actor_type: "system".to_string(),
            actor_id: "test".to_string(),
            entity_type: "vm".to_string(),
            entity_id: format!("vm_{seq}"),
            op: "update".to_string(),
            payload: payload.to_string(),
            prev_hash: "a".repeat(64),
            hash: format!("{seq:064x}"),
        }
    }

    /// The room a frame with these numbers leaves for its entries.
    fn room(from_seq: u64, head_seq: u64) -> usize {
        FRAME_BUDGET - log_entries_frame(&new_msg_id(), from_seq, head_seq, Vec::new()).len()
    }

    fn frame_of(batch: Batch, from_seq: u64, head_seq: u64) -> (usize, String) {
        let Batch::Entries { entries, .. } = batch else {
            panic!("not a batch of entries: {batch:?}");
        };
        let count = entries.len();
        (
            count,
            log_entries_frame(&new_msg_id(), from_seq, head_seq, entries),
        )
    }

    #[test]
    fn a_batch_holds_at_most_500_entries() {
        let rows: Vec<StoredEntry> = (1..=600).map(|seq| row(seq, "{}")).collect();
        let batch = build_batch(&rows, room(1, 600));
        let Batch::Entries {
            entries,
            last_seq,
            last_hash,
        } = batch
        else {
            panic!("not a batch of entries: {batch:?}");
        };
        assert_eq!(entries.len(), MAX_LOG_ENTRIES);
        assert_eq!(entries[0]["seq"], 1);
        assert_eq!((last_seq, last_hash), (500, format!("{:064x}", 500)));
        assert!(matches!(build_batch(&[], room(1, 0)), Batch::Empty));
    }

    #[test]
    fn a_batch_stops_before_the_frame_would_pass_the_limit() {
        // 100 KiB payloads: about ten fit in 1 MiB.
        let payload = format!("{{\"blob\":\"{}\"}}", "x".repeat(100 * 1024));
        let rows: Vec<StoredEntry> = (1..=20).map(|seq| row(seq, &payload)).collect();
        let (count, text) = frame_of(build_batch(&rows, room(1, 20)), 1, 20);
        assert!((1..20).contains(&count), "{count} entries");
        assert!(text.len() <= FRAME_BUDGET, "{} bytes", text.len());
        assert!(text.len() < wire::MAX_MESSAGE_BYTES);
        // It stopped only because the next entry would not fit.
        let next = serde_json::to_string(&rows[count].wire_entry().unwrap())
            .unwrap()
            .len();
        assert!(text.len() + 1 + next > FRAME_BUDGET);
        // The frame parses back with every entry it was built with.
        let raw = wire::RawFrame::parse(&text).unwrap();
        let ControlMessage::LogEntries(body) = ControlMessage::from_raw(&raw).unwrap() else {
            panic!("not log_entries");
        };
        assert_eq!(body.entries.len(), count);
        assert_eq!(body.head_seq, Some(20));
    }

    #[test]
    fn an_entry_too_large_alone_blocks_the_batch() {
        let huge = format!("{{\"blob\":\"{}\"}}", "x".repeat(wire::MAX_MESSAGE_BYTES));
        let rows = [row(7, &huge)];
        let batch = build_batch(&rows, room(7, 7));
        assert!(matches!(batch, Batch::Blocked { seq: 7, .. }), "{batch:?}");
        // After a smaller entry, the batch ends before it; the next batch is
        // blocked on it.
        let rows = [row(6, "{}"), row(7, &huge)];
        let (count, _) = frame_of(build_batch(&rows, room(6, 7)), 6, 7);
        assert_eq!(count, 1);
    }

    #[test]
    fn an_entry_that_does_not_canonicalize_blocks_the_batch() {
        let rows = [row(3, "{\"n\":9007199254740993}")];
        let batch = build_batch(&rows, room(3, 3));
        assert!(matches!(batch, Batch::Blocked { seq: 3, .. }), "{batch:?}");
        let rows = [row(2, "{\"n\":1}"), row(3, "{\"n\":9007199254740993}")];
        let (count, _) = frame_of(build_batch(&rows, room(2, 3)), 2, 3);
        assert_eq!(count, 1);
    }
}
