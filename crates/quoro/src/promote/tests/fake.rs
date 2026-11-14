//! A recording `Steps`: every call is an event, in order, and each effect
//! does what the test set. Its followed log, freeze, vote rounds and clock
//! are its own, so the tests run steps 2 to 6 without root, without a voter
//! and without a control plane.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use quoro_control_plane::promotion::{Promotion, Written};
use quoro_control_plane::rebuild::database::{Built, FollowedEntry};
use quoro_node_agent::follow::LogEntry;
use quoro_node_agent::freeze::PromotionReads;
use quoro_node_agent::vote_client::{CheckOutcome, Reply, Round, VoterReply};
use quoro_proto::wire::{SpecVoter, SpecVotes, VoteAnswer, VoteOutcome, VoteRefusal};
use serde_json::{Value, json};

use super::{NODE_ID, OTHER_NODE_ID, hash_of};
use crate::host::Out;
use crate::promote::refusals::Refusal;
use crate::promote::steps::Steps;

/// The third voter, a witness.
pub(super) const WITNESS_ID: &str = "nod_01JC9Z0000000000000000000W";
/// Every voter, this machine first.
pub(super) const VOTERS: [&str; 3] = [NODE_ID, OTHER_NODE_ID, WITNESS_ID];
/// The voter-set version of [`spec_votes`].
pub(super) const VOTERS_VERSION: u64 = 7;
/// The `lease_wait_ms` of every granting answer.
pub(super) const WAIT_MS: u64 = 12_500;
/// The URL `prepare_host` returns.
pub(super) const URL: &str = "https://127.0.0.1:8443";
/// When `freeze` froze the agent.
pub(super) const FROZEN_AT: &str = "2026-11-13T10:02:00.000Z";
/// The token cap, 2^53 - 1.
pub(super) const MAX_TOKEN: i64 = (1 << 53) - 1;

/// What the fake does, and what it was asked.
pub(super) struct Fake {
    /// Every call, in order.
    pub(super) events: RefCell<Vec<&'static str>>,
    /// The followed log `log` returns, and `reads` describes.
    pub(super) log: RefCell<Vec<LogEntry>>,
    /// When the agent was frozen; `None` while it is not.
    pub(super) frozen_at: RefCell<Option<String>>,
    /// Whether `reads` says the log is quarantined. Only the steps read it:
    /// the refusals read the fixture's store.
    pub(super) quarantined: Cell<bool>,
    /// The highest term this machine has seen.
    pub(super) seen: Cell<u64>,
    /// What `check` returns; `None`: settled at one above `seen`, granted
    /// by [`Fake::grant`].
    pub(super) check: RefCell<Option<CheckOutcome>>,
    /// The voters that grant.
    pub(super) grant: RefCell<Vec<&'static str>>,
    /// The call that fails, by its event's name.
    pub(super) fail: Cell<Option<&'static str>>,
    /// The real vote adds an entry to the log: the agent stored one after
    /// the freeze.
    pub(super) grow_during_vote: Cell<bool>,
    /// The term `vote` was asked for.
    pub(super) voted: Cell<Option<u64>>,
    /// The term `build` was asked for.
    pub(super) built: Cell<Option<u64>>,
    /// What `write` was asked to write.
    pub(super) written: RefCell<Option<Promotion>>,
    /// The URL `point_agent` was asked to point at.
    pub(super) pointed: RefCell<Option<String>>,
    /// The time each `sleep` slept.
    pub(super) slept: RefCell<Vec<Duration>>,
    /// The clock: its start and how far the sleeps moved it.
    start: Instant,
    elapsed: Cell<Duration>,
}

impl Fake {
    /// Every voter grants; the log holds two nodes, the other one primary,
    /// and a lease that node holds below the token cap.
    pub(super) fn new() -> Self {
        let fake = Self {
            events: RefCell::new(Vec::new()),
            log: RefCell::new(Vec::new()),
            frozen_at: RefCell::new(None),
            quarantined: Cell::new(false),
            seen: Cell::new(4),
            check: RefCell::new(None),
            grant: RefCell::new(VOTERS.to_vec()),
            fail: Cell::new(None),
            grow_during_vote: Cell::new(false),
            voted: Cell::new(None),
            built: Cell::new(None),
            written: RefCell::new(None),
            pointed: RefCell::new(None),
            slept: RefCell::new(Vec::new()),
            start: Instant::now(),
            elapsed: Cell::new(Duration::ZERO),
        };
        fake.set_log(&default_log(5));
        fake
    }

    /// Makes `changes` the followed log, from `seq` 1.
    pub(super) fn set_log(&self, changes: &[(&str, &str, Value)]) {
        let entries = changes
            .iter()
            .zip(1u64..)
            .map(|((entity_type, entity_id, payload), seq)| {
                entry(seq, entity_type, entity_id, payload)
            })
            .collect();
        *self.log.borrow_mut() = entries;
    }

    /// The names of the events, in order.
    pub(super) fn events(&self) -> Vec<&'static str> {
        self.events.borrow().clone()
    }

    /// Records the call `name`; fails when the test said so.
    fn step(&self, name: &'static str) -> Result<(), String> {
        self.events.borrow_mut().push(name);
        if self.fail.get() == Some(name) {
            return Err(format!("{name} failed"));
        }
        Ok(())
    }

    /// The clock now.
    fn clock(&self) -> Instant {
        self.start + self.elapsed.get()
    }

    /// A round at `term` in which the voters of [`Fake::grant`] grant and
    /// the others refuse with `support_held`, every answer received now.
    pub(super) fn round(&self, term: u64, check: bool) -> Round {
        let received = self.clock();
        let grant = self.grant.borrow();
        let replies: Vec<VoterReply> = VOTERS
            .iter()
            .map(|voter| {
                let granted = grant.contains(voter);
                VoterReply {
                    node_id: (*voter).to_string(),
                    reply: Reply::Answer {
                        answer: VoteAnswer {
                            node_id: (*voter).to_string(),
                            term,
                            vote: if granted {
                                VoteOutcome::Granted
                            } else {
                                VoteOutcome::Refused
                            },
                            reason: (!granted).then_some(VoteRefusal::SupportHeld),
                            retry_after_ms: (!granted).then_some(1_000),
                            lease_wait_ms: granted.then_some(WAIT_MS),
                            voters_version: VOTERS_VERSION,
                            voters: None,
                        },
                        received,
                    },
                }
            })
            .collect();
        let granted = VOTERS
            .iter()
            .filter(|&&voter| grant.contains(&voter))
            .count();
        Round {
            term,
            check,
            voters_version: VOTERS_VERSION,
            replies,
            voters: VOTERS.len(),
            granted,
            majority: granted * 2 > VOTERS.len(),
            highest_term: Some(term),
            wait_until: (granted > 0).then(|| received + Duration::from_millis(WAIT_MS)),
        }
    }
}

/// The default followed log: the other node, which holds the primary
/// status, this machine's node, and a lease the other node holds at
/// `token`.
pub(super) fn default_log(token: i64) -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "node",
            OTHER_NODE_ID,
            json!({"id": OTHER_NODE_ID, "cp_status": "primary", "status": "online"}),
        ),
        ("node", NODE_ID, json!({"id": NODE_ID, "status": "online"})),
        (
            "lease",
            "lse_1",
            json!({"id": "lse_1", "holder_node_id": OTHER_NODE_ID, "fencing_token": token}),
        ),
    ]
}

/// A followed entry at `seq`, chained on fake hashes.
fn entry(seq: u64, entity_type: &str, entity_id: &str, payload: &Value) -> LogEntry {
    LogEntry {
        seq,
        term: 1,
        time: "2026-11-13T10:00:00.000Z".to_string(),
        actor_type: "system".to_string(),
        actor_id: "test".to_string(),
        entity_type: entity_type.to_string(),
        entity_id: entity_id.to_string(),
        op: "create".to_string(),
        payload: payload.to_string(),
        prev_hash: hash_of(seq - 1),
        hash: hash_of(seq),
    }
}

/// The voter set: every voter of [`VOTERS`], votes open.
fn spec_votes() -> SpecVotes {
    SpecVotes {
        version: VOTERS_VERSION,
        port: 7443,
        voters: VOTERS
            .iter()
            .zip(1u8..)
            .map(|(voter, n)| SpecVoter {
                node_id: (*voter).to_string(),
                roles: vec![
                    if *voter == WITNESS_ID {
                        "witness"
                    } else {
                        "node"
                    }
                    .to_string(),
                ],
                mesh_address: Some(format!("10.99.0.{n}")),
            })
            .collect(),
        listen: Some(true),
    }
}

impl Steps for &Fake {
    type Keys = ();
    type Lock = ();

    fn check_host(&self) -> Result<(), String> {
        self.step("check_host")
    }

    async fn log(&self) -> Result<Vec<LogEntry>, String> {
        self.step("log")?;
        Ok(self.log.borrow().clone())
    }

    async fn reads(&self) -> Result<PromotionReads, String> {
        self.step("reads")?;
        let log = self.log.borrow();
        let (log_seq, log_hash) = log
            .last()
            .map_or((0, hash_of(0)), |last| (last.seq, last.hash.clone()));
        Ok(PromotionReads {
            log_seq,
            log_hash,
            head: None,
            mark: None,
            below_mark: false,
            refollowing: None,
            quarantined: self.quarantined.get(),
            welcome_schema_version: None,
            frozen_at: self.frozen_at.borrow().clone(),
        })
    }

    async fn open_keys(&self, _entries: &[LogEntry]) -> Result<(), Vec<String>> {
        self.step("open_keys").map_err(|e| vec![e])
    }

    async fn check_log(&self, _entries: &[FollowedEntry]) -> Result<(), String> {
        self.step("check_log")
    }

    async fn votes(&self) -> Result<SpecVotes, String> {
        self.step("votes")?;
        Ok(spec_votes())
    }

    async fn seen(&self) -> Result<u64, String> {
        self.step("seen")?;
        Ok(self.seen.get())
    }

    async fn check(&self, votes: &SpecVotes, seen: u64) -> Result<CheckOutcome, String> {
        self.step("check")?;
        assert_eq!(*votes, spec_votes());
        let set = self.check.borrow().clone();
        Ok(set.unwrap_or_else(|| CheckOutcome::Settled(self.round(seen + 1, true))))
    }

    fn lock(&self) -> Result<(), Refusal> {
        self.step("lock").map_err(|_| Refusal::AnotherPromotion)
    }

    async fn freeze(&self) -> Result<(), String> {
        self.step("freeze")?;
        self.frozen_at
            .borrow_mut()
            .get_or_insert_with(|| FROZEN_AT.to_string());
        Ok(())
    }

    async fn vote(&self, votes: &SpecVotes, term: u64) -> Result<Round, String> {
        self.step("vote")?;
        assert_eq!(*votes, spec_votes());
        assert!(
            self.frozen_at.borrow().is_some(),
            "a real vote before the freeze"
        );
        self.voted.set(Some(term));
        if self.grow_during_vote.get() {
            let mut log = self.log.borrow_mut();
            let seq = u64::try_from(log.len()).unwrap() + 1;
            log.push(entry(seq, "project", "prj_9", &json!({"id": "prj_9"})));
        }
        Ok(self.round(term, false))
    }

    fn now(&self) -> Instant {
        self.clock()
    }

    async fn sleep(&self, duration: Duration) {
        self.events.borrow_mut().push("sleep");
        self.slept.borrow_mut().push(duration);
        self.elapsed.set(self.elapsed.get() + duration);
    }

    async fn build(&self, entries: &[FollowedEntry], term: u64) -> Result<Built, String> {
        self.step("build")?;
        self.built.set(Some(term));
        Ok(Built {
            head_seq: entries.last().map_or(0, |last| last.seq),
            rows: BTreeMap::new(),
            schema_version: 1,
            webhook_cursor: 0,
            cascaded: Vec::new(),
            mismatches: Vec::new(),
            dropped: Vec::new(),
        })
    }

    async fn write(&self, promotion: &Promotion) -> Result<Written, String> {
        self.step("write")?;
        *self.written.borrow_mut() = Some(promotion.clone());
        Ok(Written {
            first_seq: 4,
            last_seq: 6,
            audit_seq: 1,
            from_node_id: Some(OTHER_NODE_ID.to_string()),
        })
    }

    fn install_keys(&self, _keys: &(), _out: &mut Out) -> Result<(), String> {
        self.step("install_keys")
    }

    async fn prepare_host(&self, _out: &mut Out) -> Result<String, String> {
        self.step("prepare_host")?;
        Ok(URL.to_string())
    }

    async fn unfreeze(&self) -> Result<(), String> {
        self.step("unfreeze")?;
        *self.frozen_at.borrow_mut() = None;
        Ok(())
    }

    async fn point_agent(&self, url: &str, _out: &mut Out) -> Result<(), String> {
        self.step("point_agent")?;
        *self.pointed.borrow_mut() = Some(url.to_string());
        Ok(())
    }
}
