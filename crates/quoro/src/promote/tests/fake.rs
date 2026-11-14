//! A recording `Steps`: every call is an event, in order, and each effect
//! does what the test set. Its followed log, freeze, vote rounds and clock
//! are its own, so the tests run steps 2 to 6 without root, without a voter
//! and without a control plane.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use quoro_control_plane::promotion::{Promotion, Written};
use quoro_control_plane::rebuild::FoldError;
use quoro_control_plane::rebuild::database::{BuildError, Built, CheckError, FollowedEntry};
use quoro_node_agent::follow::LogEntry;
use quoro_node_agent::freeze::PromotionReads;
use quoro_node_agent::vote::StoredVote;
use quoro_node_agent::vote_client::{CheckOutcome, NoAnswer, Reply, Round, VoterReply};
use quoro_proto::wire::{SpecVoter, SpecVotes, VoteAnswer, VoteOutcome, VoteRefusal};
use serde_json::{Value, json};

use super::{NODE_ID, OTHER_NODE_ID, hash_of};
use crate::host::Out;
use crate::promote::prepare::{self, Accounts};
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
    /// `check_log` fails as a check that could not run, not as a log that
    /// does not fold.
    pub(super) check_unrun: Cell<bool>,
    /// The call, by its event's name, during which the agent stores the
    /// entries after the log, from the next `seq`.
    pub(super) grow: RefCell<Option<(&'static str, Changes)>>,
    /// The host `check_host` runs `prepare::check_host` on; `None`: it
    /// passes.
    pub(super) host: RefCell<Option<HostCheck>>,
    /// The term `vote` was asked for.
    pub(super) voted: Cell<Option<u64>>,
    /// The vote this machine's agent stored: the real round's term and this
    /// machine, when it is among the voters that grant.
    pub(super) stored_vote: RefCell<Option<StoredVote>>,
    /// This machine's answer to the real round is lost after its agent
    /// stored its vote.
    pub(super) lose_own_answer: Cell<bool>,
    /// The last `seq` of the entries each `check_log` received, in order.
    pub(super) checked: RefCell<Vec<u64>>,
    /// The term `build` was asked for.
    pub(super) built: Cell<Option<u64>>,
    /// The last `seq` of the entries `build` received.
    pub(super) built_seq: Cell<Option<u64>>,
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
            check_unrun: Cell::new(false),
            grow: RefCell::new(None),
            host: RefCell::new(None),
            voted: Cell::new(None),
            stored_vote: RefCell::new(None),
            lose_own_answer: Cell::new(false),
            checked: RefCell::new(Vec::new()),
            built: Cell::new(None),
            built_seq: Cell::new(None),
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
        self.log.borrow_mut().clear();
        self.store(changes);
    }

    /// Stores `changes` after the followed log, as the agent stores a batch.
    fn store(&self, changes: &[(&str, &str, Value)]) {
        let mut log = self.log.borrow_mut();
        let next = log.last().map_or(1, |last| last.seq + 1);
        log.extend(
            changes
                .iter()
                .zip(next..)
                .map(|((entity_type, entity_id, payload), seq)| {
                    entry(seq, entity_type, entity_id, payload)
                }),
        );
    }

    /// The names of the events, in order.
    pub(super) fn events(&self) -> Vec<&'static str> {
        self.events.borrow().clone()
    }

    /// Records the call `name`, during which the log grows when the test
    /// said so; fails when the test said so.
    fn step(&self, name: &'static str) -> Result<(), String> {
        self.events.borrow_mut().push(name);
        let grows = matches!(&*self.grow.borrow(), Some((during, _)) if *during == name);
        if grows && let Some((_, changes)) = self.grow.borrow_mut().take() {
            self.store(&changes);
        }
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
    /// the others refuse with `support_held`, every answer received now. A
    /// `check` answer carries the voter's current term, one below `term`,
    /// as in a check round that settled; a real answer carries `term`. With
    /// [`Fake::lose_own_answer`], this machine's real answer is lost.
    pub(super) fn round(&self, term: u64, check: bool) -> Round {
        let received = self.clock();
        let grant = self.grant.borrow();
        let answered = if check { term - 1 } else { term };
        let replies: Vec<VoterReply> = VOTERS
            .iter()
            .map(|voter| {
                let granted = grant.contains(voter);
                if !check && *voter == NODE_ID && self.lose_own_answer.get() {
                    return VoterReply {
                        node_id: (*voter).to_string(),
                        reply: Reply::NoAnswer(NoAnswer::Timeout(
                            "the answer did not arrive in time".to_string(),
                        )),
                    };
                }
                VoterReply {
                    node_id: (*voter).to_string(),
                    reply: Reply::Answer {
                        answer: VoteAnswer {
                            node_id: (*voter).to_string(),
                            term: answered,
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
        let granted = replies
            .iter()
            .filter(|reply| {
                matches!(&reply.reply, Reply::Answer { answer, .. } if answer.vote == VoteOutcome::Granted)
            })
            .count();
        Round {
            term,
            check,
            voters_version: VOTERS_VERSION,
            replies,
            voters: VOTERS.len(),
            granted,
            majority: granted * 2 > VOTERS.len(),
            highest_term: Some(answered),
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

/// Changes the agent stores, as [`Fake::set_log`] takes them.
pub(super) type Changes = Vec<(&'static str, &'static str, Value)>;

/// A node host under a directory of the fixture's, for `check_host`:
/// `etc/quoro/quoro.toml` with a `[node]` table and `var/lib/quoro` without
/// the database or the keys, which the promotion writes after its votes,
/// and the account database `quoro-api` is looked up in.
pub(super) struct HostCheck {
    pub(super) root: PathBuf,
    /// `quoro-api`'s uid and gid; `None`: no such user or group.
    pub(super) uid: Cell<Option<u32>>,
    pub(super) gid: Cell<Option<u32>>,
}

impl HostCheck {
    pub(super) fn new(root: &Path) -> Self {
        for dir in ["etc/quoro", "var/lib/quoro/engine"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        fs::write(
            root.join("etc/quoro/quoro.toml"),
            "engine = \"cloud-hypervisor\"\n\n[node]\n",
        )
        .unwrap();
        Self {
            root: root.to_path_buf(),
            uid: Cell::new(None),
            gid: Cell::new(None),
        }
    }

    /// `etc/quoro`, the configuration directory.
    pub(super) fn etc(&self) -> PathBuf {
        self.root.join("etc/quoro")
    }
}

impl Accounts for HostCheck {
    fn uid(&self, _name: &str) -> Result<Option<u32>, String> {
        Ok(self.uid.get())
    }

    fn gid(&self, _name: &str) -> Result<Option<u32>, String> {
        Ok(self.gid.get())
    }

    fn create_system_user(&self, name: &str, _home: &Path, _shell: &Path) -> Result<(), String> {
        panic!("check_host created the user {name}");
    }
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
        self.step("check_host")?;
        match &*self.host.borrow() {
            Some(host) => {
                let options = prepare::Options {
                    root: host.root.clone(),
                    ..prepare::Options::host(PathBuf::from(prepare::CONFIG_PATH))
                };
                prepare::check_host(&options, host)
            }
            None => Ok(()),
        }
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

    async fn check_log(&self, entries: &[FollowedEntry]) -> Result<(), CheckError> {
        self.checked
            .borrow_mut()
            .push(entries.last().map_or(0, |last| last.seq));
        if self.check_unrun.get() {
            self.events.borrow_mut().push("check_log");
            return Err(CheckError::Schema(BuildError::Io(std::io::Error::other(
                "out of memory",
            ))));
        }
        self.step("check_log").map_err(|_| {
            CheckError::Log(BuildError::Fold(FoldError::NoCreate {
                entity_type: "user".to_string(),
                entity_id: "usr_9".to_string(),
                first_seq: 2,
            }))
        })
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
        if self.grant.borrow().contains(&NODE_ID) {
            *self.stored_vote.borrow_mut() = Some(StoredVote {
                term,
                candidate: NODE_ID.to_string(),
            });
        }
        Ok(self.round(term, false))
    }

    async fn own_vote(&self) -> Result<Option<StoredVote>, String> {
        self.step("own_vote")?;
        Ok(self.stored_vote.borrow().clone())
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
        self.built_seq.set(entries.last().map(|last| last.seq));
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
