// SPDX-License-Identifier: AGPL-3.0-only
//! The voter set and its version (`node-agent-protocol.md` sections 6.12 and
//! 8.10, ADR 0170).
//!
//! - **The set** is every `nodes` row with the `node` or `witness` role that
//!   is not `removed`, connected or not, oldest enrolled first, the order of
//!   `Cluster.votes` (section 6.12). One machine has one vote whatever its
//!   roles; an edge-only machine has none.
//! - **The version** is the change-log `seq` of the entry that last changed
//!   the set: the enrollment of a voter, a change of a node's roles that adds
//!   or removes `node` or `witness` membership, or a removal (section 8.10).
//!   It is derived from the log, never stored (migration 0010): the `node`
//!   entries are replayed in `seq` order, each merged over the node's facts as
//!   ADR 0297 says (a create carries the whole row, an update the fields it
//!   writes, a delete removes the node), and an entry that makes its node join
//!   or leave the set sets the version to its `seq`. A promoted primary
//!   replays the same log and gets the same version.
//!
//! [`VoterLog`] keeps the replay in memory and reads, on each call, only the
//! entries after the last one it applied, in the same read transaction as the
//! rows, so a set is never older than the log it was read with. The newest
//! set read is kept for the support count ([`VoterLog::latest`]).
//!
//! - **The two must agree.** The set comes from the rows and the version from
//!   the log, so a write whose change-log payload leaves out a `roles` or
//!   `status` it changed would give the voters another set at the same
//!   version, which sections 6.12 and 8.10 rule 4 rule out. Each read
//!   compares the members the replay makes voters with the rows' voters.
//!   While they differ, and for the rest of that version once they have
//!   differed, the set is not [`VoterSet::countable`]: no support is counted
//!   at it, and a warning names the machines on each side.

use quoro_proto::wire::{SpecVoter, SpecVotes, SpecWitness};
use serde_json::Value;
use sqlx::{SqliteConnection, SqlitePool};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

/// `spec.votes.port`: the TCP port of the vote listener on a voter's mesh
/// address (section 6.12).
pub const VOTE_PORT: u16 = 51821;

/// Whether `roles` make a machine a voter: the `node` or the `witness` role
/// (section 6.12, ADR 0170).
#[must_use]
pub fn is_voter_roles(roles: &[String]) -> bool {
    roles.iter().any(|role| role == "node" || role == "witness")
}

/// One voter, as `spec.votes.voters` lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Voter {
    pub node_id: String,
    /// The roles as enrolled.
    pub roles: Vec<String>,
    /// The voter's mesh address, `None` while it has none.
    pub mesh_address: Option<String>,
}

/// Where the voters of the `nodes` rows and those of the change-log replay
/// differ ([module docs](self)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Disagreement {
    /// Voters in the rows that the replay does not make voters.
    pub only_in_rows: Vec<String>,
    /// Voters in the replay that the rows do not list as voters.
    pub only_in_log: Vec<String>,
}

/// The voter set and its version, as one read transaction saw them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoterSet {
    /// The newest change-log `seq` of that transaction: a set read with a
    /// higher one is newer.
    pub head: u64,
    /// The voter-set version (section 8.10).
    pub version: u64,
    /// Every voter, oldest enrolled first.
    pub voters: Vec<Voter>,
    /// How the rows and the log differ in this read, `None` when they agree.
    pub disagreement: Option<Disagreement>,
    /// Whether support may be counted at this set: the rows and the log
    /// agree in this read and have agreed in every read at this version
    /// (sections 6.12 and 8.10 rule 4). Once they have differed, a voter may
    /// hold another set at this version, so nothing counts until the version
    /// moves on.
    pub countable: bool,
}

impl VoterSet {
    /// The voter `node_id`, when it is one.
    #[must_use]
    pub fn get(&self, node_id: &str) -> Option<&Voter> {
        self.voters.iter().find(|voter| voter.node_id == node_id)
    }

    /// Whether `node_id` is a voter.
    #[must_use]
    pub fn contains(&self, node_id: &str) -> bool {
        self.get(node_id).is_some()
    }

    /// Whether `other` names the same voters at the same version: what a
    /// machine's `spec.votes` carries, whatever log it was read with.
    #[must_use]
    pub fn same_voters(&self, other: &Self) -> bool {
        self.version == other.version && self.voters == other.voters
    }

    /// `spec.votes` and `spec.witness` for `node_id` (section 6.12):
    /// `spec.votes` on every voter, `spec.witness` on a machine with the
    /// `witness` role, which is always a voter. Neither on any other machine.
    #[must_use]
    pub fn spec_for(&self, node_id: &str) -> (Option<SpecVotes>, Option<SpecWitness>) {
        let Some(voter) = self.get(node_id) else {
            return (None, None);
        };
        let witness = voter
            .roles
            .iter()
            .any(|role| role == "witness")
            .then_some(SpecWitness {});
        let votes = SpecVotes {
            version: self.version,
            port: VOTE_PORT,
            voters: self
                .voters
                .iter()
                .map(|voter| SpecVoter {
                    node_id: voter.node_id.clone(),
                    roles: voter.roles.clone(),
                    mesh_address: voter.mesh_address.clone(),
                })
                .collect(),
        };
        (Some(votes), witness)
    }
}

/// What the replay knows of one node.
#[derive(Clone, Debug, Default)]
struct NodeFacts {
    roles: Vec<String>,
    removed: bool,
    /// The `seq` of the entry that turned the node `removed`, its removal
    /// entry (sections 8.4 and 10), while it is.
    removed_at: Option<u64>,
}

impl NodeFacts {
    fn votes(&self) -> bool {
        !self.removed && is_voter_roles(&self.roles)
    }

    fn has_node_role(&self) -> bool {
        self.roles.iter().any(|role| role == "node")
    }

    /// Merges the fields an entry's payload writes (ADR 0297): `roles` and
    /// `status` are the only ones the voter set reads.
    fn merge(&mut self, payload: &Value) {
        if let Some(roles) = payload.get("roles").and_then(Value::as_array) {
            self.roles = roles
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect();
        }
        if let Some(status) = payload.get("status").and_then(Value::as_str) {
            self.removed = status == "removed";
        }
    }
}

/// The replay of the `node` entries up to `applied`.
#[derive(Clone, Debug, Default)]
struct Replay {
    /// The `seq` of the last `node` entry applied.
    applied: u64,
    nodes: HashMap<String, NodeFacts>,
    /// The `seq` of the entry that last changed the voter set, 0 before any.
    version: u64,
}

impl Replay {
    /// Applies one `node` entry. Only its own node can join or leave the set.
    fn apply(&mut self, seq: u64, op: &str, node_id: &str, payload: &Value) {
        let before = self.nodes.get(node_id).is_some_and(NodeFacts::votes);
        match op {
            "delete" => {
                self.nodes.remove(node_id);
            }
            "create" => {
                let mut facts = NodeFacts::default();
                facts.merge(payload);
                self.nodes.insert(node_id.to_string(), facts);
            }
            _ => self
                .nodes
                .entry(node_id.to_string())
                .or_default()
                .merge(payload),
        }
        if let Some(facts) = self.nodes.get_mut(node_id) {
            if facts.removed {
                facts.removed_at.get_or_insert(seq);
            } else {
                facts.removed_at = None;
            }
        }
        let after = self.nodes.get(node_id).is_some_and(NodeFacts::votes);
        if before != after {
            self.version = seq;
        }
        self.applied = self.applied.max(seq);
    }

    /// The nodes the replay makes voters.
    fn members(&self) -> BTreeSet<&str> {
        self.nodes
            .iter()
            .filter(|(_, facts)| facts.votes())
            .map(|(node_id, _)| node_id.as_str())
            .collect()
    }
}

/// The machines whose acknowledgement a write must have before it is
/// replicated (`node-agent-protocol.md` section 8.4, "When R is
/// replicated"): every machine with the `node` role, from its enrollment
/// entry on, and a removed one until its removal entry is itself replicated
/// over the others. A primary host without the `node` role is not in it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequiredSet {
    /// Every machine with the `node` role that is not removed, in the
    /// `nodes` rows or in the change-log replay: where the two disagree, the
    /// machine counts, which waits longer, never shorter.
    pub members: BTreeSet<String>,
    /// Every removed machine with the `node` role, with the `seq` of its
    /// removal entry as the change log carries it.
    pub removed: BTreeMap<String, u64>,
}

impl RequiredSet {
    /// Whether `seq` is replicated over the set, given each machine's
    /// countable acknowledgement (`acked`): `Err` lists the machines that
    /// have not acknowledged it, oldest id first (`Lease.grant_waiting_on`).
    ///
    /// A removed machine is still in the set while some member that is not
    /// removed has not acknowledged its removal entry. Two removals pending
    /// at once do not hold each other in, because only members count for a
    /// removal entry ("A removed machine", ADR 0280).
    ///
    /// # Errors
    ///
    /// The machines that have not acknowledged `seq`.
    pub fn replicated(&self, seq: u64, acked: impl Fn(&str) -> u64) -> Result<(), Vec<String>> {
        let mut waiting: BTreeSet<&str> = self
            .members
            .iter()
            .map(String::as_str)
            .filter(|id| acked(id) < seq)
            .collect();
        for (id, removal) in &self.removed {
            let still_in = self.members.iter().any(|member| acked(member) < *removal);
            if still_in && acked(id) < seq {
                waiting.insert(id);
            }
        }
        if waiting.is_empty() {
            Ok(())
        } else {
            Err(waiting.into_iter().map(str::to_string).collect())
        }
    }
}

/// How the rows' voters `listed` and the replay's `logged` differ, `None`
/// when they are the same machines.
fn disagreement(listed: &BTreeSet<&str>, logged: &BTreeSet<&str>) -> Option<Disagreement> {
    (listed != logged).then(|| Disagreement {
        only_in_rows: listed
            .difference(logged)
            .map(|id| (*id).to_string())
            .collect(),
        only_in_log: logged
            .difference(listed)
            .map(|id| (*id).to_string())
            .collect(),
    })
}

/// The voters among the non-removed `nodes` rows `(id, roles, mesh_address)`,
/// in the rows' order.
fn voters_of(rows: Vec<(String, String, Option<String>)>) -> Result<Vec<Voter>, String> {
    let mut voters = Vec::new();
    for (node_id, roles, mesh_address) in rows {
        let roles: Vec<String> = serde_json::from_str(&roles)
            .map_err(|e| format!("cannot read the roles of {node_id}: {e}"))?;
        if is_voter_roles(&roles) {
            voters.push(Voter {
                node_id,
                roles,
                mesh_address,
            });
        }
    }
    Ok(voters)
}

/// Rule 2 of section 8.10: whether the voter set at `version`, whose voters
/// are `voters`, may change now. It may once more than half of `voters`,
/// connected or not, has acknowledged with `command_result` `ok` a
/// `desired_state` that carries exactly `version`; `acked` gives each
/// machine's newest such version. Only the current voters count. An empty
/// set may always change: the first voter needs nobody's acknowledgement.
#[must_use]
pub fn change_allowed(version: u64, voters: &[Voter], acked: impl Fn(&str) -> Option<u64>) -> bool {
    if voters.is_empty() {
        return true;
    }
    let holding = voters
        .iter()
        .filter(|voter| acked(&voter.node_id) == Some(version))
        .count();
    holding * 2 > voters.len()
}

#[derive(Debug, Default)]
struct Cache {
    replay: Replay,
    latest: Option<Arc<VoterSet>>,
    /// The newest version at which a read found the rows and the log
    /// differing.
    disagreed: Option<u64>,
}

/// The voter set's replay of the change log, kept in memory ([module
/// docs](self)).
#[derive(Debug, Default)]
pub struct VoterLog {
    cache: Mutex<Cache>,
}

impl VoterLog {
    fn cache(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The voter set now: the rows and the `node` entries after the last one
    /// replayed, read in one read transaction. Takes one connection from
    /// `pool` for the read, so a caller must not hold one of an in-memory
    /// store's.
    ///
    /// # Errors
    ///
    /// A database error, or a `roles` column that is not a list of roles.
    pub async fn current(&self, pool: &SqlitePool) -> Result<Arc<VoterSet>, String> {
        let mut replay = self.cache().replay.clone();
        let read = |e: sqlx::Error| format!("cannot read the voter set: {e}");
        let mut tx = pool.begin().await.map_err(read)?;
        let head: Option<i64> = sqlx::query_scalar("SELECT MAX(seq) FROM change_log")
            .fetch_one(&mut *tx)
            .await
            .map_err(read)?;
        let head = head.map_or(0, |seq| u64::try_from(seq).unwrap_or(0));
        let entries: Vec<(i64, String, String, String)> = sqlx::query_as(
            "SELECT seq, op, entity_id, payload FROM change_log \
             WHERE entity_type = 'node' AND seq > ? AND seq <= ? ORDER BY seq",
        )
        .bind(i64::try_from(replay.applied).unwrap_or(i64::MAX))
        .bind(i64::try_from(head).unwrap_or(i64::MAX))
        .fetch_all(&mut *tx)
        .await
        .map_err(read)?;
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, roles, mesh_address FROM nodes WHERE status <> 'removed' \
             ORDER BY created_at, id",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(read)?;
        // A read transaction: nothing to keep.
        tx.rollback().await.map_err(read)?;

        for (seq, op, node_id, payload) in entries {
            let seq = u64::try_from(seq).unwrap_or(0);
            let payload: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
            replay.apply(seq, &op, &node_id, &payload);
        }
        let voters = voters_of(rows)?;
        let version = replay.version;
        let listed: BTreeSet<&str> = voters.iter().map(|voter| voter.node_id.as_str()).collect();
        let disagreement = disagreement(&listed, &replay.members());

        let mut cache = self.cache();
        if replay.applied > cache.replay.applied {
            cache.replay = replay;
        }
        if disagreement.is_some() {
            cache.disagreed = cache.disagreed.max(Some(version));
        }
        let countable = disagreement.is_none() && cache.disagreed != Some(version);
        let set = Arc::new(VoterSet {
            head,
            version,
            voters,
            disagreement,
            countable,
        });
        if cache
            .latest
            .as_ref()
            .is_none_or(|latest| latest.head <= set.head)
        {
            let before = cache
                .latest
                .as_ref()
                .and_then(|latest| latest.disagreement.as_ref());
            if let Some(differs) = &set.disagreement
                && before != Some(differs)
            {
                tracing::warn!(
                    version,
                    only_in_rows = ?differs.only_in_rows,
                    only_in_log = ?differs.only_in_log,
                    "the voters in the nodes rows and those the change log replays differ; no voter support counts at this version (sections 6.12 and 8.10 rule 4)"
                );
            }
            cache.latest = Some(Arc::clone(&set));
        }
        Ok(set)
    }

    /// The required set of section 8.4 ("When R is replicated",
    /// [`RequiredSet`]), read on `conn`: the replay kept in memory, the
    /// `node` entries after it and the `nodes` rows. The removal entries come
    /// from the same replay that gives the voter-set version, so a promoted
    /// primary with the same log gets the same set. The kept replay is not
    /// advanced, because `conn` may be a write transaction whose entries
    /// could still roll back. Nodes are never deleted (a removal is a
    /// status), so a `delete` entry, which the replay drops, never occurs.
    ///
    /// A machine whose replayed roles lack `node` is left out. That is right
    /// today: only a removal takes a machine out of the set, and it writes
    /// `status` alone, keeping the roles, so `removed_at` holds it in until
    /// the others acknowledge the removal. A role change that clears `node`
    /// (the roles operation, #687) must likewise keep the machine in the set
    /// until that change is replicated; #687 has to add that before it ships.
    ///
    /// # Errors
    ///
    /// A database error, or a `roles` column that is not a list of roles.
    pub async fn required_set(&self, conn: &mut SqliteConnection) -> Result<RequiredSet, String> {
        let mut replay = self.cache().replay.clone();
        let read = |e: sqlx::Error| format!("cannot read the required set: {e}");
        let entries: Vec<(i64, String, String, String)> = sqlx::query_as(
            "SELECT seq, op, entity_id, payload FROM change_log \
             WHERE entity_type = 'node' AND seq > ? ORDER BY seq",
        )
        .bind(i64::try_from(replay.applied).unwrap_or(i64::MAX))
        .fetch_all(&mut *conn)
        .await
        .map_err(read)?;
        let rows: Vec<(String, String, String)> =
            sqlx::query_as("SELECT id, roles, status FROM nodes")
                .fetch_all(&mut *conn)
                .await
                .map_err(read)?;
        for (seq, op, node_id, payload) in entries {
            let seq = u64::try_from(seq).unwrap_or(0);
            let payload: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
            replay.apply(seq, &op, &node_id, &payload);
        }
        let mut set = RequiredSet::default();
        for (node_id, roles, status) in rows {
            let roles: Vec<String> = serde_json::from_str(&roles)
                .map_err(|e| format!("cannot read the roles of {node_id}: {e}"))?;
            if status != "removed" && roles.iter().any(|role| role == "node") {
                set.members.insert(node_id);
            }
        }
        for (node_id, facts) in &replay.nodes {
            // Without the `node` role: not in the set (see the doc comment
            // for what #687 must add).
            if !facts.has_node_role() {
                continue;
            }
            match facts.removed_at {
                Some(removal) if facts.removed => {
                    if !set.members.contains(node_id) {
                        set.removed.insert(node_id.clone(), removal);
                    }
                }
                _ => {
                    set.members.insert(node_id.clone());
                }
            }
        }
        Ok(set)
    }

    /// The voter-set version and the voters as `conn` sees them: the replay
    /// kept in memory, the `node` entries after it and the `nodes` rows. The
    /// kept replay is not advanced, because `conn` may be a write transaction
    /// whose entries could still roll back (as [`VoterLog::required_set`]).
    /// A change of the voter set reads it here, in its own write
    /// transaction, so rule 2 of section 8.10 counts against the version that
    /// transaction sees ([`change_allowed`]).
    ///
    /// # Errors
    ///
    /// A database error, or a `roles` column that is not a list of roles.
    pub async fn read_on(&self, conn: &mut SqliteConnection) -> Result<(u64, Vec<Voter>), String> {
        let mut replay = self.cache().replay.clone();
        let read = |e: sqlx::Error| format!("cannot read the voter set: {e}");
        let entries: Vec<(i64, String, String, String)> = sqlx::query_as(
            "SELECT seq, op, entity_id, payload FROM change_log \
             WHERE entity_type = 'node' AND seq > ? ORDER BY seq",
        )
        .bind(i64::try_from(replay.applied).unwrap_or(i64::MAX))
        .fetch_all(&mut *conn)
        .await
        .map_err(read)?;
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, roles, mesh_address FROM nodes WHERE status <> 'removed' \
             ORDER BY created_at, id",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(read)?;
        for (seq, op, node_id, payload) in entries {
            let seq = u64::try_from(seq).unwrap_or(0);
            let payload: Value = serde_json::from_str(&payload).unwrap_or(Value::Null);
            replay.apply(seq, &op, &node_id, &payload);
        }
        Ok((replay.version, voters_of(rows)?))
    }

    /// The newest set [`VoterLog::current`] read, `None` before the first.
    #[must_use]
    pub fn latest(&self) -> Option<Arc<VoterSet>> {
        self.cache().latest.clone()
    }

    /// Test seam: makes `set` the newest one read.
    #[cfg(test)]
    pub(crate) fn set_latest(&self, set: VoterSet) {
        self.cache().latest = Some(Arc::new(set));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(roles: &[&str], status: &str) -> Value {
        json!({ "roles": roles, "status": status, "name": "n" })
    }

    /// Section 8.10: the version is the `seq` of the entry that made a node
    /// join or leave the set, and no other entry moves it.
    #[test]
    fn the_version_moves_only_when_a_node_joins_or_leaves_the_set() {
        let mut replay = Replay::default();
        // An enrollment writes the subnet before the row's create.
        replay.apply(
            3,
            "update",
            "nod_a",
            &json!({ "id": "nod_a", "subnet": "10.42.1.0/24" }),
        );
        assert_eq!(replay.version, 0);
        replay.apply(5, "create", "nod_a", &node(&["node"], "pending"));
        assert_eq!(replay.version, 5);
        // A connect, a mesh address, an edge-only machine: no change.
        replay.apply(6, "update", "nod_a", &node(&["node"], "connected"));
        replay.apply(
            7,
            "update",
            "nod_a",
            &json!({ "id": "nod_a", "mesh_address": "10.43.0.1" }),
        );
        replay.apply(8, "create", "nod_e", &node(&["edge"], "pending"));
        assert_eq!(replay.version, 5);
        // An edge that gains the witness role joins; one that loses it leaves.
        replay.apply(
            9,
            "update",
            "nod_e",
            &json!({ "id": "nod_e", "roles": ["edge", "witness"] }),
        );
        assert_eq!(replay.version, 9);
        replay.apply(
            10,
            "update",
            "nod_e",
            &json!({ "id": "nod_e", "roles": ["edge"] }),
        );
        assert_eq!(replay.version, 10);
        // A partial payload without the roles keeps them (ADR 0297).
        replay.apply(
            11,
            "update",
            "nod_a",
            &json!({ "id": "nod_a", "desired_rev": 4 }),
        );
        assert_eq!(replay.version, 10);
        // A removal, then a delete of a node that no longer votes.
        replay.apply(
            12,
            "update",
            "nod_a",
            &json!({ "id": "nod_a", "status": "removed" }),
        );
        assert_eq!(replay.version, 12);
        replay.apply(13, "delete", "nod_a", &json!({}));
        assert_eq!(replay.version, 12);
        replay.apply(14, "create", "nod_w", &node(&["witness"], "pending"));
        replay.apply(15, "delete", "nod_w", &json!({}));
        assert_eq!(replay.version, 15);
        assert_eq!(replay.applied, 15);
    }

    /// Sections 6.12 and 8.10: one machine has one vote, so a node that
    /// gains or loses the `witness` role neither joins nor leaves the set
    /// and the version stays; only its roles in `spec.votes.voters` change.
    #[test]
    fn a_node_that_gains_the_witness_role_keeps_the_version() {
        let mut replay = Replay::default();
        replay.apply(4, "create", "nod_a", &node(&["node"], "pending"));
        assert_eq!(replay.version, 4);
        replay.apply(
            9,
            "update",
            "nod_a",
            &json!({ "id": "nod_a", "roles": ["node", "witness"] }),
        );
        assert_eq!(replay.version, 4, "still one vote");
        assert_eq!(replay.nodes["nod_a"].roles, ["node", "witness"]);
        replay.apply(
            11,
            "update",
            "nod_a",
            &json!({ "id": "nod_a", "roles": ["node"] }),
        );
        assert_eq!(replay.version, 4);
        assert_eq!(replay.members(), BTreeSet::from(["nod_a"]));
    }

    /// Section 8.10 rule 2: a majority of the current voters, connected or
    /// not, must hold exactly the current version; an empty set passes.
    #[test]
    fn a_change_waits_for_a_majority_at_the_current_version() {
        let voter = |id: &str| Voter {
            node_id: id.to_string(),
            roles: vec!["node".to_string()],
            mesh_address: None,
        };
        let voters = [voter("nod_a"), voter("nod_b"), voter("nod_c")];
        let acks = |acks: &'static [(&'static str, u64)]| {
            move |id: &str| {
                acks.iter()
                    .find(|(node, _)| *node == id)
                    .map(|(_, version)| *version)
            }
        };
        assert!(change_allowed(7, &[], acks(&[])), "the first voter");
        assert!(!change_allowed(7, &voters, acks(&[("nod_a", 7)])), "1 of 3");
        assert!(change_allowed(
            7,
            &voters,
            acks(&[("nod_a", 7), ("nod_c", 7)])
        ));
        // An older version, or a machine outside the set, does not count.
        assert!(!change_allowed(
            7,
            &voters,
            acks(&[("nod_a", 7), ("nod_b", 6), ("nod_x", 7)])
        ));
        // Two voters: both.
        assert!(!change_allowed(7, &voters[..2], acks(&[("nod_a", 7)])));
        assert!(change_allowed(
            7,
            &voters[..2],
            acks(&[("nod_a", 7), ("nod_b", 7)])
        ));
    }

    /// Section 8.4, "A removed machine": the replay keeps each removal
    /// entry's `seq`; a removed machine stays in the required set until
    /// every member has acknowledged its removal entry, and two removals
    /// pending at once do not hold each other in.
    #[test]
    fn a_removed_machine_leaves_the_set_once_the_others_acknowledged_its_removal() {
        let mut replay = Replay::default();
        replay.apply(1, "create", "nod_a", &node(&["node"], "pending"));
        replay.apply(2, "create", "nod_b", &node(&["node"], "pending"));
        replay.apply(3, "create", "nod_c", &node(&["node"], "pending"));
        replay.apply(4, "create", "nod_d", &node(&["node"], "pending"));
        replay.apply(5, "create", "nod_w", &node(&["witness"], "pending"));
        replay.apply(6, "update", "nod_c", &json!({ "status": "removed" }));
        replay.apply(
            7,
            "update",
            "nod_c",
            &json!({ "status": "removed", "name": "c" }),
        );
        replay.apply(8, "update", "nod_d", &json!({ "status": "removed" }));
        assert_eq!(
            replay.nodes["nod_c"].removed_at,
            Some(6),
            "the first removal entry"
        );
        assert_eq!(replay.nodes["nod_d"].removed_at, Some(8));
        let set = RequiredSet {
            members: ["nod_a", "nod_b"].map(str::to_string).into(),
            removed: [("nod_c".to_string(), 6), ("nod_d".to_string(), 8)].into(),
        };
        let acks = |acks: &'static [(&'static str, u64)]| {
            move |id: &str| {
                acks.iter()
                    .find(|(node, _)| *node == id)
                    .map_or(0, |(_, seq)| *seq)
            }
        };
        // Neither removal is acknowledged by B: C and D still count.
        assert_eq!(
            set.replicated(9, acks(&[("nod_a", 9), ("nod_b", 5)])),
            Err(["nod_b", "nod_c", "nod_d"].map(str::to_string).to_vec())
        );
        // B acknowledged C's removal but not D's: D still counts. C does not,
        // and D's own acknowledgement never counts for C.
        assert_eq!(
            set.replicated(9, acks(&[("nod_a", 9), ("nod_b", 9), ("nod_d", 0)])),
            Ok(()),
            "both removals acknowledged by every member"
        );
        assert_eq!(
            set.replicated(9, acks(&[("nod_a", 9), ("nod_b", 7)])),
            Err(["nod_b", "nod_d"].map(str::to_string).to_vec())
        );
        assert_eq!(
            set.replicated(7, acks(&[("nod_a", 9), ("nod_b", 7), ("nod_d", 7)])),
            Ok(())
        );
    }

    /// Section 6.12: `spec.votes` on every voter, `spec.witness` on one with
    /// the `witness` role, neither on an edge-only machine.
    #[test]
    fn votes_go_to_voters_and_the_witness_role_to_witnesses() {
        let voter = |id: &str, roles: &[&str], mesh: Option<&str>| Voter {
            node_id: id.to_string(),
            roles: roles.iter().map(|role| (*role).to_string()).collect(),
            mesh_address: mesh.map(str::to_string),
        };
        let set = VoterSet {
            head: 40,
            version: 31,
            voters: vec![
                voter("nod_a", &["node"], Some("10.43.0.2")),
                voter("nod_w", &["edge", "witness"], None),
            ],
            disagreement: None,
            countable: true,
        };
        let (votes, witness) = set.spec_for("nod_a");
        let votes = votes.unwrap();
        assert_eq!((votes.version, votes.port), (31, 51821));
        assert_eq!(votes.voters.len(), 2);
        assert_eq!(votes.voters[0].mesh_address.as_deref(), Some("10.43.0.2"));
        assert_eq!(votes.voters[1].roles, ["edge", "witness"]);
        assert_eq!(witness, None);
        let (votes, witness) = set.spec_for("nod_w");
        assert!(votes.is_some());
        assert_eq!(witness, Some(SpecWitness {}));
        assert_eq!(set.spec_for("nod_edge"), (None, None));
    }

    mod agreement {
        use super::super::*;
        use super::node;
        use crate::authority::VoterSupport;
        use crate::changelog::{self, Actor, Op};
        use crate::store::{Store, now_rfc3339};
        use crate::support::{BootClock, ManualBootClock, SUPPORT_MS, SupportTracker};
        use quoro_proto::wire::{SupportGrant, SupportRequest};
        use quoro_types::{Prefix, new_id};
        use serde_json::json;

        const D: u64 = 15_000;

        struct Cluster {
            store: Store,
            voters: Arc<VoterLog>,
            tracker: SupportTracker,
        }

        impl Cluster {
            async fn new() -> Self {
                let voters = Arc::new(VoterLog::default());
                let clock = Arc::new(ManualBootClock::default()) as Arc<dyn BootClock>;
                Self {
                    store: Store::open_in_memory().await.unwrap(),
                    tracker: SupportTracker::new(clock, Arc::clone(&voters)),
                    voters,
                }
            }

            /// Enrolls a machine with the `node` role: its row and its create
            /// entry, in one write.
            async fn enroll(&self) -> String {
                let id = new_id(Prefix::Node);
                let mut w = self.store.begin_write().await.unwrap();
                sqlx::query(
                    "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, \
                     behind_nat, labels, capacity, allocated, kvm, engine_version, agent_version, \
                     desired_rev, applied_rev, log_seq, last_seen_at, created_at) \
                     VALUES (?, ?, '[\"node\"]', NULL, 'pending', '', NULL, 1, '{}', '{}', '{}', \
                     '{}', NULL, NULL, 0, 0, 0, NULL, ?)",
                )
                .bind(&id)
                .bind(&id)
                .bind(now_rfc3339())
                .execute(&mut *w.tx)
                .await
                .unwrap();
                changelog::append(
                    &mut w.tx,
                    &Actor::system("test"),
                    "node",
                    &id,
                    Op::Create,
                    node(&["node"], "pending"),
                )
                .await
                .unwrap();
                w.commit().await.unwrap();
                id
            }

            /// Sets `node_id`'s row status without a change-log entry.
            async fn set_row_status(&self, node_id: &str, status: &str) {
                sqlx::query("UPDATE nodes SET status = ? WHERE id = ?")
                    .bind(status)
                    .bind(node_id)
                    .execute(self.store.pool())
                    .await
                    .unwrap();
            }

            /// Writes a change-log entry for `node_id` without changing its row.
            async fn log_only(&self, node_id: &str, payload: Value) {
                let mut w = self.store.begin_write().await.unwrap();
                changelog::append(
                    &mut w.tx,
                    &Actor::system("test"),
                    "node",
                    node_id,
                    Op::Update,
                    payload,
                )
                .await
                .unwrap();
                w.commit().await.unwrap();
            }

            async fn read(&self) -> Arc<VoterSet> {
                self.voters.current(self.store.pool()).await.unwrap()
            }

            /// Asks each of `ids` for support at `version`, and each answers.
            fn answer(&self, ids: &[&String], version: u64) {
                for id in ids {
                    let msg_id = format!("msg_{version}_{id}");
                    self.tracker.sent(
                        &msg_id,
                        id,
                        1,
                        &SupportRequest {
                            term: 1,
                            primary_node_id: "nod_p".to_string(),
                            support_ms: SUPPORT_MS,
                            max_lease_ms: D,
                            voters_version: version,
                        },
                    );
                    self.tracker.granted(
                        id,
                        1,
                        Some(&msg_id),
                        &SupportGrant {
                            term: 1,
                            support_ms: SUPPORT_MS,
                            max_lease_ms: D,
                            voters_version: version,
                        },
                    );
                }
            }
        }

        /// Sections 6.12 and 8.10 rule 4: a row whose status changes without
        /// a change-log entry leaves the rows' set and the log's apart at one
        /// version. No support counts while they differ, nor at that version
        /// once they agree again; a change of the set that both carry moves
        /// the version and support counts again.
        #[tokio::test]
        async fn a_row_change_the_log_does_not_carry_counts_no_support() {
            let cluster = Cluster::new().await;
            let a = cluster.enroll().await;
            let b = cluster.enroll().await;
            let c = cluster.enroll().await;
            let set = cluster.read().await;
            assert!(set.countable && set.disagreement.is_none(), "{set:?}");
            cluster.answer(&[&a, &b], set.version);
            assert!(cluster.tracker.supports(D), "2 of 3");

            cluster.set_row_status(&c, "removed").await;
            let apart = cluster.read().await;
            assert_eq!(apart.version, set.version, "the log saw no change");
            assert_eq!(apart.voters.len(), 2);
            assert_eq!(
                apart.disagreement,
                Some(Disagreement {
                    only_in_rows: Vec::new(),
                    only_in_log: vec![c.clone()],
                })
            );
            assert!(!apart.countable);
            assert!(!cluster.tracker.supports(D), "the sets differ");
            cluster.answer(&[&a, &b], apart.version);
            assert!(!cluster.tracker.supports(D), "2 of the rows' 2");

            cluster.set_row_status(&c, "pending").await;
            let again = cluster.read().await;
            assert!(again.disagreement.is_none());
            assert!(!again.countable, "they differed at this version");
            cluster.answer(&[&a, &b], again.version);
            assert!(!cluster.tracker.supports(D));

            let d = cluster.enroll().await;
            let moved = cluster.read().await;
            assert!(moved.version > again.version);
            assert!(moved.countable && moved.disagreement.is_none(), "{moved:?}");
            cluster.answer(&[&a, &b, &d], moved.version);
            assert!(cluster.tracker.supports(D), "3 of 4");
        }

        /// The reverse: a log entry that removes a voter while its row stays
        /// moves the version, but the rows still list it, so nothing counts
        /// at that version, even with every listed voter answering.
        #[tokio::test]
        async fn a_log_change_the_rows_do_not_carry_counts_no_support() {
            let cluster = Cluster::new().await;
            let a = cluster.enroll().await;
            let b = cluster.enroll().await;
            let c = cluster.enroll().await;
            let set = cluster.read().await;
            assert!(set.countable);

            cluster
                .log_only(&c, json!({ "id": c, "status": "removed" }))
                .await;
            let apart = cluster.read().await;
            assert!(apart.version > set.version);
            assert_eq!(apart.voters.len(), 3);
            assert_eq!(
                apart.disagreement,
                Some(Disagreement {
                    only_in_rows: vec![c.clone()],
                    only_in_log: Vec::new(),
                })
            );
            assert!(!apart.countable);
            cluster.answer(&[&a, &b, &c], apart.version);
            assert!(!cluster.tracker.supports(D), "3 of the rows' 3");
        }
    }
}
