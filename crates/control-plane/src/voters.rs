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

use quoro_proto::wire::{SpecVoter, SpecVotes, SpecWitness};
use serde_json::Value;
use sqlx::SqlitePool;
use std::collections::HashMap;
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
}

impl NodeFacts {
    fn votes(&self) -> bool {
        !self.removed && is_voter_roles(&self.roles)
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
        let after = self.nodes.get(node_id).is_some_and(NodeFacts::votes);
        if before != after {
            self.version = seq;
        }
        self.applied = self.applied.max(seq);
    }
}

#[derive(Debug, Default)]
struct Cache {
    replay: Replay,
    latest: Option<Arc<VoterSet>>,
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
        let set = Arc::new(VoterSet {
            head,
            version: replay.version,
            voters,
        });

        let mut cache = self.cache();
        if replay.applied > cache.replay.applied {
            cache.replay = replay;
        }
        if cache
            .latest
            .as_ref()
            .is_none_or(|latest| latest.head <= set.head)
        {
            cache.latest = Some(Arc::clone(&set));
        }
        Ok(set)
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
}
