// SPDX-License-Identifier: AGPL-3.0-only
//! One guest subnet per node (ADR 0144).
//!
//! Every machine with the `node` role owns a subnet, stored in `nodes.subnet`
//! and sent to it as `spec.network`. The oldest node keeps the primary's
//! configured guest network ([`crate::ServerConfig::node_network`]); every
//! other node gets the next free `/24` of `10.42.0.0/16` that overlaps neither
//! a subnet already taken nor the configured network. A subnet is never reused
//! while any row, removed nodes included, still records it.

use crate::NodeNetwork;
use crate::changelog::{self, Actor, Op};
use crate::store::Store;
use serde_json::json;
use std::net::Ipv4Addr;

/// The message of an exhausted `10.42.0.0/16`.
const EXHAUSTED: &str = "no free guest subnet in 10.42.0.0/16";

/// An IPv4 network: its network address and its prefix length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl Cidr {
    /// Parses `a.b.c.d/n`, rejecting anything that is not a canonical IPv4
    /// CIDR.
    fn parse(cidr: &str) -> Result<Self, String> {
        let (address, prefix) = cidr
            .split_once('/')
            .ok_or_else(|| format!("invalid IPv4 CIDR: {cidr}"))?;
        let address: Ipv4Addr = address
            .parse()
            .map_err(|_| format!("invalid IPv4 CIDR: {cidr}"))?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|_| format!("invalid IPv4 CIDR: {cidr}"))?;
        if prefix > 32 {
            return Err(format!("invalid IPv4 CIDR: {cidr}"));
        }
        let network = Ipv4Addr::from(u32::from(address) & mask(prefix));
        Ok(Self { network, prefix })
    }

    /// The first address of the network.
    fn first(self) -> u64 {
        u64::from(u32::from(self.network))
    }

    /// The last address of the network.
    fn last(self) -> u64 {
        self.first() + (1u64 << (32 - self.prefix)) - 1
    }

    /// Whether the two networks share an address.
    fn overlaps(self, other: Self) -> bool {
        self.first() <= other.last() && other.first() <= self.last()
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

/// The network mask of `prefix` as a `u32`.
fn mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

/// The first host address of `cidr`, as a dotted string.
///
/// # Errors
///
/// A message when `cidr` is not a valid IPv4 CIDR or has no host address.
pub fn gateway_of(cidr: &str) -> Result<String, String> {
    let cidr = Cidr::parse(cidr)?;
    if cidr.prefix >= 32 {
        return Err(format!("{cidr:?} has no host address"));
    }
    Ok(Ipv4Addr::from(u32::from(cidr.network) + 1).to_string())
}

/// Whether `a` and `b` overlap.
///
/// # Errors
///
/// A message when either value is not a valid IPv4 CIDR.
pub fn overlaps(a: &str, b: &str) -> Result<bool, String> {
    Ok(Cidr::parse(a)?.overlaps(Cidr::parse(b)?))
}

/// Whether `other` blocks `candidate`: any shared address does. Two nodes'
/// guests must never share an address, and the edge routes each subnet to one
/// node, so a configured network wider than a `/24` (a `/8`, say) blocks every
/// `/24` inside it, and a cluster whose oldest node uses all of `10.42.0.0/16`
/// cannot add a second node (ADR 0144).
fn blocks(candidate: &str, other: &str) -> Result<bool, String> {
    overlaps(candidate, other)
}

/// Assigns a subnet to a node.
///
/// `taken` holds every non-NULL `nodes.subnet`, of removed nodes too. When it
/// is empty the node gets `configured` exactly as configured; otherwise the
/// first `/24` of `10.42.0.0/16` that [`blocks`] neither `taken` nor
/// `configured`.
///
/// # Errors
///
/// A message when a value is not a valid IPv4 CIDR or every `/24` of
/// `10.42.0.0/16` is taken.
pub fn allocate(configured: &str, taken: &[String]) -> Result<String, String> {
    Cidr::parse(configured)?;
    if taken.is_empty() {
        return Ok(configured.to_string());
    }
    for i in 0..=255u8 {
        let candidate = format!("10.42.{i}.0/24");
        let mut blocked = blocks(&candidate, configured)?;
        if !blocked {
            for other in taken {
                if blocks(&candidate, other)? {
                    blocked = true;
                    break;
                }
            }
        }
        if !blocked {
            return Ok(candidate);
        }
    }
    Err(EXHAUSTED.to_string())
}

/// Assigns a subnet to every `node` role row that has none, oldest first, in
/// one write transaction (ADR 0144).
///
/// The oldest row keeps the configured network; every later row gets the next
/// free `/24`. An edge-only row is left NULL. Each assignment appends a
/// `node` change-log entry as the `subnet` system actor.
///
/// # Errors
///
/// A message when the rows cannot be read or written, or the `/16` is
/// exhausted.
pub async fn backfill(store: &Store, network: &NodeNetwork) -> Result<(), String> {
    let mut w = store
        .begin_write()
        .await
        .map_err(|e| format!("cannot start the subnet backfill: {e}"))?;
    let taken: Vec<(String,)> = sqlx::query_as("SELECT subnet FROM nodes WHERE subnet IS NOT NULL")
        .fetch_all(&mut *w.tx)
        .await
        .map_err(|e| format!("cannot read the taken subnets: {e}"))?;
    let mut taken: Vec<String> = taken.into_iter().map(|(subnet,)| subnet).collect();
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT id FROM nodes \
         WHERE subnet IS NULL \
           AND EXISTS (SELECT 1 FROM json_each(nodes.roles) WHERE json_each.value = 'node') \
         ORDER BY created_at, id",
    )
    .fetch_all(&mut *w.tx)
    .await
    .map_err(|e| format!("cannot read the nodes without a subnet: {e}"))?;

    let actor = Actor::system("subnet");
    for (node_id,) in rows {
        let subnet = allocate(&network.subnet, &taken)?;
        sqlx::query("UPDATE nodes SET subnet = ? WHERE id = ?")
            .bind(&subnet)
            .bind(&node_id)
            .execute(&mut *w.tx)
            .await
            .map_err(|e| format!("cannot assign a subnet to {node_id}: {e}"))?;
        changelog::append(
            &mut w.tx,
            &actor,
            "node",
            &node_id,
            Op::Update,
            json!({ "id": node_id, "subnet": subnet }),
        )
        .await
        .map_err(|e| format!("cannot record the subnet of {node_id}: {e:?}"))?;
        tracing::info!(node_id = %node_id, subnet = %subnet, "assigned a guest subnet");
        taken.push(subnet);
    }
    w.commit()
        .await
        .map_err(|e| format!("cannot commit the subnet backfill: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::now_rfc3339;
    use quoro_types::{Prefix, new_id};

    async fn insert_node(
        store: &Store,
        roles: &str,
        subnet: Option<&str>,
        status: &str,
        created_at: &str,
    ) -> String {
        let id = new_id(Prefix::Node);
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at, subnet) \
             VALUES (?, ?, ?, NULL, ?, '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, 0, NULL, ?, ?)",
        )
        .bind(&id)
        .bind(&id)
        .bind(roles)
        .bind(status)
        .bind(created_at)
        .bind(subnet)
        .execute(store.pool())
        .await
        .unwrap();
        id
    }

    async fn subnet_of(store: &Store, node_id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT subnet FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    #[test]
    fn the_first_node_keeps_the_configured_network() {
        let subnet = allocate("10.42.0.0/24", &[]).unwrap();
        assert_eq!(subnet, "10.42.0.0/24");
    }

    #[test]
    fn the_second_node_gets_the_next_free_24() {
        let subnet = allocate("10.42.0.0/24", &["10.42.0.0/24".to_string()]).unwrap();
        assert_eq!(subnet, "10.42.1.0/24");
    }

    #[test]
    fn a_configured_network_outside_the_pool_leaves_it_free() {
        let subnet = allocate("192.168.100.0/24", &["192.168.100.0/24".to_string()]).unwrap();
        assert_eq!(subnet, "10.42.0.0/24");
    }

    #[test]
    fn a_configured_network_covering_the_pool_is_never_carved_into() {
        let error = allocate("10.0.0.0/8", &["10.0.0.0/8".to_string()]).unwrap_err();
        assert_eq!(error, EXHAUSTED);
    }

    #[test]
    fn an_exhausted_range_is_an_error() {
        let error = allocate("10.42.0.0/16", &["10.0.0.0/8".to_string()]).unwrap_err();
        assert_eq!(error, EXHAUSTED);
    }

    #[tokio::test]
    async fn removed_nodes_subnets_are_never_reused() {
        let store = Store::open_in_memory().await.unwrap();
        let now = now_rfc3339();
        let removed =
            insert_node(&store, "[\"node\"]", Some("10.42.0.0/24"), "removed", &now).await;
        let waiting = insert_node(&store, "[\"node\"]", None, "pending", &now).await;
        let network = NodeNetwork {
            subnet: "192.168.100.0/24".to_string(),
            gateway: "192.168.100.1".to_string(),
        };
        backfill(&store, &network).await.unwrap();
        assert_eq!(
            subnet_of(&store, &removed).await.as_deref(),
            Some("10.42.0.0/24")
        );
        assert_eq!(
            subnet_of(&store, &waiting).await.as_deref(),
            Some("10.42.1.0/24")
        );
    }

    #[tokio::test]
    async fn backfill_gives_the_oldest_node_the_configured_network() {
        let store = Store::open_in_memory().await.unwrap();
        let newer = insert_node(
            &store,
            "[\"node\"]",
            None,
            "pending",
            "2026-11-02T10:00:00.000Z",
        )
        .await;
        let older = insert_node(
            &store,
            "[\"node\"]",
            None,
            "pending",
            "2026-11-01T10:00:00.000Z",
        )
        .await;
        let network = NodeNetwork {
            subnet: "10.42.0.0/24".to_string(),
            gateway: "10.42.0.1".to_string(),
        };
        backfill(&store, &network).await.unwrap();
        assert_eq!(
            subnet_of(&store, &older).await.as_deref(),
            Some("10.42.0.0/24")
        );
        assert_eq!(
            subnet_of(&store, &newer).await.as_deref(),
            Some("10.42.1.0/24")
        );
    }

    #[tokio::test]
    async fn an_edge_only_row_stays_null() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = insert_node(&store, "[\"edge\"]", None, "pending", &now_rfc3339()).await;
        let network = NodeNetwork {
            subnet: "10.42.0.0/24".to_string(),
            gateway: "10.42.0.1".to_string(),
        };
        backfill(&store, &network).await.unwrap();
        assert_eq!(subnet_of(&store, &edge).await, None);
    }

    #[test]
    fn the_gateway_is_the_first_host_address() {
        assert_eq!(gateway_of("10.42.1.0/24").unwrap(), "10.42.1.1");
    }
}
