// SPDX-License-Identifier: AGPL-3.0-only
//! The primary's side of the M5a WireGuard star (ADR 0144,
//! `node-agent-protocol.md` sections 5.1, 5.11 and 6.9).
//!
//! Every machine stores the WireGuard public key it sent in `hello`
//! (`nodes.mesh_public_key`) and gets one host address of `10.43.0.0/16`
//! (`nodes.mesh_address`), never reused while any row, removed nodes included,
//! still records it (ADR 0144). From those two the primary builds `spec.mesh`:
//! the edge has one peer per node, and every node has the edge as its only
//! peer (ADR 0144).
//!
//! The edge's endpoint is the source address of its session (ADR 0144). When
//! that is loopback or unspecified the edge runs on the primary's host, and
//! the first non-loopback IPv4 host among `welcome.primary_urls` is used. The
//! endpoint lives only in the hub's memory, never in a column.

use crate::changelog::{self, Actor, Op};
use crate::store::Store;
use base64::Engine as _;
use quoro_proto::wire::{MeshAllow, MeshPeer, SpecMesh};
use serde_json::json;
use sqlx::Row;
use sqlx::sqlite::SqliteConnection;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// The UDP port every mesh interface listens on (section 5.11).
pub const LISTEN_PORT: u16 = 51820;

/// The keepalive a node sends to the edge: every node may be behind NAT
/// (section 6.9, ADR 0144).
const NODE_KEEPALIVE_S: u16 = 25;

/// The message of an exhausted `10.43.0.0/16`.
const EXHAUSTED: &str = "no free mesh address in 10.43.0.0/16";

/// Whether `key` is standard base64 of exactly 32 bytes, the shape of a
/// WireGuard public key (section 5.1).
#[must_use]
pub(crate) fn valid_public_key(key: &str) -> bool {
    key.len() == 44
        && base64::engine::general_purpose::STANDARD
            .decode(key.as_bytes())
            .is_ok_and(|bytes| bytes.len() == 32)
}

/// The lowest free host address of `10.43.0.0/16`, skipping the network
/// (`10.43.0.0`) and broadcast (`10.43.255.255`) addresses.
///
/// `taken` holds every non-NULL `nodes.mesh_address`, of removed nodes too, so
/// an address is never handed to a second machine (ADR 0144).
///
/// # Errors
///
/// A message when every host address of the `/16` is taken.
pub fn allocate(taken: &[String]) -> Result<String, String> {
    for third in 0..=255u16 {
        for fourth in 0..=255u16 {
            if (third == 0 && fourth == 0) || (third == 255 && fourth == 255) {
                continue;
            }
            let candidate = format!("10.43.{third}.{fourth}");
            if !taken.iter().any(|other| other == &candidate) {
                return Ok(candidate);
            }
        }
    }
    Err(EXHAUSTED.to_string())
}

/// Stores the mesh public key a `hello` carried and assigns the node's mesh
/// address when it has a key and none yet, in the caller's write transaction
/// (section 5.1, ADR 0144).
///
/// A key that is not standard base64 of 32 bytes is ignored with a warning and
/// never stored. Each stored field appends a `node` change-log entry.
///
/// Returns whether the key or the address was newly stored, so the caller can
/// rebuild every snapshot: peers depend on other rows.
///
/// # Errors
///
/// A message when the row cannot be read or written, or the `/16` is
/// exhausted.
pub(crate) async fn store_hello(
    conn: &mut SqliteConnection,
    actor: &Actor,
    node_id: &str,
    hello_key: Option<&str>,
) -> Result<bool, String> {
    let row: Option<(Option<String>, Option<String>)> =
        sqlx::query_as("SELECT mesh_public_key, mesh_address FROM nodes WHERE id = ?")
            .bind(node_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| format!("cannot read the mesh fields of {node_id}: {e}"))?;
    let Some((stored_key, stored_address)) = row else {
        return Ok(false);
    };
    let hello_key = match hello_key {
        Some(key) if valid_public_key(key) => Some(key.to_string()),
        Some(_) => {
            tracing::warn!(node_id, "ignoring an invalid mesh public key");
            None
        }
        None => None,
    };
    let mut changed = false;
    let key = match hello_key {
        Some(key) if stored_key.as_deref() != Some(key.as_str()) => {
            sqlx::query("UPDATE nodes SET mesh_public_key = ? WHERE id = ?")
                .bind(&key)
                .bind(node_id)
                .execute(&mut *conn)
                .await
                .map_err(|e| format!("cannot store the mesh key of {node_id}: {e}"))?;
            changelog::append(
                conn,
                actor,
                "node",
                node_id,
                Op::Update,
                json!({ "id": node_id, "mesh_public_key": key }),
            )
            .await
            .map_err(|e| format!("cannot record the mesh key of {node_id}: {e:?}"))?;
            changed = true;
            Some(key)
        }
        _ => stored_key,
    };
    if key.is_some() && stored_address.is_none() {
        let taken: Vec<(String,)> =
            sqlx::query_as("SELECT mesh_address FROM nodes WHERE mesh_address IS NOT NULL")
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| format!("cannot read the taken mesh addresses: {e}"))?;
        let taken: Vec<String> = taken.into_iter().map(|(address,)| address).collect();
        let address = allocate(&taken)?;
        sqlx::query("UPDATE nodes SET mesh_address = ? WHERE id = ?")
            .bind(&address)
            .bind(node_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| format!("cannot store the mesh address of {node_id}: {e}"))?;
        changelog::append(
            conn,
            actor,
            "node",
            node_id,
            Op::Update,
            json!({ "id": node_id, "mesh_address": address }),
        )
        .await
        .map_err(|e| format!("cannot record the mesh address of {node_id}: {e:?}"))?;
        tracing::info!(node_id, mesh_address = %address, "assigned a mesh address");
        changed = true;
    }
    Ok(changed)
}

/// What a mesh build needs from the hub's memory (ADR 0144).
pub struct MeshLookup<'a> {
    /// The source address of a node's session, when the primary recorded one.
    pub session_ip: &'a (dyn Fn(&str) -> Option<IpAddr> + Send + Sync),
    /// `welcome.primary_urls` of this primary (section 5.10).
    pub advertised_urls: Vec<String>,
}

/// One non-removed `nodes` row as the mesh sees it.
struct MeshNode {
    id: String,
    roles: Vec<String>,
    subnet: Option<String>,
    public_key: Option<String>,
    address: Option<String>,
}

impl MeshNode {
    /// Whether the row carries `role`.
    fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|held| held == role)
    }

    /// Whether the row can be either end of a tunnel.
    fn is_mesh_member(&self) -> bool {
        self.public_key.is_some() && self.address.is_some()
    }
}

/// Builds `spec.mesh` for `node_id` (section 5.11, ADR 0144).
///
/// The edge is the non-removed row with the `edge` role that has a mesh key
/// and address, the smallest id if several. Without one, or for a machine
/// that is neither the edge nor a `node`, the result is `None`.
///
/// # Errors
///
/// A message when a row cannot be read, a stored `roles` value is not a JSON
/// array, or an app `port` is not a valid port.
pub async fn build_mesh(
    store: &Store,
    lookup: &MeshLookup<'_>,
    node_id: &str,
) -> Result<Option<SpecMesh>, String> {
    let rows = sqlx::query(
        "SELECT id, roles, subnet, mesh_public_key, mesh_address FROM nodes \
         WHERE status <> 'removed' ORDER BY id",
    )
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the nodes for the mesh: {e}"))?;
    let mut nodes = Vec::with_capacity(rows.len());
    for row in &rows {
        let id: String = row.try_get("id").map_err(|e| e.to_string())?;
        let roles: String = row.try_get("roles").map_err(|e| e.to_string())?;
        let roles: Vec<String> = serde_json::from_str(&roles)
            .map_err(|e| format!("cannot read the roles of {id}: {e}"))?;
        nodes.push(MeshNode {
            id,
            roles,
            subnet: row.try_get("subnet").map_err(|e| e.to_string())?,
            public_key: row.try_get("mesh_public_key").map_err(|e| e.to_string())?,
            address: row.try_get("mesh_address").map_err(|e| e.to_string())?,
        });
    }

    // Rows are ordered by id, so this is the smallest edge id.
    let Some(edge) = nodes
        .iter()
        .find(|node| node.has_role("edge") && node.is_mesh_member())
    else {
        return Ok(None);
    };
    let Some(machine) = nodes.iter().find(|node| node.id == node_id) else {
        return Ok(None);
    };

    if machine.id == edge.id {
        let Some(edge_address) = edge.address.as_deref() else {
            return Ok(None);
        };
        let peers = nodes
            .iter()
            .filter(|node| node.id != edge.id && node.has_role("node") && node.is_mesh_member())
            .filter_map(|node| {
                let public_key = node.public_key.clone()?;
                let address = node.address.clone()?;
                let mut allowed_ips = vec![format!("{address}/32")];
                if let Some(subnet) = &node.subnet {
                    allowed_ips.push(subnet.clone());
                }
                Some(MeshPeer {
                    node_id: node.id.clone(),
                    public_key,
                    endpoint: None,
                    allowed_ips,
                    persistent_keepalive_s: None,
                })
            })
            .collect();
        return Ok(Some(SpecMesh {
            address: format!("{edge_address}/16"),
            listen_port: LISTEN_PORT,
            peers,
            allow: Vec::new(),
        }));
    }

    if !machine.has_role("node") || !machine.is_mesh_member() {
        return Ok(None);
    }
    let Some(machine_address) = machine.address.as_deref() else {
        return Ok(None);
    };
    let Some(edge_key) = edge.public_key.as_deref() else {
        return Ok(None);
    };
    let Some(edge_address) = edge.address.as_deref() else {
        return Ok(None);
    };
    let endpoint =
        endpoint_ip(&edge.id, lookup).map(|ip| SocketAddr::new(ip, LISTEN_PORT).to_string());
    let allow = allow_list(store, node_id, edge_address).await?;
    Ok(Some(SpecMesh {
        address: format!("{machine_address}/16"),
        listen_port: LISTEN_PORT,
        peers: vec![MeshPeer {
            node_id: edge.id.clone(),
            public_key: edge_key.to_string(),
            endpoint,
            allowed_ips: vec![format!("{edge_address}/32")],
            persistent_keepalive_s: Some(NODE_KEEPALIVE_S),
        }],
        allow,
    }))
}

/// `spec.mesh.allow` on a node: one entry per backend the edge serves from
/// it, that is, a running app VM with an address whose app has more than one
/// replica (ADR 0144). Ordered by VM id, like the rest of the snapshot.
async fn allow_list(
    store: &Store,
    node_id: &str,
    edge_address: &str,
) -> Result<Vec<MeshAllow>, String> {
    let rows = sqlx::query(
        "SELECT v.ip, a.port FROM vms v JOIN apps a ON a.id = v.app_id \
         WHERE v.node_id = ? AND v.app_id IS NOT NULL AND v.desired_status = 'running' \
           AND v.ip IS NOT NULL AND a.replicas > 1 ORDER BY v.id",
    )
    .bind(node_id)
    .fetch_all(store.pool())
    .await
    .map_err(|e| format!("cannot read the backends of {node_id}: {e}"))?;
    rows.iter()
        .map(|row| {
            let address: String = row.try_get("ip").map_err(|e| e.to_string())?;
            let port: i64 = row.try_get("port").map_err(|e| e.to_string())?;
            Ok(MeshAllow {
                source: edge_address.to_string(),
                address,
                port: u16::try_from(port).map_err(|e| format!("apps.port: {e}"))?,
            })
        })
        .collect()
}

/// The address a node dials to reach `edge_id` (ADR 0144): the IP its session
/// came from, or, when that is loopback or unspecified -- the edge runs on the
/// primary's host -- the first non-loopback IPv4 host the primary advertises.
/// An edge whose session address is not known yet (it has not connected since
/// the primary started) has no endpoint: guessing the primary's address would
/// point every node at the wrong host.
fn endpoint_ip(edge_id: &str, lookup: &MeshLookup<'_>) -> Option<IpAddr> {
    match (lookup.session_ip)(edge_id) {
        Some(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(ip),
        Some(_) => lookup
            .advertised_urls
            .iter()
            .find_map(|url| advertised_host_ip(url).map(IpAddr::V4)),
        None => None,
    }
}

/// The IPv4 host of a `https://host:port` entry, when it is not loopback or
/// unspecified.
fn advertised_host_ip(url: &str) -> Option<Ipv4Addr> {
    let authority = url.split_once("://")?.1.split('/').next()?;
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    let ip: Ipv4Addr = host.parse().ok()?;
    if ip.is_loopback() || ip.is_unspecified() {
        None
    } else {
        Some(ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::now_rfc3339;
    use quoro_types::{Prefix, new_id};

    /// A key of exactly 32 bytes in standard base64 (section 5.1).
    const KEY: &str = "yAnz5TF+lXXJte14tji3zlMNq+hd2rYUIgJBgB3fBmk=";

    async fn insert_node(
        store: &Store,
        roles: &str,
        subnet: Option<&str>,
        key: Option<&str>,
        address: Option<&str>,
        status: &str,
    ) -> String {
        let id = new_id(Prefix::Node);
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO nodes (id, name, roles, cp_status, status, public_key, address, behind_nat, \
             labels, capacity, allocated, kvm, engine_version, agent_version, desired_rev, applied_rev, \
             log_seq, last_seen_at, created_at, subnet, mesh_public_key, mesh_address) \
             VALUES (?, ?, ?, NULL, ?, '', NULL, 1, '{}', '{}', '{}', '{}', NULL, NULL, 0, 0, 0, NULL, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(&id)
        .bind(roles)
        .bind(status)
        .bind(&now)
        .bind(subnet)
        .bind(key)
        .bind(address)
        .execute(store.pool())
        .await
        .unwrap();
        id
    }

    /// A lookup with no recorded session and no advertised URL.
    fn no_lookup() -> MeshLookup<'static> {
        fn none(_: &str) -> Option<IpAddr> {
            None
        }
        MeshLookup {
            session_ip: &none,
            advertised_urls: Vec::new(),
        }
    }

    #[tokio::test]
    async fn without_an_edge_there_is_no_mesh() {
        let store = Store::open_in_memory().await.unwrap();
        let node = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            Some(KEY),
            Some("10.43.0.2"),
            "pending",
        )
        .await;
        let mesh = build_mesh(&store, &no_lookup(), &node).await.unwrap();
        assert_eq!(mesh, None);

        // A witness-only machine is never on the mesh.
        let witness = insert_node(
            &store,
            "[\"witness\"]",
            None,
            Some(KEY),
            Some("10.43.0.3"),
            "pending",
        )
        .await;
        let edge = insert_node(
            &store,
            "[\"edge\"]",
            None,
            Some(KEY),
            Some("10.43.0.1"),
            "pending",
        )
        .await;
        assert_eq!(
            build_mesh(&store, &no_lookup(), &witness).await.unwrap(),
            None
        );
        // The edge without a key or an address is no edge at all.
        let keyless = insert_node(&store, "[\"edge\"]", None, None, None, "pending").await;
        assert_eq!(
            build_mesh(&store, &no_lookup(), &keyless).await.unwrap(),
            None
        );
        assert_eq!(
            build_mesh(&store, &no_lookup(), &edge)
                .await
                .unwrap()
                .map(|mesh| mesh.peers.len()),
            Some(1),
            "the first node is the edge's only peer"
        );
    }

    #[tokio::test]
    async fn the_edge_gets_one_peer_per_node_with_its_subnet() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = insert_node(
            &store,
            "[\"edge\"]",
            None,
            Some(KEY),
            Some("10.43.0.1"),
            "connected",
        )
        .await;
        let second = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.2.0/24"),
            Some(KEY),
            Some("10.43.0.3"),
            "pending",
        )
        .await;
        let first = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            Some(KEY),
            Some("10.43.0.2"),
            "pending",
        )
        .await;
        // A removed node is no peer.
        insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.9.0/24"),
            Some(KEY),
            Some("10.43.0.9"),
            "removed",
        )
        .await;

        let mesh = build_mesh(&store, &no_lookup(), &edge)
            .await
            .unwrap()
            .expect("the edge is on the mesh");
        assert_eq!(mesh.address, "10.43.0.1/16");
        assert_eq!(mesh.listen_port, 51820);
        assert!(mesh.allow.is_empty());
        let ids: Vec<&str> = mesh
            .peers
            .iter()
            .map(|peer| peer.node_id.as_str())
            .collect();
        let mut expected = vec![first.as_str(), second.as_str()];
        expected.sort_unstable();
        assert_eq!(ids, expected);
        let peer = &mesh.peers[ids.iter().position(|id| *id == first).unwrap()];
        assert_eq!(
            peer.allowed_ips,
            ["10.43.0.2/32".to_string(), "10.42.1.0/24".to_string()]
        );
        assert_eq!(peer.endpoint, None);
        assert_eq!(peer.persistent_keepalive_s, None);
    }

    #[tokio::test]
    async fn a_node_gets_the_edge_peer_and_only_its_multi_replica_backends() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = insert_node(
            &store,
            "[\"edge\"]",
            None,
            Some(KEY),
            Some("10.43.0.1"),
            "connected",
        )
        .await;
        let node = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            Some(KEY),
            Some("10.43.0.2"),
            "connected",
        )
        .await;
        let project_id = new_id(Prefix::Project);
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO projects (id, slug, name, created_at, updated_at) VALUES (?, 'p', 'p', ?, ?)",
        )
        .bind(&project_id)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
        for (name, replicas, desired, ip) in [
            ("replicated", 2_i64, "running", Some("10.42.1.10")),
            ("single", 1, "running", Some("10.42.1.11")),
            ("stopped", 2, "stopped", Some("10.42.1.12")),
            ("addressed", 2, "running", None),
        ] {
            let app_id = new_id(Prefix::Project).replacen("prj_", "app_", 1);
            sqlx::query(
                "INSERT INTO apps (id, project_id, name, stateful, ha_policy, image, port, health_check, \
                 resources, placement, status, created_at, updated_at, replicas) \
                 VALUES (?, ?, ?, 0, 'none', 'ghcr.io/example/x:1', 8080, \
                 '{\"path\":\"/\",\"interval_s\":10,\"timeout_s\":2,\"failures\":3}', \
                 '{\"vcpus\":1,\"memory_mb\":512,\"disk_gb\":2}', '{\"node_ids\":[]}', 'running', ?, ?, ?)",
            )
            .bind(&app_id)
            .bind(&project_id)
            .bind(name)
            .bind(&now)
            .bind(&now)
            .bind(replicas)
            .execute(store.pool())
            .await
            .unwrap();
            let vm_id = new_id(Prefix::Vm);
            sqlx::query(
                "INSERT INTO vms (id, project_id, app_id, node_id, name, vcpus, memory_mb, disk_gb, \
                 image, desired_status, observed_status, ip, restart_generation, restart_count, \
                 last_exit_code, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, 1, 512, 2, 'ghcr.io/example/x:1', ?, 'running', ?, 0, 0, NULL, ?, ?)",
            )
            .bind(&vm_id)
            .bind(&project_id)
            .bind(&app_id)
            .bind(&node)
            .bind(name)
            .bind(desired)
            .bind(ip)
            .bind(&now)
            .bind(&now)
            .execute(store.pool())
            .await
            .unwrap();
        }

        let edge_id = edge.clone();
        let session_ip = move |id: &str| {
            if id == edge_id {
                Some(IpAddr::V4(Ipv4Addr::new(10, 9, 8, 7)))
            } else {
                None
            }
        };
        let lookup = MeshLookup {
            session_ip: &session_ip,
            advertised_urls: vec!["https://primary.example:8443".to_string()],
        };
        let mesh = build_mesh(&store, &lookup, &node)
            .await
            .unwrap()
            .expect("a node with a key and address is on the mesh");
        assert_eq!(mesh.address, "10.43.0.2/16");
        assert_eq!(mesh.listen_port, 51820);
        assert_eq!(mesh.peers.len(), 1);
        let peer = &mesh.peers[0];
        assert_eq!(peer.node_id, edge);
        assert_eq!(peer.public_key, KEY);
        assert_eq!(peer.endpoint.as_deref(), Some("10.9.8.7:51820"));
        assert_eq!(peer.allowed_ips, ["10.43.0.1/32"]);
        assert_eq!(peer.persistent_keepalive_s, Some(25));
        // Only the running VM of the app with 2 replicas, and only because it
        // has an address.
        assert_eq!(mesh.allow.len(), 1);
        assert_eq!(mesh.allow[0].source, "10.43.0.1");
        assert_eq!(mesh.allow[0].address, "10.42.1.10");
        assert_eq!(mesh.allow[0].port, 8080);
    }

    #[tokio::test]
    async fn a_loopback_session_ip_falls_back_to_the_advertised_host() {
        let store = Store::open_in_memory().await.unwrap();
        let edge = insert_node(
            &store,
            "[\"edge\"]",
            None,
            Some(KEY),
            Some("10.43.0.1"),
            "connected",
        )
        .await;
        let node = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            Some(KEY),
            Some("10.43.0.2"),
            "connected",
        )
        .await;
        let session_ip = move |id: &str| {
            if id == edge {
                Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
            } else {
                None
            }
        };
        let lookup = MeshLookup {
            session_ip: &session_ip,
            advertised_urls: vec![
                "https://primary.example:8443".to_string(),
                "https://127.0.0.1:8443".to_string(),
                "https://10.1.2.3:8443".to_string(),
                "https://10.4.5.6:8443".to_string(),
            ],
        };
        let mesh = build_mesh(&store, &lookup, &node)
            .await
            .unwrap()
            .expect("the node is on the mesh");
        assert_eq!(
            mesh.peers[0].endpoint.as_deref(),
            Some("10.1.2.3:51820"),
            "the first non-loopback IPv4 host"
        );

        // Nothing usable means no endpoint, which is a node that dials out.
        let lookup = MeshLookup {
            session_ip: &session_ip,
            advertised_urls: vec!["https://primary.example:8443".to_string()],
        };
        let mesh = build_mesh(&store, &lookup, &node)
            .await
            .unwrap()
            .expect("the node is on the mesh");
        assert_eq!(mesh.peers[0].endpoint, None);
    }

    #[test]
    fn a_mesh_address_is_the_lowest_free_host_address() {
        assert_eq!(allocate(&[]).unwrap(), "10.43.0.1");
        assert_eq!(
            allocate(&["10.43.0.1".to_string(), "10.43.0.2".to_string()]).unwrap(),
            "10.43.0.3"
        );
        let mut taken: Vec<String> = (1..=255).map(|last| format!("10.43.0.{last}")).collect();
        assert_eq!(allocate(&taken).unwrap(), "10.43.1.0");
        taken.push("10.43.1.0".to_string());
        assert_eq!(allocate(&taken).unwrap(), "10.43.1.1");
    }

    #[tokio::test]
    async fn a_mesh_address_is_never_reused_from_a_removed_row() {
        let store = Store::open_in_memory().await.unwrap();
        let removed = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            Some(KEY),
            Some("10.43.0.1"),
            "removed",
        )
        .await;
        let fresh = insert_node(&store, "[\"edge\"]", None, None, None, "pending").await;
        let mut tx = store.begin_write().await.unwrap();
        let actor = Actor::system("test");
        assert!(
            store_hello(&mut tx.tx, &actor, &fresh, Some(KEY))
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let stored: Option<String> =
            sqlx::query_scalar("SELECT mesh_address FROM nodes WHERE id = ?")
                .bind(&fresh)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(stored.as_deref(), Some("10.43.0.2"));
        let removed_address: Option<String> =
            sqlx::query_scalar("SELECT mesh_address FROM nodes WHERE id = ?")
                .bind(&removed)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(removed_address.as_deref(), Some("10.43.0.1"));
    }

    #[tokio::test]
    async fn an_invalid_hello_key_is_not_stored() {
        let store = Store::open_in_memory().await.unwrap();
        let node = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            None,
            None,
            "pending",
        )
        .await;
        let actor = Actor::system("test");
        let mut tx = store.begin_write().await.unwrap();
        assert!(
            !store_hello(&mut tx.tx, &actor, &node, Some("not-a-wireguard-key"))
                .await
                .unwrap()
        );
        // 44 characters of base64 that decode to fewer than 32 bytes.
        assert!(
            !store_hello(&mut tx.tx, &actor, &node, Some(&"A".repeat(44)))
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let stored: Option<String> =
            sqlx::query_scalar("SELECT mesh_public_key FROM nodes WHERE id = ?")
                .bind(&node)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(stored, None);
        let address: Option<String> =
            sqlx::query_scalar("SELECT mesh_address FROM nodes WHERE id = ?")
                .bind(&node)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(address, None, "no key, so no address");
    }

    #[tokio::test]
    async fn a_valid_hello_key_stores_the_key_and_assigns_an_address() {
        let store = Store::open_in_memory().await.unwrap();
        let node = insert_node(
            &store,
            "[\"node\"]",
            Some("10.42.1.0/24"),
            None,
            None,
            "pending",
        )
        .await;
        let actor = Actor::system("test");
        let mut tx = store.begin_write().await.unwrap();
        assert!(
            store_hello(&mut tx.tx, &actor, &node, Some(KEY))
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let (key, address): (Option<String>, Option<String>) =
            sqlx::query_as("SELECT mesh_public_key, mesh_address FROM nodes WHERE id = ?")
                .bind(&node)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(key.as_deref(), Some(KEY));
        assert_eq!(address.as_deref(), Some("10.43.0.1"));

        // The same key again changes nothing, and the address stays.
        let mut tx = store.begin_write().await.unwrap();
        assert!(
            !store_hello(&mut tx.tx, &actor, &node, Some(KEY))
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let address: Option<String> =
            sqlx::query_scalar("SELECT mesh_address FROM nodes WHERE id = ?")
                .bind(&node)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(address.as_deref(), Some("10.43.0.1"));
    }
}
