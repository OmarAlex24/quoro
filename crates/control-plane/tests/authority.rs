//! The primary's gates of `expiring` leases across its own restart
//! (`node-agent-protocol.md` section 8.4, "What the authority must enforce";
//! ADR 0203).

mod common;

use quoro_control_plane::ReferenceAsDigest;
use quoro_control_plane::driver::{Capacity, Kvm, LocalNode};
use std::sync::Arc;

/// Inserts an app lease held by a node, in `mode`, revoked or not.
async fn lease(
    server: &common::Server,
    id: &str,
    mode: &str,
    duration_ms: Option<i64>,
    revoked: bool,
) -> String {
    let node_id = quoro_control_plane::driver::register_local_node(
        &server.state.store,
        &LocalNode {
            name: format!("node-{}", id[4..10].to_lowercase()),
            capacity: Capacity {
                cpus: 2,
                memory_mb: 2048,
                disk_gb: 20,
            },
            kvm: Kvm {
                available: true,
                kernel: "6.8.0".to_string(),
                nested: None,
            },
            engine_version: "test".to_string(),
            agent_version: "test".to_string(),
        },
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO leases (id, resource_type, resource_id, holder_node_id, mode, fencing_token, \
         duration_ms, granted_at, revoked_at) VALUES (?, 'app', ?, ?, ?, 3, ?, \
         '2026-11-07T10:00:00.000Z', ?)",
    )
    .bind(id)
    .bind(format!("app_{}", &id[4..]))
    .bind(&node_id)
    .bind(mode)
    .bind(duration_ms)
    .bind(revoked.then_some("2026-11-07T10:00:01.000Z"))
    .execute(server.state.store.pool())
    .await
    .unwrap();
    node_id
}

/// No gate survives a restart, so a restarted primary counts every
/// `expiring` lease, revoked or not, as granted at its start for the stored
/// duration; a `local` lease has no gate. The view stays empty until a grant
/// (ADR 0203).
#[tokio::test]
async fn a_restarted_primary_counts_every_expiring_gate_from_its_start() {
    let server = common::start().await;
    let held = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8A";
    let revoked = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8B";
    let local = "lse_01JC9Z8K2M4P6R8T0V2X4Z6B8C";
    lease(&server, held, "expiring", Some(15_000), false).await;
    lease(&server, revoked, "expiring", Some(15_000), true).await;
    lease(&server, local, "local", None, false).await;
    for id in [held, revoked, local] {
        assert!(
            server.state.hub.authority().passed(id),
            "no gate before the restart"
        );
    }

    let restarted = common::restart_with_resolver(&server, Arc::new(ReferenceAsDigest)).await;
    let authority = restarted.state.hub.authority();
    assert!(!authority.passed(held));
    assert!(!authority.passed(revoked), "a revoked lease keeps its gate");
    assert!(authority.passed(local));
    assert_eq!(authority.view(held), None);
    assert!(
        !authority.supports(15_000),
        "no build holds voter support yet"
    );
}
