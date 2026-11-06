//! Per-domain serving status (`Domain.serving_status`, ADR 0167, migration
//! 0009).
//!
//! `domains.serving_status` says whether a machine can serve a hostname,
//! independently of its certificate. The primary recomputes it for every
//! domain whenever the session hub is told that a desired state changed (an
//! app's placement, replicas or routes, a domain, a tunnel mapping or the
//! edge), every [`RECOMPUTE_INTERVAL`] as a safety net, and once at start,
//! so the domains added before migration 0009 leave `pending`. The rules are
//! the ones the desired state is built with:
//!
//! - The routes of an app with one replica go to the nodes that run one of
//!   its instances meant to run (`spec.routes`). An `acme` hostname is served
//!   there; a `tunnel` hostname only when one of those nodes has a tunnel
//!   (`spec.ingress`).
//! - The routes of an app with more replicas go to the edge (`spec.edge`).
//!   The hostname is served when there is an edge, and a `tunnel` hostname
//!   only when the edge has a tunnel.
//!
//! An app with no instance meant to run leaves its domains `pending`.
//!
//! Every `domain.unserved` gets its `domain.served`. A domain that becomes
//! `unserved` opens an unserved period and emits `domain.unserved`; the next
//! time it becomes `serving` the period closes and `domain.served` is emitted
//! with `unserved_since`, whatever came in between: a stopped app's domain
//! goes `unserved`, `pending`, `serving` and is still announced as served
//! again. A domain that becomes `unserved` while its period is still open
//! emits nothing more. The period is read from the domain's latest serving
//! event, so it survives a primary restart.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::json;
use sqlx::Row;
use sqlx::sqlite::SqliteConnection;

use crate::AppState;
use crate::api::domains::{DOMAIN_COLUMNS, Domain, ServingStatus, TlsMode, domain_from_row};
use crate::changelog::{self, Actor, Op};
use crate::error::ApiError;
use crate::events::{self, NewEvent, Source};
use crate::store::Store;

/// The safety-net period of the recompute: a change that reached no hub
/// notification is picked up within it.
pub const RECOMPUTE_INTERVAL: Duration = Duration::from_secs(30);

/// How long a recompute waits after a notification, so a burst of changes
/// lands in one pass.
const SETTLE: Duration = Duration::from_millis(100);

/// `last_error` of a one-replica app's `tunnel` hostname that no tunnel
/// serves (ADR 0167).
pub const NO_TUNNEL_NODE: &str = "no instance of the app runs on a node with a tunnel";

/// `last_error` of a replicated app's hostname when no machine has the edge
/// role.
pub const NO_EDGE: &str =
    "the app has more than one replica and no edge machine serves its hostnames";

/// `last_error` of a replicated app's `tunnel` hostname when the edge has no
/// tunnel.
pub const NO_EDGE_TUNNEL: &str = "the app has more than one replica and the edge has no tunnel";

/// What decides one domain's status.
#[derive(Clone, Copy, Debug)]
struct Facts {
    tls_mode: TlsMode,
    replicas: i64,
    /// Instances of the app meant to run.
    running: i64,
    /// Instances of the app meant to run on a node with a tunnel.
    running_on_tunnel: i64,
    /// Whether an edge machine exists.
    edge: bool,
    /// Whether the edge machine has a tunnel.
    edge_tunnel: bool,
}

/// The status and `last_error` a domain with `facts` has.
fn decide(facts: Facts) -> (ServingStatus, Option<&'static str>) {
    if facts.running == 0 {
        return (ServingStatus::Pending, None);
    }
    let tunnel = facts.tls_mode == TlsMode::Tunnel;
    if facts.replicas == 1 {
        if tunnel && facts.running_on_tunnel == 0 {
            return (ServingStatus::Unserved, Some(NO_TUNNEL_NODE));
        }
        return (ServingStatus::Serving, None);
    }
    if !facts.edge {
        return (ServingStatus::Unserved, Some(NO_EDGE));
    }
    if tunnel && !facts.edge_tunnel {
        return (ServingStatus::Unserved, Some(NO_EDGE_TUNNEL));
    }
    (ServingStatus::Serving, None)
}

/// Starts the recompute task. Errors are logged, never propagated: a failed
/// pass is retried on the next notification or tick.
pub fn spawn(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(error) = recompute(&state.store).await {
                tracing::warn!(error = ?error, "the domain serving status recompute failed");
            }
            tokio::select! {
                () = state.hub.serving_inputs_changed() => {}
                () = tokio::time::sleep(RECOMPUTE_INTERVAL) => {}
            }
            tokio::time::sleep(SETTLE).await;
        }
    });
}

/// Recomputes every domain's `serving_status` and `last_error` in one write
/// transaction, writing and announcing only the domains that changed.
/// Returns how many changed.
///
/// # Errors
///
/// A database error.
pub async fn recompute(store: &Store) -> Result<usize, ApiError> {
    let mut w = store.begin_write().await?;
    let edge_id = super::mesh::edge_node_id(&mut *w.tx)
        .await
        .map_err(|error| ApiError::internal(&error))?;
    let edge_tunnel = match &edge_id {
        Some(edge_id) => has_tunnel(&mut w.tx, edge_id).await?,
        None => false,
    };
    let apps = app_facts(&mut w.tx).await?;
    let rows = sqlx::query(&format!(
        "SELECT {DOMAIN_COLUMNS} FROM domains ORDER BY hostname"
    ))
    .fetch_all(&mut *w.tx)
    .await?;
    let actor = Actor::system("serving");
    let mut changed = 0;
    for row in &rows {
        let mut domain = domain_from_row(row)?;
        let Some(app) = apps.get(&domain.app_id) else {
            continue;
        };
        let (status, error) = decide(Facts {
            tls_mode: domain.tls_mode,
            replicas: app.replicas,
            running: app.running,
            running_on_tunnel: app.running_on_tunnel,
            edge: edge_id.is_some(),
            edge_tunnel,
        });
        let error = error.map(str::to_string);
        if domain.serving_status == status && domain.last_error == error {
            continue;
        }
        domain.serving_status = status;
        domain.last_error = error;
        sqlx::query("UPDATE domains SET serving_status = ?, last_error = ? WHERE id = ?")
            .bind(domain.serving_status.as_str())
            .bind(&domain.last_error)
            .bind(&domain.id)
            .execute(&mut *w.tx)
            .await?;
        let payload = serde_json::to_value(&domain)
            .map_err(|error| ApiError::internal(&format!("domain payload: {error}")))?;
        changelog::append(&mut w.tx, &actor, "domain", &domain.id, Op::Update, payload).await?;
        announce(&mut w.tx, &domain, &app.project_id).await?;
        changed += 1;
    }
    w.commit().await?;
    Ok(changed)
}

/// The event a domain announces when its status changes to `now`, given
/// whether its unserved period is open.
fn event_kind(now: ServingStatus, open: bool) -> Option<&'static str> {
    match (now, open) {
        (ServingStatus::Unserved, false) => Some("domain.unserved"),
        (ServingStatus::Serving, true) => Some("domain.served"),
        _ => None,
    }
}

/// Emits `domain.unserved` when `domain` became `unserved` with no unserved
/// period open, and `domain.served` when it became `serving` with one open.
async fn announce(
    conn: &mut SqliteConnection,
    domain: &Domain,
    project_id: &str,
) -> Result<(), ApiError> {
    let since = open_unserved_since(conn, &domain.id).await?;
    let Some(kind) = event_kind(domain.serving_status, since.is_some()) else {
        return Ok(());
    };
    let mut data = json!({
        "domain_id": domain.id,
        "app_id": domain.app_id,
        "hostname": domain.hostname,
        "tls_mode": domain.tls_mode.as_str(),
    });
    if let Some(since) = since {
        data["unserved_since"] = json!(since);
    } else {
        tracing::warn!(
            domain_id = %domain.id,
            hostname = %domain.hostname,
            error = domain.last_error.as_deref().unwrap_or_default(),
            "no machine serves the hostname"
        );
        data["error"] = json!(domain.last_error.as_deref().unwrap_or_default());
    }
    events::emit(
        conn,
        NewEvent {
            kind,
            source: Source::ControlPlane,
            subject_type: "domain",
            subject_id: &domain.id,
            project_id: Some(project_id),
            request_id: None,
            actor: None,
            data,
        },
    )
    .await?;
    Ok(())
}

/// When `domain_id`'s open unserved period began: the time of its latest
/// serving event when that event is a `domain.unserved`. `None` when the
/// domain has no such event or a `domain.served` closed the period.
async fn open_unserved_since(
    conn: &mut SqliteConnection,
    domain_id: &str,
) -> Result<Option<String>, ApiError> {
    let row: Option<(String, String)> = sqlx::query_as(
        "SELECT type, time FROM events \
         WHERE type IN ('domain.unserved', 'domain.served') \
         AND json_extract(subject, '$.id') = ? ORDER BY seq DESC LIMIT 1",
    )
    .bind(domain_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.and_then(|(kind, time)| (kind == "domain.unserved").then_some(time)))
}

/// Per app: its replicas, its project and its instances meant to run, in
/// total and on a node with a tunnel.
struct AppFacts {
    replicas: i64,
    project_id: String,
    running: i64,
    running_on_tunnel: i64,
}

async fn app_facts(conn: &mut SqliteConnection) -> Result<HashMap<String, AppFacts>, ApiError> {
    // A node "has a tunnel" when `spec.ingress` carries one: the row names a
    // tunnel that exists at the provider and holds its run token.
    let rows = sqlx::query(
        "SELECT a.id, a.replicas, a.project_id, \
                (SELECT COUNT(*) FROM vms v \
                 WHERE v.app_id = a.id AND v.desired_status = 'running') AS running, \
                (SELECT COUNT(*) FROM vms v JOIN node_tunnels t ON t.node_id = v.node_id \
                 WHERE v.app_id = a.id AND v.desired_status = 'running' \
                   AND t.tunnel_id IS NOT NULL AND t.token_enc IS NOT NULL) AS running_on_tunnel \
         FROM apps a",
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut apps = HashMap::new();
    for row in &rows {
        apps.insert(
            row.try_get::<String, _>("id")?,
            AppFacts {
                replicas: row.try_get("replicas")?,
                project_id: row.try_get("project_id")?,
                running: row.try_get("running")?,
                running_on_tunnel: row.try_get("running_on_tunnel")?,
            },
        );
    }
    Ok(apps)
}

/// Whether `node_id` has a tunnel `spec.ingress` would carry.
async fn has_tunnel(conn: &mut SqliteConnection, node_id: &str) -> Result<bool, ApiError> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT 1 FROM node_tunnels WHERE node_id = ? \
         AND tunnel_id IS NOT NULL AND token_enc IS NOT NULL",
    )
    .bind(node_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(tls_mode: TlsMode, replicas: i64, running: i64) -> Facts {
        Facts {
            tls_mode,
            replicas,
            running,
            running_on_tunnel: 0,
            edge: false,
            edge_tunnel: false,
        }
    }

    #[test]
    fn every_unserved_event_is_closed_by_one_served_event() {
        // Becoming unserved opens a period, once.
        assert_eq!(
            event_kind(ServingStatus::Unserved, false),
            Some("domain.unserved")
        );
        assert_eq!(event_kind(ServingStatus::Unserved, true), None);
        // Becoming serving closes an open period, from any status.
        assert_eq!(
            event_kind(ServingStatus::Serving, true),
            Some("domain.served")
        );
        assert_eq!(event_kind(ServingStatus::Serving, false), None);
        // Pending neither opens nor closes one.
        assert_eq!(event_kind(ServingStatus::Pending, true), None);
        assert_eq!(event_kind(ServingStatus::Pending, false), None);
    }

    #[test]
    fn an_app_with_no_instance_meant_to_run_leaves_its_domains_pending() {
        for tls_mode in [TlsMode::Acme, TlsMode::Tunnel] {
            for replicas in [1, 3] {
                assert_eq!(
                    decide(facts(tls_mode, replicas, 0)),
                    (ServingStatus::Pending, None)
                );
            }
        }
    }

    #[test]
    fn a_one_replica_app_is_served_by_its_node_and_a_tunnel_needs_a_tunnel_node() {
        assert_eq!(
            decide(facts(TlsMode::Acme, 1, 1)),
            (ServingStatus::Serving, None)
        );
        assert_eq!(
            decide(facts(TlsMode::Tunnel, 1, 1)),
            (ServingStatus::Unserved, Some(NO_TUNNEL_NODE))
        );
        let on_tunnel = Facts {
            running_on_tunnel: 1,
            ..facts(TlsMode::Tunnel, 1, 1)
        };
        assert_eq!(decide(on_tunnel), (ServingStatus::Serving, None));
    }

    #[test]
    fn a_replicated_app_needs_the_edge_and_a_tunnel_needs_the_edge_tunnel() {
        assert_eq!(
            decide(facts(TlsMode::Acme, 2, 2)),
            (ServingStatus::Unserved, Some(NO_EDGE))
        );
        let edge = Facts {
            edge: true,
            ..facts(TlsMode::Acme, 2, 2)
        };
        assert_eq!(decide(edge), (ServingStatus::Serving, None));
        let tunnel = Facts {
            edge: true,
            // A node tunnel does not serve a replicated app's hostname.
            running_on_tunnel: 2,
            ..facts(TlsMode::Tunnel, 2, 2)
        };
        assert_eq!(
            decide(tunnel),
            (ServingStatus::Unserved, Some(NO_EDGE_TUNNEL))
        );
        let edge_tunnel = Facts {
            edge_tunnel: true,
            ..tunnel
        };
        assert_eq!(decide(edge_tunnel), (ServingStatus::Serving, None));
    }
}
