//! HTTP handlers. Each handler is annotated with `#[utoipa::path]` so the
//! generated document can be diffed against the frozen `openapi.yaml`
//! (D-0048, `tests/openapi_diff.rs`).

pub mod agents;
pub mod approvals;
pub mod apps;
pub mod audit;
pub mod audit_exports;
pub mod auth;
pub mod backups;
pub mod builds;
pub mod cluster;
pub mod deployments;
pub mod domains;
pub mod enroll;
pub mod events;
pub mod grants;
pub mod health;
pub mod incidents;
pub mod interactive;
pub mod logs;
pub mod metrics;
pub mod nodes;
pub mod page;
pub mod projects;
pub mod providers;
pub mod users;
pub mod vms;
pub mod volumes;
pub mod vulnerabilities;
pub mod webhooks;

use crate::AppState;
use axum::Router;
use axum::routing::{delete, get, post, put};

/// Every implemented route.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v1/health", get(health::get_health))
        .route("/api/v1/setup", post(auth::setup))
        .route("/api/v1/auth/login", post(auth::login))
        .route("/api/v1/auth/logout", post(auth::logout))
        .route("/api/v1/auth/me", get(auth::get_me))
        .route("/api/v1/auth/password", post(auth::change_password))
        .route("/api/v1/auth/agent-token", post(agents::create_agent_token))
        .route("/api/v1/cluster", get(cluster::get_cluster))
        .route(
            "/api/v1/agents",
            get(agents::list_agents).post(agents::create_agent),
        )
        .route(
            "/api/v1/agents/{agent_id}",
            get(agents::get_agent).patch(agents::update_agent),
        )
        .route(
            "/api/v1/agents/{agent_id}/keys",
            get(agents::list_agent_keys).post(agents::create_agent_key),
        )
        .route(
            "/api/v1/agents/{agent_id}/keys/{key_id}",
            delete(agents::revoke_agent_key),
        )
        .route(
            "/api/v1/grants",
            get(grants::list_grants).post(grants::create_grant),
        )
        .route(
            "/api/v1/approvals",
            get(approvals::list_approvals).post(approvals::create_approval),
        )
        .route(
            "/api/v1/approvals/{approval_id}",
            get(approvals::get_approval),
        )
        .route(
            "/api/v1/approvals/{approval_id}/approve",
            post(approvals::approve_approval),
        )
        .route(
            "/api/v1/approvals/{approval_id}/deny",
            post(approvals::deny_approval),
        )
        .route("/api/v1/grants/{grant_id}", get(grants::get_grant))
        .route(
            "/api/v1/grants/{grant_id}/revoke",
            post(grants::revoke_grant),
        )
        .route(
            "/api/v1/users",
            get(users::list_users).post(users::create_user),
        )
        .route(
            "/api/v1/users/{user_id}",
            axum::routing::patch(users::update_user).delete(users::delete_user),
        )
        .route("/api/v1/audit", get(audit::list_audit))
        .route("/api/v1/audit/verify", post(audit::verify_audit))
        .route("/api/v1/audit/{seq}", get(audit::get_audit_record))
        .route("/api/v1/events", get(events::list_events))
        .route("/api/v1/events/stream", get(events::stream_events))
        .route("/api/v1/incidents", get(incidents::list_incidents))
        .route(
            "/api/v1/incidents/{incident_id}",
            get(incidents::get_incident),
        )
        .route(
            "/api/v1/incidents/{incident_id}/resolve",
            post(incidents::resolve_incident),
        )
        .route("/api/v1/nodes", get(nodes::list_nodes))
        .route("/api/v1/nodes/join-tokens", post(enroll::create_join_token))
        .route("/api/v1/nodes/enroll", post(enroll::enroll_node))
        .route(
            "/api/v1/nodes/session",
            get(crate::session::open_node_session),
        )
        .route("/api/v1/nodes/{node_id}", get(nodes::get_node))
        .route(
            "/api/v1/nodes/{node_id}/tunnel",
            put(nodes::set_node_tunnel).delete(nodes::delete_node_tunnel),
        )
        .route(
            "/api/v1/projects",
            get(projects::list_projects).post(projects::create_project),
        )
        .route("/api/v1/projects/{project_id}", get(projects::get_project))
        .route(
            "/api/v1/providers",
            get(providers::list_providers).post(providers::create_provider),
        )
        .route(
            "/api/v1/providers/{provider_id}",
            get(providers::get_provider).delete(providers::delete_provider),
        )
        .route(
            "/api/v1/providers/{provider_id}/rotate",
            post(providers::rotate_provider_secret),
        )
        .route(
            "/api/v1/webhooks",
            get(webhooks::list_webhooks).post(webhooks::create_webhook),
        )
        .route(
            "/api/v1/webhooks/{webhook_id}",
            get(webhooks::get_webhook)
                .patch(webhooks::update_webhook)
                .delete(webhooks::delete_webhook),
        )
        .route(
            "/api/v1/webhooks/{webhook_id}/test",
            post(webhooks::test_webhook),
        )
        .route(
            "/api/v1/webhooks/{webhook_id}/deliveries",
            get(webhooks::list_webhook_deliveries),
        )
        .route("/api/v1/vms", get(vms::list_vms).post(vms::create_vm))
        .route(
            "/api/v1/vms/{vm_id}",
            get(vms::get_vm).delete(vms::delete_vm),
        )
        .route("/api/v1/vms/{vm_id}/start", post(vms::start_vm))
        .route("/api/v1/vms/{vm_id}/stop", post(vms::stop_vm))
        // Apps, deployments and domains (`apps::routes`), the on-demand
        // log reads and the live-log SSE stream (`logs::routes`), the
        // metrics reads and live streams (`metrics::routes`), the terminal
        // and console sessions (`interactive::routes`), the builds
        // (`builds::routes`) and the volumes (`volumes::routes`).
        .merge(apps::routes())
        .merge(logs::routes())
        .merge(metrics::routes())
        .merge(interactive::routes())
        .merge(builds::routes())
        .merge(volumes::routes())
        .merge(backups::routes())
        .merge(audit_exports::routes())
        .merge(vulnerabilities::routes())
}
