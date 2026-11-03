//! HTTP handlers. Each handler is annotated with `#[utoipa::path]` so the
//! generated document can be diffed against the frozen `openapi.yaml`
//! (D-0048, `tests/openapi_diff.rs`).

pub mod apps;
pub mod auth;
pub mod deployments;
pub mod domains;
pub mod enroll;
pub mod health;
pub mod logs;
pub mod nodes;
pub mod page;
pub mod projects;
pub mod providers;
pub mod vms;

use crate::AppState;
use axum::Router;
use axum::routing::{get, post, put};

/// Every implemented route.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/api/v1/health", get(health::get_health))
        .route("/api/v1/setup", post(auth::setup))
        .route("/api/v1/auth/login", post(auth::login))
        .route("/api/v1/auth/logout", post(auth::logout))
        .route("/api/v1/auth/me", get(auth::get_me))
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
        .route("/api/v1/vms", get(vms::list_vms).post(vms::create_vm))
        .route(
            "/api/v1/vms/{vm_id}",
            get(vms::get_vm).delete(vms::delete_vm),
        )
        .route("/api/v1/vms/{vm_id}/start", post(vms::start_vm))
        .route("/api/v1/vms/{vm_id}/stop", post(vms::stop_vm))
        // Apps, deployments and domains (`apps::routes`), and the on-demand
        // log reads and the live-log SSE stream (`logs::routes`).
        .merge(apps::routes())
        .merge(logs::routes())
}
