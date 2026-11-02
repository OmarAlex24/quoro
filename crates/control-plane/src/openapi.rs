//! The generated OpenAPI document. It is never published as the contract:
//! `tests/openapi_diff.rs` compares it with the frozen
//! `docs/contracts/openapi.yaml` for every implemented operation and fails on
//! a semantic difference (D-0048).

#![allow(dead_code)] // The response newtypes exist only for their schema.

use crate::error::Problem;
use utoipa::{OpenApi, ToResponse};

macro_rules! problem_response {
    ($name:ident, $description:literal) => {
        #[derive(ToResponse)]
        #[response(
            description = $description,
            content_type = "application/problem+json",
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))
        )]
        pub struct $name(Problem);
    };
}

problem_response!(
    Unauthenticated,
    "No valid credential was presented (`code: unauthenticated`, 401)."
);
problem_response!(
    Forbidden,
    "The caller is authenticated but not allowed (`code: forbidden`, 403)."
);
problem_response!(
    ApprovalRequired,
    "The action needs just-in-time approval (`code: approval_required`, 403)."
);
problem_response!(
    NotFound,
    "The addressed resource does not exist (`code: not_found`, 404)."
);
problem_response!(
    Conflict,
    "The request conflicts with the current state (`code: conflict`, 409)."
);
problem_response!(
    ValidationFailed,
    "The body or query is invalid (`code: validation_failed`, 422)."
);
problem_response!(
    NotPrimary,
    "The write reached a replica that cannot forward it (`code: not_primary`, 503)."
);
problem_response!(
    Internal,
    "An unexpected error occurred (`code: internal`, 500)."
);

/// Too many requests (`code: rate_limited`, 429).
#[derive(ToResponse)]
#[response(
    description = "Too many requests (`code: rate_limited`, 429).",
    content_type = "application/problem+json",
    headers(
        ("Quoro-Request-Id" = String, description = "Correlation id of this call."),
        ("Retry-After" = u32, description = "Seconds to wait before retrying."),
    )
)]
pub struct RateLimited(Problem);

/// The document generated from the handler annotations.
#[derive(OpenApi)]
#[openapi(
    modifiers(&Security),
    security(("bearerAuth" = []), ("sessionCookie" = [])),
    paths(
        crate::api::health::get_health,
        crate::api::auth::setup,
        crate::api::auth::login,
        crate::api::auth::logout,
        crate::api::auth::get_me,
        crate::api::nodes::list_nodes,
        crate::api::nodes::get_node,
        crate::api::projects::list_projects,
        crate::api::projects::create_project,
        crate::api::projects::get_project,
        crate::api::vms::list_vms,
        crate::api::vms::create_vm,
        crate::api::vms::get_vm,
        crate::api::vms::delete_vm,
        crate::api::vms::start_vm,
        crate::api::vms::stop_vm,
    ),
    components(
        schemas(
            Problem,
            crate::error::Target,
            crate::api::auth::User,
            crate::api::auth::MeResponse,
            crate::api::auth::Agent,
            crate::api::auth::Grant,
            crate::api::auth::Selector,
            crate::api::auth::ScopeSchema,
            crate::api::nodes::Node,
            crate::api::nodes::NodeList,
            crate::api::nodes::NodeCapacity,
            crate::api::nodes::KvmInfo,
            crate::api::nodes::NodeFenceFailure,
            crate::api::projects::Project,
            crate::api::projects::ProjectList,
            crate::api::projects::CreateProjectRequest,
            crate::api::vms::Vm,
            crate::api::vms::VmList,
            crate::api::vms::CreateVmRequest,
        ),
        responses(
            Unauthenticated,
            Forbidden,
            ApprovalRequired,
            NotFound,
            Conflict,
            ValidationFailed,
            NotPrimary,
            Internal,
            RateLimited
        )
    )
)]
pub struct ApiDoc;

/// Adds the security schemes of the frozen document: `bearerAuth` (human API
/// token or agent access token) and `sessionCookie` (`quoro_session`). Other
/// schemes (`agentKey`, `nodeCredential`) arrive with their operations.
struct Security;

impl utoipa::Modify for Security {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{
            ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityScheme,
        };
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );
        components.add_security_scheme(
            "sessionCookie",
            SecurityScheme::ApiKey(ApiKey::Cookie(ApiKeyValue::new(
                crate::auth::SESSION_COOKIE,
            ))),
        );
    }
}
