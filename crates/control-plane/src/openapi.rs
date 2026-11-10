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
        crate::api::auth::get_setup,
        crate::api::auth::setup,
        crate::api::auth::login,
        crate::api::auth::logout,
        crate::api::auth::change_password,
        crate::api::auth::get_me,
        crate::api::agents::create_agent_token,
        crate::api::agents::list_agents,
        crate::api::agents::create_agent,
        crate::api::agents::get_agent,
        crate::api::agents::update_agent,
        crate::api::agents::list_agent_keys,
        crate::api::agents::create_agent_key,
        crate::api::agents::revoke_agent_key,
        crate::api::tokens::list_tokens,
        crate::api::tokens::create_token,
        crate::api::tokens::revoke_token,
        crate::api::users::list_users,
        crate::api::users::create_user,
        crate::api::users::update_user,
        crate::api::users::delete_user,
        crate::api::grants::list_grants,
        crate::api::grants::create_grant,
        crate::api::grants::get_grant,
        crate::api::grants::revoke_grant,
        crate::api::approvals::list_approvals,
        crate::api::approvals::create_approval,
        crate::api::approvals::get_approval,
        crate::api::approvals::approve_approval,
        crate::api::approvals::deny_approval,
        crate::api::audit::list_audit,
        crate::api::audit::get_audit_record,
        crate::api::audit::verify_audit,
        crate::api::cluster::get_cluster,
        crate::api::nodes::list_nodes,
        crate::api::nodes::get_node,
        crate::api::nodes::set_node_tunnel,
        crate::api::nodes::delete_node_tunnel,
        crate::api::nodes::ack_node_fence,
        crate::api::nodes::update_node_roles,
        crate::api::nodes::declare_node_lost,
        crate::api::enroll::create_join_token,
        crate::api::enroll::enroll_node,
        crate::session::open_node_session,
        crate::api::projects::list_projects,
        crate::api::projects::create_project,
        crate::api::projects::get_project,
        crate::api::providers::list_providers,
        crate::api::providers::create_provider,
        crate::api::providers::get_provider,
        crate::api::providers::delete_provider,
        crate::api::providers::rotate_provider_secret,
        crate::api::webhooks::list_webhooks,
        crate::api::webhooks::create_webhook,
        crate::api::webhooks::get_webhook,
        crate::api::webhooks::update_webhook,
        crate::api::webhooks::delete_webhook,
        crate::api::webhooks::test_webhook,
        crate::api::webhooks::list_webhook_deliveries,
        crate::api::vms::list_vms,
        crate::api::vms::create_vm,
        crate::api::vms::get_vm,
        crate::api::vms::delete_vm,
        crate::api::vms::start_vm,
        crate::api::vms::stop_vm,
        crate::api::metrics::get_node_metrics,
        crate::api::metrics::stream_node_metrics,
        crate::api::metrics::get_app_metrics,
        crate::api::metrics::stream_app_metrics,
        crate::api::vulnerabilities::get_app_vulnerabilities,
        crate::api::vulnerabilities::get_vulnerability_summary,
    ),
    components(
        schemas(
            Problem,
            crate::error::Target,
            crate::api::auth::User,
            crate::api::auth::MeResponse,
            crate::api::auth::ChangePasswordRequest,
            crate::api::tokens::ApiToken,
            crate::api::tokens::ApiTokenList,
            crate::api::tokens::ApiTokenCreated,
            crate::api::tokens::CreateTokenRequest,
            crate::api::users::UserList,
            crate::api::users::CreateUserRequest,
            crate::api::users::UpdateUserRequest,
            crate::api::agents::Agent,
            crate::api::agents::AgentList,
            crate::api::agents::CreateAgentRequest,
            crate::api::agents::UpdateAgentRequest,
            crate::api::agents::AgentKey,
            crate::api::agents::AgentKeyCreated,
            crate::api::agents::AgentKeyList,
            crate::api::agents::AccessTokenResponse,
            crate::api::audit::AuditRecord,
            crate::api::audit::AuditList,
            crate::api::audit::AuditVerificationRequest,
            crate::api::audit::AuditVerificationResult,
            crate::api::cluster::Cluster,
            crate::api::cluster::ClusterVotes,
            crate::api::cluster::ClusterVoter,
            crate::api::auth::Grant,
            crate::api::grants::GrantList,
            crate::api::grants::CreateGrantRequest,
            crate::api::approvals::Approval,
            crate::api::approvals::ApprovalList,
            crate::api::approvals::ApprovalStatus,
            crate::api::approvals::CreateApprovalRequest,
            crate::api::approvals::DecisionRequest,
            crate::api::auth::Selector,
            crate::api::auth::ScopeSchema,
            crate::api::nodes::Node,
            crate::api::nodes::NodeList,
            crate::api::nodes::NodeCapacity,
            crate::api::nodes::KvmInfo,
            crate::api::nodes::NodeFenceFailure,
            crate::api::nodes::NodeTunnel,
            crate::api::nodes::SetNodeTunnelRequest,
            crate::api::nodes::FenceAckRequest,
            crate::api::nodes::NodeRolesRequest,
            crate::api::enroll::NodeJoinTokenRequest,
            crate::api::enroll::NodeJoinTokenCreated,
            crate::api::enroll::EnrollNodeRequest,
            crate::api::enroll::EnrollNodeResponse,
            crate::api::projects::Project,
            crate::api::projects::ProjectList,
            crate::api::projects::CreateProjectRequest,
            crate::api::providers::ProviderKind,
            crate::api::providers::Provider,
            crate::api::providers::ProviderList,
            crate::api::providers::CreateProviderRequest,
            crate::api::providers::RotateProviderSecretRequest,
            crate::api::vms::Vm,
            crate::api::vms::VmList,
            crate::api::vms::CreateVmRequest,
            crate::api::webhooks::EventType,
            crate::api::webhooks::WebhookWake,
            crate::api::webhooks::Webhook,
            crate::api::webhooks::WebhookList,
            crate::api::webhooks::WebhookCreated,
            crate::api::webhooks::CreateWebhookRequest,
            crate::api::webhooks::UpdateWebhookRequest,
            crate::api::webhooks::WebhookDelivery,
            crate::api::webhooks::WebhookDeliveryList,
            crate::api::webhooks::DeliveryStatus,
            crate::api::metrics::NodeMetricSample,
            crate::api::metrics::NodeMetrics,
            crate::api::metrics::VmMetricSample,
            crate::api::metrics::AppInstanceMetrics,
            crate::api::metrics::AppMetrics,
            crate::api::vulnerabilities::VulnerabilitySeverity,
            crate::api::vulnerabilities::VulnerabilityFinding,
            crate::api::vulnerabilities::VulnerabilityCounts,
            crate::api::vulnerabilities::VulnerabilityDataSource,
            crate::api::vulnerabilities::ImageInventoryStatus,
            crate::api::vulnerabilities::AppVulnerabilities,
            crate::api::vulnerabilities::AppVulnerabilitySummary,
            crate::api::vulnerabilities::NodeKernelVulnerabilities,
            crate::api::vulnerabilities::QuoroVulnerabilities,
            crate::api::vulnerabilities::VulnerabilitySummary,
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
/// token or agent access token), `sessionCookie` (`quoro_session`) and
/// `nodeCredential` (the node session credential).
struct Security;

impl utoipa::Modify for Security {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        // The apps and logs modules keep their own documents; they are merged
        // first so the fixes below see every operation.
        openapi.merge(crate::api::apps::AppsDoc::openapi());
        openapi.merge(crate::api::logs::LogsDoc::openapi());
        openapi.merge(crate::api::interactive::InteractiveDoc::openapi());
        openapi.merge(crate::api::builds::BuildsDoc::openapi());
        openapi.merge(crate::api::volumes::VolumesDoc::openapi());
        openapi.merge(crate::api::backups::BackupsDoc::openapi());
        openapi.merge(crate::api::audit_exports::AuditExportsDoc::openapi());
        openapi.merge(crate::api::events::EventsDoc::openapi());
        openapi.merge(crate::api::incidents::IncidentsDoc::openapi());

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
        components.add_security_scheme(
            "nodeCredential",
            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
        );

        // utoipa has no `required = false` for a request body: an optional
        // body is written `Option<T>`, which also marks the schema nullable.
        // The contract's `NodeJoinTokenRequest` is a plain object, so the
        // annotation names the type and only the body is marked optional here.
        for path in [
            "/api/v1/nodes/join-tokens",
            "/api/v1/audit/verify",
            "/api/v1/vms/{vm_id}/console-sessions",
            "/api/v1/apps/{app_id}/builds",
        ] {
            if let Some(body) = openapi
                .paths
                .paths
                .get_mut(path)
                .and_then(|item| item.post.as_mut())
                .and_then(|operation| operation.request_body.as_mut())
            {
                body.required = Some(utoipa::openapi::Required::False);
            }
        }

        // utoipa derives no length bounds for array items: the contract's
        // `CreateTerminalSessionRequest.command` items are 1-4096 characters.
        if let Some(components) = openapi.components.as_mut()
            && let Some(utoipa::openapi::RefOr::T(utoipa::openapi::schema::Schema::Object(request))) =
                components.schemas.get_mut("CreateTerminalSessionRequest")
            && let Some(utoipa::openapi::RefOr::T(utoipa::openapi::schema::Schema::Array(command))) =
                request.properties.get_mut("command")
            && let utoipa::openapi::schema::ArrayItems::RefOrSchema(items) = &mut command.items
            && let utoipa::openapi::RefOr::T(utoipa::openapi::schema::Schema::Object(items)) =
                items.as_mut()
        {
            items.min_length = Some(1);
            items.max_length = Some(4096);
        }

        // The approve and deny requests take an optional note; like
        // `NodeJoinTokenRequest` the schema itself is a plain object.
        for path in [
            "/api/v1/approvals/{approval_id}/approve",
            "/api/v1/approvals/{approval_id}/deny",
        ] {
            if let Some(body) = openapi
                .paths
                .paths
                .get_mut(path)
                .and_then(|item| item.post.as_mut())
                .and_then(|operation| operation.request_body.as_mut())
            {
                body.required = Some(utoipa::openapi::Required::False);
            }
        }
        // The resolution note is optional: resolving needs no body at all.
        if let Some(body) = openapi
            .paths
            .paths
            .get_mut("/api/v1/incidents/{incident_id}/resolve")
            .and_then(|item| item.post.as_mut())
            .and_then(|operation| operation.request_body.as_mut())
        {
            body.required = Some(utoipa::openapi::Required::False);
        }
    }
}
