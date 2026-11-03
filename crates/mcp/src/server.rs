//! The rmcp server handler: lists the tools of [`crate::tools`] and hands each
//! call, with the identity the auth middleware resolved, to
//! [`crate::tools::call`].

use crate::tools;
use quoro_control_plane::AppState;
use quoro_control_plane::auth::Identity;
use rmcp::ErrorData;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{RequestContext, RoleServer};

/// One instance per request (the transport is stateless); it only carries the
/// control plane's state.
#[derive(Clone)]
pub struct QuoroMcp {
    state: AppState,
}

impl QuoroMcp {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

/// The identity [`crate::auth::require_agent`] stored on the HTTP request.
/// rmcp copies the request's parts into the context's extensions.
fn identity(context: &RequestContext<RoleServer>) -> Result<Identity, ErrorData> {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<Identity>())
        .cloned()
        .ok_or_else(|| {
            tracing::error!("an MCP call reached the handler without an identity");
            ErrorData::internal_error("an unexpected error occurred", None)
        })
}

impl ServerHandler for QuoroMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("quoro", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Quoro operates microVM apps. Every mutating tool needs a `justification`; \
                 pass the `incident_id` you were woken for. Calls are authorized against \
                 your grants and recorded in the audit log.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult::with_all_items(tools::list()))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        tools::list().into_iter().find(|tool| tool.name == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let identity = identity(&context)?;
        let result = tools::call(&self.state, identity, &request.name, request.arguments).await?;
        Ok(result.into())
    }
}
