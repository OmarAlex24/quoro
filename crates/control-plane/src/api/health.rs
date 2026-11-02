//! `GET /api/v1/health`.

use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

/// Liveness answer of the node that served the call.
#[derive(Serialize, ToSchema)]
#[schema(title = "HealthStatus")]
pub struct HealthStatus {
    /// Fixed liveness value.
    #[schema(inline)]
    pub status: HealthValue,
}

/// The only liveness value.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum HealthValue {
    Ok,
}

/// Liveness of the node that served the call.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "health",
    operation_id = "getHealth",
    security(()),
    extensions(("x-quoro-scope" = json!("public"))),
    responses(
        (status = 200, description = "The node is alive.", body = HealthStatus,
            headers(("Quoro-Request-Id" = String, description = "Correlation id of this call."))),
        (status = 500, response = crate::openapi::Internal),
    )
)]
pub async fn get_health() -> Json<HealthStatus> {
    Json(HealthStatus {
        status: HealthValue::Ok,
    })
}
