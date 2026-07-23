//! Fallback response for routes that do not match any registered endpoint.

use axum::{http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

/// Produces the stable JSON 404 response used by the router fallback.
pub async fn global_error_handler() -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "status": "error",
            "details": "Not Found"
        })),
    )
}
