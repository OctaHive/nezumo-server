//! Stable reference-data lookup handlers used by clients.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Json},
};
use serde_json::json;
use std::collections::HashMap;
use tracing::instrument; // For logging

use crate::models::error::ErrorResponse;
use crate::referencedata::{countries::countries, languages::languages};

type RefDataFn = fn() -> &'static HashMap<&'static str, &'static str>;

fn reference_data_map() -> HashMap<&'static str, RefDataFn> {
    HashMap::from([
        ("countries", countries as RefDataFn),
        ("languages", languages as RefDataFn),
        // Add more datasets here
    ])
}

#[utoipa::path(
    get,
    path = "/referencedata/{id}",
    tag = "reference_data",
    responses(
        (status = 200, description = "Successfully fetched reference data", body = HashMap<String, String>),
        (status = 404, description = "Reference data not found", body = ErrorResponse)
    )
)]
#[instrument]
pub async fn get_referencedata(Path(id): Path<String>) -> impl IntoResponse {
    if let Some(fetch_fn) = reference_data_map().get(id.as_str()) {
        let data = fetch_fn();
        Json(json!(data)).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Reference data '{}' not found", id) })),
        )
            .into_response()
    }
}
