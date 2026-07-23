//! Generic success and error schemas shared by OpenAPI-documented endpoints.

use serde::Serialize;
use utoipa::ToSchema;

/// Represents a successful response from the API.
#[derive(Serialize, ToSchema)]
pub struct SuccessResponse {
    /// A message describing the successful operation.
    pub message: String,
}

/// Represents an error response from the API.
#[derive(Serialize, ToSchema)]
pub struct ErrorResponse {
    /// A description of the error that occurred.
    pub error: String,
}
