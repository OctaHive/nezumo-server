//! Standard serializable API error representation.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Error response structure to standardize error outputs
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
    pub details: Option<String>, // Optional field to provide more error details
}

#[allow(dead_code)]
impl ErrorResponse {
    /// Creates an API error with an explicit public code and optional details.
    pub fn new(error: &str, details: Option<String>) -> Self {
        Self {
            error: error.to_string(),
            details,
        }
    }

    // For convenience, you could create a helper function for common error messages
    pub fn bad_request(details: Option<String>) -> Self {
        Self::new("Bad request", details)
    }

    /// Creates a standardized authentication-required error.
    pub fn unauthorized(details: Option<String>) -> Self {
        Self::new("Unauthorized", details)
    }

    /// Creates a standardized insufficient-permissions error.
    pub fn forbidden(details: Option<String>) -> Self {
        Self::new("Forbidden", details)
    }

    /// Creates a standardized internal-server-error response.
    pub fn internal_server_error(details: Option<String>) -> Self {
        Self::new("Internal server error", details)
    }
}
