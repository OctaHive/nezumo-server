//! Authentication claims, login payloads, challenges, and API error responses.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Represents the claims to be included in a JWT payload.
#[derive(Serialize, Deserialize, ToSchema)]
pub struct Claims {
    /// Subject of the token (e.g., user ID or email).
    pub sub: String,

    /// Timestamp when the token was issued.
    pub iat: usize,

    /// Timestamp when the token will expire.
    pub exp: usize,

    /// Issuer of the token (optional).
    pub iss: String,

    /// Intended audience for the token (optional).
    pub aud: String,
}

/// Custom error type for handling authentication-related errors.
/// Struct for authentication and authorization errors
#[derive(Debug, Serialize)]
pub struct AuthError {
    pub message: String,
    #[serde(skip_serializing)]
    pub status_code: StatusCode,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "error": self.message,
        }));

        (self.status_code, body).into_response()
    }
}

/// Data structure for user sign-in information.
///
/// This includes the user's email and password.
#[derive(Deserialize, ToSchema)]
pub struct LoginData {
    /// User's email address.
    pub email: String,
    /// User's password.
    pub password: String,
}

/// Data structure for completing a TOTP login challenge.
#[derive(Deserialize, ToSchema)]
pub struct LoginTotpData {
    /// Login challenge id returned from /login.
    pub challenge_id: Uuid,
    /// TOTP code for two-factor authentication.
    pub totp: String,
}

/// Response returned when TOTP is required after password verification.
#[derive(Serialize, ToSchema)]
pub struct LoginChallengeResponse {
    pub challenge_id: Uuid,
    pub expires_at: DateTime<Utc>,
}

/// Current TOTP configuration state for the authenticated user.
#[derive(Serialize, ToSchema)]
pub struct TotpStatusResponse {
    pub enabled: bool,
    pub setup_pending: bool,
}

/// One-time enrollment material returned when starting TOTP setup.
#[derive(Serialize, ToSchema)]
pub struct TotpSetupResponse {
    pub secret: String,
    pub provisioning_uri: String,
    pub expires_at: DateTime<Utc>,
    pub algorithm: String,
    pub digits: u8,
    pub period: u16,
}

/// Confirms a pending TOTP enrollment with the first authenticator code.
#[derive(Deserialize, ToSchema)]
pub struct TotpConfirmData {
    pub totp: String,
}

/// Confirms disabling TOTP with a current authenticator code.
#[derive(Deserialize, ToSchema)]
pub struct TotpDisableData {
    pub totp: String,
}
