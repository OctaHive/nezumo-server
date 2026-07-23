//! Authenticated user profile, credential, and security-setting updates.

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    Json,
};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;

use crate::database::oauth_accounts::user_has_oauth_account;
use crate::database::users::{
    activate_user_in_db, deactivate_user_in_db, update_user_in_db, update_user_password_in_db,
};
use crate::models::error::ErrorResponse;
use crate::models::user::{
    User, UserChangePasswordBody, UserStatusResponse, UserUpdateBody, UserUpdateResponse,
};
use crate::routes::AppState;
use crate::utils::auth::{hash_password, verify_password};

use validator::Validate;

// --- Route Handler ---

/// Updates a user's profile fields with comprehensive validation
///
/// This endpoint allows a user to update their own profile, or an admin to update any user's profile.
/// Fields not included in the request body will remain unchanged. Fields set to `null` (if supported by the struct)
/// will be set to `NULL` in the database.
///
/// # Validation Layers
/// 1. **Structural Validation**: Handled by `UserUpdateBody`'s `deny_unknown_fields` attribute
/// 2. **Business Logic Validation**: Manual checks for role_level, tier_level, and birthday
///
/// # Request Flow
/// 1. Permission check (self or admin)
/// 2. UUID validation
/// 3. Business logic validation
/// 4. Database update
///
/// # Error Responses
/// - **400 Bad Request**: Automatic for unknown fields + manual validation errors
/// - **403 Forbidden**: Authorization failures
/// - **500 Internal Server Error**: Database errors
///
///  ToDo: Haven't been able to clean up the error messages. Deserialization fails in most cases.
#[utoipa::path(
    patch,
    path = "/users/{id}",
    tag = "user",
    security(("jwt_token" = [])),
    request_body = UserUpdateBody,
    params(("id" = String, Path, description = "User UUID or 'current'")),
    responses(
        (status = 200, description = "Profile updated successfully", body = UserUpdateResponse),
        (status = 400, description = "Validation error", body = ErrorResponse),
        (status = 403, description = "Not allowed", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
)]
#[instrument(skip(state, current_user, update))]
pub async fn patch_user_profile(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(update): Json<UserUpdateBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // --- Permission Validation ---
    let is_admin = current_user.role_level == 2;
    let target_user_id = if id == "current" {
        current_user.id
    } else {
        match uuid::Uuid::parse_str(&id) {
            Ok(uuid) => {
                if uuid != current_user.id && !is_admin {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(json!({ "error": "Not allowed" })),
                    ));
                }
                uuid
            }
            Err(_) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "Invalid UUID" })),
                ))
            }
        }
    };

    if target_user_id == current_user.id
        && update
            .status
            .as_deref()
            .is_some_and(|status| status.eq_ignore_ascii_case("disabled"))
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Administrators cannot deactivate their own account." })),
        ));
    }

    // --- Business Logic Validation ---
    let mut validation_errors = Vec::new();

    // Role Level Validation
    if let Some(role_level) = update.role_level {
        validate_role_level(
            role_level,
            is_admin,
            current_user.role_level,
            &mut validation_errors,
        );
    }

    // Tier Level Validation
    if let Some(tier_level) = update.tier_level {
        validate_tier_level(
            tier_level,
            is_admin,
            current_user.tier_level,
            &mut validation_errors,
        );
    }

    // Status Validation (admin only)
    if let Some(ref status) = update.status {
        validate_status(status, is_admin, &mut validation_errors);
    }

    // Birthday Validation
    if let Some(birthday) = update.birthday {
        validate_birthday(birthday, &mut validation_errors);
    }

    // --- Error Handling ---
    if !validation_errors.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Validation failed",
                "details": validation_errors
            })),
        ));
    }

    if let Err(validation_errors) = update.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Validation failed",
                "details": validation_errors
            })),
        ));
    }

    // --- Database Operation ---
    match update_user_in_db(&state.database, target_user_id, update).await {
        Ok(_) => Ok(Json(json!({ "success": true }))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("Database error: {}", e) })),
        )),
    }
}

// --- Validation Helpers ---

/// Validates role level changes
/// - Admins can only set 1 (regular) or 2 (admin)
/// - Regular users can't change their role
fn validate_role_level(
    new_level: i32,
    is_admin: bool,
    current_level: i32,
    errors: &mut Vec<String>,
) {
    if is_admin {
        if ![1, 2].contains(&new_level) {
            errors.push("Role level must be 1 (regular) or 2 (admin)".into());
        }
    } else if new_level != current_level {
        errors.push("Cannot modify your own role level".into());
    }
}

/// Validates tier level changes
/// - Admins can set 1-4
/// - Regular users can't change their tier
fn validate_tier_level(
    new_level: i32,
    is_admin: bool,
    current_level: i32,
    errors: &mut Vec<String>,
) {
    if is_admin {
        if !(1..=4).contains(&new_level) {
            errors.push("Tier level must be between 1-4".into());
        }
    } else if new_level != current_level {
        errors.push("Cannot modify your own tier level".into());
    }
}

/// Validates status changes
/// - Admins can set "active" or "disabled"
/// - Regular users cannot change status
fn validate_status(status: &str, is_admin: bool, errors: &mut Vec<String>) {
    if !is_admin {
        errors.push("Cannot modify user status".into());
        return;
    }

    let normalized = status.to_lowercase();
    if normalized != "active" && normalized != "disabled" {
        errors.push("Status must be 'active' or 'disabled'".into());
    }
}

/// Validates birthday is not in the future
fn validate_birthday(birthday: Option<chrono::NaiveDate>, errors: &mut Vec<String>) {
    if let Some(bdate) = birthday {
        let today = chrono::Utc::now().naive_utc().date();
        if bdate > today {
            errors.push("Birthday cannot be in the future".into());
        }
    }
}

#[utoipa::path(
    post,
    path = "/users/{id}/activate",
    tag = "user",
    params(("id" = String, Path, description = "User UUID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "User activated", body = UserStatusResponse),
        (status = 400, description = "Invalid user ID"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state))]
pub async fn activate_user(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<UserStatusResponse>, (StatusCode, Json<serde_json::Value>)> {
    change_user_status(&state, &id, true).await
}

#[utoipa::path(
    post,
    path = "/users/{id}/deactivate",
    tag = "user",
    params(("id" = String, Path, description = "User UUID")),
    security(("jwt_token" = [])),
    responses(
        (status = 200, description = "User deactivated", body = UserStatusResponse),
        (status = 400, description = "Invalid user ID or self-deactivation"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[instrument(skip(state, current_user))]
pub async fn deactivate_user(
    Path(id): Path<String>,
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
) -> Result<Json<UserStatusResponse>, (StatusCode, Json<serde_json::Value>)> {
    let user_id = parse_user_id(&id)?;
    if user_id == current_user.id {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Administrators cannot deactivate their own account." })),
        ));
    }
    change_user_status_by_id(&state, user_id, false).await
}

async fn change_user_status(
    state: &AppState,
    id: &str,
    active: bool,
) -> Result<Json<UserStatusResponse>, (StatusCode, Json<serde_json::Value>)> {
    change_user_status_by_id(state, parse_user_id(id)?, active).await
}

async fn change_user_status_by_id(
    state: &AppState,
    user_id: uuid::Uuid,
    active: bool,
) -> Result<Json<UserStatusResponse>, (StatusCode, Json<serde_json::Value>)> {
    let changed = if active {
        activate_user_in_db(&state.database, user_id).await
    } else {
        deactivate_user_in_db(&state.database, user_id).await
    }
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Could not update user status." })),
        )
    })?;
    if !changed {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "User not found." })),
        ));
    }
    Ok(Json(UserStatusResponse {
        user_id,
        status: if active { "active" } else { "disabled" }.to_string(),
    }))
}

fn parse_user_id(id: &str) -> Result<uuid::Uuid, (StatusCode, Json<serde_json::Value>)> {
    uuid::Uuid::parse_str(id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Invalid UUID" })),
        )
    })
}

/// Changes the authenticated user's password
///
/// Verifies the current password before updating to the new one.
#[utoipa::path(
    post,
    path = "/users/current/change-password",
    tag = "user",
    security(("jwt_token" = [])),
    request_body = UserChangePasswordBody,
    responses(
        (status = 200, description = "Password changed successfully"),
        (status = 400, description = "Validation error", body = ErrorResponse),
        (status = 401, description = "Current password is incorrect", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
)]
#[instrument(skip(state, current_user, body))]
pub async fn change_password(
    State(state): State<Arc<AppState>>,
    Extension(current_user): Extension<User>,
    Json(body): Json<UserChangePasswordBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // Validate request body
    if let Err(validation_errors) = body.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Validation failed",
                "details": validation_errors
            })),
        ));
    }

    // Check if user has an OAuth account (can skip current_password)
    let has_oauth = user_has_oauth_account(&state.database, current_user.id)
        .await
        .unwrap_or(false);

    // Verify current password (required for non-OAuth users)
    match &body.current_password {
        Some(current_pw) => {
            match verify_password(current_pw.clone(), current_user.password_hash.clone()).await {
                Ok(true) => {}
                Ok(false) => {
                    return Err((
                        StatusCode::UNAUTHORIZED,
                        Json(json!({ "error": "Current password is incorrect" })),
                    ));
                }
                Err(_) => {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "error": "Failed to verify password" })),
                    ));
                }
            }
        }
        None => {
            if !has_oauth {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "Current password is required" })),
                ));
            }
        }
    }

    // Hash the new password
    let new_hash = hash_password(&body.new_password).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "Failed to hash new password" })),
        )
    })?;

    // Update password in database
    update_user_password_in_db(&state.database, current_user.id, &new_hash)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Database error: {}", e) })),
            )
        })?;

    Ok(Json(json!({ "success": true })))
}
