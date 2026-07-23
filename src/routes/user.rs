//! User registration, profile, preferences, password, and account routes.
//!
//! Public registration/reset endpoints and authenticated user-management
//! endpoints are assembled separately before being merged into the API router.

use crate::routes::AppState;
use axum::Router;
use std::sync::Arc;

use crate::handlers::{
    delete_users::delete_user_by_id,
    get_users::{get_all_users, get_users_by_id, search_users},
    patch_users::{activate_user, change_password, deactivate_user, patch_user_profile},
    post_users::{
        post_user, post_user_password_reset, post_user_password_reset_verify,
        post_user_profilepicture, post_user_register, post_user_register_complete,
        post_user_register_verify,
    },
    preferences::{get_color_preferences, update_color_preferences_handler},
    quotas::{get_current_quota, get_tiers},
    totp::{confirm_totp, disable_totp, get_totp_status, reset_user_totp, setup_totp},
};
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds authenticated user profile and account-management routes.
pub fn create_user_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        // Route for getting all users (requires role 2)
        .get("/all", get_all_users, vec![2])
        // Authoritative tier names and resource limits for administration UI.
        .get("/tiers", get_tiers, vec![2])
        // Route for creating a new user (requires role 2)
        .post("/new", post_user, vec![2])
        // Route for requesting a password reset (unauthenticated)
        .unauthenticated_post("/password-reset", post_user_password_reset)
        // Route for confirming password reset (unauthenticated)
        .unauthenticated_post("/password-reset/confirm", post_user_password_reset_verify)
        // Route for requesting a password reset (unauthenticated)
        .unauthenticated_post("/register", post_user_register)
        // Route for verifying email (unauthenticated)
        .unauthenticated_post("/register/confirm", post_user_register_verify)
        // Route for completing registration (unauthenticated)
        .unauthenticated_post("/register/complete", post_user_register_complete)
        // Route for changing password (authenticated)
        .post("/current/change-password", change_password, vec![1, 2])
        // The personal quota is used for proactive UI limits. Mutating routes
        // still enforce the same policy transactionally.
        .get("/current/quota", get_current_quota, vec![1, 2])
        // Explicit administrator actions used by the user-management UI.
        .post("/{id}/activate", activate_user, vec![2])
        .post("/{id}/deactivate", deactivate_user, vec![2])
        .post("/{id}/totp/reset", reset_user_totp, vec![2])
        // Route for searching users by email or username
        .get("/search", search_users, vec![1, 2])
        // Per-user color picker preferences (recent + custom colors). Literal
        // paths registered before the `/{id}` capture group below.
        .get("/current/preferences", get_color_preferences, vec![1, 2])
        .patch(
            "/current/preferences",
            update_color_preferences_handler,
            vec![1, 2],
        )
        .get("/current/totp", get_totp_status, vec![1, 2])
        .post("/current/totp/setup", setup_totp, vec![1, 2])
        .post("/current/totp/confirm", confirm_totp, vec![1, 2])
        .post("/current/totp/disable", disable_totp, vec![1, 2])
        // Route for adding profile pictures.
        .post(
            "/{id}/profile-picture",
            post_user_profilepicture,
            vec![1, 2],
        )
        // Route for getting user by ID (requires roles 1 or 2)
        .get("/{id}", get_users_by_id, vec![1, 2])
        // Route for updating user profile fields (requires roles 1 or 2)
        .patch("/{id}", patch_user_profile, vec![1, 2])
        // Route for deleting a user by ID (requires role 2)
        .delete("/{id}", delete_user_by_id, vec![2])
        .build()
}

/// Builds root-level user routes that do not share the `/users` prefix.
pub fn create_user_root_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        // Route for requesting a password reset (unauthenticated)
        .unauthenticated_post("/reset", post_user_password_reset)
        // Route for confirming password reset (unauthenticated)
        .unauthenticated_post("/reset/verify", post_user_password_reset_verify)
        // Route for requesting a password reset (unauthenticated)
        .unauthenticated_post("/register", post_user_register)
        // Route for confirming password reset (unauthenticated)
        .unauthenticated_post("/register/verify", post_user_register_verify)
        // Route for completing registration (unauthenticated)
        .unauthenticated_post("/register/complete", post_user_register_complete)
        .build()
}
