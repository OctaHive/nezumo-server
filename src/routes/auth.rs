//! Authentication routes for login, TOTP completion, logout, and OAuth flows.

use crate::routes::AppState;
use axum::Router;
use std::sync::Arc;

use crate::handlers::{
    login::{login, login_totp},
    logout::logout,
    oauth::{oauth_google, oauth_google_callback, oauth_google_exchange},
    protected::protected,
};
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds login, logout, OAuth, registration, and password-reset routes.
pub fn create_auth_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_post("/login", login)
        .unauthenticated_post("/login/totp", login_totp)
        .unauthenticated_post("/logout", logout)
        .unauthenticated_get("/oauth/google", oauth_google)
        .unauthenticated_get("/oauth/google/callback", oauth_google_callback)
        .unauthenticated_post("/oauth/google/exchange", oauth_google_exchange)
        .get("/me", protected, vec![1, 2])
        .build()
}
