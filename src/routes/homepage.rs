//! Unauthenticated HTML landing page for the versioned API root.

use crate::routes::AppState;
use axum::Router;
use std::sync::Arc;

use crate::handlers::homepage::homepage;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds the public homepage route.
pub fn create_homepage_route(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_get("/", homepage)
        .build()
}
