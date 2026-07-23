//! Unauthenticated application health-check route.

use crate::routes::AppState;
use axum::Router;
use std::sync::Arc;

use crate::handlers::get_health::get_health;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds the service health-check route.
pub fn create_health_route(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_get("/health", get_health)
        .build()
}
