//! Unauthenticated country and language reference-data routes.

use crate::routes::AppState;
use axum::Router;
use std::sync::Arc;

use crate::handlers::get_referencedata::get_referencedata;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds public country and language reference-data routes.
pub fn create_referencedata_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        // Route for getting the usage from the last day
        .unauthenticated_get("/referencedata/{id}", get_referencedata)
        .build()
}
