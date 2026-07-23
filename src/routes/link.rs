//! Link-related utility routes.
//!
//! Currently just the favicon resolver (`GET /link/favicon?url=…`), which is
//! public: it reveals no board content and must work on embed/public boards where
//! the viewer has no session.

use axum::Router;
use std::sync::Arc;

use crate::handlers::favicon::get_link_favicon;
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds public link-preview and favicon routes.
pub fn create_link_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_get("/link/favicon", get_link_favicon)
        .build()
}
