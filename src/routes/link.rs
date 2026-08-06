//! Link-related utility routes.
//!
//! Public link metadata used by board cards. These routes reveal no board
//! content and must work on embed/public boards where the viewer has no session.

use axum::Router;
use std::sync::Arc;

use crate::handlers::embed_policy::get_link_embed_policy;
use crate::handlers::favicon::get_link_favicon;
use crate::handlers::site_preview::get_link_site_preview;
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds public link-preview and favicon routes.
pub fn create_link_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_get("/link/embed-policy", get_link_embed_policy)
        .unauthenticated_get("/link/favicon", get_link_favicon)
        .get("/link/site-preview", get_link_site_preview, vec![1, 2])
        .build()
}
