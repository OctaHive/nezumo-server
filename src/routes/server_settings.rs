//! Public capability flags and administrator-only server settings routes.

use std::sync::Arc;

use axum::Router;

use crate::handlers::server_settings::{
    get_admin_server_settings, get_public_server_settings, patch_admin_server_settings,
    patch_tier_settings,
};
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

pub fn create_server_settings_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_get("/server-settings/public", get_public_server_settings)
        // Register both variants because some reverse proxies remove a trailing
        // slash while others preserve it.
        .get("/server-settings", get_admin_server_settings, vec![2])
        .get("/server-settings/", get_admin_server_settings, vec![2])
        .patch("/server-settings", patch_admin_server_settings, vec![2])
        .patch("/server-settings/", patch_admin_server_settings, vec![2])
        .patch(
            "/server-settings/tiers/{level}",
            patch_tier_settings,
            vec![2],
        )
        .build()
}
