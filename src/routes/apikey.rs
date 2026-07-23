//! API-key listing, creation, rotation, and revocation routes.

use axum::Router;
use std::sync::Arc;

use crate::routes::AppState;

use crate::handlers::{
    delete_apikeys::delete_apikey_by_id,
    get_apikeys::{get_all_apikeys, get_apikeys_by_id},
    post_apikeys::post_apikey,
    rotate_apikeys::rotate_apikey,
};
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds authenticated API-key management routes.
pub fn create_apikey_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .get("/all", get_all_apikeys, vec![1, 2]) // Admins and managers
        .post("/new", post_apikey, vec![1]) // Admins only
        .get("/{id}", get_apikeys_by_id, vec![1, 2, 3]) // Admins, managers, and users
        .delete("/{id}", delete_apikey_by_id, vec![1]) // Admins only
        .post("/rotate/{id}", rotate_apikey, vec![1, 2]) // Admins and managers
        .build()
}
