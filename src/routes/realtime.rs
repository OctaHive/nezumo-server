//! Board WebSocket collaboration and current-session routes.

use axum::Router;
use std::sync::Arc;

use crate::handlers::realtime::{get_board_sessions, ws_board};
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

/// Builds websocket and active-session inspection routes.
pub fn create_realtime_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .maybe_authenticated_get("/boards/{board_id}/ws", ws_board)
        .maybe_authenticated_get("/boards/{board_id}/sessions", get_board_sessions)
        .build()
}
