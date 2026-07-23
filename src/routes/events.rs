//! Board commit, event history, and snapshot routes.

use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use crate::handlers::events::{
    get_events_by_session, get_events_since, get_latest_snapshot, get_yrs_base, get_yrs_updates,
    post_commit_event, post_snapshot,
};
use crate::middlewares::auth::{authorize, optional_authorize};
use crate::routes::AppState;
use axum::middleware::from_fn_with_state;
use axum::{body::Body, extract::State, http::Request, middleware::Next};
use std::sync::Arc as StdArc;

/// Builds legacy event and canonical Yrs synchronization routes.
pub fn create_event_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let allowed_roles = StdArc::new(vec![1, 2]);

    let auth_layer = from_fn_with_state(
        state.clone(),
        move |State(state): State<Arc<AppState>>, req: Request<Body>, next: Next| {
            let allowed_roles = StdArc::clone(&allowed_roles);
            async move { authorize(allowed_roles, state, req, next).await }
        },
    );

    let optional_auth_layer = from_fn_with_state(
        state.clone(),
        |State(state): State<Arc<AppState>>, req: Request<Body>, next: Next| async move {
            optional_authorize(state, req, next).await
        },
    );

    // Authenticated-only routes (writes)
    let authed = Router::new()
        .route("/boards/{board_id}/events", post(post_commit_event))
        .route(
            "/boards/{board_id}/events/by-session",
            get(get_events_by_session),
        )
        .route("/boards/{board_id}/snapshots", post(post_snapshot))
        .layer(auth_layer);

    // Optionally-authenticated routes (reads — public boards accessible without auth)
    let maybe_authed = Router::new()
        .route("/boards/{board_id}/events", get(get_events_since))
        .route("/boards/{board_id}/yrs/base", get(get_yrs_base))
        .route("/boards/{board_id}/yrs/updates", get(get_yrs_updates))
        .route(
            "/boards/{board_id}/snapshots/latest",
            get(get_latest_snapshot),
        )
        .layer(optional_auth_layer);

    authed.merge(maybe_authed)
}
