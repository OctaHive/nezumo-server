//! Support report, feature request, attachment, and proxy routes.
//!
//! Applies support-specific body limits, authentication, and rate-limit
//! middleware around handlers that may create external GitHub issues.

use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn_with_state;
use axum::{
    body::Body,
    extract::State,
    http::Request,
    middleware::Next,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use crate::handlers::support::{get_support_attachment, post_feature_request, post_support_report};
use crate::middlewares::auth::authorize;
use crate::routes::AppState;

/// `POST /support/reports` — "report a problem" → GitHub issue. Requires an
/// authenticated user (roles 1/2); the reporter's identity is attached and used
/// for the per-user daily rate limit.
pub fn create_support_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let allowed_roles = Arc::new(vec![1, 2]);
    let auth_layer = from_fn_with_state(
        state.clone(),
        move |State(state): State<Arc<AppState>>, req: Request<Body>, next: Next| {
            let allowed_roles = Arc::clone(&allowed_roles);
            async move { authorize(allowed_roles, state, req, next).await }
        },
    );

    // Report submission: authenticated only, raised body limit for media.
    let reports = Router::new()
        .route("/support/reports", post(post_support_report))
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(auth_layer);

    // Public website form. JSON-only and intentionally unauthenticated; the
    // handler applies a smaller rate limit by requester IP.
    let feature_requests = Router::new()
        .route("/support/feature-requests", post(post_feature_request))
        .layer(DefaultBodyLimit::max(64 * 1024));

    // Attachment proxy: public (the signed `t` token in the URL is the access
    // control) so GitHub's image proxy and maintainers can fetch the file.
    let attachments = Router::new().route(
        "/support/attachments/{report_id}",
        get(get_support_attachment),
    );

    reports.merge(feature_requests).merge(attachments)
}
