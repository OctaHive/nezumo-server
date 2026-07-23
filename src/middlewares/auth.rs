//! JWT authorization middleware and batched user-activity persistence.

use axum::{
    body::Body,
    http::StatusCode, // HTTP response and status codes
};

use sqlx::{PgPool, Postgres, QueryBuilder}; // For interacting with PostgreSQL databases asynchronously
use tracing::instrument;
use uuid::Uuid; // For working with UUIDs // For logging

// New imports for caching and batched writes
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::interval;

// Importing custom database query functions
use crate::database::users::fetch_active_user_by_email_from_db;

use crate::core::config::get_env_bool; // For fetching environment variables
use crate::models::auth::AuthError; // Import the AuthError struct for error handling
use crate::routes::AppState;
use crate::utils::auth::{decode_jwt, extract_token_from_cookie, extract_token_from_header}; // For extacting the application state from the request

// New struct for caching rate limit data
#[derive(Clone)]
struct CachedRateLimit {
    tier_limit: i64,
    request_count: i64,
}

// New struct for batched usage records
#[allow(dead_code)]
#[derive(Clone, Debug)]
struct UsageRecord {
    user_id: Uuid,
    path: String,
}

// Global cache and batched writes queue
lazy_static::lazy_static! {
    static ref RATE_LIMIT_CACHE: Cache<(Uuid, i32), CachedRateLimit> = Cache::builder()
        .time_to_live(Duration::from_secs(300)) // 5 minutes cache lifetime
        .build();
    static ref USAGE_QUEUE: Arc<Mutex<Vec<UsageRecord>>> = Arc::new(Mutex::new(Vec::new()));
}

/// Hard cap on the pending usage queue. The flusher drains every 60s, but if DB
/// writes stall under load the queue could still balloon between ticks. Usage
/// records are analytics, so dropping them past this bound is a safe last-resort
/// guard against an unbounded RSS climb toward OOM.
const MAX_USAGE_QUEUE: usize = 50_000;

// Function to start the background task for batched writes.
// Started once at server boot (see core::server) — it is the only drainer of
// USAGE_QUEUE, so it MUST run or the queue leaks unbounded.
pub fn start_batched_writes(pool: PgPool) {
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(60)); // Run every minute
        loop {
            interval.tick().await;
            flush_usage_queue(&pool).await;
        }
    });
}

// Function to flush the usage queue and perform batch inserts
#[instrument(skip(pool))]
async fn flush_usage_queue(pool: &PgPool) {
    let mut queue = USAGE_QUEUE.lock().await;
    if queue.is_empty() {
        return;
    }

    // Prepare batch insert. The column is `endpoint` (VARCHAR(255)) — NOT `path`;
    // the old query named `path`, which does not exist, so every flush errored out
    // (which is why the flusher had been left disabled). `creation_date` is a DATE
    // with `DEFAULT CURRENT_DATE`, so we omit it rather than binding a timestamptz
    // (`Utc::now()`), which Postgres would reject against a DATE column. Endpoints
    // are truncated to the column width defensively.
    let mut query_builder: QueryBuilder<Postgres> =
        QueryBuilder::new("INSERT INTO usage (user_id, endpoint) ");

    query_builder.push_values(queue.iter(), |mut b, record| {
        let endpoint: &str = if record.path.len() > 255 {
            let mut end = 255;
            while end > 0 && !record.path.is_char_boundary(end) {
                end -= 1;
            }
            &record.path[..end]
        } else {
            &record.path
        };
        b.push_bind(record.user_id).push_bind(endpoint);
    });

    // Execute batch insert
    let result = query_builder.build().execute(pool).await;

    match result {
        Ok(_) => {
            tracing::debug!(
                "Successfully inserted {} usage records in batch.",
                queue.len()
            );
        }
        Err(e) => {
            tracing::error!("Error inserting batch usage records: {}", e);
        }
    }
    // Clear the queue
    queue.clear();
}

// Middleware for role-based access control (RBAC)
// Ensures that only users with specific roles are authorized to access certain resources
// Skip `state` in the span: its Debug dumps the entire AppState (S3 config,
// Redis password, SMTP creds) into every log event emitted within a request.
#[instrument(skip(req, next, state))]
pub async fn authorize(
    allowed_roles: Arc<Vec<i32>>,
    state: Arc<AppState>, // App state, including the database connection
    mut req: axum::extract::Request<Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, AuthError> {
    let database = &state.database;

    // Fetch environment variables for cookie-based authentication
    let allow_cookie_auth = get_env_bool("JWT_ALLOW_COOKIE_AUTH", false);
    let force_cookie_auth = get_env_bool("JWT_FORCE_COOKIE_AUTH", false);

    // Extract the token based on the environment configuration
    let token_opt = match (allow_cookie_auth, force_cookie_auth) {
        (true, true) => extract_token_from_cookie(&req),
        (true, false) => {
            extract_token_from_header(&req).or_else(|| extract_token_from_cookie(&req))
        }
        (false, _) => extract_token_from_header(&req),
    };

    // If no token is found, return an error
    let token = token_opt.ok_or_else(|| AuthError {
        message: "Authorization token missing.".to_string(),
        status_code: StatusCode::UNAUTHORIZED,
    })?;

    // Decode the JWT securely
    let token_data = decode_jwt(token)?;

    // Fetch the user from the database using the email from the decoded token
    let current_user = fetch_active_user_by_email_from_db(&database, &token_data.claims.sub)
        .await
        .map_err(|_| AuthError {
            message: "Unauthorized user.".to_string(),
            status_code: StatusCode::UNAUTHORIZED,
        })?
        .ok_or_else(|| AuthError {
            message: "User not found.".to_string(),
            status_code: StatusCode::UNAUTHORIZED,
        })?;

    // Check if the user's role is in the list of allowed roles
    if !allowed_roles.contains(&current_user.role_level) {
        return Err(AuthError {
            message: "Forbidden: insufficient role.".to_string(),
            status_code: StatusCode::FORBIDDEN,
        });
    }

    // Check rate limit using cached data
    check_rate_limit(&database, current_user.id, current_user.tier_level).await?;

    // Queue the usage record for batch insert instead of immediate insertion.
    // Bounded by MAX_USAGE_QUEUE so a stalled flusher can never leak unbounded.
    {
        let mut queue = USAGE_QUEUE.lock().await;
        if queue.len() < MAX_USAGE_QUEUE {
            queue.push(UsageRecord {
                user_id: current_user.id,
                path: req.uri().path().to_string(),
            });
        }
    }

    // Insert the current user into the request extensions for use in subsequent handlers
    req.extensions_mut().insert(current_user);

    // Proceed to the next middleware or handler
    Ok(next.run(req).await)
}

/// Like `authorize`, but does not reject the request when no token is present.
/// If a valid token exists, inserts `User` into extensions as usual.
/// If no token or an invalid token is found, the request proceeds without a user.
#[instrument(skip(req, next, state))]
pub async fn optional_authorize(
    state: Arc<AppState>,
    mut req: axum::extract::Request<Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let database = &state.database;
    let allow_cookie_auth = get_env_bool("JWT_ALLOW_COOKIE_AUTH", false);
    let force_cookie_auth = get_env_bool("JWT_FORCE_COOKIE_AUTH", false);

    let token_opt = match (allow_cookie_auth, force_cookie_auth) {
        (true, true) => extract_token_from_cookie(&req),
        (true, false) => {
            extract_token_from_header(&req).or_else(|| extract_token_from_cookie(&req))
        }
        (false, _) => extract_token_from_header(&req),
    };

    if let Some(token) = token_opt {
        if let Ok(token_data) = decode_jwt(token) {
            if let Ok(Some(user)) =
                fetch_active_user_by_email_from_db(database, &token_data.claims.sub).await
            {
                req.extensions_mut().insert(user);
            }
        }
    }

    next.run(req).await
}

// Function to check rate limits for a user
#[instrument(skip(database))]
async fn check_rate_limit(
    database: &PgPool,
    user_id: Uuid,
    tier_level: i32,
) -> Result<(), AuthError> {
    // Try to get cached rate limit data
    if let Some(cached) = RATE_LIMIT_CACHE.get(&(user_id, tier_level)).await {
        if cached.request_count >= cached.tier_limit {
            return Err(AuthError {
                message: "Rate limit exceeded".to_string(),
                status_code: StatusCode::TOO_MANY_REQUESTS,
            });
        }
        // Update cache with incremented request count
        RATE_LIMIT_CACHE
            .insert(
                (user_id, tier_level),
                CachedRateLimit {
                    tier_limit: cached.tier_limit,
                    request_count: cached.request_count + 1,
                },
            )
            .await;
        return Ok(());
    }

    // If not in cache, fetch from database
    let tier_limit = sqlx::query!(
        "SELECT requests_per_day FROM tiers WHERE level = $1",
        tier_level
    )
    .fetch_one(database)
    .await
    .map_err(|_| AuthError {
        message: "Failed to fetch tier information".to_string(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
    })?
    .requests_per_day as i64;

    // Count user's requests for today
    let request_count = sqlx::query!(
        "SELECT COUNT(*) as count FROM usage WHERE user_id = $1 AND creation_date > NOW() - INTERVAL '24 hours'",
        user_id
    )
    .fetch_one(database)
    .await
    .map_err(|_| AuthError {
        message: "Failed to count user requests".to_string(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
    })?
    .count
    .unwrap_or(0) as i64; // Use 0 if count is NULL

    // Cache the result
    RATE_LIMIT_CACHE
        .insert(
            (user_id, tier_level),
            CachedRateLimit {
                tier_limit,
                request_count,
            },
        )
        .await;

    if request_count >= tier_limit {
        return Err(AuthError {
            message: "Rate limit exceeded".to_string(),
            status_code: StatusCode::TOO_MANY_REQUESTS,
        });
    }

    Ok(())
}
