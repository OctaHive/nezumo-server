//! Application dependency initialization, Axum layers, jobs, and graceful shutdown.

use axum::http::{HeaderName, HeaderValue, Method};
use axum::Router;

// Middleware layers from tower_http
use tower_http::compression::{CompressionLayer, CompressionLevel}; // For HTTP response compression
use tower_http::cors::{AllowCredentials, AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer; // For HTTP request/response tracing

// Local crate imports for database connection and configuration
use crate::cache::connect::connect_to_cache; // Function to connect to cache
use crate::config; // Environment configuration helper
use crate::database::connect::connect_to_database; // Function to connect to the database
use crate::database::connect::run_database_migrations; // Function to run database migrations
use crate::jobs::previews::start_preview_job;
use crate::jobs::session_cleanup::start_session_cleanup_job;
use crate::jobs::yrs_compaction::start_yrs_compaction_job;
use crate::jobs::yrs_retention::start_yrs_retention_job;
use crate::mail::connect::connect_to_mail; // Function to connect to mail service
use crate::routes::create_routes; // Function to create application routes
use crate::storage::connect::connect_to_storage; // Function to connect to storage

use std::time::Duration;

use crate::realtime::RealtimeHub;
use crate::routes::AppState; // Application state structure
use std::sync::Arc; // For thread-safe reference counting

/// Function to create and configure the Axum server.
pub async fn create_server() -> Router<()> {
    // === Database Setup ===
    let database = connect_to_database()
        .await
        .expect("❌  Failed to connect to database.");
    println!("✔️   Connected to the database.");

    run_database_migrations(&database)
        .await
        .expect("❌  Failed to run database migrations.");

    // === Storage Setup ===
    let storage = connect_to_storage()
        .await
        .expect("❌  Failed to connect to storage.");
    println!("✔️   Connected to storage.");

    // === Cache Setup ===
    let cache = connect_to_cache()
        .await
        .expect("❌  Failed to connect to cache.");
    println!("✔️   Connected to cache.");

    // === Mail Setup ===
    let mail = connect_to_mail()
        .await
        .expect("❌  Failed to connect to mail.");
    println!("✔️   Connected to mail.");

    let realtime = RealtimeHub::new();
    // Resident canonical-coordinator registry and validator subprocess pool.
    // Boards activate lazily on their first canonical access or commit.
    let coordinators = crate::state::coordinator_registry::CoordinatorRegistry::from_env();
    let yrs_fanout = crate::state::yrs_fanout::CanonicalFanout::new(
        database.clone(),
        cache.clone(),
        realtime.clone(),
    );
    yrs_fanout.start();
    let shared_state = Arc::new(AppState {
        database: database,
        storage: storage,
        cache: cache,
        mail: mail,
        realtime: realtime,
        coordinators: coordinators,
        yrs_fanout,
    });

    start_preview_job(
        shared_state.clone(),
        config::get_env_u64("PREVIEW_JOB_INTERVAL_SECS", 60),
        config::get_env_u64("PREVIEW_JOB_BATCH_LIMIT", 10).min(i64::MAX as u64) as i64,
    );

    start_yrs_compaction_job(
        shared_state.database.clone(),
        config::get_env_u64("YRS_COMPACTION_INTERVAL_SECS", 300),
        config::get_env_u64("YRS_COMPACTION_BATCH_LIMIT", 10) as i64,
        config::get_env_u64("YRS_COMPACTION_MIN_UPDATES", 512) as i64,
        config::get_env_u64("YRS_COMPACTION_MIN_BYTES", 8 * 1024 * 1024) as i64,
    );

    // Canonical journal/event GC remains a separate opt-in and deletes only
    // data covered by verified immutable snapshots.
    if config::get_env_bool("YRS_JOURNAL_GC_ENABLED", false) {
        let interval_secs = config::get_env_u64("YRS_JOURNAL_GC_INTERVAL_SECS", 900);
        let retention_days = config::get_env_u64("YRS_JOURNAL_RETENTION_DAYS", 30) as i64;
        let batch_limit = config::get_env_u64("YRS_JOURNAL_GC_BATCH_LIMIT", 1_000) as i64;
        start_yrs_retention_job(
            shared_state.database.clone(),
            interval_secs,
            retention_days,
            batch_limit,
        );
    }

    // === Session Cleanup Job ===
    start_session_cleanup_job(shared_state.clone(), 30);

    // === Usage-record Flusher ===
    // The `authorize` middleware queues one usage record per authenticated
    // request into a global Vec (USAGE_QUEUE). This background task is the ONLY
    // thing that drains it (batch-inserts + clears every 60s). Without it the
    // queue grows unbounded for the whole process lifetime — a monotonic RSS
    // leak that raises the memory baseline and, under load, contributes to OOM.
    crate::middlewares::auth::start_batched_writes(shared_state.database.clone());

    // === Application Routes ===
    let mut app = create_routes(shared_state);

    // === Tracing Middleware ===
    if config::get_env_bool("SERVER_TRACE_ENABLED", true) {
        app = app.layer(TraceLayer::new_for_http());
        println!("✔️   Trace has been enabled.");
    }

    // === Compression Middleware ===
    if config::get_env_bool("SERVER_COMPRESSION_ENABLED", true) {
        let level = config::get_env("SERVER_COMPRESSION_LEVEL")
            .parse()
            .unwrap_or(6);
        app = app.layer(
            CompressionLayer::new()
                .br(true)
                .quality(CompressionLevel::Precise(level)),
        );
        println!(
            "✔️   Brotli compression enabled with compression quality level {}.",
            level
        );
    }

    // === CORS Middleware Configuration ===

    // Allowed HTTP methods for CORS
    let methods: Vec<Method> = config::get_env("CORS_ALLOW_METHODS")
        .parse()
        .unwrap_or("GET,POST,PUT,DELETE,OPTIONS".to_string())
        .split(',')
        .filter_map(|m| m.trim().parse().ok())
        .collect();

    // Allowed origins for CORS (comma-separated in .env)
    let cors_origins_raw = config::get_env("CORS_ALLOW_ORIGIN");
    let cors_origin_any = cors_origins_raw.split(',').any(|s| s.trim() == "*");
    let allowed_origins: Vec<HeaderValue> = cors_origins_raw
        .split(',')
        .filter(|s| !s.trim().is_empty() && s.trim() != "*")
        .map(|s| HeaderValue::from_str(s.trim()).expect("Invalid CORS_ALLOW_ORIGIN value."))
        .collect();

    // Allowed headers for CORS
    let allowed_headers = config::get_env("CORS_ALLOW_HEADERS")
        .parse()
        .unwrap_or("Authorization,Content-Type,Origin".to_string())
        .split(',')
        .map(|h| h.trim())
        .filter(|h| !h.is_empty())
        .map(|h| {
            HeaderName::from_bytes(h.as_bytes()).expect("Invalid header in CORS_ALLOW_HEADERS.")
        })
        .collect::<Vec<_>>();

    // CORS max age (preflight cache)
    let max_age_secs = config::get_env("CORS_MAX_AGE").parse().unwrap_or(3600);

    // Allow credentials in CORS
    let allow_credentials = config::get_env_bool("CORS_ALLOW_CREDENTIALS", false);

    // Print allowed origins for debugging
    println!(
        "✔️   CORS will be allowed for origin(s): {}",
        if cors_origin_any {
            "*".to_string()
        } else {
            allowed_origins
                .iter()
                .map(|hv| hv.to_str().unwrap_or("<invalid UTF-8>"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    );

    // Build the CORS layer
    let allow_origin = if cors_origin_any {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(allowed_origins)
    };
    let mut cors = CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods(methods)
        .allow_headers(allowed_headers)
        .max_age(Duration::from_secs(max_age_secs));
    if allow_credentials && !cors_origin_any {
        cors = cors.allow_credentials(AllowCredentials::yes());
    } else if allow_credentials && cors_origin_any {
        println!("⚠️   CORS_ALLOW_CREDENTIALS=true is not compatible with CORS_ALLOW_ORIGIN=\"*\". Credentials will be disabled.");
    }

    // === Attach CORS Middleware Globally ===
    app = app.layer(cors);

    // === Return the fully configured application ===
    app
}
