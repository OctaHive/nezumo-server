//! PostgreSQL pool creation and embedded migration execution.
//!
//! Connection settings come from environment variables. Startup retries are
//! bounded, database URLs are redacted before logging, and migrations are
//! applied before the application begins serving requests.

use crate::core::config::{get_env, get_env_with_default};
use dotenvy::dotenv;
use sqlx::{migrate::MigrateError, migrate::Migrator, postgres::PgPoolOptions, PgPool};
use std::{fs, path::Path, time::Duration};
use thiserror::Error;

// ---------------------------
// Error Handling
// ---------------------------

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum DatabaseError {
    #[error("❌  Environment error: {0}")]
    EnvError(String),

    #[error("❌  Connection error: {0}")]
    ConnectionError(#[from] sqlx::Error),

    #[error("❌  File system error: {0}")]
    FileSystemError(String),

    #[error("❌  Configuration error: {0}")]
    ConfigError(String),

    #[error("❌  Migration error: {0}")]
    MigrationError(#[from] MigrateError),
}

// ---------------------------
// Database Connection
// ---------------------------

/// Establishes a secure connection to PostgreSQL with connection pooling
///
/// # Security Features
/// - Validates database URL format
/// - Enforces connection limits
/// - Uses environment variables securely
/// - Implements connection timeouts
///
/// # Returns
/// `Result<PgPool, DatabaseError>` - Connection pool or detailed error
pub async fn connect_to_database() -> Result<PgPool, DatabaseError> {
    // Load environment variables securely
    dotenv().ok();

    // Validate database URL presence and format
    let database_url = get_env("DATABASE_URL");
    if !database_url.starts_with("postgres://") {
        return Err(DatabaseError::ConfigError(
            "❌  Invalid DATABASE_URL format - must start with postgres://".to_string(),
        ));
    }
    let redacted_url = if let Some(at) = database_url.rfind('@') {
        let (left, right) = database_url.split_at(at);
        if let Some(scheme_end) = left.find("://") {
            format!("{}://***:***{}", &left[..scheme_end], right)
        } else {
            format!("***{}", right)
        }
    } else {
        "***".to_string()
    };
    println!("ℹ️  DATABASE_URL={}", redacted_url);

    // Configure connection pool with safety defaults
    let max_connections: u32 = get_env("DATABASE_MAX_CONNECTIONS").parse().unwrap_or(10);
    let min_connections: u32 = get_env("DATABASE_MIN_CONNECTIONS").parse().unwrap_or(2);

    let connect_retries: u32 = get_env_with_default("DATABASE_CONNECT_RETRIES", "10")
        .parse()
        .unwrap_or(10);
    let connect_delay_ms: u64 = get_env_with_default("DATABASE_CONNECT_RETRY_DELAY_MS", "1000")
        .parse()
        .unwrap_or(1000);
    let acquire_timeout_secs: u64 = get_env_with_default("DATABASE_ACQUIRE_TIMEOUT_SECS", "15")
        .parse()
        .unwrap_or(15);

    let mut last_err: Option<sqlx::Error> = None;
    for attempt in 1..=connect_retries {
        let pool_result = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(300))
            .test_before_acquire(true)
            .connect(&database_url)
            .await;

        match pool_result {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_err = Some(err);
                if attempt < connect_retries {
                    tokio::time::sleep(Duration::from_millis(connect_delay_ms)).await;
                }
            }
        }
    }

    Err(DatabaseError::ConnectionError(
        last_err.unwrap_or_else(|| sqlx::Error::PoolTimedOut),
    ))
}

// ---------------------------
// Database Migrations
// ---------------------------

/// Executes database migrations with safety checks
///
/// # Security Features
/// - Validates migrations directory existence
/// - Limits migration execution to development/staging environments
/// - Uses transactional migrations where supported
///
/// # Returns
/// `Result<(), DatabaseError>` - Success or detailed error
pub async fn run_database_migrations(pool: &PgPool) -> Result<(), DatabaseError> {
    // Skip migrations entirely in production
    let environment = get_env_with_default("ENVIRONMENT", "development");
    if environment == "production" {
        println!("🛑  Migration execution skipped in production.");
        return Ok(());
    }

    let migrations_path = Path::new("./migrations");

    // Validate migrations directory
    if !migrations_path.exists() {
        fs::create_dir_all(migrations_path).map_err(|e| {
            DatabaseError::FileSystemError(format!(
                "❌  Failed to create migrations directory: {}",
                e
            ))
        })?;
    }

    // Verify directory permissions
    let metadata = fs::metadata(migrations_path).map_err(|e| {
        DatabaseError::FileSystemError(format!("❌  Cannot access migrations directory: {}", e))
    })?;

    if metadata.permissions().readonly() {
        return Err(DatabaseError::FileSystemError(
            "❌  Migrations directory is read-only".to_string(),
        ));
    }

    // Initialize migrator with production safety checks
    let migrator = Migrator::new(migrations_path)
        .await
        .map_err(|e| DatabaseError::MigrationError(e))?;

    // Execute migrations in transaction if supported
    migrator
        .run(pool)
        .await
        .map_err(DatabaseError::MigrationError)?;

    Ok(())
}
