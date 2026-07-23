//! Per-board embed-token persistence — view-only secrets that let a board be
//! embedded in a third-party `<iframe>` (incl. private boards) without a login
//! session. Runtime queries (no compile-time `query!` macro) so the crate builds
//! without a live DB / offline cache.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Creates a revocable token for unauthenticated embedded board access.
pub async fn create_embed_token(
    pool: &PgPool,
    board_id: Uuid,
    token: &str,
    created_by: Uuid,
    expires_at: Option<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO board_embed_tokens (board_id, token, created_by, expires_at)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(board_id)
    .bind(token)
    .bind(created_by)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Resolve a token secret to its board if it exists and has not expired.
pub async fn get_valid_embed_token(
    pool: &PgPool,
    token: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT board_id FROM board_embed_tokens
         WHERE token = $1 AND (expires_at IS NULL OR expires_at > now())",
    )
    .bind(token)
    .fetch_optional(pool)
    .await
}

/// The most recent non-expired token for a board (the one the embed UI shows).
pub async fn latest_embed_token(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT token FROM board_embed_tokens
         WHERE board_id = $1 AND (expires_at IS NULL OR expires_at > now())
         ORDER BY created_at DESC
         LIMIT 1",
    )
    .bind(board_id)
    .fetch_optional(pool)
    .await
}

/// Revoke every embed token for a board (used to rotate / disable embedding).
pub async fn delete_embed_tokens(pool: &PgPool, board_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM board_embed_tokens WHERE board_id = $1")
        .bind(board_id)
        .execute(pool)
        .await?;
    Ok(())
}
