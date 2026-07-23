//! Short-lived login challenge persistence for multi-step authentication.
//!
//! A newly created challenge replaces older challenges for the same user.
//! Consumers must validate expiry and delete challenges after successful use.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug)]
pub struct LoginChallengeRow {
    pub id: Uuid,
    pub user_id: Uuid,
    pub expires_at: DateTime<Utc>,
}

/// Persists a short-lived login challenge for a multi-step authentication flow.
pub async fn create_login_challenge(
    pool: &PgPool,
    user_id: Uuid,
    expires_at: DateTime<Utc>,
) -> Result<LoginChallengeRow, sqlx::Error> {
    sqlx::query!("DELETE FROM login_challenges WHERE user_id = $1", user_id)
        .execute(pool)
        .await?;

    sqlx::query_as!(
        LoginChallengeRow,
        r#"
        INSERT INTO login_challenges (user_id, expires_at)
        VALUES ($1, $2)
        RETURNING id, user_id, expires_at
        "#,
        user_id,
        expires_at
    )
    .fetch_one(pool)
    .await
}

/// Fetches a non-expired login challenge by identifier.
pub async fn fetch_login_challenge(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<LoginChallengeRow>, sqlx::Error> {
    sqlx::query_as!(
        LoginChallengeRow,
        r#"
        SELECT id, user_id, expires_at
        FROM login_challenges
        WHERE id = $1
        "#,
        id
    )
    .fetch_optional(pool)
    .await
}

/// Deletes a consumed or expired login challenge.
pub async fn delete_login_challenge(pool: &PgPool, id: Uuid) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!("DELETE FROM login_challenges WHERE id = $1", id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Records an invalid TOTP code and consumes an exhausted login challenge.
pub async fn record_failed_attempt(
    pool: &PgPool,
    id: Uuid,
    max_attempts: i16,
) -> Result<Option<i16>, sqlx::Error> {
    let attempts = sqlx::query_scalar::<_, i16>(
        r#"
        UPDATE login_challenges
        SET attempts = attempts + 1
        WHERE id = $1
        RETURNING attempts
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    if attempts.is_some_and(|attempts| attempts >= max_attempts) {
        delete_login_challenge(pool, id).await?;
    }
    Ok(attempts)
}
