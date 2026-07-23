//! Persistence for external OAuth/OpenID account links.
//!
//! Provider identities are upserted against local users so repeated sign-ins
//! update the existing association instead of creating duplicate links.

use sqlx::PgPool;
use uuid::Uuid;

/// Returns `true` if the user has at least one linked OAuth account.
pub async fn user_has_oauth_account(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let row = sqlx::query("SELECT 1 FROM oauth_accounts WHERE user_id = $1 LIMIT 1")
        .bind(user_id)
        .fetch_optional(pool)
        .await?;

    Ok(row.is_some())
}

/// Inserts or refreshes the provider identity linked to a local user.
pub async fn upsert_oauth_account(
    pool: &PgPool,
    user_id: Uuid,
    provider: &str,
    provider_user_id: &str,
    access_token: &str,
    refresh_token: Option<&str>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO oauth_accounts (user_id, provider, provider_user_id, access_token, refresh_token, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (provider, provider_user_id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            access_token = EXCLUDED.access_token,
            refresh_token = EXCLUDED.refresh_token,
            expires_at = EXCLUDED.expires_at
        "#,
        user_id,
        provider,
        provider_user_id,
        access_token,
        refresh_token,
        expires_at
    )
    .execute(pool)
    .await?;

    Ok(())
}
