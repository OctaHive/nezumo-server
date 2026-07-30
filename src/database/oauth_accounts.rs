//! Persistence for external OAuth/OpenID account links.
//!
//! Provider identities are upserted against local users so repeated sign-ins
//! update the existing association instead of creating duplicate links.

use sqlx::PgPool;
use uuid::Uuid;

use crate::models::user::{User, UserRow};

/// Returns `true` if the user has at least one linked OAuth account.
pub async fn user_has_oauth_account(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let row = sqlx::query("SELECT 1 FROM oauth_accounts WHERE user_id = $1 LIMIT 1")
        .bind(user_id)
        .fetch_optional(pool)
        .await?;

    Ok(row.is_some())
}

/// Finds the local user already linked to an external provider identity.
pub async fn fetch_user_by_oauth_account(
    pool: &PgPool,
    provider: &str,
    provider_user_id: &str,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as::<_, UserRow>(
        r#"
        SELECT users.id, users.username, users.email, users.password_hash, users.totp_secret,
               users.role_level, users.tier_level, users.status, users.creation_date,
               users.profile_picture_url, users.first_name, users.last_name,
               users.country_code, users.language_code, users.birthday, users.description
        FROM oauth_accounts
        JOIN users ON users.id = oauth_accounts.user_id
        WHERE oauth_accounts.provider = $1 AND oauth_accounts.provider_user_id = $2
        "#,
    )
    .bind(provider)
    .bind(provider_user_id)
    .fetch_optional(pool)
    .await
    .map(|row| row.map(User::from))
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
