//! Temporary TOTP enrollment state and activation persistence.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct TotpEnrollmentRow {
    pub secret: String,
    pub expires_at: DateTime<Utc>,
    pub attempts: i16,
}

/// Creates or replaces the pending enrollment for a user.
pub async fn upsert(
    pool: &PgPool,
    user_id: Uuid,
    secret: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO user_totp_enrollments (user_id, secret, expires_at, attempts)
        VALUES ($1, $2, $3, 0)
        ON CONFLICT (user_id)
        DO UPDATE SET
            secret = EXCLUDED.secret,
            expires_at = EXCLUDED.expires_at,
            attempts = 0,
            created_at = NOW()
        "#,
    )
    .bind(user_id)
    .bind(secret)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Reads the pending enrollment, including expired rows for explicit handling.
pub async fn fetch(pool: &PgPool, user_id: Uuid) -> Result<Option<TotpEnrollmentRow>, sqlx::Error> {
    sqlx::query_as::<_, TotpEnrollmentRow>(
        r#"
        SELECT secret, expires_at, attempts
        FROM user_totp_enrollments
        WHERE user_id = $1
        "#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
}

/// Deletes pending enrollment state.
pub async fn delete(pool: &PgPool, user_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM user_totp_enrollments WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Records an invalid confirmation code and removes an exhausted enrollment.
pub async fn record_failed_attempt(
    pool: &PgPool,
    user_id: Uuid,
    max_attempts: i16,
) -> Result<Option<i16>, sqlx::Error> {
    let attempts = sqlx::query_scalar::<_, i16>(
        r#"
        UPDATE user_totp_enrollments
        SET attempts = attempts + 1
        WHERE user_id = $1
        RETURNING attempts
        "#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    if attempts.is_some_and(|attempts| attempts >= max_attempts) {
        delete(pool, user_id).await?;
    }
    Ok(attempts)
}

/// Atomically consumes a matching pending secret and enables TOTP.
pub async fn activate(
    pool: &PgPool,
    user_id: Uuid,
    expected_secret: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let consumed = sqlx::query_scalar::<_, String>(
        r#"
        DELETE FROM user_totp_enrollments
        WHERE user_id = $1 AND secret = $2 AND expires_at > NOW()
        RETURNING secret
        "#,
    )
    .bind(user_id)
    .bind(expected_secret)
    .fetch_optional(&mut *tx)
    .await?;

    if consumed.is_none() {
        tx.rollback().await?;
        return Ok(false);
    }

    let updated =
        sqlx::query("UPDATE users SET totp_secret = $2 WHERE id = $1 AND totp_secret IS NULL")
            .bind(user_id)
            .bind(expected_secret)
            .execute(&mut *tx)
            .await?;
    if updated.rows_affected() != 1 {
        tx.rollback().await?;
        return Ok(false);
    }

    tx.commit().await?;
    Ok(true)
}

/// Disables TOTP and invalidates pending setup and login challenges.
pub async fn disable(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let updated = sqlx::query("UPDATE users SET totp_secret = NULL WHERE id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM user_totp_enrollments WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM login_challenges WHERE user_id = $1")
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(updated.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    #[ignore = "requires NEZUMO_TEST_DATABASE_URL pointing to an isolated migrated PostgreSQL database"]
    async fn pending_enrollment_activation_and_disable_are_atomic() {
        let database_url = std::env::var("NEZUMO_TEST_DATABASE_URL")
            .expect("NEZUMO_TEST_DATABASE_URL must point to an isolated migrated database");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect test database");

        let user_id = Uuid::new_v4();
        let username = format!("totp_{}", &user_id.simple().to_string()[..12]);
        sqlx::query(
            "INSERT INTO users (id, username, email, password_hash) VALUES ($1, $2, $3, 'test')",
        )
        .bind(user_id)
        .bind(&username)
        .bind(format!("{username}@example.test"))
        .execute(&pool)
        .await
        .expect("insert test user");

        let secret = "JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP";
        upsert(&pool, user_id, secret, Utc::now() + Duration::minutes(10))
            .await
            .expect("start enrollment");
        assert_eq!(fetch(&pool, user_id).await.unwrap().unwrap().secret, secret);
        assert!(activate(&pool, user_id, secret).await.unwrap());
        assert!(fetch(&pool, user_id).await.unwrap().is_none());

        let active_secret: Option<String> =
            sqlx::query_scalar("SELECT totp_secret FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("read active secret");
        assert_eq!(active_secret.as_deref(), Some(secret));

        assert!(disable(&pool, user_id).await.unwrap());
        let disabled_secret: Option<String> =
            sqlx::query_scalar("SELECT totp_secret FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("read disabled secret");
        assert!(disabled_secret.is_none());

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("delete test user");
    }
}
