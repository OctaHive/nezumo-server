//! User, credential, registration, profile, and password-reset persistence.
//!
//! This is the broadest database module because it supports both account
//! lifecycle operations and public profile queries. Callers must continue to
//! enforce authorization and avoid exposing password hashes, TOTP secrets, or
//! verification/reset codes in API responses and logs.

use crate::models::user::*;
use chrono::{DateTime, Utc};
use regex::Regex;
use sqlx::postgres::PgPool;
use sqlx::Error;
use uuid::Uuid;
use validator::Validate;

/// Retrieves all users with security considerations
///
/// # Security
/// - Requires admin privileges (enforced at application layer)
/// - Excludes sensitive fields like password_hash and totp_secret
/// - Limits maximum results in production (enforced at application layer)
#[allow(dead_code)]
pub async fn fetch_all_users_from_db(pool: &PgPool) -> Result<Vec<UserGetResponse>, sqlx::Error> {
    sqlx::query_as!(
        UserGetResponse,
        "SELECT id, username, email, role_level, tier_level, status, creation_date,
        profile_picture_url, first_name, last_name, country_code, language_code,
        birthday, description
        FROM users"
    )
    .fetch_all(pool)
    .await
}

/// Safely retrieves user by allowed fields using whitelist validation
///
/// # Allowed Fields
/// - id: UUID
/// - email: valid email
/// - username: valid username
///
/// # Security
/// - Only whitelisted fields
/// - No sensitive data returned
#[allow(dead_code)]
pub async fn fetch_user_by_field_from_db(
    pool: &PgPool,
    field: &str,
    value: &str,
) -> Result<Option<UserGetResponse>, Error> {
    match field {
        "id" => {
            let uuid = value.parse::<Uuid>().map_err(|_| {
                Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid UUID format",
                )))
            })?;

            sqlx::query_as!(
                UserGetResponse,
                r#"
                SELECT id, username, email, role_level, tier_level, status, creation_date,
                       profile_picture_url, first_name, last_name, country_code,
                       language_code, birthday, description
                FROM users
                WHERE id = $1
                "#,
                uuid
            )
            .fetch_optional(pool)
            .await
        }
        "email" => {
            sqlx::query_as!(
                UserGetResponse,
                r#"
                SELECT id, username, email, role_level, tier_level, status, creation_date,
                       profile_picture_url, first_name, last_name, country_code,
                       language_code, birthday, description
                FROM users
                WHERE email = $1
                "#,
                value
            )
            .fetch_optional(pool)
            .await
        }
        "username" => {
            sqlx::query_as!(
                UserGetResponse,
                r#"
                SELECT id, username, email, role_level, tier_level, status, creation_date,
                       profile_picture_url, first_name, last_name, country_code,
                       language_code, birthday, description
                FROM users
                WHERE username = $1
                "#,
                value
            )
            .fetch_optional(pool)
            .await
        }
        _ => Err(Error::ColumnNotFound(field.to_string())),
    }
}

/// Safely retrieves only active user by allowed fields using whitelist validation
///
/// # Allowed Fields
/// - id: UUID
/// - email: valid email
/// - username: valid username
///
/// # Security
/// - Only whitelisted fields
/// - No sensitive data returned
/// - Only users with status = 'active'
pub async fn fetch_active_user_by_field_from_db(
    pool: &PgPool,
    field: &str,
    value: &str,
) -> Result<Option<UserGetResponse>, Error> {
    match field {
        "id" => {
            let uuid = value.parse::<Uuid>().map_err(|_| {
                Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid UUID format",
                )))
            })?;

            sqlx::query_as!(
                UserGetResponse,
                r#"
                SELECT id, username, email, role_level, tier_level, status, creation_date,
                       profile_picture_url, first_name, last_name, country_code,
                       language_code, birthday, description
                FROM users
                WHERE id = $1 AND status = 'active'
                "#,
                uuid
            )
            .fetch_optional(pool)
            .await
        }
        "email" => {
            sqlx::query_as!(
                UserGetResponse,
                r#"
                SELECT id, username, email, role_level, tier_level, status, creation_date,
                       profile_picture_url, first_name, last_name, country_code,
                       language_code, birthday, description
                FROM users
                WHERE email = $1 AND status = 'active'
                "#,
                value
            )
            .fetch_optional(pool)
            .await
        }
        "username" => {
            sqlx::query_as!(
                UserGetResponse,
                r#"
                SELECT id, username, email, role_level, tier_level, status, creation_date,
                       profile_picture_url, first_name, last_name, country_code,
                       language_code, birthday, description
                FROM users
                WHERE username = $1 AND status = 'active'
                "#,
                value
            )
            .fetch_optional(pool)
            .await
        }
        _ => Err(Error::ColumnNotFound(field.to_string())),
    }
}

/// Retrieves user by email with validation
///
/// # Security
/// - Parameterized query prevents SQL injection
/// - Returns Option to avoid user enumeration risks
pub async fn fetch_user_by_email_from_db(
    pool: &PgPool,
    email: &str,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"SELECT id, username, email, password_hash, totp_secret,
           role_level, tier_level, status, creation_date, profile_picture_url,
           first_name, last_name, country_code, language_code,
           birthday, description
           FROM users WHERE email = $1"#,
        email
    )
    .fetch_optional(pool)
    .await
}

/// Retrieves user by email, only if status is 'active'
///
/// # Security
/// - Parameterized query prevents SQL injection
/// - Returns Option to avoid user enumeration risks
pub async fn fetch_active_user_by_email_from_db(
    pool: &PgPool,
    email: &str,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"SELECT id, username, email, password_hash, totp_secret,
           role_level, tier_level, status, creation_date, profile_picture_url,
           first_name, last_name, country_code, language_code,
           birthday, description
           FROM users
           WHERE email = $1 AND status = 'active'"#,
        email
    )
    .fetch_optional(pool)
    .await
}

/// Retrieves user by ID, only if status is 'active'
///
/// # Security
/// - Parameterized query prevents SQL injection
/// - Returns Option to avoid user enumeration risks
/// Lightweight identity fields for the realtime presence list (see
/// `build_sessions_users`). Deliberately excludes secrets (password_hash, totp)
/// and rarely-needed columns so the hot presence-rebuild query stays cheap.
pub struct UserIdentity {
    pub id: Uuid,
    pub username: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub profile_picture_url: Option<String>,
}

/// Batch-fetch identity for many users in ONE query (`id = ANY($1)`), replacing
/// the per-user N+1 the presence rebuild used to run. Runtime query (not the
/// `query_as!` macro) so no offline `.sqlx` regeneration is needed.
pub async fn fetch_active_user_identities(
    pool: &PgPool,
    ids: &[Uuid],
) -> Result<Vec<UserIdentity>, sqlx::Error> {
    use sqlx::Row;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        r#"SELECT id, username, first_name, last_name, profile_picture_url
           FROM users
           WHERE id = ANY($1) AND status = 'active'"#,
    )
    .bind(ids)
    .fetch_all(pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(UserIdentity {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            first_name: row.try_get("first_name")?,
            last_name: row.try_get("last_name")?,
            profile_picture_url: row.try_get("profile_picture_url")?,
        });
    }
    Ok(out)
}

/// Fetches an active user by identifier for authorization and profile endpoints.
pub async fn fetch_active_user_by_id_from_db(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<User>, sqlx::Error> {
    sqlx::query_as!(
        User,
        r#"SELECT id, username, email, password_hash, totp_secret,
           role_level, tier_level, status, creation_date, profile_picture_url,
           first_name, last_name, country_code, language_code,
           birthday, description
           FROM users
           WHERE id = $1 AND status = 'active'"#,
        user_id
    )
    .fetch_optional(pool)
    .await
}

/// Securely deletes a user by ID
///
/// # Security
/// - Requires authentication and authorization
/// - Parameterized query prevents SQL injection
/// - Returns affected rows without sensitive data
pub async fn delete_user_from_db(pool: &PgPool, id: Uuid) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!("DELETE FROM users WHERE id = $1", id)
        .execute(pool)
        .await?;

    Ok(result.rows_affected())
}

async fn set_user_status_in_db(
    pool: &PgPool,
    user_id: Uuid,
    status: &str,
) -> Result<bool, sqlx::Error> {
    let disabled = status == "disabled";
    let result = sqlx::query("UPDATE users SET status = $2, disabled = $3 WHERE id = $1")
        .bind(user_id)
        .bind(status)
        .bind(disabled)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// Activates an account and makes it eligible for password, OAuth, and JWT auth.
pub async fn activate_user_in_db(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    set_user_status_in_db(pool, user_id, "active").await
}

/// Disables an account. Existing JWTs stop working because auth reloads an
/// active user from PostgreSQL on every authenticated request.
pub async fn deactivate_user_in_db(pool: &PgPool, user_id: Uuid) -> Result<bool, sqlx::Error> {
    set_user_status_in_db(pool, user_id, "disabled").await
}

/// Creates new user with comprehensive validation
///
/// # Validation
/// - Username: 3-30 alphanumeric characters
/// - Email: Valid format with domain verification
/// - Password: Minimum strength requirements (enforced at application layer)
pub async fn insert_user_into_db(
    pool: &PgPool,
    username: &str,
    email: &str,
    password_hash: &str,
    totp_secret: Option<&str>,
    role_level: i32,
    tier_level: i32,
) -> Result<UserInsertResponse, Error> {
    // Validate username
    let username = username.trim();
    if username.len() < 3 || username.len() > 30 {
        return Err(Error::Protocol(
            "Username must be between 3 and 30 characters.".into(),
        ));
    }
    if !username.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(Error::Protocol(
            "Invalid username format: only alphanumeric and underscores allowed.".into(),
        ));
    }

    // Validate email
    let email = email.trim().to_lowercase();
    if !is_valid_email(&email) {
        return Err(Error::Protocol("Invalid email format.".into()));
    }

    // Insert user into database
    let row = sqlx::query_as!(
        UserInsertResponse,
        r#"INSERT INTO users
           (username, email, password_hash, totp_secret, role_level, tier_level, creation_date)
           VALUES ($1, $2, $3, $4, $5, $6, NOW()::timestamp)
           RETURNING id, username, email, totp_secret, role_level, tier_level, creation_date,
                     first_name, last_name, country_code, language_code, birthday, description,
                     profile_picture_url"#,
        username,
        email,
        password_hash,
        totp_secret,
        role_level,
        tier_level,
    )
    .fetch_one(pool)
    .await?;

    Ok(row)
}

/// Inserts or refreshes a pending registration record.
///
/// # Returns
/// - `Ok(PendingRegistrationRow)` on success.
/// - `Err(Error)` on failure.
pub async fn upsert_pending_registration(
    pool: &PgPool,
    email: &str,
    verification_code: &str,
    verification_expires_at: DateTime<Utc>,
) -> Result<PendingRegistrationRow, Error> {
    // Validate email
    let email = email.trim().to_lowercase();
    if !is_valid_email(&email) {
        return Err(Error::Protocol("Invalid email format.".into()));
    }

    let row = sqlx::query_as!(
        PendingRegistrationRow,
        r#"
        INSERT INTO pending_registrations
            (email, verification_code, verification_expires_at)
        VALUES
            ($1, $2, $3)
        ON CONFLICT (email) DO UPDATE SET
            verification_code = EXCLUDED.verification_code,
            verification_expires_at = EXCLUDED.verification_expires_at,
            verified_at = NULL,
            completion_token = NULL,
            completion_expires_at = NULL
        RETURNING id, email, verification_code, verification_expires_at, verified_at, completion_token, completion_expires_at, created_at
        "#,
        email,
        verification_code,
        verification_expires_at,
    )
    .fetch_one(pool)
    .await?;

    Ok(row)
}

/// Fetch pending registration by email.
pub async fn fetch_pending_registration_by_email(
    pool: &PgPool,
    email: &str,
) -> Result<Option<PendingRegistrationRow>, sqlx::Error> {
    sqlx::query_as!(
        PendingRegistrationRow,
        r#"
        SELECT id, email, verification_code, verification_expires_at, verified_at, completion_token, completion_expires_at, created_at
        FROM pending_registrations
        WHERE email = $1
        "#,
        email
    )
    .fetch_optional(pool)
    .await
}

/// Mark a pending registration as verified if code matches and not expired.
pub async fn mark_pending_registration_verified(
    pool: &PgPool,
    email: &str,
    code: &str,
    completion_token: &str,
    completion_expires_at: DateTime<Utc>,
) -> Result<Option<PendingRegistrationRow>, sqlx::Error> {
    sqlx::query_as!(
        PendingRegistrationRow,
        r#"
        UPDATE pending_registrations
        SET verified_at = NOW(),
            completion_token = $3,
            completion_expires_at = $4
        WHERE email = $1
          AND verification_code = $2
          AND verification_expires_at > NOW()
        RETURNING id, email, verification_code, verification_expires_at, verified_at, completion_token, completion_expires_at, created_at
        "#,
        email,
        code,
        completion_token,
        completion_expires_at
    )
    .fetch_optional(pool)
    .await
}

/// Delete pending registration after completion.
pub async fn delete_pending_registration(pool: &PgPool, email: &str) -> Result<(), sqlx::Error> {
    sqlx::query!("DELETE FROM pending_registrations WHERE email = $1", email)
        .execute(pool)
        .await?;
    Ok(())
}

/// Retrieves the profile picture URL for a specific user
///
/// # Arguments
/// - `pool`: Database connection pool
/// - `user_id`: The user's unique identifier
///
/// # Returns
/// - `Ok(Some(String))` if user exists and has a profile picture
/// - `Ok(None)` if user exists but has no profile picture
/// - `Err(sqlx::Error)` on database errors
pub async fn fetch_profile_picture_url_from_db(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let result: Option<Option<String>> = sqlx::query_scalar!(
        r#"
        SELECT profile_picture_url
        FROM users
        WHERE id = $1
        "#,
        user_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(result.flatten())
}

/// Reads the raw `color_preferences` JSON string for a user (None if the row is
/// missing or the column is NULL). Uses the runtime query form (no `query!`
/// macro) so the new column needs no `.sqlx` cache regeneration.
pub async fn fetch_color_preferences(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let result: Option<Option<String>> =
        sqlx::query_scalar("SELECT color_preferences FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(pool)
            .await?;

    Ok(result.flatten())
}

/// Stores the user's `color_preferences` as a JSON string. Runtime query form.
pub async fn update_color_preferences(
    pool: &PgPool,
    user_id: Uuid,
    prefs_json: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE users SET color_preferences = $1 WHERE id = $2")
        .bind(prefs_json)
        .bind(user_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Updates the profile picture URL for a user
///
/// # Arguments
/// - `pool`: Database connection pool
/// - `user_id`: The user's ID
/// - `profile_picture_url`: The new URL or path for the profile picture
///
/// # Returns
/// - `Ok(())` on success
/// - `Err(sqlx::Error)` on failure
pub async fn update_user_profile_picture_in_db(
    pool: &PgPool,
    user_id: Uuid,
    profile_picture_url: &str,
) -> Result<(), Error> {
    sqlx::query!(
        r#"
        UPDATE users
        SET profile_picture_url = $1
        WHERE id = $2
        "#,
        profile_picture_url,
        user_id
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Searches active users by normalized display name or email.
pub async fn search_users(
    pool: &PgPool,
    query: &str,
    limit: i64,
) -> Result<Vec<crate::models::user::UserSearchResult>, sqlx::Error> {
    let pattern = format!("%{}%", query);
    sqlx::query_as!(
        crate::models::user::UserSearchResult,
        r#"
        SELECT id, username, email, profile_picture_url
        FROM users
        WHERE (username ILIKE $1 OR email ILIKE $1) AND status = 'active'
        LIMIT $2
        "#,
        pattern,
        limit
    )
    .fetch_all(pool)
    .await
}

/// Email validation helper function
fn is_valid_email(email: &str) -> bool {
    let email_regex =
        Regex::new(r"^[a-z0-9_+]+([a-z0-9_.-]*[a-z0-9_+])?@[a-z0-9]+([-.][a-z0-9]+)*\.[a-z]{2,6}$")
            .unwrap();
    email_regex.is_match(email)
}

/// Updates the specified user's profile fields in the database.
///
/// This function dynamically builds an `UPDATE` SQL statement using `sqlx::QueryBuilder`
/// based on the fields present in `UserUpdateBody`. Only fields that are `Some` will be updated.
/// Fields set to `Some(None)` will be set to `NULL` in the database, while fields set to
/// `Some(Some(value))` will be updated to that value. Fields that are `None` are not changed.
///
/// The update struct is validated before attempting the update.
///
/// # Arguments
///
/// * `pool` - A reference to the PostgreSQL connection pool.
/// * `user_id` - The UUID of the user whose profile is being updated.
/// * `update` - A struct containing the profile fields to update. Each field is an
///   `Option<Option<T>>`, allowing for explicit nullification or update.
///
/// # Returns
///
/// * `Ok(())` if the update was successful or if there was nothing to update.
/// * `Err(sqlx::Error)` if the database operation fails or validation fails.
///
/// # Example
///
/// ```
/// let update = UserUpdateBody {
///     first_name: Some(Some("Alice".to_string())),
///     last_name: Some(None), // Will set last_name to NULL
///     country_code: None,    // Will not update country_code
///     language_code: None,
///     birthday: None,
///     description: None,
/// };
/// update_user_in_db(&pool, user_id, update).await?;
/// ```
///
/// # Notes
///
/// - If no fields are provided to update, the function returns `Ok(())` and performs no database operation.
/// - The SQL query is constructed dynamically to update only the specified fields.
///
pub async fn update_user_in_db(
    pool: &PgPool,
    user_id: Uuid,
    update: UserUpdateBody,
) -> Result<(), sqlx::Error> {
    // Validate the update struct before proceeding
    if let Err(validation_errors) = update.validate() {
        return Err(sqlx::Error::Protocol(format!(
            "Validation error: {:?}",
            validation_errors
        )));
    }

    use sqlx::QueryBuilder;
    let mut builder = QueryBuilder::new("UPDATE users SET ");
    let mut has_updates = false;

    // For Option<T> fields (cannot be explicitly set to NULL)
    macro_rules! maybe_set_opt {
        ($field:ident) => {
            if let Some(ref val) = update.$field {
                if has_updates {
                    builder.push(", ");
                }
                builder.push(format!("{} = ", stringify!($field)));
                builder.push_bind(val);
                has_updates = true;
            }
        };
    }

    // For Option<Option<T>> fields (can be explicitly set to NULL)
    macro_rules! maybe_set_optopt {
        ($field:ident) => {
            if let Some(ref val) = update.$field {
                if has_updates {
                    builder.push(", ");
                }
                builder.push(format!("{} = ", stringify!($field)));
                match val {
                    Some(inner) => {
                        builder.push_bind(inner);
                    }
                    None => {
                        builder.push("NULL");
                    }
                }
                has_updates = true;
            }
        };
    }

    // Use parentheses and semicolons for macro calls!
    maybe_set_opt!(username);
    maybe_set_opt!(first_name);
    maybe_set_opt!(last_name);
    maybe_set_optopt!(country_code);
    maybe_set_opt!(language_code);
    maybe_set_optopt!(birthday);
    maybe_set_opt!(description);
    maybe_set_opt!(role_level);
    maybe_set_opt!(tier_level);
    if let Some(ref status) = update.status {
        let normalized = status.to_ascii_lowercase();
        if has_updates {
            builder.push(", ");
        }
        builder.push("status = ");
        builder.push_bind(normalized.clone());
        builder.push(", disabled = ");
        builder.push_bind(normalized == "disabled");
        has_updates = true;
    }

    if !has_updates {
        return Ok(());
    }

    builder.push(" WHERE id = ");
    builder.push_bind(user_id);

    let query = builder.build();
    query.execute(pool).await?;

    Ok(())
}

/// Inserts a password reset code for a user into the database.
///
/// # Arguments
/// - `pool`: Reference to the PostgreSQL connection pool.
/// - `user_id`: The UUID of the user.
/// - `code`: The password reset code (should be unique).
/// - `expires_at`: The UTC datetime when the code expires.
///
/// # Returns
/// - `Ok(())` on success.
/// - `Err(Error)` on failure.
pub async fn insert_user_password_reset_code_into_db(
    pool: &PgPool,
    user_id: Uuid,
    code: &str,
    expires_at: DateTime<Utc>,
) -> Result<(), Error> {
    let expires_at_naive = expires_at.naive_utc();
    sqlx::query!(
        r#"
        INSERT INTO users_password_reset_codes (user_id, code, expires_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (code) DO UPDATE
            SET user_id = EXCLUDED.user_id,
                expires_at = EXCLUDED.expires_at
        "#,
        user_id,
        code,
        expires_at_naive
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Updates the user's password hash in the database.
///
/// # Arguments
/// - `pool`: The database connection pool.
/// - `user_id`: The user's UUID.
/// - `new_password_hash`: The new hashed password.
///
/// # Returns
/// - `Ok(())` on success.
/// - `Err(sqlx::Error)` on failure.
pub async fn update_user_password_in_db(
    pool: &PgPool,
    user_id: Uuid,
    new_password_hash: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE users SET password_hash = $1 WHERE id = $2",
        new_password_hash,
        user_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetches the current (unexpired) password reset code for a user.
///
/// Returns `Ok(Some(UserPasswordResetCode))` if a code exists and is not expired,
/// `Ok(None)` if not found or expired, or `Err(sqlx::Error)` on DB error.
pub async fn fetch_current_password_reset_code_from_db(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<UserPasswordResetCode>, sqlx::Error> {
    let now = chrono::Utc::now().naive_utc();
    sqlx::query_as!(
        UserPasswordResetCode,
        r#"
        SELECT
            user_id as "user_id!",
            code,
            expires_at as "expires_at!"
        FROM users_password_reset_codes
        WHERE user_id = $1 AND expires_at > $2
        ORDER BY expires_at DESC
        LIMIT 1
        "#,
        user_id,
        now
    )
    .fetch_optional(pool)
    .await
}

/// Deletes all password reset codes for the specified user.
///
/// # Arguments
/// - `pool`: Reference to the PostgreSQL connection pool.
/// - `user_id`: The UUID of the user.
///
/// # Returns
/// - `Ok(())` on success.
/// - `Err(sqlx::Error)` on database error.
pub async fn delete_all_password_reset_codes_for_user(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "DELETE FROM users_password_reset_codes WHERE user_id = $1",
        user_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Existing email/username conflicts used by registration completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserExistence {
    pub email: bool,
    pub username: bool,
}

/// Checks both registration identifiers in one query and includes disabled
/// accounts. Disabled identities remain reserved and cannot be re-registered.
pub async fn check_user_exists_in_db(
    pool: &PgPool,
    email: &str,
    username: &str,
) -> Result<UserExistence, sqlx::Error> {
    let normalized_email = email.trim().to_lowercase();
    let normalized_username = username.trim();
    let (email, username): (bool, bool) = sqlx::query_as(
        "SELECT \
           EXISTS(SELECT 1 FROM users WHERE email = $1) AS email_exists, \
           EXISTS(SELECT 1 FROM users WHERE username = $2) AS username_exists",
    )
    .bind(normalized_email)
    .bind(normalized_username)
    .fetch_one(pool)
    .await?;
    Ok(UserExistence { email, username })
}

/// Returns `true` if a user with the given email exists (any status).
pub async fn check_user_exists_by_email(pool: &PgPool, email: &str) -> Result<bool, sqlx::Error> {
    let normalized = email.trim().to_lowercase();
    let user_by_email =
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM users WHERE email = $1 LIMIT 1")
            .bind(normalized)
            .fetch_optional(pool)
            .await?;

    Ok(user_by_email.is_some())
}

/// Returns `true` if a user with the given username exists (any status).
pub async fn check_user_exists_by_username(
    pool: &PgPool,
    username: &str,
) -> Result<bool, sqlx::Error> {
    let user_by_username =
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM users WHERE username = $1 LIMIT 1")
            .bind(username.trim())
            .fetch_optional(pool)
            .await?;

    Ok(user_by_username.is_some())
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[tokio::test]
    #[ignore = "requires NEZUMO_TEST_DATABASE_URL pointing to an isolated migrated PostgreSQL database"]
    async fn account_status_and_registration_identity_lifecycle() {
        let database_url = std::env::var("NEZUMO_TEST_DATABASE_URL")
            .expect("NEZUMO_TEST_DATABASE_URL must point to an isolated migrated database");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect test database");

        let user_id = Uuid::new_v4();
        let username = format!("status_{}", &user_id.simple().to_string()[..12]);
        let email = format!("{username}@example.com");
        sqlx::query(
            "INSERT INTO users (id, username, email, password_hash) VALUES ($1, $2, $3, 'test')",
        )
        .bind(user_id)
        .bind(&username)
        .bind(&email)
        .execute(&pool)
        .await
        .expect("insert test user");

        let existence = check_user_exists_in_db(&pool, &email.to_uppercase(), &username)
            .await
            .expect("check active user identity");
        assert_eq!(
            existence,
            UserExistence {
                email: true,
                username: true
            }
        );

        assert!(deactivate_user_in_db(&pool, user_id)
            .await
            .expect("deactivate user"));
        let state: (String, bool) =
            sqlx::query_as("SELECT status, disabled FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("read disabled state");
        assert_eq!(state, ("disabled".to_string(), true));
        assert!(fetch_active_user_by_email_from_db(&pool, &email)
            .await
            .expect("lookup disabled user")
            .is_none());
        assert!(
            check_user_exists_in_db(&pool, &email, &username)
                .await
                .expect("check disabled user identity")
                .email
        );

        assert!(activate_user_in_db(&pool, user_id)
            .await
            .expect("activate user"));
        let state: (String, bool) =
            sqlx::query_as("SELECT status, disabled FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&pool)
                .await
                .expect("read active state");
        assert_eq!(state, ("active".to_string(), false));
        assert!(fetch_active_user_by_email_from_db(&pool, &email)
            .await
            .expect("lookup active user")
            .is_some());
        assert!(!deactivate_user_in_db(&pool, Uuid::new_v4())
            .await
            .expect("deactivate missing user"));

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("delete test user");
    }
}
