//! Persisted non-secret server settings with environment fallbacks and audit.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::core::config::{get_env_bool, get_env_with_default};
use crate::database::quotas::TierQuota;

const PUBLIC_REGISTRATION_ENABLED: &str = "public_registration_enabled";
const GOOGLE_LOGIN_ENABLED: &str = "google_login_enabled";
const SUPPORT_MAX_REPORTS_PER_DAY: &str = "support_max_reports_per_day";
const FEATURE_REQUEST_MAX_PER_DAY: &str = "feature_request_max_per_day";
const FEATURE_REQUEST_EXPOSE_ISSUE_URL: &str = "feature_request_expose_issue_url";

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ServerSettings {
    pub public_registration_enabled: bool,
    pub google_login_enabled: bool,
    pub support_max_reports_per_day: i64,
    pub feature_request_max_per_day: i64,
    pub feature_request_expose_issue_url: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ServerSettingsPatch {
    pub public_registration_enabled: Option<bool>,
    pub google_login_enabled: Option<bool>,
    pub support_max_reports_per_day: Option<i64>,
    pub feature_request_max_per_day: Option<i64>,
    pub feature_request_expose_issue_url: Option<bool>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct TierSettingsUpdate {
    /// Null means unlimited.
    pub max_owned_boards: Option<i32>,
    pub max_upload_bytes: i64,
    pub max_storage_bytes: i64,
}

impl ServerSettings {
    fn environment_defaults() -> Self {
        Self {
            public_registration_enabled: get_env_bool("PUBLIC_REGISTRATION_ENABLED", true),
            google_login_enabled: get_env_bool("GOOGLE_LOGIN_ENABLED", true),
            support_max_reports_per_day: get_env_with_default("SUPPORT_MAX_REPORTS_PER_DAY", "10")
                .parse()
                .unwrap_or(10),
            feature_request_max_per_day: get_env_with_default("FEATURE_REQUEST_MAX_PER_DAY", "5")
                .parse()
                .unwrap_or(5),
            feature_request_expose_issue_url: get_env_bool(
                "FEATURE_REQUEST_EXPOSE_ISSUE_URL",
                false,
            ),
        }
    }
}

pub async fn load(pool: &PgPool) -> Result<ServerSettings, sqlx::Error> {
    let rows = sqlx::query_as::<_, (String, Value)>("SELECT key, value FROM server_settings")
        .fetch_all(pool)
        .await?;
    let values: HashMap<String, Value> = rows.into_iter().collect();
    let mut settings = ServerSettings::environment_defaults();

    if let Some(value) = values.get(PUBLIC_REGISTRATION_ENABLED) {
        settings.public_registration_enabled = value.as_bool().unwrap_or(true);
    }
    if let Some(value) = values.get(GOOGLE_LOGIN_ENABLED) {
        settings.google_login_enabled = value.as_bool().unwrap_or(true);
    }
    if let Some(value) = values.get(SUPPORT_MAX_REPORTS_PER_DAY) {
        settings.support_max_reports_per_day = value.as_i64().unwrap_or(10);
    }
    if let Some(value) = values.get(FEATURE_REQUEST_MAX_PER_DAY) {
        settings.feature_request_max_per_day = value.as_i64().unwrap_or(5);
    }
    if let Some(value) = values.get(FEATURE_REQUEST_EXPOSE_ISSUE_URL) {
        settings.feature_request_expose_issue_url = value.as_bool().unwrap_or(false);
    }
    Ok(settings)
}

async fn write_value(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    value: Value,
    administrator_id: Uuid,
) -> Result<(), sqlx::Error> {
    let old_value = sqlx::query_scalar::<_, Value>(
        "SELECT value FROM server_settings WHERE key = $1 FOR UPDATE",
    )
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;

    if old_value.as_ref() == Some(&value) {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO server_settings (key, value, updated_by, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (key) DO UPDATE SET
            value = EXCLUDED.value,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW()
        "#,
    )
    .bind(key)
    .bind(&value)
    .bind(administrator_id)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO server_settings_audit (key, old_value, new_value, changed_by)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(key)
    .bind(old_value)
    .bind(value)
    .bind(administrator_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn update(
    pool: &PgPool,
    patch: &ServerSettingsPatch,
    administrator_id: Uuid,
) -> Result<ServerSettings, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if let Some(value) = patch.public_registration_enabled {
        write_value(
            &mut tx,
            PUBLIC_REGISTRATION_ENABLED,
            Value::Bool(value),
            administrator_id,
        )
        .await?;
    }
    if let Some(value) = patch.google_login_enabled {
        write_value(
            &mut tx,
            GOOGLE_LOGIN_ENABLED,
            Value::Bool(value),
            administrator_id,
        )
        .await?;
    }
    if let Some(value) = patch.support_max_reports_per_day {
        write_value(
            &mut tx,
            SUPPORT_MAX_REPORTS_PER_DAY,
            Value::from(value),
            administrator_id,
        )
        .await?;
    }
    if let Some(value) = patch.feature_request_max_per_day {
        write_value(
            &mut tx,
            FEATURE_REQUEST_MAX_PER_DAY,
            Value::from(value),
            administrator_id,
        )
        .await?;
    }
    if let Some(value) = patch.feature_request_expose_issue_url {
        write_value(
            &mut tx,
            FEATURE_REQUEST_EXPOSE_ISSUE_URL,
            Value::Bool(value),
            administrator_id,
        )
        .await?;
    }
    tx.commit().await?;
    load(pool).await
}

pub async fn update_tier(
    pool: &PgPool,
    level: i32,
    body: &TierSettingsUpdate,
    administrator_id: Uuid,
) -> Result<Option<TierQuota>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let old = sqlx::query_scalar::<_, Value>(
        r#"
        SELECT jsonb_build_object(
            'max_owned_boards', max_owned_boards,
            'max_upload_bytes', max_upload_bytes,
            'max_storage_bytes', max_storage_bytes
        )
        FROM tiers WHERE level = $1 FOR UPDATE
        "#,
    )
    .bind(level)
    .fetch_optional(&mut *tx)
    .await?;
    if old.is_none() {
        tx.rollback().await?;
        return Ok(None);
    }

    let tier = sqlx::query_as::<_, TierQuota>(
        r#"
        UPDATE tiers SET
            max_owned_boards = $2,
            max_upload_bytes = $3,
            max_storage_bytes = $4
        WHERE level = $1
        RETURNING level, name, requests_per_day, max_owned_boards,
                  max_upload_bytes, max_storage_bytes
        "#,
    )
    .bind(level)
    .bind(body.max_owned_boards)
    .bind(body.max_upload_bytes)
    .bind(body.max_storage_bytes)
    .fetch_one(&mut *tx)
    .await?;

    let new_value = serde_json::to_value(&tier).unwrap_or(Value::Null);
    sqlx::query(
        r#"
        INSERT INTO server_settings_audit (key, old_value, new_value, changed_by)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(format!("tier.{level}"))
    .bind(old)
    .bind(new_value)
    .bind(administrator_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(tier))
}
