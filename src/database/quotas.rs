//! Tier definitions, current resource usage, and quota validation.
//!
//! Board and storage usage is charged to the board owner. This prevents a
//! collaborator from bypassing a plan by uploading through somebody else's
//! account and makes deletion release usage through the existing board/file
//! foreign-key cascades.

use serde::Serialize;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct TierQuota {
    pub level: i32,
    pub name: String,
    pub requests_per_day: i32,
    /// `None` means that this tier has no board-count limit.
    pub max_owned_boards: Option<i32>,
    pub max_upload_bytes: i64,
    pub max_storage_bytes: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, ToSchema)]
pub struct QuotaUsage {
    pub owned_boards: i64,
    pub storage_bytes: i64,
}

#[derive(Debug, Error)]
pub enum QuotaError {
    #[error("the user's tier does not exist")]
    TierNotFound,
    #[error("board limit reached ({used}/{limit})")]
    BoardLimit { used: i64, limit: i64 },
    #[error("file is too large ({size} bytes; limit {limit})")]
    UploadTooLarge { size: i64, limit: i64 },
    #[error("storage limit exceeded ({used} + {size} > {limit})")]
    StorageLimit { used: i64, size: i64, limit: i64 },
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub async fn list_tiers(pool: &PgPool) -> Result<Vec<TierQuota>, sqlx::Error> {
    sqlx::query_as::<_, TierQuota>(
        r#"
        SELECT level, name, requests_per_day, max_owned_boards,
               max_upload_bytes, max_storage_bytes
        FROM tiers
        ORDER BY level
        "#,
    )
    .fetch_all(pool)
    .await
}

pub async fn tier_for_user(pool: &PgPool, user_id: Uuid) -> Result<TierQuota, QuotaError> {
    tier_for_user_executor(pool, user_id).await
}

async fn tier_for_user_executor<'e, E>(executor: E, user_id: Uuid) -> Result<TierQuota, QuotaError>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, TierQuota>(
        r#"
        SELECT t.level, t.name, t.requests_per_day, t.max_owned_boards,
               t.max_upload_bytes, t.max_storage_bytes
        FROM users u
        JOIN tiers t ON t.level = u.tier_level
        WHERE u.id = $1
        "#,
    )
    .bind(user_id)
    .fetch_optional(executor)
    .await?
    .ok_or(QuotaError::TierNotFound)
}

pub async fn usage_for_owner(pool: &PgPool, owner_id: Uuid) -> Result<QuotaUsage, sqlx::Error> {
    usage_for_owner_executor(pool, owner_id).await
}

async fn usage_for_owner_executor<'e, E>(
    executor: E,
    owner_id: Uuid,
) -> Result<QuotaUsage, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let (owned_boards, storage_bytes) = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*)::BIGINT FROM boards WHERE owner_id = $1),
            COALESCE((
                SELECT SUM(bf.size_bytes)::BIGINT
                FROM board_files bf
                JOIN boards b ON b.id = bf.board_id
                WHERE b.owner_id = $1
            ), 0)
            + COALESCE((
                SELECT SUM(imported_storage_bytes)::BIGINT
                FROM boards
                WHERE owner_id = $1
            ), 0)
        "#,
    )
    .bind(owner_id)
    .fetch_one(executor)
    .await?;

    Ok(QuotaUsage {
        owned_boards,
        storage_bytes,
    })
}

/// Serializes quota-consuming mutations for one owner inside a transaction.
pub async fn lock_owner_quota(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
) -> Result<(), sqlx::Error> {
    let bytes = owner_id.as_bytes();
    let key = i64::from_be_bytes(bytes[..8].try_into().expect("UUID has 16 bytes"));
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub async fn ensure_board_available(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
) -> Result<TierQuota, QuotaError> {
    lock_owner_quota(tx, owner_id).await?;
    let tier = tier_for_user_executor(&mut **tx, owner_id).await?;
    let usage = usage_for_owner_executor(&mut **tx, owner_id).await?;
    evaluate_board(&tier, usage)?;
    Ok(tier)
}

pub async fn ensure_upload_available(
    pool: &PgPool,
    owner_id: Uuid,
    size: i64,
) -> Result<(TierQuota, QuotaUsage), QuotaError> {
    let tier = tier_for_user(pool, owner_id).await?;
    let usage = usage_for_owner(pool, owner_id).await?;
    evaluate_upload(&tier, usage, size)?;
    Ok((tier, usage))
}

pub async fn ensure_file_size_available(
    pool: &PgPool,
    owner_id: Uuid,
    size: i64,
) -> Result<TierQuota, QuotaError> {
    let tier = tier_for_user(pool, owner_id).await?;
    evaluate_file_size(&tier, size)?;
    Ok(tier)
}

pub async fn ensure_storage_available(
    pool: &PgPool,
    owner_id: Uuid,
    size: i64,
) -> Result<(TierQuota, QuotaUsage), QuotaError> {
    let tier = tier_for_user(pool, owner_id).await?;
    let usage = usage_for_owner(pool, owner_id).await?;
    evaluate_storage(&tier, usage, size)?;
    Ok((tier, usage))
}

pub async fn ensure_upload_available_tx(
    tx: &mut Transaction<'_, Postgres>,
    owner_id: Uuid,
    size: i64,
) -> Result<(TierQuota, QuotaUsage), QuotaError> {
    lock_owner_quota(tx, owner_id).await?;
    let tier = tier_for_user_executor(&mut **tx, owner_id).await?;
    let usage = usage_for_owner_executor(&mut **tx, owner_id).await?;
    evaluate_upload(&tier, usage, size)?;
    Ok((tier, usage))
}

/// Atomically accounts storage objects restored by a board import. Those
/// objects do not all have board_files rows, so their total lives on the board.
pub async fn record_imported_storage(
    pool: &PgPool,
    owner_id: Uuid,
    board_id: Uuid,
    size: i64,
) -> Result<(), QuotaError> {
    let mut tx = pool.begin().await?;
    lock_owner_quota(&mut tx, owner_id).await?;
    let tier = tier_for_user_executor(&mut *tx, owner_id).await?;
    let usage = usage_for_owner_executor(&mut *tx, owner_id).await?;
    evaluate_storage(&tier, usage, size)?;

    let updated = sqlx::query(
        "UPDATE boards SET imported_storage_bytes = $1 WHERE id = $2 AND owner_id = $3",
    )
    .bind(size)
    .bind(board_id)
    .bind(owner_id)
    .execute(&mut *tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(QuotaError::Database(sqlx::Error::RowNotFound));
    }
    tx.commit().await?;
    Ok(())
}

pub fn evaluate_board(tier: &TierQuota, usage: QuotaUsage) -> Result<(), QuotaError> {
    let Some(limit) = tier.max_owned_boards.map(i64::from) else {
        return Ok(());
    };
    if usage.owned_boards >= limit {
        return Err(QuotaError::BoardLimit {
            used: usage.owned_boards,
            limit,
        });
    }
    Ok(())
}

pub fn evaluate_upload(tier: &TierQuota, usage: QuotaUsage, size: i64) -> Result<(), QuotaError> {
    evaluate_file_size(tier, size)?;
    evaluate_storage(tier, usage, size)
}

pub fn evaluate_file_size(tier: &TierQuota, size: i64) -> Result<(), QuotaError> {
    if size < 0 || size > tier.max_upload_bytes {
        return Err(QuotaError::UploadTooLarge {
            size,
            limit: tier.max_upload_bytes,
        });
    }
    Ok(())
}

pub fn evaluate_storage(tier: &TierQuota, usage: QuotaUsage, size: i64) -> Result<(), QuotaError> {
    if usage.storage_bytes.saturating_add(size) > tier.max_storage_bytes {
        return Err(QuotaError::StorageLimit {
            used: usage.storage_bytes,
            size,
            limit: tier.max_storage_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tier() -> TierQuota {
        TierQuota {
            level: 1,
            name: "Low".into(),
            requests_per_day: 1_000,
            max_owned_boards: Some(10),
            max_upload_bytes: 20,
            max_storage_bytes: 100,
        }
    }

    #[test]
    fn board_limit_is_inclusive() {
        assert!(evaluate_board(
            &tier(),
            QuotaUsage {
                owned_boards: 9,
                storage_bytes: 0,
            }
        )
        .is_ok());
        assert!(matches!(
            evaluate_board(
                &tier(),
                QuotaUsage {
                    owned_boards: 10,
                    storage_bytes: 0,
                }
            ),
            Err(QuotaError::BoardLimit { .. })
        ));
    }

    #[test]
    fn missing_board_limit_is_unlimited() {
        let mut unlimited = tier();
        unlimited.max_owned_boards = None;
        assert!(evaluate_board(
            &unlimited,
            QuotaUsage {
                owned_boards: i64::MAX,
                storage_bytes: 0,
            }
        )
        .is_ok());
    }

    #[test]
    fn upload_checks_file_and_total_storage_limits() {
        let usage = QuotaUsage {
            owned_boards: 0,
            storage_bytes: 80,
        };
        assert!(evaluate_upload(&tier(), usage, 20).is_ok());
        assert!(matches!(
            evaluate_upload(&tier(), usage, 21),
            Err(QuotaError::UploadTooLarge { .. })
        ));
        assert!(matches!(
            evaluate_upload(
                &tier(),
                QuotaUsage {
                    storage_bytes: 90,
                    ..usage
                },
                20
            ),
            Err(QuotaError::StorageLimit { .. })
        ));
    }
}
