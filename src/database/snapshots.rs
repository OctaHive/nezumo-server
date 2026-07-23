//! Rollback snapshot persistence. Current-state consumers read the canonical
//! coordinator projection instead.

use crate::models::events::SnapshotRecord;
use sqlx::PgPool;
use uuid::Uuid;

/// Persists a materialized JSON snapshot at an exact event sequence.
pub async fn insert_snapshot(
    pool: &PgPool,
    board_id: Uuid,
    seq: i64,
    state: serde_json::Value,
) -> Result<SnapshotRecord, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query!(
        r#"
        DELETE FROM board_snapshots
        WHERE board_id = $1
        "#,
        board_id
    )
    .execute(&mut *tx)
    .await?;

    let row = sqlx::query_as!(
        SnapshotRecord,
        r#"
        INSERT INTO board_snapshots (board_id, seq, state)
        VALUES ($1, $2, $3)
        RETURNING id, board_id, seq, state, created_at
        "#,
        board_id,
        seq,
        state
    )
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(row)
}

/// Fetches the newest materialized JSON snapshot for a board.
pub async fn fetch_latest_snapshot(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Option<SnapshotRecord>, sqlx::Error> {
    let row = sqlx::query_as!(
        SnapshotRecord,
        r#"
        SELECT id, board_id, seq, state, created_at
        FROM board_snapshots
        WHERE board_id = $1
        ORDER BY seq DESC
        LIMIT 1
        "#,
        board_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(row)
}
