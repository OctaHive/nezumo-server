//! Append-only board event persistence and query helpers.
//!
//! Event sequence numbers are reserved transactionally through the board row
//! before insertion.

use crate::models::events::EventRecord;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

/// Inserts one board event with a sequence reserved by the caller.
pub async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    board_id: Uuid,
    seq: i64,
    user_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
    session_id: Option<&str>,
) -> Result<EventRecord, sqlx::Error> {
    let row = sqlx::query_as!(
        EventRecord,
        r#"
        INSERT INTO board_events (board_id, seq, user_id, event_type, payload, session_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, board_id, seq, user_id, event_type, payload, session_id, created_at
        "#,
        board_id,
        seq,
        user_id,
        event_type,
        payload,
        session_id
    )
    .fetch_one(&mut **tx)
    .await?;

    Ok(row)
}

/// Lists ordered durable events after a cursor for coordinator catch-up.
pub async fn list_events_since(
    pool: &PgPool,
    board_id: Uuid,
    since_seq: i64,
    limit: i64,
) -> Result<Vec<EventRecord>, sqlx::Error> {
    let rows = sqlx::query_as!(
        EventRecord,
        r#"
        SELECT id, board_id, seq, user_id, event_type, payload, session_id, created_at
        FROM board_events
        WHERE board_id = $1 AND seq > $2
        ORDER BY seq ASC
        LIMIT $3
        "#,
        board_id,
        since_seq,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Reads the durable event half of a canonical pair. Fan-out uses PostgreSQL,
/// not the Redis payload, as its source of truth.
pub async fn read_event_by_id(
    pool: &PgPool,
    event_id: Uuid,
) -> Result<Option<EventRecord>, sqlx::Error> {
    sqlx::query_as::<_, EventRecord>(
        "SELECT id, board_id, seq, user_id, event_type, payload, session_id, created_at \
         FROM board_events WHERE id = $1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await
}

/// Lowest event `seq` still stored for a board. Canonical retention removes
/// events covered by an older immutable Yrs checkpoint after the restore TTL.
/// `None` when no events remain. A reconnecting client whose `last_seq` is below
/// this minus one has a gap and must reload the snapshot instead of resuming
/// incrementally. Runtime query → no `sqlx prepare` needed.
pub async fn min_event_seq(pool: &PgPool, board_id: Uuid) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MIN(seq) FROM board_events WHERE board_id = $1")
        .bind(board_id)
        .fetch_one(pool)
        .await
}

/// Lists events authored by one user session for diagnostics and cleanup.
pub async fn list_events_by_user_session(
    pool: &PgPool,
    board_id: Uuid,
    user_id: Uuid,
    session_id: &str,
) -> Result<Vec<EventRecord>, sqlx::Error> {
    let rows = sqlx::query_as!(
        EventRecord,
        r#"
        SELECT id, board_id, seq, user_id, event_type, payload, session_id, created_at
        FROM board_events
        WHERE board_id = $1 AND user_id = $2 AND session_id = $3
        ORDER BY seq ASC
        "#,
        board_id,
        user_id,
        session_id
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}
