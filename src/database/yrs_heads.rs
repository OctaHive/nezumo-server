//! Durable per-board lifecycle and watermark for the canonical coordinator
//! (`board_yrs_heads`).
//!
//! `writer_epoch` fences a superseded owner after failover: every mutating write
//! is CAS'd on it. `processed_seq` is the highest event sequence fully reflected
//! by the canonical document and is never derived as `MAX(update.seq)`.
//!
//! Runtime `sqlx` is used because this table has no offline query cache. The
//! per-board atomic barrier is a `pg_advisory_xact_lock` acquired first.

use sqlx::{Executor, Postgres, Transaction};
use uuid::Uuid;

/// Lifecycle state of a canonical board owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalState {
    Activating,
    Ready,
    Quarantined,
}

impl CanonicalState {
    /// Returns the stable database/wire representation of this lifecycle state.
    pub fn as_str(self) -> &'static str {
        match self {
            CanonicalState::Activating => "activating",
            CanonicalState::Ready => "ready",
            CanonicalState::Quarantined => "quarantined",
        }
    }
    /// Parses a persisted lifecycle state, rejecting unknown future values.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "activating" => Some(CanonicalState::Activating),
            "ready" => Some(CanonicalState::Ready),
            "quarantined" => Some(CanonicalState::Quarantined),
            _ => None,
        }
    }
}

/// A persisted head row (domain view; `state` widened to the enum).
#[derive(Debug, Clone)]
pub struct YrsHead {
    pub processed_seq: i64,
    pub base_generation: i64,
    pub writer_epoch: i64,
    pub state: CanonicalState,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct YrsHeadRow {
    processed_seq: i64,
    base_generation: i64,
    writer_epoch: i64,
    state: String,
}

impl YrsHeadRow {
    fn to_domain(&self) -> YrsHead {
        YrsHead {
            processed_seq: self.processed_seq,
            base_generation: self.base_generation,
            writer_epoch: self.writer_epoch,
            state: CanonicalState::from_str(&self.state).unwrap_or(CanonicalState::Quarantined),
        }
    }
}

const SELECT_COLS: &str = "processed_seq, base_generation, writer_epoch, state";

/// Stable per-board key for `pg_advisory_xact_lock`, matching the one used by the
/// canonical-base writer so the two never interleave on a board (first 8
/// bytes of the UUID, little-endian).
pub fn advisory_key(board_id: Uuid) -> i64 {
    let b = board_id.as_bytes();
    i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// Takes the per-board transaction-scoped advisory lock. It is held until the
/// transaction ends and serializes activation, commits, and compaction.
pub async fn lock_board_xact(
    tx: &mut Transaction<'_, Postgres>,
    board_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(advisory_key(board_id))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Reads the head row for a board, if one has been activated.
pub async fn read_head<'e, E>(exec: E, board_id: Uuid) -> Result<Option<YrsHead>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!("SELECT {SELECT_COLS} FROM board_yrs_heads WHERE board_id = $1");
    let row = sqlx::query_as::<_, YrsHeadRow>(&sql)
        .bind(board_id)
        .fetch_optional(exec)
        .await?;
    Ok(row.map(|r| r.to_domain()))
}

/// Starts activation, bumping `writer_epoch` to fence stale owners and recording
/// `base_generation`. Re-entry replaces an incomplete activation.
pub async fn begin_activation(
    tx: &mut Transaction<'_, Postgres>,
    board_id: Uuid,
    base_generation: i64,
) -> Result<YrsHead, sqlx::Error> {
    let sql = format!(
        "INSERT INTO board_yrs_heads \
           (board_id, processed_seq, base_generation, writer_epoch, state, updated_at) \
         VALUES ($1, 0, $2, 1, 'activating', NOW()) \
         ON CONFLICT (board_id) DO UPDATE SET \
           state = 'activating', \
           base_generation = EXCLUDED.base_generation, \
           writer_epoch = board_yrs_heads.writer_epoch + 1, \
           updated_at = NOW() \
         RETURNING {SELECT_COLS}"
    );
    let row = sqlx::query_as::<_, YrsHeadRow>(&sql)
        .bind(board_id)
        .bind(base_generation)
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.to_domain())
}

/// Finish activation: `activating → ready`, setting the caught-up watermark.
/// CAS'd on `writer_epoch` so a superseded owner cannot complete activation.
/// Returns `true` if it applied. Must hold [`lock_board_xact`].
pub async fn mark_ready(
    tx: &mut Transaction<'_, Postgres>,
    board_id: Uuid,
    expected_writer_epoch: i64,
    processed_seq: i64,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE board_yrs_heads \
         SET state = 'ready', processed_seq = $3, updated_at = NOW() \
         WHERE board_id = $1 AND writer_epoch = $2 AND state = 'activating'",
    )
    .bind(board_id)
    .bind(expected_writer_epoch)
    .bind(processed_seq)
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// Advance `processed_seq` in the coordinator's atomic tx (same tx as the update
/// pair / backfill row). CAS'd on both `writer_epoch` and the exact prior
/// `processed_seq`: this is the optimistic multi-instance revision fence.
/// Returns `true` if it applied. Must hold
/// [`lock_board_xact`].
pub async fn advance_processed_seq(
    tx: &mut Transaction<'_, Postgres>,
    board_id: Uuid,
    expected_writer_epoch: i64,
    expected_processed_seq: i64,
    new_processed_seq: i64,
) -> Result<bool, sqlx::Error> {
    let res = sqlx::query(
        "UPDATE board_yrs_heads \
         SET processed_seq = $4, updated_at = NOW() \
         WHERE board_id = $1 AND writer_epoch = $2 AND processed_seq = $3 \
           AND state = 'ready'",
    )
    .bind(board_id)
    .bind(expected_writer_epoch)
    .bind(expected_processed_seq)
    .bind(new_processed_seq)
    .execute(&mut **tx)
    .await?;
    Ok(res.rows_affected() == 1)
}
