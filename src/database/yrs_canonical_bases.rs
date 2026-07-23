//! Persistence for the durable Yrs base, isolated from the `board_snapshots`
//! delete-and-insert lifecycle.
//!
//! Writes are serialized per board by a Postgres transaction-scoped advisory lock and
//! guarded by an optimistic CAS on `(base_seq, base_generation)`, so concurrent
//! checkpoint writers cannot clobber the last good base.
//! Uses runtime `sqlx` (not the compile-checked macros) because the table has no
//! offline query cache.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::state::canonical_base::CanonicalBase;

/// A persisted canonical base row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CanonicalBaseRow {
    pub state_update: Vec<u8>,
    pub state_vector: Vec<u8>,
    pub base_seq: i64,
    pub protocol_version: i32,
    pub schema_version: i32,
    pub min_writer_version: i32,
    pub update_encoding: String,
    pub server_client_id: i64,
    pub base_generation: i64,
    pub abandoned_at: Option<DateTime<Utc>>,
}

impl CanonicalBaseRow {
    /// Domain view (server_client_id widened back to u64; it lives in `[2^52, 2^53)`).
    pub fn to_domain(&self) -> CanonicalBase {
        CanonicalBase {
            state_update: self.state_update.clone(),
            state_vector: self.state_vector.clone(),
            base_seq: self.base_seq,
            server_client_id: self.server_client_id as u64,
            base_generation: self.base_generation,
        }
    }
}

/// Stable per-board key for `pg_advisory_xact_lock` (first 8 bytes of the UUID).
fn advisory_key(board_id: Uuid) -> i64 {
    let b = board_id.as_bytes();
    i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

/// Reads the current canonical base row for a board, if any.
pub async fn read_base(
    pool: &PgPool,
    board_id: Uuid,
) -> Result<Option<CanonicalBaseRow>, sqlx::Error> {
    sqlx::query_as::<_, CanonicalBaseRow>(
        "SELECT state_update, state_vector, base_seq, \
         protocol_version, schema_version, min_writer_version, update_encoding, \
         server_client_id, base_generation, abandoned_at \
         FROM board_yrs_canonical_bases WHERE board_id = $1",
    )
    .bind(board_id)
    .fetch_optional(pool)
    .await
}

/// Serialized, CAS-guarded upsert of a canonical base. `expected` is the
/// `(base_seq, base_generation)` the candidate was built from (`None` for the first
/// insert). Returns `Ok(true)` if written, `Ok(false)` if a concurrent winner changed
/// the row first (candidate discarded, recomputed next cycle). Setting a new base
/// clears `abandoned_at` (a fresh generation re-activates the row).
pub async fn upsert_base_cas(
    pool: &PgPool,
    board_id: Uuid,
    candidate: &CanonicalBase,
    expected: Option<(i64, i64)>,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(advisory_key(board_id))
        .fetch_one(&mut *tx)
        .await?;
    if !locked {
        tx.rollback().await?;
        return Ok(false);
    }

    let current: Option<(i64, i64)> = sqlx::query_as(
        "SELECT base_seq, base_generation FROM board_yrs_canonical_bases \
         WHERE board_id = $1 FOR UPDATE",
    )
    .bind(board_id)
    .fetch_optional(&mut *tx)
    .await?;
    if current != expected {
        tx.rollback().await?;
        return Ok(false);
    }

    sqlx::query(
        "INSERT INTO board_yrs_canonical_bases \
           (board_id, state_update, state_vector, base_seq, \
            protocol_version, schema_version, min_writer_version, update_encoding, \
            server_client_id, base_generation, abandoned_at, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,'v1',$8,$9,NULL,NOW()) \
         ON CONFLICT (board_id) DO UPDATE SET \
           state_update = EXCLUDED.state_update, \
           state_vector = EXCLUDED.state_vector, \
           base_seq = EXCLUDED.base_seq, \
           protocol_version = EXCLUDED.protocol_version, \
           schema_version = EXCLUDED.schema_version, \
           min_writer_version = EXCLUDED.min_writer_version, \
           update_encoding = EXCLUDED.update_encoding, \
           server_client_id = EXCLUDED.server_client_id, \
           base_generation = EXCLUDED.base_generation, \
           abandoned_at = NULL, \
           updated_at = NOW()",
    )
    .bind(board_id)
    .bind(&candidate.state_update)
    .bind(&candidate.state_vector)
    .bind(candidate.base_seq)
    .bind(crate::state::yrs_model::PROTOCOL_VERSION as i32)
    .bind(crate::state::yrs_model::SCHEMA_VERSION as i32)
    .bind(crate::state::yrs_model::MIN_WRITER_VERSION as i32)
    .bind(candidate.server_client_id as i64)
    .bind(candidate.base_generation)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(true)
}
