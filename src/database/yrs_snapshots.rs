//! Canonical Yrs binary snapshot persistence.

use sha2::{Digest, Sha256};
use sqlx::{Executor, PgPool, Postgres};
use uuid::Uuid;

use crate::state::yrs_compaction::CompactedBinarySnapshot;

/// Stable head selected for optimistic background compaction.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CompactionCandidate {
    pub board_id: Uuid,
    pub processed_seq: i64,
    pub base_generation: i64,
    pub writer_epoch: i64,
    pub server_client_id: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct YrsSnapshotRow {
    pub base_generation: i64,
    pub last_event_seq: i64,
    pub state_update: Vec<u8>,
    pub state_vector: Vec<u8>,
    pub protocol_version: i16,
    pub schema_version: i32,
    pub server_client_id: i64,
    pub output_bytes: i64,
    pub state_sha256: Vec<u8>,
}

impl YrsSnapshotRow {
    /// Recomputes and compares the snapshot integrity hash.
    pub fn content_hash_matches(&self) -> bool {
        Sha256::digest(&self.state_update).as_slice() == self.state_sha256.as_slice()
    }
}

const SELECT_COLS: &str = "base_generation, last_event_seq, state_update, state_vector, \
    protocol_version, schema_version, server_client_id, output_bytes, state_sha256";

/// Latest immutable canonical barrier inside an already-read durable head.
/// Bounding by `through_seq` prevents a concurrent compactor publication from
/// changing the base chosen for the same HTTP/activation revision.
pub async fn read_latest_at_or_before<'e, E>(
    exec: E,
    board_id: Uuid,
    base_generation: i64,
    server_client_id: i64,
    through_seq: i64,
) -> Result<Option<YrsSnapshotRow>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!(
        "SELECT {SELECT_COLS} FROM board_yrs_snapshots \
         WHERE board_id = $1 AND base_generation = $2 AND server_client_id = $3 \
           AND source = 'canonical_compaction' AND last_event_seq <= $4 \
         ORDER BY last_event_seq DESC LIMIT 1"
    );
    sqlx::query_as::<_, YrsSnapshotRow>(&sql)
        .bind(board_id)
        .bind(base_generation)
        .bind(server_client_id)
        .bind(through_seq)
        .fetch_optional(exec)
        .await
}

/// Insert an immutable binary barrier. Exact retries are idempotent; a different
/// payload for the same `(board, writer lineage, generation, seq)` is a hard DB conflict rather
/// than an overwrite of the last known-good bytes.
pub async fn insert_snapshot<'e, E>(
    exec: E,
    board_id: Uuid,
    source: &str,
    snapshot: &CompactedBinarySnapshot,
) -> Result<bool, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let result = sqlx::query(
        "INSERT INTO board_yrs_snapshots \
           (board_id, base_generation, last_event_seq, state_update, state_vector, \
            protocol_version, schema_version, update_encoding, server_client_id, source, \
            input_update_count, input_tail_bytes, output_bytes, state_sha256) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,'v1',$8,$9,$10,$11,$12,$13) \
         ON CONFLICT (board_id, server_client_id, base_generation, last_event_seq) DO NOTHING",
    )
    .bind(board_id)
    .bind(snapshot.base_generation)
    .bind(snapshot.last_event_seq)
    .bind(&snapshot.state_update)
    .bind(&snapshot.state_vector)
    .bind(snapshot.protocol_version)
    .bind(snapshot.schema_version)
    .bind(snapshot.server_client_id as i64)
    .bind(source)
    .bind(snapshot.input_update_count)
    .bind(snapshot.input_tail_bytes)
    .bind(snapshot.output_bytes)
    .bind(&snapshot.state_sha256)
    .execute(exec)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Verify that an idempotent insert collided with byte-identical content.
pub async fn exact_snapshot_exists<'e, E>(
    exec: E,
    board_id: Uuid,
    snapshot: &CompactedBinarySnapshot,
) -> Result<bool, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM board_yrs_snapshots \
         WHERE board_id = $1 AND server_client_id = $2 AND base_generation = $3 \
           AND last_event_seq = $4 AND state_sha256 = $5 \
           AND state_update = $6 AND state_vector = $7)",
    )
    .bind(board_id)
    .bind(snapshot.server_client_id as i64)
    .bind(snapshot.base_generation)
    .bind(snapshot.last_event_seq)
    .bind(&snapshot.state_sha256)
    .bind(&snapshot.state_update)
    .bind(&snapshot.state_vector)
    .fetch_one(exec)
    .await
}

/// Selects ready boards whose journal tail needs a new immutable checkpoint.
pub async fn list_compaction_candidates(
    pool: &PgPool,
    min_updates: i64,
    min_bytes: i64,
    limit: i64,
) -> Result<Vec<CompactionCandidate>, sqlx::Error> {
    sqlx::query_as::<_, CompactionCandidate>(
        "SELECT h.board_id, h.processed_seq, h.base_generation, h.writer_epoch, \
                c.server_client_id \
         FROM board_yrs_heads h \
         JOIN board_yrs_canonical_bases c ON c.board_id = h.board_id \
           AND c.base_generation = h.base_generation AND c.abandoned_at IS NULL \
         LEFT JOIN LATERAL ( \
           SELECT s.last_event_seq FROM board_yrs_snapshots s \
           WHERE s.board_id = h.board_id \
             AND s.base_generation = h.base_generation \
             AND s.server_client_id = c.server_client_id \
             AND s.source = 'canonical_compaction' \
             AND s.last_event_seq <= h.processed_seq \
           ORDER BY s.last_event_seq DESC LIMIT 1 \
         ) checkpoint ON TRUE \
         LEFT JOIN LATERAL ( \
           SELECT COUNT(*)::BIGINT AS update_count, \
                  COALESCE(SUM(OCTET_LENGTH(u.yupdate)), 0)::BIGINT AS update_bytes \
           FROM board_yrs_updates u \
           WHERE u.board_id = h.board_id \
             AND u.base_generation = h.base_generation \
             AND u.seq > COALESCE(checkpoint.last_event_seq, c.base_seq) \
             AND u.seq <= h.processed_seq \
         ) tail ON TRUE \
         WHERE h.state = 'ready' \
           AND (checkpoint.last_event_seq IS NULL \
             OR tail.update_count >= $1 OR tail.update_bytes >= $2) \
         ORDER BY COALESCE(checkpoint.last_event_seq, c.base_seq), h.board_id \
         LIMIT $3",
    )
    .bind(min_updates.max(1))
    .bind(min_bytes.max(1))
    .bind(limit.clamp(1, 1_000))
    .fetch_all(pool)
    .await
}

/// Publishes a compacted checkpoint only if the selected head is unchanged.
pub async fn publish_snapshot_cas(
    pool: &PgPool,
    candidate: &CompactionCandidate,
    snapshot: &CompactedBinarySnapshot,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;
    crate::database::yrs_heads::lock_board_xact(&mut tx, candidate.board_id).await?;
    let current = crate::database::yrs_heads::read_head(&mut *tx, candidate.board_id).await?;
    let unchanged = current.is_some_and(|head| {
        head.state == crate::database::yrs_heads::CanonicalState::Ready
            && head.processed_seq == candidate.processed_seq
            && head.base_generation == candidate.base_generation
            && head.writer_epoch == candidate.writer_epoch
    });
    if !unchanged {
        tx.rollback().await?;
        return Ok(false);
    }

    let inserted = insert_snapshot(
        &mut *tx,
        candidate.board_id,
        "canonical_compaction",
        snapshot,
    )
    .await?;
    if !inserted && !exact_snapshot_exists(&mut *tx, candidate.board_id, snapshot).await? {
        return Err(sqlx::Error::Protocol(
            "canonical snapshot key collided with different bytes".to_string(),
        ));
    }
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_detects_snapshot_corruption() {
        let bytes = b"binary yrs state".to_vec();
        let mut row = YrsSnapshotRow {
            base_generation: 1,
            last_event_seq: 0,
            state_update: bytes.clone(),
            state_vector: vec![0],
            protocol_version: 1,
            schema_version: 1,
            server_client_id: (1i64 << 52) + 1,
            output_bytes: bytes.len() as i64,
            state_sha256: Sha256::digest(&bytes).to_vec(),
        };
        assert!(row.content_hash_matches());
        row.state_update.push(0xff);
        assert!(!row.content_hash_matches());
    }
}
