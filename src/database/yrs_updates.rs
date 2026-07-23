//! Persistence for the durable canonical Yrs update journal
//! (`board_yrs_updates`). Each accepted commit and its canonical update are
//! written in the same transaction; DB uniqueness constraints are the
//! correctness boundary.
//!
//! Uses runtime `sqlx` (not the compile-checked macros) because the table has no
//! offline query cache. Read helpers are generic over `Executor` so they work
//! against both the pool (pre-checks) and inside the coordinator's atomic tx
//! (TOCTOU re-check).

use sqlx::{Executor, PgPool, Postgres};
use uuid::Uuid;

/// Rows removed by one bounded canonical-retention transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneResult {
    pub update_rows: u64,
    pub event_rows: u64,
}

impl PruneResult {
    pub fn is_empty(self) -> bool {
        self.update_rows == 0 && self.event_rows == 0
    }
}

/// A persisted canonical-update row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct YrsUpdateRow {
    pub board_id: Uuid,
    pub seq: i64,
    pub event_id: Option<Uuid>,
    pub client_event_id: String,
    pub payload_hash: Vec<u8>,
    pub base_generation: i64,
    pub schema_version: i32,
    pub protocol_version: i16,
    pub source: String,
    pub writer_client_id: Option<i64>,
    pub update_hash: Vec<u8>,
    pub yupdate: Vec<u8>,
}

const SELECT_COLS: &str = "board_id, seq, event_id, client_event_id, \
     payload_hash, base_generation, schema_version, protocol_version, \
     source, writer_client_id, update_hash, yupdate";

/// Insert one canonical update row. Fails on `UNIQUE(board_id, seq)` or
/// `UNIQUE(board_id, client_event_id)` — the caller (coordinator) treats a
/// unique violation as a dedup race and reconciles against the persisted winner,
/// not as a validation or compare-and-swap failure.
pub async fn insert_update<'e, E>(exec: E, row: &YrsUpdateRow) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        "INSERT INTO board_yrs_updates \
         (board_id, seq, event_id, client_event_id, payload_hash, \
          base_generation, schema_version, protocol_version, source, writer_client_id, \
          update_hash, yupdate) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(row.board_id)
    .bind(row.seq)
    .bind(row.event_id)
    .bind(&row.client_event_id)
    .bind(&row.payload_hash)
    .bind(row.base_generation)
    .bind(row.schema_version)
    .bind(row.protocol_version)
    .bind(&row.source)
    .bind(row.writer_client_id)
    .bind(&row.update_hash)
    .bind(&row.yupdate)
    .execute(exec)
    .await?;
    Ok(())
}

/// Read the persisted update for `(board_id, client_event_id)`, if any. Used for
/// the early dedup pre-check and the dedup-race loser's reconciliation.
pub async fn read_by_client_event_id<'e, E>(
    exec: E,
    board_id: Uuid,
    client_event_id: &str,
) -> Result<Option<YrsUpdateRow>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!(
        "SELECT {SELECT_COLS} FROM board_yrs_updates \
         WHERE board_id = $1 AND client_event_id = $2"
    );
    sqlx::query_as::<_, YrsUpdateRow>(&sql)
        .bind(board_id)
        .bind(client_event_id)
        .fetch_optional(exec)
        .await
}

/// Reads the persisted update at `(board_id, seq)`, if any.
pub async fn read_by_seq<'e, E>(
    exec: E,
    board_id: Uuid,
    seq: i64,
) -> Result<Option<YrsUpdateRow>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql =
        format!("SELECT {SELECT_COLS} FROM board_yrs_updates WHERE board_id = $1 AND seq = $2");
    sqlx::query_as::<_, YrsUpdateRow>(&sql)
        .bind(board_id)
        .bind(seq)
        .fetch_optional(exec)
        .await
}

/// Ordered canonical updates with `seq > since_seq` for bounded catch-up.
pub async fn list_updates_since<'e, E>(
    exec: E,
    board_id: Uuid,
    since_seq: i64,
    limit: i64,
) -> Result<Vec<YrsUpdateRow>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!(
        "SELECT {SELECT_COLS} FROM board_yrs_updates \
         WHERE board_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3"
    );
    sqlx::query_as::<_, YrsUpdateRow>(&sql)
        .bind(board_id)
        .bind(since_seq)
        .bind(limit)
        .fetch_all(exec)
        .await
}

/// A generation-fenced catch-up page at one already-read durable head. Reading
/// the head first and querying only `seq <= through_seq` gives the HTTP response
/// a stable upper boundary even when a new commit lands concurrently.
pub async fn list_updates_page<'e, E>(
    exec: E,
    board_id: Uuid,
    since_seq: i64,
    through_seq: i64,
    base_generation: i64,
    limit: i64,
) -> Result<Vec<YrsUpdateRow>, sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let sql = format!(
        "SELECT {SELECT_COLS} FROM board_yrs_updates \
         WHERE board_id = $1 AND seq > $2 AND seq <= $3 AND base_generation = $4 \
         ORDER BY seq ASC LIMIT $5"
    );
    sqlx::query_as::<_, YrsUpdateRow>(&sql)
        .bind(board_id)
        .bind(since_seq)
        .bind(through_seq)
        .bind(base_generation)
        .bind(limit)
        .fetch_all(exec)
        .await
}

/// Delete bounded sets of canonical updates and their obsolete event records.
///
/// Both tables use the *previous* immutable binary barrier, never the newest
/// one. Requiring two verified lineage-matched snapshots preserves a tested
/// recovery base plus its tail. The time window protects offline idempotency
/// keys and operational restore TTLs.
///
/// Journal rows are deleted first. Event rows are eligible only when no journal
/// row with the same `(board_id, seq)` remains, so a concurrent fan-out reader
/// can never observe an update whose event half was removed by retention. Both
/// bounded deletes commit atomically.
pub async fn prune_compacted_batch(
    pool: &PgPool,
    retention_days: i64,
    limit: i64,
) -> Result<PruneResult, sqlx::Error> {
    let retention_days = retention_days.clamp(7, 3650);
    let limit = limit.clamp(1, 10_000);
    let mut tx = pool.begin().await?;

    let updates = sqlx::query(
        "WITH ranked AS ( \
           SELECT s.board_id, s.server_client_id, s.base_generation, s.last_event_seq, \
                  ROW_NUMBER() OVER ( \
                    PARTITION BY s.board_id, s.server_client_id, s.base_generation \
                    ORDER BY s.last_event_seq DESC \
                  ) AS snapshot_rank \
           FROM board_yrs_snapshots s \
           JOIN board_yrs_canonical_bases b ON b.board_id = s.board_id \
             AND b.server_client_id = s.server_client_id \
             AND b.base_generation = s.base_generation AND b.abandoned_at IS NULL \
           JOIN board_yrs_heads h ON h.board_id = s.board_id \
             AND h.state = 'ready' AND h.base_generation = s.base_generation \
           WHERE s.source = 'canonical_compaction' \
         ), barriers AS ( \
           SELECT board_id, base_generation, last_event_seq \
           FROM ranked WHERE snapshot_rank = 2 \
         ), victims AS ( \
           SELECT u.ctid FROM board_yrs_updates u \
           JOIN barriers b ON b.board_id = u.board_id \
             AND b.base_generation = u.base_generation \
           WHERE u.seq <= b.last_event_seq \
             AND u.created_at < NOW() - ($1 * INTERVAL '1 day') \
           ORDER BY u.created_at, u.board_id, u.seq \
           LIMIT $2 FOR UPDATE OF u SKIP LOCKED \
         ) \
         DELETE FROM board_yrs_updates u USING victims v WHERE u.ctid = v.ctid",
    )
    .bind(retention_days)
    .bind(limit)
    .execute(&mut *tx)
    .await?;

    // This second statement sees the journal deletions made above. Existing
    // historical event orphans are collected too, but an event with a retained
    // canonical update is never eligible.
    let events = sqlx::query(
        "WITH ranked AS ( \
           SELECT s.board_id, s.server_client_id, s.base_generation, s.last_event_seq, \
                  ROW_NUMBER() OVER ( \
                    PARTITION BY s.board_id, s.server_client_id, s.base_generation \
                    ORDER BY s.last_event_seq DESC \
                  ) AS snapshot_rank \
           FROM board_yrs_snapshots s \
           JOIN board_yrs_canonical_bases b ON b.board_id = s.board_id \
             AND b.server_client_id = s.server_client_id \
             AND b.base_generation = s.base_generation AND b.abandoned_at IS NULL \
           JOIN board_yrs_heads h ON h.board_id = s.board_id \
             AND h.state = 'ready' AND h.base_generation = s.base_generation \
           WHERE s.source = 'canonical_compaction' \
         ), barriers AS ( \
           SELECT board_id, last_event_seq FROM ranked WHERE snapshot_rank = 2 \
         ), victims AS ( \
           SELECT e.ctid FROM board_events e \
           JOIN barriers b ON b.board_id = e.board_id \
           WHERE e.seq <= b.last_event_seq \
             AND e.created_at < NOW() - ($1 * INTERVAL '1 day') \
             AND NOT EXISTS ( \
               SELECT 1 FROM board_yrs_updates u \
               WHERE u.board_id = e.board_id AND u.seq = e.seq \
             ) \
           ORDER BY e.created_at, e.board_id, e.seq \
           LIMIT $2 FOR UPDATE OF e SKIP LOCKED \
         ) \
         DELETE FROM board_events e USING victims v WHERE e.ctid = v.ctid",
    )
    .bind(retention_days)
    .bind(limit)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(PruneResult {
        update_rows: updates.rows_affected(),
        event_rows: events.rows_affected(),
    })
}

#[cfg(test)]
mod retention_tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    /// Exercises the real retention SQL against an isolated, migrated
    /// PostgreSQL database. Kept ignored because it intentionally mutates its
    /// target database; CI or local verification can opt in explicitly.
    #[tokio::test]
    #[ignore = "requires NEZUMO_TEST_DATABASE_URL pointing to an isolated migrated PostgreSQL database"]
    async fn prunes_only_old_rows_below_previous_barrier() {
        let database_url = std::env::var("NEZUMO_TEST_DATABASE_URL")
            .expect("NEZUMO_TEST_DATABASE_URL must point to an isolated migrated database");
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect test database");

        let user_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let board_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO users (id, username, email, password_hash) VALUES ($1, $2, $3, 'test')",
        )
        .bind(user_id)
        .bind(format!("retention-{user_id}"))
        .bind(format!("retention-{user_id}@example.test"))
        .execute(&pool)
        .await
        .expect("insert user");
        sqlx::query("INSERT INTO projects (id, owner_id, name) VALUES ($1, $2, 'retention')")
            .bind(project_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("insert project");
        sqlx::query(
            "INSERT INTO boards (id, project_id, owner_id, title) \
             VALUES ($1, $2, $3, 'retention')",
        )
        .bind(board_id)
        .bind(project_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("insert board");

        let server_client_id = 4_503_599_627_370_496_i64;
        sqlx::query(
            "INSERT INTO board_yrs_canonical_bases \
             (board_id, state_update, state_vector, base_seq, protocol_version, \
              schema_version, min_writer_version, update_encoding, server_client_id, \
              base_generation) \
             VALUES ($1, '\\x00', '\\x00', 0, 1, 2, 1, 'v1', $2, 1)",
        )
        .bind(board_id)
        .bind(server_client_id)
        .execute(&pool)
        .await
        .expect("insert canonical base");
        sqlx::query(
            "INSERT INTO board_yrs_heads \
             (board_id, processed_seq, base_generation, writer_epoch, state) \
             VALUES ($1, 20, 1, 1, 'ready')",
        )
        .bind(board_id)
        .execute(&pool)
        .await
        .expect("insert canonical head");
        for seq in [10_i64, 20_i64] {
            sqlx::query(
                "INSERT INTO board_yrs_snapshots \
                 (board_id, base_generation, last_event_seq, state_update, state_vector, \
                  protocol_version, schema_version, server_client_id, source, output_bytes, \
                  state_sha256) \
                 VALUES ($1, 1, $2, '\\x00', '\\x00', 1, 2, $3, \
                         'canonical_compaction', 1, decode(repeat('00', 32), 'hex'))",
            )
            .bind(board_id)
            .bind(seq)
            .bind(server_client_id)
            .execute(&pool)
            .await
            .expect("insert immutable barrier");
        }

        for (seq, age_days) in [(1_i64, 10_i64), (3, 10), (4, 1), (11, 10)] {
            insert_pair(&pool, board_id, user_id, seq, age_days).await;
        }
        sqlx::query(
            "INSERT INTO board_events \
             (board_id, seq, user_id, event_type, payload, created_at) \
             VALUES ($1, 2, $2, 'legacy_orphan', '{}'::jsonb, NOW() - INTERVAL '10 days')",
        )
        .bind(board_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("insert historical orphan event");

        let deleted = prune_compacted_batch(&pool, 7, 100)
            .await
            .expect("run canonical retention");
        assert_eq!(
            deleted,
            PruneResult {
                update_rows: 2,
                event_rows: 3,
            }
        );

        let event_seqs: Vec<i64> =
            sqlx::query_scalar("SELECT seq FROM board_events WHERE board_id = $1 ORDER BY seq")
                .bind(board_id)
                .fetch_all(&pool)
                .await
                .expect("read retained events");
        let update_seqs: Vec<i64> = sqlx::query_scalar(
            "SELECT seq FROM board_yrs_updates WHERE board_id = $1 ORDER BY seq",
        )
        .bind(board_id)
        .fetch_all(&pool)
        .await
        .expect("read retained updates");
        assert_eq!(event_seqs, vec![4, 11]);
        assert_eq!(update_seqs, vec![4, 11]);
        assert!(prune_compacted_batch(&pool, 7, 100)
            .await
            .expect("rerun canonical retention")
            .is_empty());

        sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("remove test fixtures");
    }

    async fn insert_pair(pool: &PgPool, board_id: Uuid, user_id: Uuid, seq: i64, age_days: i64) {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO board_events \
             (id, board_id, seq, user_id, event_type, payload, created_at) \
             VALUES ($1, $2, $3, $4, 'board_commit', '{}'::jsonb, \
                     NOW() - ($5 * INTERVAL '1 day'))",
        )
        .bind(event_id)
        .bind(board_id)
        .bind(seq)
        .bind(user_id)
        .bind(age_days)
        .execute(pool)
        .await
        .expect("insert event");
        sqlx::query(
            "INSERT INTO board_yrs_updates \
             (board_id, seq, event_id, client_event_id, payload_hash, base_generation, \
              schema_version, protocol_version, source, update_hash, yupdate, created_at) \
             VALUES ($1, $2, $3, $4, '\\x00', 1, 2, 1, 'server', '\\x00', '\\x00', \
                     NOW() - ($5 * INTERVAL '1 day'))",
        )
        .bind(board_id)
        .bind(seq)
        .bind(event_id)
        .bind(format!("retention-{seq}"))
        .bind(age_days)
        .execute(pool)
        .await
        .expect("insert canonical update");
    }
}
