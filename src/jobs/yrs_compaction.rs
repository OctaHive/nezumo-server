//! Periodic creation of immutable canonical Yrs checkpoints.

use std::time::Duration;

use sqlx::PgPool;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::database::{yrs_canonical_bases, yrs_heads, yrs_snapshots, yrs_updates};
use crate::state::yrs_compaction::{compact_binary_snapshot, BinarySnapshotBase, BinaryTailUpdate};

const UPDATE_PAGE: i64 = 1_000;

/// Starts bounded optimistic compaction of canonical update journals.
pub fn start_yrs_compaction_job(
    pool: PgPool,
    interval_secs: u64,
    batch_limit: i64,
    min_updates: i64,
    min_bytes: i64,
) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_secs.max(60)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            match compact_batch(&pool, batch_limit, min_updates, min_bytes).await {
                Ok(0) => {}
                Ok(count) => info!("yrs compaction published={count}"),
                Err(error) => warn!("yrs compaction batch failed: {error}"),
            }
        }
    });
}

async fn compact_batch(
    pool: &PgPool,
    batch_limit: i64,
    min_updates: i64,
    min_bytes: i64,
) -> Result<usize, String> {
    let candidates =
        yrs_snapshots::list_compaction_candidates(pool, min_updates, min_bytes, batch_limit)
            .await
            .map_err(|error| format!("select candidates: {error}"))?;
    let mut published = 0;
    for candidate in candidates {
        match compact_one(pool, &candidate).await {
            Ok(true) => published += 1,
            Ok(false) => {}
            Err(error) => warn!(board_id = %candidate.board_id, "yrs compaction skipped: {error}"),
        }
    }
    Ok(published)
}

async fn compact_one(
    pool: &PgPool,
    candidate: &yrs_snapshots::CompactionCandidate,
) -> Result<bool, String> {
    let head = yrs_heads::read_head(pool, candidate.board_id)
        .await
        .map_err(|error| format!("read head: {error}"))?
        .ok_or_else(|| "canonical head disappeared".to_string())?;
    if head.state != yrs_heads::CanonicalState::Ready
        || head.processed_seq != candidate.processed_seq
        || head.base_generation != candidate.base_generation
        || head.writer_epoch != candidate.writer_epoch
    {
        return Ok(false);
    }

    let canonical = yrs_canonical_bases::read_base(pool, candidate.board_id)
        .await
        .map_err(|error| format!("read canonical base: {error}"))?
        .ok_or_else(|| "canonical base disappeared".to_string())?;
    let checkpoint = yrs_snapshots::read_latest_at_or_before(
        pool,
        candidate.board_id,
        candidate.base_generation,
        candidate.server_client_id,
        candidate.processed_seq,
    )
    .await
    .map_err(|error| format!("read checkpoint: {error}"))?;
    let base = checkpoint.map_or_else(
        || BinarySnapshotBase {
            state_update: canonical.state_update,
            state_vector: canonical.state_vector,
            last_event_seq: canonical.base_seq,
            base_generation: canonical.base_generation,
            server_client_id: canonical.server_client_id as u64,
        },
        |snapshot| BinarySnapshotBase {
            state_update: snapshot.state_update,
            state_vector: snapshot.state_vector,
            last_event_seq: snapshot.last_event_seq,
            base_generation: snapshot.base_generation,
            server_client_id: snapshot.server_client_id as u64,
        },
    );

    let mut cursor = base.last_event_seq;
    let mut tail = Vec::new();
    while cursor < candidate.processed_seq {
        let page = yrs_updates::list_updates_page(
            pool,
            candidate.board_id,
            cursor,
            candidate.processed_seq,
            candidate.base_generation,
            UPDATE_PAGE,
        )
        .await
        .map_err(|error| format!("read journal tail: {error}"))?;
        if page.is_empty() {
            break;
        }
        let page_len = page.len();
        for row in page {
            cursor = row.seq;
            tail.push(BinaryTailUpdate {
                seq: row.seq,
                yupdate: row.yupdate,
            });
        }
        if page_len < UPDATE_PAGE as usize {
            break;
        }
    }

    let snapshot = compact_binary_snapshot(&base, candidate.processed_seq, &tail)
        .map_err(|error| format!("compact document: {error}"))?;
    yrs_snapshots::publish_snapshot_cas(pool, candidate, &snapshot)
        .await
        .map_err(|error| format!("publish checkpoint: {error}"))
}
