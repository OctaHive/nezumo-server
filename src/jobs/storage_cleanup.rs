//! Retriable cleanup of S3 prefixes queued by board-deletion DB triggers.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::core::config::get_env_with_default;
use crate::database::storage_deletion_jobs::{
    complete, get_by_board_id, list_pending, record_failure, BoardStorageDeletionJob,
};
use crate::routes::AppState;
use crate::storage::delete::{delete_from_storage, delete_objects_with_prefix};

async fn delete_job_objects(
    state: &AppState,
    bucket: &str,
    job: &BoardStorageDeletionJob,
) -> Result<usize, String> {
    let mut deleted =
        delete_objects_with_prefix(&state.storage, bucket, &job.object_prefix).await?;

    if let Some(preview_key) = job
        .preview_object_key
        .as_deref()
        .filter(|key| !key.trim().is_empty() && !key.starts_with(&job.object_prefix))
    {
        delete_from_storage(&state.storage, bucket, preview_key).await?;
        deleted += 1;
    }

    Ok(deleted)
}

pub fn start_storage_cleanup_job(state: Arc<AppState>, interval_secs: u64, batch_limit: i64) {
    let tick_interval = Duration::from_secs(interval_secs.max(5));
    tokio::spawn(async move {
        let mut ticker = interval(tick_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(error) = run_cleanup_cycle(&state, batch_limit.clamp(1, 100)).await {
                warn!("board storage cleanup cycle failed: {error}");
            }
        }
    });
}

/// Performs an immediate first pass but intentionally leaves the durable job
/// queued. The delayed worker makes a second pass after in-flight uploads have
/// had time to observe the deleted board and clean themselves up.
pub async fn cleanup_board_storage_now(state: &AppState, board_id: Uuid) {
    let job = match get_by_board_id(&state.database, board_id).await {
        Ok(Some(job)) => job,
        Ok(None) => return,
        Err(error) => {
            warn!("could not load storage cleanup job for board {board_id}: {error}");
            return;
        }
    };

    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    match delete_job_objects(state, &bucket, &job).await {
        Ok(deleted) => {
            info!(
                "immediate board storage cleanup removed {deleted} object(s) for board {board_id}"
            );
        }
        Err(error) => {
            warn!("immediate board storage cleanup failed for board {board_id}: {error}");
            if let Err(database_error) =
                record_failure(&state.database, board_id, &job.object_prefix, &error).await
            {
                warn!(
                    "could not record immediate storage cleanup failure for board {board_id}: {database_error}"
                );
            }
        }
    }
}

async fn run_cleanup_cycle(state: &AppState, batch_limit: i64) -> Result<(), sqlx::Error> {
    let jobs = list_pending(&state.database, batch_limit).await?;
    for job in jobs {
        process_job(state, job).await;
    }
    Ok(())
}

async fn process_job(state: &AppState, job: BoardStorageDeletionJob) {
    let bucket = get_env_with_default("STORAGE_BUCKET_BOARD_FILES", "board-files");
    match delete_job_objects(state, &bucket, &job).await {
        Ok(deleted) => {
            if let Err(error) = complete(&state.database, job.board_id, &job.object_prefix).await {
                warn!(
                    "could not complete storage cleanup job for board {}: {error}",
                    job.board_id
                );
                return;
            }
            info!(
                "board storage cleanup removed {deleted} object(s) for board {}",
                job.board_id
            );
        }
        Err(error) => {
            warn!(
                "board storage cleanup failed for board {}: {error}",
                job.board_id
            );
            if let Err(database_error) =
                record_failure(&state.database, job.board_id, &job.object_prefix, &error).await
            {
                warn!(
                    "could not record storage cleanup failure for board {}: {database_error}",
                    job.board_id
                );
            }
        }
    }
}
