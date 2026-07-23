//! Canonical Yrs journal retention.
//!
//! Disabled by default. The database query itself requires two immutable
//! binary barriers in the active writer lineage. It deletes canonical journal
//! rows and their obsolete event records only below the older barrier and past
//! the configured offline/restore TTL.

use std::time::Duration;

use sqlx::PgPool;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::database::yrs_updates;

/// Starts periodic compaction and retention of canonical Yrs update history.
pub fn start_yrs_retention_job(
    pool: PgPool,
    interval_secs: u64,
    retention_days: i64,
    batch_limit: i64,
) {
    let tick = Duration::from_secs(interval_secs.max(60));
    tokio::spawn(async move {
        let mut ticker = interval(tick);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            match yrs_updates::prune_compacted_batch(&pool, retention_days, batch_limit).await {
                Ok(deleted) if deleted.is_empty() => {}
                Ok(deleted) => info!(
                    "canonical retention updates_deleted={} events_deleted={} retention_days={}",
                    deleted.update_rows, deleted.event_rows, retention_days
                ),
                Err(error) => warn!("yrs journal retention failed: {error}"),
            }
        }
    });
}
