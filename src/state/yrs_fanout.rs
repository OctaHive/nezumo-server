//! Cross-instance canonical realtime fan-out.
//!
//! PostgreSQL remains the source of truth. Redis Pub/Sub carries only a small
//! wake-up notification `(board, seq, origin)`; receiving instances read the
//! durable `board_events + board_yrs_updates` pair before broadcasting it to
//! their local WebSocket hub. A periodic reconciliation of boards with local
//! receivers repairs a dropped final notification, while the per-board cursor
//! makes duplicate notifications harmless.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use deadpool_redis::redis::AsyncCommands;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::database::{events, yrs_heads, yrs_updates};
use crate::realtime::{broadcast_ws, WsMessage};

/// Redis Pub/Sub channel carrying versioned durable-update notifications.
const CHANNEL: &str = "nezumo:yrs:durable:v1";
/// Maximum number of journal rows replayed by one reconciliation query.
const CATCH_UP_BATCH: i64 = 512;

/// Minimal cross-instance notification; canonical content remains in PostgreSQL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DurableNotice {
    /// Server instance that published the notification.
    origin: Uuid,
    /// Board whose durable journal advanced.
    board_id: Uuid,
    /// Sequence that caused the notification.
    seq: i64,
}

/// Shared dependencies and per-board delivery cursors owned by the fan-out handle.
struct Inner {
    /// Stable identifier used to ignore notifications published by this process.
    instance_id: Uuid,
    /// PostgreSQL pool used to replay authoritative event/update pairs.
    database: PgPool,
    /// Redis pool used to publish lightweight wake-up notifications.
    cache: deadpool_redis::Pool,
    /// Local WebSocket channel registry receiving replayed durable messages.
    realtime: crate::realtime::RealtimeHub,
    /// Highest sequence delivered locally for each active board.
    delivered: Mutex<HashMap<Uuid, i64>>,
}

/// Cloneable multi-instance durable fan-out handle.
#[derive(Clone)]
pub struct CanonicalFanout {
    /// Shared state used by publishers, subscriber, and repair task.
    inner: Arc<Inner>,
}

impl std::fmt::Debug for CanonicalFanout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalFanout")
            .field("instance_id", &self.inner.instance_id)
            .finish_non_exhaustive()
    }
}

impl CanonicalFanout {
    /// Creates fan-out state from the application database, Redis, and realtime hubs.
    pub fn new(
        database: PgPool,
        cache: deadpool_redis::Pool,
        realtime: crate::realtime::RealtimeHub,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                instance_id: instance_id(),
                database,
                cache,
                realtime,
                delivered: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Starts the reconnecting Redis subscriber and durable repair poller.
    pub fn start(&self) {
        let subscriber = self.clone();
        tokio::spawn(async move { subscriber.run_subscriber().await });
        let repair = self.clone();
        tokio::spawn(async move { repair.run_repair().await });
        tracing::info!("yrs fan-out enabled (instance={})", self.inner.instance_id);
    }

    /// Initialize the repair cursor when a board gains a local WS receiver.
    /// The client bootstrap/catch-up owns history through this durable head;
    /// fan-out owns later updates.
    pub async fn register_board(&self, board_id: Uuid) {
        let already_active = self
            .inner
            .realtime
            .active_board_ids()
            .await
            .contains(&board_id);
        if already_active && self.cursor(board_id).is_some() {
            return;
        }
        match yrs_heads::read_head(&self.inner.database, board_id).await {
            Ok(Some(head)) => self.set_cursor(board_id, head.processed_seq),
            Ok(None) => self.set_cursor(board_id, 0),
            Err(error) => {
                tracing::warn!("yrs fan-out: failed to initialize board {board_id} cursor: {error}")
            }
        }
    }

    /// Broadcast the just-committed durable pair locally, then notify other
    /// instances. Redis failure does not fail the commit; their repair pollers
    /// will discover the PostgreSQL head. The supplied update is intentionally
    /// reloaded from PostgreSQL so handler completion order cannot reorder it.
    pub async fn publish(&self, commit: WsMessage, _yrs_update: Option<WsMessage>) {
        let (board_id, seq) = match &commit {
            WsMessage::Commit { board_id, seq, .. } => (*board_id, *seq),
            _ => return,
        };
        // Read from the journal even on the originating node. Two commits can
        // finish their handler futures in reverse publish order after releasing
        // the resident mutex; journal replay preserves seq order and never drops
        // the earlier pair behind a monotonic dedup cursor.
        if self
            .inner
            .realtime
            .active_board_ids()
            .await
            .contains(&board_id)
        {
            if self.cursor(board_id).is_none() {
                self.initialize_cursor(board_id, seq.saturating_sub(1));
            }
            self.reconcile_board(board_id).await;
        }
        let notice = DurableNotice {
            origin: self.inner.instance_id,
            board_id,
            seq,
        };
        let Ok(payload) = serde_json::to_string(&notice) else {
            return;
        };
        match self.inner.cache.get().await {
            Ok(mut connection) => {
                let result: Result<i64, _> = connection.publish(CHANNEL, payload).await;
                if let Err(error) = result {
                    tracing::warn!("yrs fan-out publish failed at {board_id}/{seq}: {error}");
                }
            }
            Err(error) => {
                tracing::warn!("yrs fan-out connection unavailable at {board_id}/{seq}: {error}")
            }
        }
    }

    /// Runs the subscriber forever, reconnecting with bounded exponential backoff.
    async fn run_subscriber(self) {
        let mut retry = Duration::from_millis(250);
        loop {
            let error = self
                .subscribe_once()
                .await
                .err()
                .unwrap_or_else(|| "subscriber stopped".to_string());
            tracing::warn!("yrs fan-out subscriber disconnected: {error}; reconnecting");
            tokio::time::sleep(retry).await;
            retry = (retry * 2).min(Duration::from_secs(5));
        }
    }

    /// Consumes one Redis Pub/Sub connection until it fails or its stream ends.
    async fn subscribe_once(&self) -> Result<(), String> {
        let url = crate::cache::connect::redis_url().map_err(|error| error.to_string())?;
        let client = deadpool_redis::redis::Client::open(url).map_err(|e| e.to_string())?;
        let mut pubsub = client.get_async_pubsub().await.map_err(|e| e.to_string())?;
        pubsub.subscribe(CHANNEL).await.map_err(|e| e.to_string())?;
        let mut stream = pubsub.on_message();
        while let Some(message) = stream.next().await {
            let payload: String = message.get_payload().map_err(|e| e.to_string())?;
            let notice: DurableNotice = match serde_json::from_str(&payload) {
                Ok(notice) => notice,
                Err(error) => {
                    tracing::warn!("yrs fan-out ignored malformed notice: {error}");
                    continue;
                }
            };
            if notice.origin == self.inner.instance_id {
                continue;
            }
            if !self
                .inner
                .realtime
                .active_board_ids()
                .await
                .contains(&notice.board_id)
            {
                continue;
            }
            self.reconcile_board(notice.board_id).await;
        }
        Err("pubsub stream ended".to_string())
    }

    /// Periodically reconciles every board that currently has local receivers.
    async fn run_repair(self) {
        let interval_ms = crate::core::config::get_env_u64("YRS_FANOUT_REPAIR_MS", 1000).max(100);
        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            for board_id in self.inner.realtime.active_board_ids().await {
                if self.cursor(board_id).is_none() {
                    self.register_board(board_id).await;
                } else {
                    self.reconcile_board(board_id).await;
                }
            }
        }
    }

    /// Replays ordered durable rows after the board's local delivery cursor.
    async fn reconcile_board(&self, board_id: Uuid) {
        let Some(mut cursor) = self.cursor(board_id) else {
            self.register_board(board_id).await;
            return;
        };
        loop {
            let rows = match yrs_updates::list_updates_since(
                &self.inner.database,
                board_id,
                cursor,
                CATCH_UP_BATCH,
            )
            .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!("yrs fan-out catch-up failed for {board_id}: {error}");
                    return;
                }
            };
            if rows.is_empty() {
                return;
            }
            let count = rows.len();
            for row in rows {
                let Some(event_id) = row.event_id else {
                    self.request_resync(board_id, row.seq).await;
                    cursor = row.seq;
                    continue;
                };
                let event = match events::read_event_by_id(&self.inner.database, event_id).await {
                    Ok(Some(event)) => event,
                    Ok(None) => {
                        // Compaction won the race. A full canonical bootstrap is
                        // the only safe recovery path.
                        self.request_resync(board_id, row.seq).await;
                        cursor = row.seq;
                        continue;
                    }
                    Err(error) => {
                        tracing::warn!(
                            "yrs fan-out event read failed for {board_id}/{}: {error}",
                            row.seq
                        );
                        return;
                    }
                };
                let commit = WsMessage::Commit {
                    board_id,
                    seq: row.seq,
                    server_event_id: event.id,
                    client_event_id: Some(row.client_event_id.clone()),
                    user_id: event.user_id,
                    event_type: event.event_type,
                    payload: event.payload,
                    session_id: event.session_id,
                };
                let yrs = WsMessage::YrsUpdate {
                    board_id,
                    seq: row.seq,
                    client_event_id: Some(row.client_event_id),
                    schema_version: row.schema_version,
                    yupdate: base64::engine::general_purpose::STANDARD.encode(row.yupdate),
                };
                self.deliver_pair(board_id, row.seq, commit, Some(yrs))
                    .await;
                cursor = row.seq;
            }
            if count < CATCH_UP_BATCH as usize {
                return;
            }
        }
    }

    /// Advances the deduplication cursor and broadcasts one ordered durable pair.
    async fn deliver_pair(
        &self,
        board_id: Uuid,
        seq: i64,
        commit: WsMessage,
        yrs_update: Option<WsMessage>,
    ) {
        if !self.advance_cursor(board_id, seq) {
            return;
        }
        self.broadcast_pair(board_id, commit, yrs_update).await;
    }

    /// Sends an event envelope followed by its optional Yrs update locally.
    async fn broadcast_pair(
        &self,
        board_id: Uuid,
        commit: WsMessage,
        yrs_update: Option<WsMessage>,
    ) {
        let sender = self.inner.realtime.channels_for(board_id).await.commits;
        broadcast_ws(&sender, commit);
        if let Some(update) = yrs_update {
            broadcast_ws(&sender, update);
        }
    }

    /// Requests a full client bootstrap when the durable event/update pair is incomplete.
    async fn request_resync(&self, board_id: Uuid, through_seq: i64) {
        self.advance_cursor(board_id, through_seq);
        let sender = self.inner.realtime.channels_for(board_id).await.commits;
        let _ = sender.send(axum::extract::ws::Utf8Bytes::from(r#"{"type":"resync"}"#));
    }

    /// Returns the highest locally delivered sequence for a board.
    fn cursor(&self, board_id: Uuid) -> Option<i64> {
        self.inner
            .delivered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&board_id)
            .copied()
    }

    /// Initializes a missing cursor without overwriting a concurrent value.
    fn initialize_cursor(&self, board_id: Uuid, seq: i64) {
        self.inner
            .delivered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(board_id)
            .or_insert(seq);
    }

    /// Replaces a board cursor with a known durable bootstrap boundary.
    fn set_cursor(&self, board_id: Uuid, seq: i64) {
        self.inner
            .delivered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(board_id, seq);
    }

    /// Monotonically advances a board cursor and reports whether delivery should proceed.
    fn advance_cursor(&self, board_id: Uuid, seq: i64) -> bool {
        advance_cursor_map(
            &mut self
                .inner
                .delivered
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
            board_id,
            seq,
        )
    }
}

/// Reads the configured process identifier or creates an ephemeral UUID.
fn instance_id() -> Uuid {
    std::env::var("YRS_FANOUT_INSTANCE_ID")
        .ok()
        .and_then(|value| Uuid::parse_str(value.trim()).ok())
        .unwrap_or_else(Uuid::new_v4)
}

/// Applies the cursor's monotonic deduplication rule to an in-memory map.
fn advance_cursor_map(cursors: &mut HashMap<Uuid, i64>, board_id: Uuid, seq: i64) -> bool {
    let cursor = cursors.entry(board_id).or_insert(i64::MIN);
    if seq <= *cursor {
        return false;
    }
    *cursor = seq;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notice_roundtrip_is_stable() {
        let notice = DurableNotice {
            origin: Uuid::new_v4(),
            board_id: Uuid::new_v4(),
            seq: 42,
        };
        let wire = serde_json::to_string(&notice).unwrap();
        assert_eq!(
            serde_json::from_str::<DurableNotice>(&wire).unwrap(),
            notice
        );
    }

    #[test]
    fn delivery_cursor_deduplicates_and_advances_monotonically() {
        let board_id = Uuid::new_v4();
        let mut cursors = HashMap::new();
        assert!(advance_cursor_map(&mut cursors, board_id, 10));
        assert!(!advance_cursor_map(&mut cursors, board_id, 10));
        assert!(!advance_cursor_map(&mut cursors, board_id, 9));
        assert!(advance_cursor_map(&mut cursors, board_id, 11));
    }
}
