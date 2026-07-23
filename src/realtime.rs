//! Shared websocket wire messages, connection registry, and board broadcasts.

use axum::extract::ws::Utf8Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

/// Client-authored canonical update envelope. JSON ingress carries base64,
/// which is decoded once and persisted as BYTEA.
#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct ClientYrsUpdate {
    pub encoding: String,
    pub protocol_version: i32,
    pub schema_version: i32,
    pub base_generation: i64,
    pub observed_seq: i64,
    pub writer_client_id: u64,
    pub update_b64: String,
    /// Populated only by negotiated binary WS ingress. Never serialized or
    /// logged; HTTP/rollback JSON continues to use `update_b64`.
    #[serde(skip)]
    pub update_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage {
    Preview {
        board_id: Uuid,
        user_id: Uuid,
        payload: Value,
    },
    Commit {
        board_id: Uuid,
        seq: i64,
        server_event_id: Uuid,
        /// Stable optimistic id echoed to every client. Additive for legacy
        /// readers and required for crash-safe pending/echo deduplication.
        client_event_id: Option<String>,
        user_id: Uuid,
        event_type: String,
        payload: Value,
        session_id: Option<String>,
    },
    Ack {
        board_id: Uuid,
        seq: i64,
        server_event_id: Uuid,
        client_event_id: Option<String>,
    },
    Nack {
        board_id: Uuid,
        client_event_id: Option<String>,
        code: String,
        reason: String,
        retryable: bool,
        rebootstrap_required: bool,
    },
    Snapshot {
        board_id: Uuid,
        seq: i64,
        state: Value,
    },
    SessionsUpdate {
        board_id: Uuid,
        users: Vec<Value>,
    },
    /// Canonical incremental Yrs update for one committed board change.
    /// `yupdate` is
    /// base64(std) of `encode_state_as_update_v1` for text WebSocket frames;
    /// negotiated binary connections receive the raw update payload instead.
    YrsUpdate {
        board_id: Uuid,
        seq: i64,
        client_event_id: Option<String>,
        schema_version: i32,
        yupdate: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientWsMessage {
    Preview {
        payload: Value,
    },
    Commit {
        event_type: String,
        payload: Value,
        client_event_id: Option<String>,
        session_id: Option<String>,
        #[serde(default)]
        yrs: Option<ClientYrsUpdate>,
    },
}

/// Binary client commit: `u32 header_json_len`, JSON `ClientWsMessage::Commit`
/// with an empty `yrs.update_b64`, then the raw Yrs V1 update bytes.
pub fn decode_binary_client_commit(bytes: &[u8]) -> Result<ClientWsMessage, &'static str> {
    const MAX_HEADER: usize = 4 * 1024 * 1024;
    const MAX_UPDATE: usize = 8 * 1024 * 1024;
    if bytes.len() < 4 {
        return Err("binary commit too short");
    }
    let header_len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    if header_len == 0 || header_len > MAX_HEADER || bytes.len() < 4 + header_len {
        return Err("invalid binary commit header length");
    }
    let update = &bytes[4 + header_len..];
    if update.is_empty() || update.len() > MAX_UPDATE {
        return Err("invalid binary commit update length");
    }
    let mut message: ClientWsMessage = serde_json::from_slice(&bytes[4..4 + header_len])
        .map_err(|_| "invalid binary commit header")?;
    match &mut message {
        ClientWsMessage::Commit { yrs: Some(yrs), .. } if yrs.update_b64.is_empty() => {
            yrs.update_bytes = Some(update.to_vec());
            Ok(message)
        }
        _ => Err("binary frame must contain a client yrs commit"),
    }
}

/// The two per-board fan-out streams. Durable state (commits) and transient
/// previews (live drag/cursor frames, presence/session updates) are kept on
/// SEPARATE broadcast channels so a flood of previews can't evict committed
/// events from the buffer window. A lagged receiver on `commits` genuinely
/// missed state and is asked to resync; a lagged receiver on `previews` only
/// skipped ephemeral frames and silently resumes (no resync).
///
/// Both channels carry messages **already serialized once** into immutable,
/// Arc-backed [`Utf8Bytes`] (see [`encode_ws`]). At broadcast time we pay one
/// serialization; each of the N subscribers then only clones the shared buffer
/// (an Arc bump) instead of deep-cloning the `serde_json::Value` payload and
/// re-serializing it per connection.
/// One remote user's latest cursor, held server-side between aggregator ticks.
#[derive(Clone, Debug)]
pub struct CursorEntry {
    pub user_id: Uuid,
    /// The original preview `payload` (cursor:{x,y}, display_name, client_id,
    /// color…) so the aggregated frame carries everything the client renders.
    pub payload: Value,
    pub updated_at: Instant,
}

/// Per-board latest-cursor-per-user map. Cursor previews UPDATE this instead of
/// each fanning out individually; a single aggregator broadcasts all of them at
/// a fixed rate. This turns cursor fan-out from O(moving_users × rate × N) into
/// O(N × agg_rate) — the key change that lets one board hold 500+ users.
pub type CursorMap = Arc<Mutex<HashMap<Uuid, CursorEntry>>>;

/// Aborts the per-board cursor aggregator task when the last [`BoardChannels`]
/// handle (held only by the hub map + transient callers) is dropped — i.e. when
/// the board is evicted. The aggregator does no DB work, so aborting is safe.
#[derive(Debug)]
struct AggregatorGuard(tokio::task::AbortHandle);
impl Drop for AggregatorGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Debug)]
pub struct BoardChannels {
    pub commits: broadcast::Sender<Utf8Bytes>,
    pub previews: broadcast::Sender<Utf8Bytes>,
    /// Latest cursor per user; drained by the aggregator, not fanned out per move.
    pub cursors: CursorMap,
    /// Keeps the aggregator alive exactly as long as this board's channels exist.
    _aggregator: Arc<AggregatorGuard>,
}

/// How often the per-board aggregator broadcasts the merged cursor frame. 15 Hz
/// matches the client's remote-cursor render cadence; the frame cost per user is
/// then independent of how many other users are moving.
const CURSOR_AGG_INTERVAL: Duration = Duration::from_millis(66);
/// Drop a user's cursor from the aggregate if it hasn't moved in this long (they
/// paused or disconnected without a clean cursor-remove).
const CURSOR_TTL: Duration = Duration::from_secs(15);
/// Max cursors carried in one aggregate frame. Without viewport info we can't
/// cull to what each user actually sees, so the frame is shared (serialized ONCE)
/// and bounded to the most-recently-moved cursors — otherwise the frame is O(N)
/// bytes × O(N) recipients = O(N²) bandwidth (≈2 Gbps at 500 all-moving users).
/// Capped, egress is O(N × cap); a board can't usefully show 500 live cursors
/// anyway. Raise/lower via env; viewport-culling is the later refinement.
const DEFAULT_CURSOR_MAX_PER_FRAME: u64 = 60;

fn cursor_max_per_frame() -> usize {
    crate::core::config::get_env_u64(
        "REALTIME_CURSOR_MAX_PER_FRAME",
        DEFAULT_CURSOR_MAX_PER_FRAME,
    )
    .max(1) as usize
}

/// Whether cursor aggregation is enabled (env `REALTIME_CURSOR_AGGREGATION`,
/// default ON). When OFF, cursor previews fan out individually (legacy path) —
/// a kill switch to roll back without a redeploy if a client can't yet render
/// the aggregated `{"type":"cursors"}` frame.
pub fn cursor_aggregation_enabled() -> bool {
    crate::core::config::get_env_bool("REALTIME_CURSOR_AGGREGATION", true)
}

/// Spawn the per-board cursor aggregator. Ticks at [`CURSOR_AGG_INTERVAL`]; when
/// there are live subscribers it broadcasts one `{"type":"cursors","users":[…]}`
/// frame on the preview channel carrying every non-stale cursor. Idle boards tick
/// cheaply (no subscribers → skip) until the hub evicts them and the guard aborts.
fn spawn_cursor_aggregator(
    board_id: Uuid,
    previews: broadcast::Sender<Utf8Bytes>,
    cursors: CursorMap,
) -> tokio::task::AbortHandle {
    let cap = cursor_max_per_frame();
    let join = tokio::spawn(async move {
        let mut tick = tokio::time::interval(CURSOR_AGG_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if previews.receiver_count() == 0 {
                continue;
            }
            let now = Instant::now();
            let users: Vec<Value> = {
                let mut map = cursors.lock().unwrap_or_else(|p| p.into_inner());
                map.retain(|_, e| now.duration_since(e.updated_at) < CURSOR_TTL);
                let mut entries: Vec<&CursorEntry> = map.values().collect();
                // Bound the shared frame: keep the most-recently-moved `cap`.
                if entries.len() > cap {
                    entries.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
                    entries.truncate(cap);
                }
                entries
                    .iter()
                    .map(|e| serde_json::json!({ "user_id": e.user_id, "payload": e.payload }))
                    .collect()
            };
            if users.is_empty() {
                continue;
            }
            let frame = serde_json::json!({
                "type": "cursors",
                "board_id": board_id,
                "users": users,
            });
            if let Ok(s) = serde_json::to_string(&frame) {
                let _ = previews.send(Utf8Bytes::from(s));
            }
        }
    });
    join.abort_handle()
}

/// Serialize a [`WsMessage`] exactly once into Arc-backed UTF-8 bytes ready to
/// hand straight to `Message::Text`. Returns `None` (and logs) on the very rare
/// serialization failure.
pub fn encode_ws(msg: &WsMessage) -> Option<Utf8Bytes> {
    match serde_json::to_string(msg) {
        Ok(s) => Some(Utf8Bytes::from(s)),
        Err(e) => {
            tracing::warn!("failed to serialize WsMessage for broadcast: {e}");
            None
        }
    }
}

/// Encode `msg` once and publish it to `sender`. No-op on serialization error or
/// when the channel currently has no subscribers.
pub fn broadcast_ws(sender: &broadcast::Sender<Utf8Bytes>, msg: WsMessage) {
    if let Some(bytes) = encode_ws(&msg) {
        let _ = sender.send(bytes);
    }
}

// Per-board ring buffers hold the last N serialized messages until every
// receiver consumes them. tokio preallocates the whole ring up front, and the
// hub keeps a board's channels alive for the process lifetime, so capacity is a
// direct, persistent memory cost PER BOARD (× the buffered payload bytes under a
// lagging-client flood). 8192×2 proved too memory-hungry on a busy board and
// contributed to an OOM, so default conservatively — a smaller buffer just means
// a lagged client resyncs sooner (commits) or skips a few frames (previews),
// both already handled gracefully (resync is debounced). Raise via env per board
// load if the host has headroom.
/// Default ring-buffer capacity for the durable commit/ack/session stream.
/// Raised 2048→4096 now that the host has 8 GB: the commit ring is the single
/// knob that buys resync headroom (a slow consumer only resyncs once it lags
/// past this many un-drained commits). A slot is a small `Arc`+generation
/// (~tens of bytes preallocated); the PEAK cost is `capacity × avg-commit-size`
/// and only materializes under a resync storm. The old OOM was at 8192×8192 on
/// far less RAM — 4096 commit / 2048 preview is well inside 8 GB. Push higher
/// per board load via `REALTIME_BROADCAST_CAPACITY`.
const DEFAULT_COMMIT_CAPACITY: u64 = 4096;
/// Default ring-buffer capacity for the transient preview stream. Previews are
/// droppable (a lagged receiver silently skips frames, never resyncs) AND
/// coalesce-to-newest on backpressure, so this can stay well below the commit
/// ring. Raised 1024→2048 for the same 8 GB headroom.
const DEFAULT_PREVIEW_CAPACITY: u64 = 2048;

fn commit_capacity() -> usize {
    crate::core::config::get_env_u64("REALTIME_BROADCAST_CAPACITY", DEFAULT_COMMIT_CAPACITY).max(1)
        as usize
}

fn preview_capacity() -> usize {
    crate::core::config::get_env_u64("REALTIME_PREVIEW_CAPACITY", DEFAULT_PREVIEW_CAPACITY).max(1)
        as usize
}

#[derive(Clone, Debug)]
pub struct RealtimeHub {
    inner: Arc<RwLock<HashMap<Uuid, BoardChannels>>>,
}

impl RealtimeHub {
    /// Creates an empty in-process websocket connection registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get (or lazily create) both broadcast channels for a board.
    pub async fn channels_for(&self, board_id: Uuid) -> BoardChannels {
        {
            let map = self.inner.read().await;
            if let Some(channels) = map.get(&board_id) {
                return channels.clone();
            }
        }

        let mut map = self.inner.write().await;
        if let Some(channels) = map.get(&board_id) {
            return channels.clone();
        }

        // Opportunistic eviction: a board with no live receivers (all clients
        // disconnected) has no consumers, yet its preallocated ring buffers
        // linger forever otherwise — an unbounded leak across every board ever
        // opened. We're already holding the write lock to add a NEW board, so
        // sweep dead ones now (cheap: O(#boards), and only on new-board creation).
        // Reconnecting clients resync anyway, so dropping a dormant board's buffer
        // is harmless.
        map.retain(|_, ch| ch.commits.receiver_count() > 0 || ch.previews.receiver_count() > 0);

        let (commits, _c_rx) = broadcast::channel(commit_capacity());
        let (previews, _p_rx) = broadcast::channel(preview_capacity());
        let cursors: CursorMap = Arc::new(Mutex::new(HashMap::new()));
        // One aggregator per board; its guard lives inside BoardChannels, so it
        // is aborted precisely when the board is evicted (last handle dropped).
        let aggregator = spawn_cursor_aggregator(board_id, previews.clone(), cursors.clone());
        let channels = BoardChannels {
            commits,
            previews,
            cursors,
            _aggregator: Arc::new(AggregatorGuard(aggregator)),
        };
        map.insert(board_id, channels.clone());
        channels
    }

    /// Convenience accessor for callers that publish droppable presence/preview
    /// state. Missing one of these messages must not force a board-state resync.
    pub async fn preview_sender_for(&self, board_id: Uuid) -> broadcast::Sender<Utf8Bytes> {
        self.channels_for(board_id).await.previews
    }

    /// Boards that currently have at least one local realtime receiver. Used by
    /// the durable catch-up poller; dormant boards need no fan-out.
    pub async fn active_board_ids(&self) -> Vec<Uuid> {
        self.inner
            .read()
            .await
            .iter()
            .filter_map(|(board_id, channels)| {
                (channels.commits.receiver_count() > 0).then_some(*board_id)
            })
            .collect()
    }

    /// Drop a board's channels if it has no live receivers left — call this when a
    /// client disconnects. Frees the preallocated ring buffers + every retained
    /// message immediately, instead of leaving a dormant board's buffers pinned in
    /// RAM until some *other* new board happens to trigger the `channels_for`
    /// sweep (the prior behaviour, where idle boards accumulated toward OOM). A
    /// reconnecting client just re-creates the channels and resyncs, so evicting an
    /// idle board's buffer is harmless.
    pub async fn evict_if_idle(&self, board_id: Uuid) {
        // Fast path: check under the read lock; only take the write lock to remove.
        {
            let map = self.inner.read().await;
            match map.get(&board_id) {
                Some(ch) if ch.commits.receiver_count() > 0 || ch.previews.receiver_count() > 0 => {
                    return
                }
                None => return,
                _ => {}
            }
        }
        let mut map = self.inner.write().await;
        if let Some(ch) = map.get(&board_id) {
            // Re-check under the write lock — a client may have (re)subscribed in
            // the gap between the read and write locks.
            if ch.commits.receiver_count() == 0 && ch.previews.receiver_count() == 0 {
                map.remove(&board_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_binary_client_commit, encode_ws, ClientWsMessage, WsMessage};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn commit_echo_carries_client_event_id() {
        let board_id = Uuid::new_v4();
        let wire = encode_ws(&WsMessage::Commit {
            board_id,
            seq: 9,
            server_event_id: Uuid::new_v4(),
            client_event_id: Some("optimistic-9".into()),
            user_id: Uuid::new_v4(),
            event_type: "world_commit".into(),
            payload: json!({"actions": []}),
            session_id: Some("session".into()),
        })
        .expect("serialize");
        let value: serde_json::Value = serde_json::from_str(wire.as_str()).unwrap();
        assert_eq!(value["client_event_id"], "optimistic-9");
    }

    #[test]
    fn binary_client_commit_keeps_update_out_of_json_header() {
        let header = json!({
            "type": "commit",
            "event_type": "world_commit",
            "payload": {"actions": [], "ops": []},
            "client_event_id": "binary-1",
            "session_id": "session",
            "yrs": {
                "encoding": "yrs-v1",
                "protocol_version": 1,
                "schema_version": 1,
                "base_generation": 3,
                "observed_seq": 9,
                "writer_client_id": 42,
                "update_b64": ""
            }
        });
        let header = serde_json::to_vec(&header).unwrap();
        let update = [0_u8, 1, 255, 7];
        let mut frame = (header.len() as u32).to_be_bytes().to_vec();
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&update);

        let ClientWsMessage::Commit { yrs: Some(yrs), .. } =
            decode_binary_client_commit(&frame).unwrap()
        else {
            panic!("expected yrs commit");
        };
        assert_eq!(yrs.update_bytes.as_deref(), Some(update.as_slice()));
        assert!(yrs.update_b64.is_empty());
        assert!(decode_binary_client_commit(&frame[..frame.len() - update.len()]).is_err());
    }
}
