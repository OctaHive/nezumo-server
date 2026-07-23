//! Resident per-board coordinator registry and activation lifecycle.
//!
//! One owner per board (an `Arc<tokio::Mutex<ResidentBoard>>`), looked up under a
//! short std-mutex over the map so the per-board lock is never a global
//! bottleneck. Activation:
//!   1. TX under the per-board advisory lock: `begin_activation` (fence via
//!      `writer_epoch`) + read `activation_head = MAX(committed seq)`.
//!   2. Catch-up fold `(base_seq, activation_head]` with idempotent backfill of
//!      any missing `board_yrs_updates` row while live commits queue.
//!   3. Validate the reconstructed document, then publish the ready state.
//!
//! The catch-up decision core lives on `BoardCoordinator` (unit-tested); this
//! module provides the database and validator integration.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use sqlx::PgPool;
use tokio::sync::Mutex;
use uuid::Uuid;

use base64::Engine;

use crate::database::events::insert_event;
use crate::database::yrs_heads::CanonicalState;
use crate::database::yrs_updates::YrsUpdateRow;
use crate::database::{boards, events, yrs_assets, yrs_canonical_bases, yrs_heads, yrs_updates};
use crate::realtime::ClientYrsUpdate;
use crate::state::canonical_base::CanonicalBase;
use crate::state::yrs_coordinator::BoardCoordinator;
use crate::state::yrs_validator::{ValidateRequest, ValidatorPool};

/// Catch-up page size when scanning the ordered event tail.
const CATCH_UP_BATCH: i64 = 512;

/// Resident canonical state for one active board.
pub struct ResidentBoard {
    pub coord: BoardCoordinator,
    /// The owner epoch this instance holds; every mutating DB write is CAS'd on it.
    pub writer_epoch: i64,
    pub state: CanonicalState,
}

/// One lock-consistent export barrier. The flat projection and Yrs bytes are
/// captured from the same resident revision so a portable backup can verify
/// that its human-readable compatibility state matches the binary authority.
pub struct CanonicalExportState {
    pub state: Value,
    pub last_event_seq: i64,
    pub state_update: Vec<u8>,
    pub state_vector: Vec<u8>,
    pub base_generation: i64,
    pub server_client_id: u64,
    pub schema_version: u64,
}

struct Inner {
    boards: std::sync::Mutex<HashMap<Uuid, Arc<Mutex<ResidentBoard>>>>,
    /// Per-board async activation gate: concurrent first-commits for one board
    /// serialize here so activation runs exactly once (double-checked against
    /// `boards`), without a global lock across the (slow) activation.
    activation_gates: std::sync::Mutex<HashMap<Uuid, Arc<Mutex<()>>>>,
    validator: ValidatorPool,
}

/// Cloneable handle to the resident coordinator registry (lives in `AppState`).
#[derive(Clone)]
pub struct CoordinatorRegistry {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for CoordinatorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorRegistry")
            .finish_non_exhaustive()
    }
}

impl CoordinatorRegistry {
    /// Creates an empty coordinator registry backed by the validator pool.
    pub fn new(validator: ValidatorPool) -> Self {
        Self {
            inner: Arc::new(Inner {
                boards: std::sync::Mutex::new(HashMap::new()),
                activation_gates: std::sync::Mutex::new(HashMap::new()),
                validator,
            }),
        }
    }

    /// Builds the registry with its validator pool.
    pub fn from_env() -> Self {
        Self::new(ValidatorPool::from_env())
    }

    fn get_resident(&self, board_id: Uuid) -> Option<Arc<Mutex<ResidentBoard>>> {
        self.inner
            .boards
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&board_id)
            .cloned()
    }

    fn evict_if_same(&self, board_id: Uuid, resident: &Arc<Mutex<ResidentBoard>>) {
        let mut boards = self.inner.boards.lock().unwrap_or_else(|p| p.into_inner());
        if boards
            .get(&board_id)
            .is_some_and(|current| Arc::ptr_eq(current, resident))
        {
            boards.remove(&board_id);
        }
    }

    /// Returns the resident coordinator, activating it on first use.
    pub async fn ensure_active(
        &self,
        pool: &PgPool,
        board_id: Uuid,
    ) -> Result<Arc<Mutex<ResidentBoard>>, String> {
        if let Some(resident) = self.get_resident(board_id) {
            return Ok(resident);
        }
        // Serialize activation per board (double-checked locking): the first
        // caller activates; the rest await the gate then find the resident.
        let gate = {
            let mut gates = self
                .inner
                .activation_gates
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            gates
                .entry(board_id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _held = gate.lock().await;
        if let Some(resident) = self.get_resident(board_id) {
            return Ok(resident);
        }
        let resident = self.activate(pool, board_id).await?;
        Ok(resident)
    }

    /// Materializes the authoritative current board state for load, preview,
    /// rendering, and export through the same resident Doc that owns commits.
    pub async fn current_canonical_state(
        &self,
        pool: &PgPool,
        board_id: Uuid,
    ) -> Result<(Value, i64), String> {
        let resident = self.ensure_active(pool, board_id).await?;
        let resident = resident.lock().await;
        let state = resident
            .coord
            .resident_snapshot()
            .map_err(|error| format!("canonical projection: {error}"))?;
        Ok((state, resident.coord.processed_seq()))
    }

    /// Captures a portable backup payload under the per-board coordinator lock.
    /// Keeping this separate from `current_canonical_state`
    /// prevents a commit between projecting JSON and encoding the Yrs base.
    pub async fn current_canonical_export(
        &self,
        pool: &PgPool,
        board_id: Uuid,
    ) -> Result<CanonicalExportState, String> {
        let resident = self.ensure_active(pool, board_id).await?;
        let resident = resident.lock().await;
        let state = resident
            .coord
            .resident_snapshot()
            .map_err(|error| format!("canonical export projection: {error}"))?;
        let schema_version = resident
            .coord
            .schema_version()
            .map_err(|error| format!("canonical export schema: {error}"))?;
        let (state_update, state_vector) = resident.coord.canonical_base();
        Ok(CanonicalExportState {
            state,
            last_event_seq: resident.coord.processed_seq(),
            state_update,
            state_vector,
            base_generation: resident.coord.base_generation(),
            server_client_id: resident.coord.server_client_id(),
            schema_version,
        })
    }

    /// Loads the canonical checkpoint and installs the resident board owner.
    async fn activate(
        &self,
        pool: &PgPool,
        board_id: Uuid,
    ) -> Result<Arc<Mutex<ResidentBoard>>, String> {
        // Activate from the persisted canonical base checkpoint.
        let base_row = yrs_canonical_bases::read_base(pool, board_id)
            .await
            .map_err(|e| format!("read base: {e}"))?;
        let base_row = base_row.ok_or("canonical base is missing")?;
        if base_row.abandoned_at.is_some() {
            return Err("yrs base is abandoned; cannot activate".to_string());
        }
        let canonical_base = base_row.to_domain();

        // Under the per-board advisory lock, reuse an equivalent durable
        // revision or resume activation and fence an abandoned owner.
        let mut tx = pool.begin().await.map_err(|e| format!("begin tx1: {e}"))?;
        yrs_heads::lock_board_xact(&mut tx, board_id)
            .await
            .map_err(|e| format!("lock: {e}"))?;
        let existing = yrs_heads::read_head(&mut *tx, board_id)
            .await
            .map_err(|e| format!("read head: {e}"))?;
        if let Some(head) = existing.as_ref() {
            if head.state == CanonicalState::Quarantined {
                let _ = tx.rollback().await;
                return Err("canonical board is quarantined".to_string());
            }
        }
        let ready = existing.as_ref().is_some_and(|head| {
            head.state == CanonicalState::Ready
                && head.base_generation == canonical_base.base_generation
                && head.processed_seq >= canonical_base.base_seq
        });
        let (writer_epoch, activation_head) = if ready {
            let head = existing.expect("ready head checked above");
            (head.writer_epoch, head.processed_seq)
        } else {
            let head =
                yrs_heads::begin_activation(&mut tx, board_id, canonical_base.base_generation)
                    .await
                    .map_err(|e| format!("begin_activation: {e}"))?;
            let activation_head: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(seq), $2) FROM board_events WHERE board_id = $1",
            )
            .bind(board_id)
            .bind(canonical_base.base_seq)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| format!("read activation_head: {e}"))?;
            (head.writer_epoch, activation_head)
        };
        tx.commit().await.map_err(|e| format!("commit tx1: {e}"))?;

        let base = self
            .preferred_activation_base(pool, board_id, canonical_base, activation_head)
            .await?;

        // Replay the durable event tail not covered by the selected checkpoint.
        let mut coord = BoardCoordinator::from_base(&base, base.base_seq)
            .map_err(|e| format!("from_base: {e:?}"))?;
        let mut cursor = base.base_seq;
        'outer: while cursor < activation_head {
            let batch = events::list_events_since(pool, board_id, cursor, CATCH_UP_BATCH)
                .await
                .map_err(|e| format!("list events: {e}"))?;
            if batch.is_empty() {
                break;
            }
            for ev in batch {
                if ev.seq > activation_head {
                    break 'outer;
                }
                self.catch_up_one(pool, &mut coord, base.base_generation, &ev)
                    .await?;
                cursor = ev.seq;
            }
        }

        // Validate the reconstructed state before publishing a new resident.
        // Reconstructing an existing resident must not steal its writer epoch.
        coord
            .assert_valid_projection()
            .map_err(|e| format!("activation assert: {e:?}"))?;
        if !ready {
            let mut tx = pool.begin().await.map_err(|e| format!("begin tx2: {e}"))?;
            yrs_heads::lock_board_xact(&mut tx, board_id)
                .await
                .map_err(|e| format!("lock2: {e}"))?;
            let ok = yrs_heads::mark_ready(&mut tx, board_id, writer_epoch, activation_head)
                .await
                .map_err(|e| format!("mark_ready: {e}"))?;
            if !ok {
                let _ = tx.rollback().await;
                return Err("mark_ready CAS failed — superseded by another owner".to_string());
            }
            tx.commit().await.map_err(|e| format!("commit tx2: {e}"))?;
        }

        let resident = Arc::new(Mutex::new(ResidentBoard {
            coord,
            writer_epoch,
            state: CanonicalState::Ready,
        }));
        self.inner
            .boards
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(board_id, resident.clone());
        tracing::info!(
            "yrs coordinator: board {board_id} resident ready (head={activation_head}, epoch={writer_epoch}, reconstructed={ready})"
        );
        Ok(resident)
    }

    async fn preferred_activation_base(
        &self,
        pool: &PgPool,
        board_id: Uuid,
        canonical_base: CanonicalBase,
        activation_head: i64,
    ) -> Result<CanonicalBase, String> {
        let Some(snapshot) = crate::database::yrs_snapshots::read_latest_at_or_before(
            pool,
            board_id,
            canonical_base.base_generation,
            canonical_base.server_client_id as i64,
            activation_head,
        )
        .await
        .map_err(|error| format!("read activation binary snapshot: {error}"))?
        else {
            return Ok(canonical_base);
        };
        if !snapshot.content_hash_matches()
            || snapshot.output_bytes != snapshot.state_update.len() as i64
            || snapshot.protocol_version != crate::state::yrs_model::PROTOCOL_VERSION as i16
        {
            return Err("binary activation snapshot metadata/hash mismatch".to_string());
        }
        let doc = crate::state::yrs_model::doc_from_base(
            &snapshot.state_update,
            &snapshot.state_vector,
            snapshot.server_client_id as u64,
        )
        .map_err(|error| format!("reload activation binary snapshot: {error}"))?;
        let schema = crate::state::yrs_model::doc_schema_version(&doc)
            .map_err(|error| format!("read activation binary schema: {error}"))?;
        if schema as i32 != snapshot.schema_version {
            return Err("binary activation snapshot schema mismatch".to_string());
        }
        crate::state::yrs_model::doc_to_snapshot(&doc)
            .map_err(|error| format!("project activation binary snapshot: {error}"))?;
        tracing::info!(
            "yrs coordinator: board {board_id} using binary barrier seq={} generation={}",
            snapshot.last_event_seq,
            snapshot.base_generation
        );
        Ok(CanonicalBase {
            state_update: snapshot.state_update,
            state_vector: snapshot.state_vector,
            base_seq: snapshot.last_event_seq,
            server_client_id: snapshot.server_client_id as u64,
            base_generation: snapshot.base_generation,
        })
    }

    /// Commits through the canonical coordinator. Sequence reservation, event
    /// insertion, and the Yrs journal write share one atomic transaction; the
    /// candidate is validated before that transaction. The per-board resident
    /// mutex serializes concurrent commits.
    ///
    /// `client_event_id` is the client's idempotency key. Returns the durable result
    /// for Ack + broadcast, or a [`CommitError`] the caller maps to a `nack`.
    pub async fn commit(
        &self,
        pool: &PgPool,
        resident: &Arc<Mutex<ResidentBoard>>,
        board_id: Uuid,
        actor_user_id: Uuid,
        event_type: &str,
        payload: &Value,
        client_event_id: Option<&str>,
        session_id: Option<&str>,
        client_yrs: Option<&ClientYrsUpdate>,
    ) -> Result<CommitResult, CommitError> {
        let mut current = resident.clone();
        for attempt in 0..2 {
            match self
                .commit_once(
                    pool,
                    &current,
                    board_id,
                    actor_user_id,
                    event_type,
                    payload,
                    client_event_id,
                    session_id,
                    client_yrs,
                )
                .await
            {
                Err(CommitError::StaleRevision) if attempt == 0 => {
                    current = self
                        .ensure_active(pool, board_id)
                        .await
                        .map_err(|error| CommitError::Db(format!("rebuild resident: {error}")))?;
                }
                result => return result,
            }
        }
        Err(CommitError::StaleRevision)
    }

    async fn commit_once(
        &self,
        pool: &PgPool,
        resident: &Arc<Mutex<ResidentBoard>>,
        board_id: Uuid,
        actor_user_id: Uuid,
        event_type: &str,
        payload: &Value,
        client_event_id: Option<&str>,
        session_id: Option<&str>,
        client_yrs: Option<&ClientYrsUpdate>,
    ) -> Result<CommitResult, CommitError> {
        let mut rb = resident.lock().await;
        if rb.state != CanonicalState::Ready {
            return Err(CommitError::ClientUpdateRejected(format!(
                "board writes fenced: {}",
                rb.state.as_str()
            )));
        }
        let base_generation = rb.coord.base_generation();
        let writer_epoch = rb.writer_epoch;
        let writer_client_id = rb.coord.server_client_id();
        let processed_before = rb.coord.processed_seq();
        let this_hash = payload_hash(payload);

        let client_candidate = match client_yrs {
            Some(envelope) => Some(decode_client_candidate(envelope, event_type)?),
            None => None,
        };

        // A matching client_event_id plus identical payload is an
        // idempotent retry → re-broadcast the persisted update; a matching id with
        // a different payload is a protocol conflict.
        if let Some(cid) = client_event_id {
            if let Some(existing) = yrs_updates::read_by_client_event_id(pool, board_id, cid)
                .await
                .map_err(|e| CommitError::Db(format!("dedup read: {e}")))?
            {
                let update_matches =
                    client_candidate
                        .as_ref()
                        .map_or(existing.source == "server", |candidate| {
                            existing.source == "client"
                                && existing.update_hash == candidate.update_hash
                        });
                if existing.payload_hash == this_hash && update_matches {
                    let event_payload = if let Some(event_id) = existing.event_id {
                        events::read_event_by_id(pool, event_id)
                            .await
                            .map_err(|e| CommitError::Db(format!("dedup event read: {e}")))?
                            .map(|event| event.payload)
                            .unwrap_or_else(|| payload.clone())
                    } else {
                        payload.clone()
                    };
                    return Ok(CommitResult::from_persisted(&existing, event_payload));
                }
                return Err(CommitError::ProtocolConflict);
            }
        }

        // Once structural text v2 is authoritative, a world edit without a
        // same-schema client-authored Yrs update is an old writer. Keep this
        // after durable dedup so an exact retry committed before the fence can
        // still receive its original Ack.
        let current_schema = rb
            .coord
            .schema_version()
            .map_err(|error| CommitError::Model(format!("read writer fence: {error}")))?;
        enforce_writer_fence(
            event_type,
            current_schema,
            client_candidate
                .as_ref()
                .map(|candidate| candidate.schema_version as u64),
        )?;

        // Canonical Yrs bytes are the sole mutation format. Exact retries were
        // handled above, so field-op payloads cannot enter the event log.
        enforce_canonical_payload(event_type, payload)?;

        // Revision checks deliberately happen after durable dedup. An exact
        // retry must still Ack after compaction moved the board to a newer
        // generation; otherwise a persisted edit could be reported as failed.
        if let Some(envelope) = client_yrs {
            if envelope.base_generation != base_generation {
                return Err(CommitError::ClientUpdateRejected(
                    "CANONICAL_REBASE_REQUIRED: base generation changed".to_string(),
                ));
            }
            if envelope.observed_seq < 0 || envelope.observed_seq > processed_before {
                return Err(CommitError::ClientUpdateRejected(
                    "invalid observed_seq".to_string(),
                ));
            }
        }

        // Build the sequence-independent candidate, then validate it before the
        // database transaction so the transaction never spans a subprocess call.
        let (mut prepared, persisted_payload) = if let Some(candidate) = client_candidate.as_ref() {
            rb.coord
                .validate_candidate_schema(&candidate.bytes, candidate.schema_version as u64)
                .map_err(|error| {
                    CommitError::ClientUpdateRejected(format!("schema fence: {error}"))
                })?;
            rb.coord
                .prepare_external(0, payload, &candidate.bytes)
                .map_err(|e| CommitError::Model(format!("prepare external: {e:?}")))?
        } else {
            rb.coord
                .prepare_noop(0, payload)
                .map_err(|e| CommitError::Model(format!("prepare metadata event: {e:?}")))?
        };
        let expected_projection_value = prepared
            .expected_projection()
            .map_err(|e| CommitError::Model(format!("expected projection: {e:?}")))?;
        let expected_projection = serde_json::to_vec(&expected_projection_value)
            .map_err(|e| CommitError::Model(format!("encode expected projection: {e}")))?;
        let asset_refs =
            crate::state::yrs_assets::project_stable_refs(&expected_projection_value, board_id);
        let (source, protocol_version, persisted_writer_id, validator_writer_id, update_hash) =
            if let Some(candidate) = client_candidate {
                (
                    "client".to_string(),
                    candidate.protocol_version,
                    Some(candidate.writer_client_id as i64),
                    candidate.writer_client_id,
                    candidate.update_hash,
                )
            } else {
                (
                    "server".to_string(),
                    crate::state::yrs_model::PROTOCOL_VERSION as i16,
                    Some(writer_client_id as i64),
                    writer_client_id,
                    bytes_hash(&prepared.yupdate),
                )
            };

        self.inner
            .validator
            .validate(ValidateRequest {
                seq: 0,
                base_seq: processed_before as u64,
                base_generation: base_generation as u64,
                writer_client_id: validator_writer_id,
                base_update: prepared.base_update.clone(),
                base_state_vector: prepared.base_state_vector.clone(),
                candidate: prepared.yupdate.clone(),
                expected_projection,
            })
            .await
            .map_err(CommitError::ValidationFailed)?;

        // Atomic tx: advisory barrier → reserve seq → insert event (+config)
        // → insert canonical update → advance watermark.
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| CommitError::Db(format!("begin: {e}")))?;
        yrs_heads::lock_board_xact(&mut tx, board_id)
            .await
            .map_err(|e| CommitError::Db(format!("lock: {e}")))?;

        let seq = boards::reserve_next_event_seq_tx(&mut tx, board_id)
            .await
            .map_err(|e| CommitError::Db(format!("reserve seq: {e}")))?;
        let event = insert_event(
            &mut tx,
            board_id,
            seq,
            actor_user_id,
            event_type,
            persisted_payload.clone(),
            session_id,
        )
        .await
        .map_err(|e| CommitError::Db(format!("insert event: {e}")))?;

        if event_type == "board_commit" {
            apply_board_config(&mut tx, board_id, &persisted_payload)
                .await
                .map_err(|e| CommitError::Db(format!("board config: {e}")))?;
        }

        // A live commit's idempotency id: the client's, else a deterministic
        // per-event one (not the reserved historical-event namespace).
        let cid = client_event_id
            .map(String::from)
            .unwrap_or_else(|| format!("live:{}", event.id));
        let row = YrsUpdateRow {
            board_id,
            seq,
            event_id: Some(event.id),
            client_event_id: cid.clone(),
            payload_hash: this_hash,
            base_generation,
            schema_version: prepared.schema_version,
            protocol_version,
            source,
            writer_client_id: persisted_writer_id,
            update_hash,
            yupdate: prepared.yupdate.clone(),
        };
        yrs_updates::insert_update(&mut *tx, &row)
            .await
            .map_err(|e| CommitError::Db(format!("insert update: {e}")))?;

        if !yrs_heads::advance_processed_seq(&mut tx, board_id, writer_epoch, processed_before, seq)
            .await
            .map_err(|e| CommitError::Db(format!("advance watermark: {e}")))?
        {
            let _ = tx.rollback().await;
            drop(rb);
            self.evict_if_same(board_id, resident);
            return Err(CommitError::StaleRevision);
        }
        yrs_assets::replace_projection_tx(
            &mut tx,
            yrs_assets::AssetProjection {
                board_id,
                last_event_seq: seq,
                base_generation,
                object_keys: &asset_refs.object_keys,
                pdf_doc_ids: &asset_refs.pdf_doc_ids,
                blocker_code: asset_refs
                    .has_unstable_internal_url
                    .then_some("unrecognized_internal_url"),
            },
        )
        .await
        .map_err(|e| CommitError::Db(format!("asset projection: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| CommitError::Db(format!("commit: {e}")))?;

        // Apply to the resident document only after the durable commit succeeds.
        // Keep the update inside `prepared`: resident apply must consume the
        // exact bytes that were validated and persisted. The broadcast owns a
        // separate copy after the durable commit.
        let yupdate = prepared.yupdate_for_broadcast();
        prepared.seq = seq;
        let committed_schema_version = prepared.schema_version;
        if let Err(e) = rb.coord.apply_committed(prepared) {
            // The DB transaction is already committed: returning Nack here would
            // lie to the client and could cause a retry of a durable event. Evict
            // the suspect in-memory owner; the next ensure_active reconstructs it
            // from the base + persisted journal. Ack/broadcast the durable result.
            tracing::error!(
                "canonical post-commit resident apply failed for board {board_id} at seq {seq}: {e:?}; evicting resident"
            );
            drop(rb);
            self.inner
                .boards
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&board_id);
        }

        Ok(CommitResult {
            seq,
            server_event_id: event.id,
            client_event_id: Some(cid),
            schema_version: committed_schema_version,
            yupdate_b64: base64::engine::general_purpose::STANDARD.encode(&yupdate),
            event_payload: persisted_payload,
        })
    }

    /// Applies the persisted canonical update paired with one ordered event.
    async fn catch_up_one(
        &self,
        pool: &PgPool,
        coord: &mut BoardCoordinator,
        _base_generation: i64,
        ev: &crate::models::events::EventRecord,
    ) -> Result<(), String> {
        // A missing row is a durable journal gap; canonical code never
        // reconstructs canonical updates from JSON event payloads.
        if let Some(row) = yrs_updates::read_by_seq(pool, ev.board_id, ev.seq)
            .await
            .map_err(|e| format!("read_by_seq: {e}"))?
        {
            return coord
                .catch_up_persisted(ev.seq, &row.yupdate)
                .map_err(|e| format!("apply persisted {}: {e:?}", ev.seq));
        }

        Err(format!(
            "canonical journal gap at seq {}: no Yrs update exists",
            ev.seq
        ))
    }
}

/// Durable outcome of a canonical commit, for Ack and broadcast.
pub struct CommitResult {
    pub seq: i64,
    pub server_event_id: Uuid,
    pub client_event_id: Option<String>,
    pub schema_version: i32,
    /// base64(std) of the canonical incremental update (for `WsMessage::YrsUpdate`).
    pub yupdate_b64: String,
    /// Exact durable event payload paired with the update. It may differ from
    /// the projection supplied by an offline client.
    pub event_payload: Value,
}

impl CommitResult {
    fn from_persisted(row: &YrsUpdateRow, event_payload: Value) -> Self {
        CommitResult {
            seq: row.seq,
            // A dedup re-hit has no fresh event insert; the originating event id
            // is not re-derived here (the Ack path only needs seq + the id the
            // client already holds). Use a nil placeholder.
            server_event_id: Uuid::nil(),
            client_event_id: Some(row.client_event_id.clone()),
            schema_version: row.schema_version,
            yupdate_b64: base64::engine::general_purpose::STANDARD.encode(&row.yupdate),
            event_payload,
        }
    }
}

/// Why a canonical commit could not be durably applied. All map to a retryable or
/// terminal `nack`; neither the event nor the Yrs update is committed.
#[derive(Debug)]
pub enum CommitError {
    /// Same `client_event_id`, different payload/actor — a protocol violation.
    ProtocolConflict,
    /// Client-authored ingress is disabled or its envelope metadata is invalid.
    ClientUpdateRejected(String),
    /// The validator rejected the candidate (bridge/schema/mapping bug guard).
    ValidationFailed(String),
    /// Another instance committed after this resident built its candidate, or
    /// activation bumped the writer epoch. Nothing was persisted by this
    /// attempt; rebuild from the durable journal.
    StaleRevision,
    /// A yrs mapping / apply error.
    Model(String),
    /// A database error.
    Db(String),
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitError::ProtocolConflict => write!(f, "protocol conflict (client_event_id reuse)"),
            CommitError::ClientUpdateRejected(e) => write!(f, "client update rejected: {e}"),
            CommitError::ValidationFailed(e) => write!(f, "validation failed: {e}"),
            CommitError::StaleRevision => write!(f, "canonical revision advanced; retry"),
            CommitError::Model(e) => write!(f, "model error: {e}"),
            CommitError::Db(e) => write!(f, "db error: {e}"),
        }
    }
}

impl CommitError {
    /// Returns the stable machine-readable error code sent to clients.
    pub fn code(&self) -> &'static str {
        match self {
            CommitError::ProtocolConflict => "PROTOCOL_CONFLICT",
            CommitError::ClientUpdateRejected(reason)
                if reason.contains("CANONICAL_REBASE_REQUIRED") =>
            {
                "CANONICAL_REBASE_REQUIRED"
            }
            CommitError::ClientUpdateRejected(reason)
                if reason.contains("WRITER_UPGRADE_REQUIRED") =>
            {
                "WRITER_UPGRADE_REQUIRED"
            }
            CommitError::ClientUpdateRejected(_) => "CLIENT_UPDATE_REJECTED",
            CommitError::ValidationFailed(_) => "CLIENT_UPDATE_VALIDATION_FAILED",
            CommitError::StaleRevision => "CANONICAL_REVISION_RETRY",
            CommitError::Model(_) => "CANONICAL_MODEL_ERROR",
            CommitError::Db(_) => "CANONICAL_DB_ERROR",
        }
    }

    /// Transient infrastructure failures retain the client's FIFO pending head;
    /// malformed/semantic/protocol failures require a clean rebootstrap.
    pub fn retryable(&self) -> bool {
        match self {
            CommitError::StaleRevision | CommitError::Db(_) => true,
            CommitError::ValidationFailed(reason) => {
                reason.contains("timeout")
                    || reason.contains("spawn")
                    || reason.contains("io error")
                    || reason.contains("pool closed")
                    || reason.contains("dropped reply")
            }
            CommitError::ProtocolConflict
            | CommitError::ClientUpdateRejected(_)
            | CommitError::Model(_) => false,
        }
    }
}

/// Apply the `board_commit` config side-effects (grid/background/privacy/sticker
/// authors) inside the canonical commit transaction.
async fn apply_board_config(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    board_id: Uuid,
    payload: &Value,
) -> Result<(), sqlx::Error> {
    let grid_type = payload
        .get("gridType")
        .or_else(|| payload.get("grid_type"))
        .and_then(|v| v.as_str());
    let background_color = payload
        .get("backgroundColor")
        .or_else(|| payload.get("background_color"))
        .and_then(|v| v.as_str());
    let privacy_mode = payload
        .get("privacyMode")
        .or_else(|| payload.get("privacy_mode"))
        .and_then(|v| v.as_bool());
    let sticker_authors = payload
        .get("stickerAuthors")
        .or_else(|| payload.get("sticker_authors"))
        .and_then(|v| v.as_bool());
    boards::update_board_config(
        tx,
        board_id,
        grid_type,
        background_color,
        privacy_mode,
        sticker_authors,
    )
    .await
}

/// A stable, deterministic hash of an event payload for deduplication. This is
/// not cryptographic; it is a fast guard that a matching `client_event_id`
/// really carries the same content; SipHash-1-3 with std's fixed keys is
/// deterministic across processes for identical bytes.
fn payload_hash(payload: &Value) -> Vec<u8> {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(payload).unwrap_or_default();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish().to_be_bytes().to_vec()
}

pub(crate) fn bytes_hash(bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).to_vec()
}

fn enforce_writer_fence(
    event_type: &str,
    current_schema: u64,
    candidate_schema: Option<u64>,
) -> Result<(), CommitError> {
    if event_type != "world_commit" || current_schema < crate::state::yrs_model::SCHEMA_VERSION {
        return Ok(());
    }
    if candidate_schema.is_some_and(|schema| schema >= current_schema) {
        return Ok(());
    }
    Err(CommitError::ClientUpdateRejected(format!(
        "WRITER_UPGRADE_REQUIRED: writer schema {} is below required schema {current_schema}",
        candidate_schema.unwrap_or(0)
    )))
}

fn enforce_canonical_payload(event_type: &str, payload: &Value) -> Result<(), CommitError> {
    if event_type == "world_commit" && payload.get("ops").is_some() {
        return Err(CommitError::ClientUpdateRejected(
            "FIELD_OPS_UNSUPPORTED".to_string(),
        ));
    }
    Ok(())
}

struct DecodedClientCandidate {
    bytes: Vec<u8>,
    protocol_version: i16,
    writer_client_id: u64,
    update_hash: Vec<u8>,
    schema_version: i32,
}

fn decode_client_candidate(
    envelope: &ClientYrsUpdate,
    event_type: &str,
) -> Result<DecodedClientCandidate, CommitError> {
    if event_type != "world_commit" {
        return Err(CommitError::ClientUpdateRejected(
            "only world_commit may carry yrs".to_string(),
        ));
    }
    if envelope.encoding != "yrs-v1"
        || envelope.protocol_version != crate::state::yrs_model::PROTOCOL_VERSION as i32
        || envelope.schema_version != crate::state::yrs_model::SCHEMA_VERSION as i32
    {
        return Err(CommitError::ClientUpdateRejected(
            "unsupported encoding/protocol/schema".to_string(),
        ));
    }
    if !(1..crate::state::yrs_model::SERVER_ID_LO).contains(&envelope.writer_client_id) {
        return Err(CommitError::ClientUpdateRejected(
            "writer client id outside CLIENT band".to_string(),
        ));
    }
    // 8 MiB decoded cap; reject oversized base64 before allocating its full body.
    const MAX_UPDATE_BYTES: usize = 8 * 1024 * 1024;
    if envelope.update_bytes.is_none()
        && envelope.update_b64.len() > ((MAX_UPDATE_BYTES * 4 / 3) + 8)
    {
        return Err(CommitError::ClientUpdateRejected(
            "update too large".to_string(),
        ));
    }
    let bytes = match &envelope.update_bytes {
        Some(bytes) => bytes.clone(),
        None => base64::engine::general_purpose::STANDARD
            .decode(&envelope.update_b64)
            .map_err(|_| CommitError::ClientUpdateRejected("invalid update base64".to_string()))?,
    };
    if bytes.len() > MAX_UPDATE_BYTES {
        return Err(CommitError::ClientUpdateRejected(
            "update too large".to_string(),
        ));
    }
    let update_hash = bytes_hash(&bytes);
    Ok(DecodedClientCandidate {
        bytes,
        protocol_version: envelope.protocol_version as i16,
        writer_client_id: envelope.writer_client_id,
        update_hash,
        schema_version: envelope.schema_version,
    })
}

#[cfg(test)]
mod authority_flag_tests {
    use super::{enforce_canonical_payload, enforce_writer_fence};

    #[test]
    fn canonical_schema_rejects_old_world_writers_but_not_board_config() {
        let missing = enforce_writer_fence("world_commit", 2, None).unwrap_err();
        assert_eq!(missing.code(), "WRITER_UPGRADE_REQUIRED");
        let old = enforce_writer_fence("world_commit", 2, Some(1)).unwrap_err();
        assert_eq!(old.code(), "WRITER_UPGRADE_REQUIRED");
        assert!(enforce_writer_fence("world_commit", 2, Some(2)).is_ok());
        assert!(enforce_writer_fence("board_commit", 2, None).is_ok());
        assert!(enforce_writer_fence("world_commit", 1, None).is_ok());
    }

    #[test]
    fn canonical_wire_rejects_field_ops_after_dedup_boundary() {
        let payload = serde_json::json!({ "actions": [], "ops": [] });
        let error = enforce_canonical_payload("world_commit", &payload).unwrap_err();
        assert_eq!(error.code(), "CLIENT_UPDATE_REJECTED");
        assert!(error.to_string().contains("FIELD_OPS_UNSUPPORTED"));
        assert!(enforce_canonical_payload("board_commit", &payload).is_ok());
        assert!(
            enforce_canonical_payload("world_commit", &serde_json::json!({"actions": []})).is_ok()
        );
    }
}
