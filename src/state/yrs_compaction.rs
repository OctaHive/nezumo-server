//! Canonical Yrs binary snapshot compaction.
//!
//! This module is deliberately DB-free. It reconstructs a real [`yrs::Doc`],
//! applies an ordered canonical tail, encodes a full state update, and reloads
//! that output before the caller is allowed to persist it. Concatenating update
//! bytes is never considered compaction.

use sha2::{Digest, Sha256};
use thiserror::Error;
use yrs::updates::decoder::Decode;
use yrs::{Transact, Update};

use crate::state::yrs_model::{
    doc_from_base, doc_schema_version, encode_base, validate_schema_metadata, ModelError,
    PROTOCOL_VERSION,
};

#[derive(Debug, Clone)]
pub struct BinarySnapshotBase {
    pub state_update: Vec<u8>,
    pub state_vector: Vec<u8>,
    pub last_event_seq: i64,
    pub base_generation: i64,
    pub server_client_id: u64,
}

#[derive(Debug, Clone)]
pub struct BinaryTailUpdate {
    pub seq: i64,
    pub yupdate: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CompactedBinarySnapshot {
    pub state_update: Vec<u8>,
    pub state_vector: Vec<u8>,
    pub last_event_seq: i64,
    pub base_generation: i64,
    pub server_client_id: u64,
    pub protocol_version: i16,
    pub schema_version: i32,
    pub input_update_count: i64,
    pub input_tail_bytes: i64,
    pub output_bytes: i64,
    pub state_sha256: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("target barrier precedes binary base")]
    BarrierBeforeBase,
    #[error("canonical tail is not strictly ordered inside the snapshot barrier")]
    TailOrder,
    #[error("canonical tail byte count overflows persistence metrics")]
    TailBytesOverflow,
    #[error("could not decode canonical tail update at seq {0}")]
    DecodeTail(i64),
    #[error("could not apply canonical tail update at seq {0}")]
    ApplyTail(i64),
    #[error("binary base is invalid: {0}")]
    InvalidBase(ModelError),
    #[error("compacted output failed reload: {0}")]
    Reload(ModelError),
    #[error("compacted output schema is invalid: {0}")]
    InvalidSchema(ModelError),
}

/// Rebuild a complete Doc at `target_event_seq`, encode it as one full update,
/// and prove the emitted `(update, vector)` pair reloads exactly.
///
/// Sequence gaps are valid because non-domain events and reserved-but-
/// unused sequence numbers do not necessarily have a canonical update row.
pub fn compact_binary_snapshot(
    base: &BinarySnapshotBase,
    target_event_seq: i64,
    tail: &[BinaryTailUpdate],
) -> Result<CompactedBinarySnapshot, CompactionError> {
    if target_event_seq < base.last_event_seq {
        return Err(CompactionError::BarrierBeforeBase);
    }

    let mut prior_seq = base.last_event_seq;
    let mut input_tail_bytes = 0i64;
    for update in tail {
        if update.seq <= prior_seq || update.seq > target_event_seq {
            return Err(CompactionError::TailOrder);
        }
        prior_seq = update.seq;
        input_tail_bytes = input_tail_bytes
            .checked_add(
                i64::try_from(update.yupdate.len())
                    .map_err(|_| CompactionError::TailBytesOverflow)?,
            )
            .ok_or(CompactionError::TailBytesOverflow)?;
    }

    let doc = doc_from_base(
        &base.state_update,
        &base.state_vector,
        base.server_client_id,
    )
    .map_err(CompactionError::InvalidBase)?;
    for update in tail {
        let decoded = Update::decode_v1(&update.yupdate)
            .map_err(|_| CompactionError::DecodeTail(update.seq))?;
        doc.transact_mut()
            .apply_update(decoded)
            .map_err(|_| CompactionError::ApplyTail(update.seq))?;
    }

    let schema_version = doc_schema_version(&doc).map_err(CompactionError::InvalidSchema)?;
    validate_schema_metadata(&doc, schema_version).map_err(CompactionError::InvalidSchema)?;
    let (state_update, state_vector) = encode_base(&doc);

    // Persistence is allowed only after an independent decode/apply/vector pass
    // over the exact bytes that will be written.
    let reloaded = doc_from_base(&state_update, &state_vector, base.server_client_id)
        .map_err(CompactionError::Reload)?;
    validate_schema_metadata(&reloaded, schema_version).map_err(CompactionError::InvalidSchema)?;

    let output_bytes =
        i64::try_from(state_update.len()).map_err(|_| CompactionError::TailBytesOverflow)?;
    let state_sha256 = Sha256::digest(&state_update).to_vec();
    Ok(CompactedBinarySnapshot {
        state_update,
        state_vector,
        last_event_seq: target_event_seq,
        base_generation: base.base_generation,
        server_client_id: base.server_client_id,
        protocol_version: PROTOCOL_VERSION as i16,
        schema_version: schema_version as i32,
        input_update_count: tail.len() as i64,
        input_tail_bytes,
        output_bytes,
        state_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::yrs_model::{canonicalize_flat, encode_base, snapshot_to_doc_with_client_id};
    use serde_json::json;
    use yrs::{Map, ReadTxn, StateVector};

    const CID: u64 = (1u64 << 52) + 808;

    fn base() -> BinarySnapshotBase {
        let canonical = canonicalize_flat(&json!({
            "entities": [{
                "id": 1,
                "components": [{"type": "element", "fill": "red"}]
            }]
        }))
        .unwrap();
        let doc = snapshot_to_doc_with_client_id(&canonical, CID).unwrap();
        let (state_update, state_vector) = encode_base(&doc);
        BinarySnapshotBase {
            state_update,
            state_vector,
            last_event_seq: 7,
            base_generation: 3,
            server_client_id: CID,
        }
    }

    fn one_tail(base: &BinarySnapshotBase) -> BinaryTailUpdate {
        let doc = doc_from_base(&base.state_update, &base.state_vector, CID).unwrap();
        let before = StateVector::decode_v1(&base.state_vector).unwrap();
        let entities = doc.get_or_insert_map("entities");
        {
            let mut txn = doc.transact_mut();
            let entity = entities
                .get(&txn, "1")
                .and_then(|value| value.cast::<yrs::MapRef>().ok())
                .unwrap();
            entity.insert(&mut txn, "compaction_probe", true);
        }
        let yupdate = doc.transact().encode_state_as_update_v1(&before);
        BinaryTailUpdate { seq: 11, yupdate }
    }

    #[test]
    fn real_doc_compaction_reloads_at_exact_barrier() {
        let base = base();
        let tail = one_tail(&base);
        let compacted = compact_binary_snapshot(&base, 12, &[tail]).unwrap();
        assert_eq!(compacted.last_event_seq, 12); // seq 12 may be a legitimate no-op event
        assert_eq!(compacted.base_generation, 3);
        assert_eq!(compacted.input_update_count, 1);
        assert_eq!(compacted.output_bytes, compacted.state_update.len() as i64);
        assert_eq!(compacted.state_sha256.len(), 32);
        doc_from_base(&compacted.state_update, &compacted.state_vector, CID).unwrap();
    }

    #[test]
    fn corrupt_base_vector_is_rejected() {
        let mut base = base();
        base.state_vector.push(0xff);
        assert!(matches!(
            compact_binary_snapshot(&base, 7, &[]),
            Err(CompactionError::InvalidBase(_))
        ));
    }

    #[test]
    fn tail_must_be_ordered_and_inside_barrier() {
        let base = base();
        let update = one_tail(&base);
        assert!(matches!(
            compact_binary_snapshot(&base, 10, &[update.clone()]),
            Err(CompactionError::TailOrder)
        ));
        assert!(matches!(
            compact_binary_snapshot(&base, 12, &[update.clone(), update]),
            Err(CompactionError::TailOrder)
        ));
    }
}
