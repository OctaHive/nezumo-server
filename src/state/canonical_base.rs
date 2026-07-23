//! Durable canonical Yrs base metadata.
//!
//! Bases are created directly from a trusted flat projection or from canonical
//! binary snapshots.

use serde_json::Value;

use crate::state::yrs_model::{
    canonicalize_flat, encode_base, snapshot_to_doc_with_client_id, ModelError, SERVER_ID_LO,
};

/// Reloadable canonical checkpoint used to initialize a resident Yrs document.
#[derive(Clone)]
pub struct CanonicalBase {
    /// Full Yrs state update used to reconstruct the document.
    pub state_update: Vec<u8>,
    /// State vector that must match the reconstructed document.
    pub state_vector: Vec<u8>,
    /// Last durable board-event sequence included in this checkpoint.
    pub base_seq: i64,
    /// Server-reserved Yrs client identifier that owns the base structures.
    pub server_client_id: u64,
    /// Monotonic lineage generation used to reject stale updates and snapshots.
    pub base_generation: i64,
}

/// Draws a random client identifier from the server-reserved Yrs ID band.
pub fn random_server_client_id() -> u64 {
    use rand::Rng;
    SERVER_ID_LO + (rand::thread_rng().gen::<u64>() % SERVER_ID_LO)
}

/// Converts a flat board projection into a reloadable canonical base.
pub fn from_flat_projection(state: &Value, base_seq: i64) -> Result<CanonicalBase, ModelError> {
    let canonical = canonicalize_flat(state)?;
    let server_client_id = random_server_client_id();
    let doc = snapshot_to_doc_with_client_id(&canonical, server_client_id)?;
    let (state_update, state_vector) = encode_base(&doc);
    Ok(CanonicalBase {
        state_update,
        state_vector,
        base_seq,
        server_client_id,
        base_generation: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::yrs_model::{doc_from_base, doc_to_snapshot, server_id_in_band};

    #[test]
    fn flat_projection_seeds_reloadable_canonical_base() {
        let flat = serde_json::json!({"entities": []});
        let base = from_flat_projection(&flat, 7).unwrap();
        assert!(server_id_in_band(base.server_client_id));
        let doc = doc_from_base(
            &base.state_update,
            &base.state_vector,
            base.server_client_id,
        )
        .unwrap();
        assert_eq!(doc_to_snapshot(&doc).unwrap(), flat);
        assert_eq!(base.base_seq, 7);
    }
}
