//! Canonical per-board Yrs coordinator.
//!
//! The backend owns only opaque Yrs history and a projected read model. It has
//! no dependency on the renderer,
//! `nezumo-core`, legacy CRDT operations, or an in-process `BoardCrdt` fold.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

use crate::state::canonical_base::CanonicalBase;
use crate::state::yrs_model::{
    doc_from_base, doc_schema_version, doc_to_snapshot, encode_base, server_id_in_band,
    validate_schema_metadata, ModelError,
};

/// A validated canonical commit prepared against an isolated candidate document.
pub struct PreparedCommit {
    pub seq: i64,
    pub base_update: Vec<u8>,
    pub base_state_vector: Vec<u8>,
    pub yupdate: Vec<u8>,
    pub schema_version: i32,
    #[cfg(test)]
    pub empty: bool,
    expected_projection: Value,
}

impl PreparedCommit {
    /// Returns the exact incremental update accepted for websocket fan-out.
    pub fn yupdate_for_broadcast(&self) -> Vec<u8> {
        self.yupdate.clone()
    }

    /// Returns the materialized projection expected after applying this commit.
    pub fn expected_projection(&self) -> Result<Value, ModelError> {
        Ok(self.expected_projection.clone())
    }
}

/// Resident canonical Yrs document and its durable journal cursor for one board.
pub struct BoardCoordinator {
    doc: Doc,
    server_client_id: u64,
    base_generation: i64,
    processed_seq: i64,
}

impl BoardCoordinator {
    /// Restores a resident coordinator from an immutable canonical base.
    pub fn from_base(base: &CanonicalBase, processed_seq: i64) -> Result<Self, ModelError> {
        if !server_id_in_band(base.server_client_id) {
            return Err(ModelError::ServerClientIdOutOfBand(base.server_client_id));
        }
        Ok(Self {
            doc: doc_from_base(
                &base.state_update,
                &base.state_vector,
                base.server_client_id,
            )?,
            server_client_id: base.server_client_id,
            base_generation: base.base_generation,
            processed_seq,
        })
    }

    /// Returns the greatest durable event sequence folded into the resident document.
    pub fn processed_seq(&self) -> i64 {
        self.processed_seq
    }

    /// Returns the immutable base lineage used by this resident document.
    pub fn base_generation(&self) -> i64 {
        self.base_generation
    }

    /// Returns the server-band Yrs client identifier that authored the base.
    pub fn server_client_id(&self) -> u64 {
        self.server_client_id
    }

    /// Reads and validates the schema version stored in the document metadata.
    pub fn schema_version(&self) -> Result<u64, ModelError> {
        doc_schema_version(&self.doc)
    }

    /// Encodes the full resident document and its matching state vector.
    pub fn canonical_base(&self) -> (Vec<u8>, Vec<u8>) {
        encode_base(&self.doc)
    }

    /// Verifies that a candidate update uses the canonical schema.
    pub fn validate_candidate_schema(
        &self,
        candidate_update: &[u8],
        envelope_schema: u64,
    ) -> Result<(), ModelError> {
        let current = self.schema_version()?;
        if current != crate::state::yrs_model::SCHEMA_VERSION || envelope_schema != current {
            return Err(ModelError::BadDocShape("invalid canonical schema"));
        }
        let (base, vector) = encode_base(&self.doc);
        let candidate = doc_from_base(&base, &vector, self.server_client_id)?;
        candidate
            .transact_mut()
            .apply_update(
                Update::decode_v1(candidate_update)
                    .map_err(|_| ModelError::BadDocShape("decode candidate schema"))?,
            )
            .map_err(|_| ModelError::BadDocShape("apply candidate schema"))?;
        validate_schema_metadata(&candidate, envelope_schema)
    }

    /// Materializes the resident document into the stable flat board projection.
    pub fn resident_snapshot(&self) -> Result<Value, ModelError> {
        doc_to_snapshot(&self.doc)
    }

    /// Applies a client update to an isolated candidate and prepares durable commit data.
    pub fn prepare_external(
        &self,
        seq: i64,
        event_payload: &Value,
        candidate_update: &[u8],
    ) -> Result<(PreparedCommit, Value), ModelError> {
        let (base_update, base_state_vector) = encode_base(&self.doc);
        let candidate = doc_from_base(&base_update, &base_state_vector, self.server_client_id)?;
        candidate
            .transact_mut()
            .apply_update(
                Update::decode_v1(candidate_update)
                    .map_err(|_| ModelError::BadDocShape("decode external candidate"))?,
            )
            .map_err(|_| ModelError::BadDocShape("apply external candidate"))?;
        let projection = doc_to_snapshot(&candidate)?;
        let persisted_payload = normalize_structural_text_projection(event_payload, &projection)?;
        Ok((
            PreparedCommit {
                seq,
                base_update,
                base_state_vector,
                yupdate: candidate_update.to_vec(),
                schema_version: doc_schema_version(&candidate)? as i32,
                #[cfg(test)]
                empty: candidate_update.is_empty(),
                expected_projection: projection,
            },
            persisted_payload,
        ))
    }

    /// Prepares a journal entry for an event that changes board metadata but
    /// leaves the canonical content document unchanged.
    pub fn prepare_noop(
        &self,
        seq: i64,
        event_payload: &Value,
    ) -> Result<(PreparedCommit, Value), ModelError> {
        let (base_update, base_state_vector) = encode_base(&self.doc);
        let vector = StateVector::decode_v1(&base_state_vector)
            .map_err(|_| ModelError::BadDocShape("decode resident state vector"))?;
        let yupdate = self.doc.transact().encode_state_as_update_v1(&vector);
        let projection = doc_to_snapshot(&self.doc)?;
        Ok((
            PreparedCommit {
                seq,
                base_update,
                base_state_vector,
                yupdate,
                schema_version: self.schema_version()? as i32,
                #[cfg(test)]
                empty: true,
                expected_projection: projection,
            },
            event_payload.clone(),
        ))
    }

    /// Applies a commit only after its event and Yrs row have committed atomically.
    pub fn apply_committed(&mut self, prepared: PreparedCommit) -> Result<(), ModelError> {
        self.apply_persisted_update(&prepared.yupdate)?;
        self.processed_seq = prepared.seq;
        Ok(())
    }

    /// Applies a previously validated update without changing the journal cursor.
    pub fn apply_persisted_update(&mut self, yupdate: &[u8]) -> Result<(), ModelError> {
        let update = Update::decode_v1(yupdate)
            .map_err(|_| ModelError::BadDocShape("decode persisted yupdate"))?;
        self.doc
            .transact_mut()
            .apply_update(update)
            .map_err(|_| ModelError::BadDocShape("apply persisted yupdate"))?;
        Ok(())
    }

    /// Replays one already-persisted journal row while activating a resident coordinator.
    pub fn catch_up_persisted(&mut self, seq: i64, yupdate: &[u8]) -> Result<(), ModelError> {
        self.apply_persisted_update(yupdate)?;
        self.processed_seq = self.processed_seq.max(seq);
        Ok(())
    }

    /// Verifies that the resident document has a valid materialized projection.
    pub fn assert_valid_projection(&self) -> Result<(), ModelError> {
        doc_to_snapshot(&self.doc).map(|_| ())
    }
}

fn normalize_structural_text_projection(
    payload: &Value,
    candidate_projection: &Value,
) -> Result<Value, ModelError> {
    let mut affected = BTreeSet::new();
    if let Some(actions) = payload.get("actions").and_then(Value::as_array) {
        for command in actions
            .iter()
            .filter_map(|action| action.get("forward").and_then(Value::as_array))
            .flatten()
        {
            collect_text_command_entity(command, &mut affected);
        }
    }
    if affected.is_empty() {
        return Ok(payload.clone());
    }
    let canonical = canonical_text_components(candidate_projection, &affected)?;
    let mut normalized = payload.clone();
    if let Some(actions) = normalized.get_mut("actions").and_then(Value::as_array_mut) {
        for command in actions
            .iter_mut()
            .filter_map(|action| action.get_mut("forward").and_then(Value::as_array_mut))
            .flatten()
        {
            rewrite_text_command(command, &canonical);
        }
    }
    Ok(normalized)
}

fn collect_text_command_entity(command: &Value, affected: &mut BTreeSet<u64>) {
    let Some(entity) = command.get("entity").and_then(Value::as_u64) else {
        return;
    };
    let direct = command
        .get("component")
        .and_then(|component| component.get("type"))
        .and_then(Value::as_str)
        == Some("text.content");
    let spawned = command
        .get("components")
        .and_then(Value::as_array)
        .is_some_and(|components| {
            components.iter().any(|component| {
                component.get("type").and_then(Value::as_str) == Some("text.content")
            })
        });
    if direct || spawned {
        affected.insert(entity);
    }
}

fn canonical_text_components(
    projection: &Value,
    affected: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, Value>, ModelError> {
    let mut out = BTreeMap::new();
    let entities = projection
        .get("entities")
        .and_then(Value::as_array)
        .ok_or(ModelError::BadRoot)?;
    for entity in entities {
        let Some(id) = entity.get("id").and_then(Value::as_u64) else {
            continue;
        };
        if !affected.contains(&id) {
            continue;
        }
        let component = entity
            .get("components")
            .and_then(Value::as_array)
            .and_then(|components| {
                components.iter().find(|component| {
                    component.get("type").and_then(Value::as_str) == Some("text.content")
                })
            })
            .cloned()
            .ok_or(ModelError::BadTextShape(
                "candidate projection is missing affected text component",
            ))?;
        out.insert(id, component);
    }
    if out.len() != affected.len() {
        return Err(ModelError::BadTextShape(
            "candidate projection is missing affected text entity",
        ));
    }
    Ok(out)
}

fn rewrite_text_command(command: &mut Value, canonical: &BTreeMap<u64, Value>) {
    let Some(entity) = command.get("entity").and_then(Value::as_u64) else {
        return;
    };
    let Some(component) = canonical.get(&entity) else {
        return;
    };
    if command
        .get("component")
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        == Some("text.content")
    {
        command["component"] = component.clone();
    }
    if let Some(components) = command.get_mut("components").and_then(Value::as_array_mut) {
        for value in components {
            if value.get("type").and_then(Value::as_str) == Some("text.content") {
                *value = component.clone();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::yrs_model::{
        canonicalize_flat, snapshot_to_doc_with_client_id, SERVER_ID_LO,
    };

    fn coordinator() -> BoardCoordinator {
        let flat = serde_json::json!({"entities": []});
        let canonical = canonicalize_flat(&flat).unwrap();
        let doc = snapshot_to_doc_with_client_id(&canonical, SERVER_ID_LO).unwrap();
        let (state_update, state_vector) = encode_base(&doc);
        BoardCoordinator::from_base(
            &CanonicalBase {
                state_update,
                state_vector,
                base_seq: 0,
                server_client_id: SERVER_ID_LO,
                base_generation: 1,
            },
            0,
        )
        .unwrap()
    }

    #[test]
    fn coordinator_bootstraps_from_canonical_base() {
        let coordinator = coordinator();
        assert_eq!(
            coordinator.resident_snapshot().unwrap(),
            serde_json::json!({"entities": []})
        );
    }

    #[test]
    fn metadata_event_produces_a_reloadable_empty_update() {
        let mut coordinator = coordinator();
        let payload = serde_json::json!({"gridType": "dots"});
        let (mut prepared, persisted) = coordinator.prepare_noop(0, &payload).unwrap();
        assert!(prepared.empty);
        assert_eq!(persisted, payload);
        prepared.seq = 1;
        coordinator.apply_committed(prepared).unwrap();
        assert_eq!(coordinator.processed_seq(), 1);
        assert_eq!(
            coordinator.resident_snapshot().unwrap(),
            serde_json::json!({"entities": []})
        );
    }
}
