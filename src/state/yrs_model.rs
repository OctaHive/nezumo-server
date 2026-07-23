//! Backend-owned canonical snapshot ↔ Yrs `Doc` model.
//!
//! This is intentionally independent of renderer/client crates. The backend
//! persists and validates Yrs state; schema-aware rendering remains in the
//! validator sidecar.
//!
//!   root["meta"]     : Map { protocol_version, schema_version, min_writer_version }
//!   root["entities"] : Map { id -> Map { alive, components: Map } }
//!   components["type"] : Map { alive, mode: "field_lww"|"opaque", fields|value }
//!
//! The backend validates and persists canonical Yrs updates through this mapping.
//! Wire compatibility with client and validator implementations is guaranteed by
//! the shared `crates/core/tests/fixtures/yrs-v1` conformance corpus.

use serde_json::{Map as JsonMap, Value};
use std::collections::HashSet;
use thiserror::Error;
use yrs::updates::decoder::{Decode, Decoder, DecoderV1};
use yrs::updates::encoder::Encode;
use yrs::{
    Any, Doc, Map, MapPrelim, MapRef, OffsetKind, Options, Out, ReadTxn, StateVector, Transact,
    Update,
};

pub mod mapping;
mod text;
use mapping::{any_to_json, json_to_any, MappingError};

/// Strict upper bound for synced entity ids / entity refs (JS-safe integer domain).
/// General component numbers may use up to 2^53 (exact f64), but ids/refs are
/// strictly `< 2^53`.
const MAX_SYNCED_ID: u64 = (1 << 53) - 1;

/// Reserved writer client-ID bands. The backend server
/// draws its per-board id from `[2^52, 2^53)`; future client replicas use
/// `[1, 2^52)`, so server and client can never mint different structures under the
/// same `(client_id, clock)`.
pub const SERVER_ID_LO: u64 = 1 << 52;
pub const SERVER_ID_HI: u64 = 1 << 53;

/// Canonical wire protocol version.
pub const PROTOCOL_VERSION: u64 = 1;
pub const SCHEMA_VERSION: u64 = 2;
/// Minimum writer capability accepted by the canonical schema.
pub const MIN_WRITER_VERSION: u64 = SCHEMA_VERSION;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeStrategy {
    FieldLww,
    Opaque,
}

fn strategy_for(component_type: &str) -> MergeStrategy {
    const OPAQUE_TYPES: &[&str] = &[
        "text-element.content",
        "text.content",
        "line.label",
        "shape-element.geometry",
        "line.element.geometry",
        "line.element.polyline",
        "highlighter.element.geometry",
        "pen.element.geometry",
        "freedraw.stroke",
        "pen_stroke",
        "svg.icon",
        "line.binding",
    ];
    if OPAQUE_TYPES.contains(&component_type) {
        MergeStrategy::Opaque
    } else {
        MergeStrategy::FieldLww
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ModelError {
    #[error("root: missing or non-array \"entities\"")]
    BadRoot,
    #[error("entity id missing or not a safe u64")]
    BadId,
    #[error("synced entity id {0} must be a non-negative integer < 2^53")]
    EntityIdOutOfRange(u64),
    #[error("duplicate entity id {0}")]
    DuplicateEntityId(u64),
    #[error("component missing string \"type\"")]
    MissingType,
    #[error("duplicate component type {0} on one entity")]
    DuplicateComponentType(String),
    #[error("component must be a JSON object")]
    BadComponentShape,
    #[error("opaque component \"type\" ({found}) != its components key ({key})")]
    OpaqueTypeMismatch { key: String, found: String },
    #[error("unexpected on-Doc shape: {0}")]
    BadDocShape(&'static str),
    #[error("ref field {path}: {value} must be a non-negative integer < 2^53")]
    RefOutOfRange { path: String, value: Value },
    #[error("server client-id {0} is outside the reserved SERVER band [2^52, 2^53)")]
    ServerClientIdOutOfBand(u64),
    #[error("invalid structural text: {0}")]
    BadTextShape(&'static str),
    #[error("unknown structural text node or attribute: {0}")]
    UnknownTextNode(String),
    #[error("structural text limit exceeded: {0}")]
    TextLimitExceeded(&'static str),
    #[error(transparent)]
    Mapping(#[from] MappingError),
}

/// A snapshot that has passed `canonicalize_flat` (validated + canonical order).
/// `snapshot_to_doc` accepts ONLY this, so an unvalidated snapshot (duplicate ids /
/// component types, out-of-range id) can never silently overwrite a Y.Map key.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalSnapshot(Value);

impl CanonicalSnapshot {
    /// Borrows the canonicalized flat JSON value.
    pub fn as_value(&self) -> &Value {
        &self.0
    }
}

/// Which fields of a component hold entity refs (renderer-owned registry). Paths are
/// dot-scalar ("entity", "start.target") or a top-level array key ("members").
pub fn ref_fields(ty: &str) -> &'static [&'static str] {
    match ty {
        "parent" => &["entity"],
        "line.binding" => &["start.target", "end.target"],
        "group" => &["members"],
        _ => &[],
    }
}

fn validate_component_refs(ty: &str, comp: &Value) -> Result<(), ModelError> {
    for path in ref_fields(ty) {
        let Some(v) = resolve_path(comp, path) else {
            continue;
        };
        match v {
            Value::Null => {}
            Value::Array(items) => {
                for item in items {
                    check_ref(path, item)?;
                }
            }
            other => check_ref(path, other)?,
        }
    }
    Ok(())
}

fn check_ref(path: &str, v: &Value) -> Result<(), ModelError> {
    match v.as_u64() {
        Some(u) if u <= MAX_SYNCED_ID => Ok(()),
        _ => Err(ModelError::RefOutOfRange {
            path: path.to_string(),
            value: v.clone(),
        }),
    }
}

fn resolve_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Bind the two named root shared types before an `apply_update` (a Doc that only
/// received an update otherwise has no typed root to read).
pub fn bind_roots(doc: &Doc) {
    doc.get_or_insert_map("meta");
    doc.get_or_insert_map("entities");
}

/// Returns whether a Yrs client ID belongs to the server-reserved band.
pub fn server_id_in_band(id: u64) -> bool {
    (SERVER_ID_LO..SERVER_ID_HI).contains(&id)
}

/// Lossless canonicalization: validate + deterministic order (entities by numeric id,
/// components by type). Does NOT drop null/defaults/unknown. Byte-stable serialized.
pub fn canonicalize_flat(flat: &Value) -> Result<CanonicalSnapshot, ModelError> {
    let entities = flat
        .get("entities")
        .and_then(Value::as_array)
        .ok_or(ModelError::BadRoot)?;
    let mut seen_ids = HashSet::new();
    let mut out: Vec<(u64, Value)> = Vec::with_capacity(entities.len());
    for ent in entities {
        let id = ent
            .get("id")
            .and_then(Value::as_u64)
            .ok_or(ModelError::BadId)?;
        if id > MAX_SYNCED_ID {
            return Err(ModelError::EntityIdOutOfRange(id));
        }
        if !seen_ids.insert(id) {
            return Err(ModelError::DuplicateEntityId(id));
        }
        let comps = ent
            .get("components")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut seen_types = HashSet::new();
        let mut comp_out: Vec<(String, Value)> = Vec::with_capacity(comps.len());
        for comp in &comps {
            comp.as_object().ok_or(ModelError::BadComponentShape)?;
            let ty = comp
                .get("type")
                .and_then(Value::as_str)
                .ok_or(ModelError::MissingType)?
                .to_string();
            if !seen_types.insert(ty.clone()) {
                return Err(ModelError::DuplicateComponentType(ty));
            }
            validate_component_refs(&ty, comp)?;
            comp_out.push((ty, canon_value(comp)?));
        }
        comp_out.sort_by(|a, b| a.0.cmp(&b.0));
        let comp_values: Vec<Value> = comp_out.into_iter().map(|(_, v)| v).collect();
        out.push((id, entity_json(id, comp_values)));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(CanonicalSnapshot(root_json(
        out.into_iter().map(|(_, v)| v).collect(),
    )))
}

/// Canonical value form: route through `json_to_any`/`any_to_json` so number
/// representation, key order and the supported domain match `doc_to_snapshot`.
fn canon_value(v: &Value) -> Result<Value, ModelError> {
    Ok(any_to_json(&json_to_any(v)?)?)
}

fn entity_json(id: u64, components: Vec<Value>) -> Value {
    let mut m = JsonMap::new();
    m.insert("id".to_string(), Value::Number(id.into()));
    m.insert("components".to_string(), Value::Array(components));
    Value::Object(m)
}

fn root_json(entities: Vec<Value>) -> Value {
    let mut m = JsonMap::new();
    m.insert("entities".to_string(), Value::Array(entities));
    Value::Object(m)
}

/// Builds a document with an explicit server-reserved client ID for durable
/// checkpoint creation. Rejects an out-of-band ID.
pub fn snapshot_to_doc_with_client_id(
    snap: &CanonicalSnapshot,
    client_id: u64,
) -> Result<Doc, ModelError> {
    if !server_id_in_band(client_id) {
        return Err(ModelError::ServerClientIdOutOfBand(client_id));
    }
    let doc = Doc::with_options(Options {
        client_id: yrs::ClientID::new(client_id),
        offset_kind: OffsetKind::Utf16,
        ..Options::default()
    });
    populate_doc(&doc, snap)?;
    Ok(doc)
}

/// Populate an existing Doc from a validated snapshot.
pub fn populate_doc(doc: &Doc, snap: &CanonicalSnapshot) -> Result<(), ModelError> {
    let meta = doc.get_or_insert_map("meta");
    let entities_root = doc.get_or_insert_map("entities");
    let arr = snap
        .as_value()
        .get("entities")
        .and_then(Value::as_array)
        .ok_or(ModelError::BadRoot)?;
    let mut txn = doc.transact_mut();
    meta.insert(
        &mut txn,
        "protocol_version",
        Any::Number(PROTOCOL_VERSION as f64),
    );
    meta.insert(
        &mut txn,
        "schema_version",
        Any::Number(SCHEMA_VERSION as f64),
    );
    meta.insert(
        &mut txn,
        "min_writer_version",
        Any::Number(MIN_WRITER_VERSION as f64),
    );
    for ent in arr {
        let id = ent
            .get("id")
            .and_then(Value::as_u64)
            .ok_or(ModelError::BadId)?;
        let ent_map: MapRef = entities_root.insert(&mut txn, id.to_string(), MapPrelim::default());
        ent_map.insert(&mut txn, "alive", true);
        let comps_map: MapRef = ent_map.insert(&mut txn, "components", MapPrelim::default());
        let comps = ent.get("components").and_then(Value::as_array);
        for comp in comps.into_iter().flatten() {
            let obj = comp.as_object().ok_or(ModelError::BadComponentShape)?;
            let ty = comp
                .get("type")
                .and_then(Value::as_str)
                .ok_or(ModelError::MissingType)?;
            let inst: MapRef = comps_map.insert(&mut txn, ty, MapPrelim::default());
            write_instance(&mut txn, &inst, ty, obj, comp)?;
        }
    }
    Ok(())
}

/// Write a fresh component instance (`alive=true`, `mode`, `fields`|`value`) into
/// `inst`, per the strategy of `ty`. Shared by `populate_doc` and the op applier.
fn write_instance(
    txn: &mut yrs::TransactionMut,
    inst: &MapRef,
    ty: &str,
    obj: &JsonMap<String, Value>,
    whole: &Value,
) -> Result<(), ModelError> {
    if ty == text::COMPONENT {
        return text::write_instance(txn, inst, obj);
    }
    inst.insert(txn, "alive", true);
    match strategy_for(ty) {
        MergeStrategy::FieldLww => {
            inst.insert(txn, "mode", "field_lww");
            let fields: MapRef = inst.insert(txn, "fields", MapPrelim::default());
            for (k, v) in obj {
                if k == "type" {
                    continue;
                }
                fields.insert(txn, k.clone(), json_to_any(v)?);
            }
        }
        MergeStrategy::Opaque => {
            inst.insert(txn, "mode", "opaque");
            inst.insert(txn, "value", json_to_any(whole)?);
        }
    }
    Ok(())
}

/// Deterministic projection back to the flat snapshot. `alive=false`
/// entities/components are hidden; entities sorted by numeric id, components by type.
pub fn doc_to_snapshot(doc: &Doc) -> Result<Value, ModelError> {
    let txn = doc.transact();
    let entities_root = txn
        .get_map("entities")
        .ok_or(ModelError::BadDocShape("no entities root"))?;
    let mut out: Vec<(u64, Value)> = Vec::new();
    for (key, val) in entities_root.iter(&txn) {
        let id: u64 = key.parse().map_err(|_| ModelError::BadId)?;
        let Out::YMap(ent) = val else {
            return Err(ModelError::BadDocShape("entity not a map"));
        };
        if !alive_checked(&txn, &ent)? {
            continue;
        }
        let Some(Out::YMap(comps)) = ent.get(&txn, "components") else {
            return Err(ModelError::BadDocShape("entity missing components map"));
        };
        let mut comp_out: Vec<(String, Value)> = Vec::new();
        for (ty, inst_val) in comps.iter(&txn) {
            let Out::YMap(inst) = inst_val else {
                return Err(ModelError::BadDocShape("component instance not a map"));
            };
            if !alive_checked(&txn, &inst)? {
                continue;
            }
            comp_out.push((ty.to_string(), materialize_component(&txn, &ty, &inst)?));
        }
        comp_out.sort_by(|a, b| a.0.cmp(&b.0));
        let comps_vec: Vec<Value> = comp_out.into_iter().map(|(_, v)| v).collect();
        out.push((id, entity_json(id, comps_vec)));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(root_json(out.into_iter().map(|(_, v)| v).collect()))
}

fn alive_checked(txn: &impl ReadTxn, m: &MapRef) -> Result<bool, ModelError> {
    match m.get(txn, "alive") {
        Some(Out::Any(Any::Bool(b))) => Ok(b),
        _ => Err(ModelError::BadDocShape("missing or non-bool `alive`")),
    }
}

fn materialize_component(txn: &impl ReadTxn, ty: &str, inst: &MapRef) -> Result<Value, ModelError> {
    let mode = match inst.get(txn, "mode") {
        Some(Out::Any(Any::String(s))) => s,
        _ => return Err(ModelError::BadDocShape("component instance missing mode")),
    };
    match &*mode {
        "field_lww" => {
            let mut obj = JsonMap::new();
            obj.insert("type".to_string(), Value::String(ty.to_string()));
            if let Some(Out::YMap(fields)) = inst.get(txn, "fields") {
                for (k, v) in fields.iter(txn) {
                    let Out::Any(a) = v else {
                        return Err(ModelError::BadDocShape("field value not Any"));
                    };
                    obj.insert(k.to_string(), any_to_json(&a)?);
                }
            }
            Ok(Value::Object(obj))
        }
        "opaque" => {
            let Some(Out::Any(a)) = inst.get(txn, "value") else {
                return Err(ModelError::BadDocShape("opaque instance missing value"));
            };
            let v = any_to_json(&a)?;
            match v.get("type").and_then(Value::as_str) {
                Some(t) if t == ty => Ok(v),
                Some(t) => Err(ModelError::OpaqueTypeMismatch {
                    key: ty.to_string(),
                    found: t.to_string(),
                }),
                None => Err(ModelError::OpaqueTypeMismatch {
                    key: ty.to_string(),
                    found: "<none>".to_string(),
                }),
            }
        }
        text::MODE if ty == text::COMPONENT => text::materialize(txn, inst),
        _ => Err(ModelError::BadDocShape("unknown component mode")),
    }
}

/// Encode the durable base bytes: `(state_update_v1, state_vector_v1)`.
pub fn encode_base(doc: &Doc) -> (Vec<u8>, Vec<u8>) {
    let txn = doc.transact();
    let update = txn.encode_state_as_update_v1(&StateVector::default());
    let vector = txn.state_vector().encode_v1();
    (update, vector)
}

/// Reconstructs a document under its persisted server client ID so appended
/// updates continue the same Yrs history. The recomputed state vector must be
/// semantically equal to the stored vector.
///
/// State-vector entries are backed by a hash map and their wire order is not
/// canonical. Comparing encoded bytes would therefore reject equivalent vectors
/// whose entries happen to be emitted in a different order.
pub fn doc_from_base(
    state_update: &[u8],
    state_vector: &[u8],
    client_id: u64,
) -> Result<Doc, ModelError> {
    if !server_id_in_band(client_id) {
        return Err(ModelError::ServerClientIdOutOfBand(client_id));
    }
    let doc = Doc::with_options(Options {
        client_id: yrs::ClientID::new(client_id),
        offset_kind: OffsetKind::Utf16,
        ..Options::default()
    });
    bind_roots(&doc);
    doc.transact_mut()
        .apply_update(
            Update::decode_v1(state_update).map_err(|_| ModelError::BadDocShape("decode base"))?,
        )
        .map_err(|_| ModelError::BadDocShape("apply base"))?;
    let mut decoder = DecoderV1::from(state_vector);
    let persisted = StateVector::decode(&mut decoder)
        .map_err(|_| ModelError::BadDocShape("decode state vector"))?;
    if !decoder
        .read_to_end()
        .map_err(|_| ModelError::BadDocShape("decode state vector"))?
        .is_empty()
    {
        return Err(ModelError::BadDocShape("trailing state-vector bytes"));
    }
    let recomputed = doc.transact().state_vector();
    if recomputed != persisted {
        return Err(ModelError::BadDocShape("state-vector mismatch"));
    }
    Ok(doc)
}

/// Reads the schema version from canonical document metadata.
pub fn doc_schema_version(doc: &Doc) -> Result<u64, ModelError> {
    let txn = doc.transact();
    let meta = txn
        .get_map("meta")
        .ok_or(ModelError::BadDocShape("missing meta root"))?;
    match meta.get(&txn, "schema_version") {
        Some(Out::Any(Any::Number(value)))
            if value.is_finite() && value >= 0.0 && value.fract() == 0.0 =>
        {
            Ok(value as u64)
        }
        _ => Err(ModelError::BadDocShape("invalid schema version")),
    }
}

/// Ensures document metadata matches the schema version advertised by the envelope.
pub fn validate_schema_metadata(doc: &Doc, envelope_schema: u64) -> Result<(), ModelError> {
    let txn = doc.transact();
    let meta = txn
        .get_map("meta")
        .ok_or(ModelError::BadDocShape("missing meta root"))?;
    let schema = doc_schema_version(doc)?;
    if schema != envelope_schema || schema != SCHEMA_VERSION {
        return Err(ModelError::BadDocShape("envelope/Doc schema mismatch"));
    }
    let minimum = match meta.get(&txn, "min_writer_version") {
        Some(Out::Any(Any::Number(value))) if value.is_finite() && value.fract() == 0.0 => {
            value as u64
        }
        _ => return Err(ModelError::BadDocShape("invalid min writer version")),
    };
    if minimum != schema {
        return Err(ModelError::BadDocShape("schema/min writer mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use yrs::encoding::write::Write;

    #[test]
    fn base_reload_accepts_a_reordered_equivalent_state_vector() {
        let snapshot = canonicalize_flat(&json!({"entities": []})).unwrap();
        let doc = snapshot_to_doc_with_client_id(&snapshot, SERVER_ID_LO + 17).unwrap();

        // Introduce a second writer so the vector has entries whose serialized
        // order can differ while their client clocks remain identical.
        let remote = Doc::with_options(Options {
            client_id: yrs::ClientID::new(23),
            offset_kind: OffsetKind::Utf16,
            ..Options::default()
        });
        remote.get_or_insert_map("state-vector-probe").insert(
            &mut remote.transact_mut(),
            "value",
            true,
        );
        let remote_update = remote
            .transact()
            .encode_state_as_update_v1(&StateVector::default());
        doc.transact_mut()
            .apply_update(Update::decode_v1(&remote_update).unwrap())
            .unwrap();

        let (state_update, state_vector) = encode_base(&doc);
        let decoded = StateVector::decode_v1(&state_vector).unwrap();
        let mut entries: Vec<_> = decoded
            .iter()
            .map(|(client, clock)| (client.get(), *clock))
            .collect();
        assert!(entries.len() > 1);
        entries.reverse();

        let mut reordered = Vec::new();
        reordered.write_var(entries.len());
        for (client, clock) in entries {
            reordered.write_var(client);
            reordered.write_var(clock);
        }
        assert_ne!(reordered, state_vector);

        doc_from_base(&state_update, &reordered, SERVER_ID_LO + 17).unwrap();
    }
}
