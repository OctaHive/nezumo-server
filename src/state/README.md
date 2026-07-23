# Canonical board state

The backend stores canonical board mutations as Yrs updates and reloadable
binary snapshots. It does not depend on renderer/client crates and does not
reconstruct an independent legacy CRDT.

Key modules:

- `yrs_coordinator.rs` owns the resident canonical `yrs::Doc` and applies only
  validated, durably paired Yrs updates;
- `yrs_compaction.rs` creates reloadable binary barriers;
- `yrs_model.rs` contains the backend-owned canonical schema mapping used for
  projection and validation;
- `yrs_validator.rs` calls the schema-aware headless renderer sidecar.

Missing canonical journal entries are treated as corruption instead of being
rebuilt from JSON event payloads.
