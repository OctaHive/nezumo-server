-- Additive binary snapshot storage and board conversion inventory. These tables do not replace or delete legacy
-- board_snapshots/board_events. They make binary coverage measurable before the
-- separate legacy-removal gate can open.

CREATE TABLE board_yrs_snapshots (
    board_id            UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    base_generation     BIGINT NOT NULL CHECK (base_generation > 0),
    last_event_seq      BIGINT NOT NULL CHECK (last_event_seq >= 0),
    state_update        BYTEA NOT NULL,
    state_vector        BYTEA NOT NULL,
    protocol_version    SMALLINT NOT NULL CHECK (protocol_version > 0),
    schema_version      INT NOT NULL CHECK (schema_version > 0),
    update_encoding     TEXT NOT NULL DEFAULT 'v1'
                            CHECK (update_encoding = 'v1'),
    server_client_id    BIGINT NOT NULL CHECK (
                            server_client_id >= 4503599627370496 AND
                            server_client_id < 9007199254740992
                        ),
    source              TEXT NOT NULL CHECK (
                            source IN ('canonical_compaction', 'legacy_fleet_migration')
                        ),
    input_update_count  BIGINT NOT NULL DEFAULT 0 CHECK (input_update_count >= 0),
    input_tail_bytes    BIGINT NOT NULL DEFAULT 0 CHECK (input_tail_bytes >= 0),
    output_bytes        BIGINT NOT NULL CHECK (output_bytes >= 0),
    state_sha256        BYTEA NOT NULL CHECK (OCTET_LENGTH(state_sha256) = 32),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (output_bytes = OCTET_LENGTH(state_update)),
    -- `base_generation` is scoped to a Yrs writer lineage. A legacy fleet copy
    -- and a later activation can both be generation 1 with different client ids.
    PRIMARY KEY (board_id, server_client_id, base_generation, last_event_seq)
);

CREATE INDEX idx_board_yrs_snapshots_latest
    ON board_yrs_snapshots(board_id, source, base_generation, last_event_seq DESC);

CREATE TABLE board_yrs_migration_inventory (
    board_id            UUID PRIMARY KEY REFERENCES boards(id) ON DELETE CASCADE,
    status              TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'migrated', 'quarantined')),
    source_kind         TEXT NULL CHECK (
                            source_kind IS NULL OR source_kind IN (
                                'empty', 'legacy_json', 'legacy_crdt_meta', 'canonical'
                            )
                        ),
    observed_event_seq  BIGINT NOT NULL DEFAULT 0 CHECK (observed_event_seq >= 0),
    last_event_seq      BIGINT NULL CHECK (last_event_seq IS NULL OR last_event_seq >= 0),
    base_generation     BIGINT NULL CHECK (base_generation IS NULL OR base_generation > 0),
    attempt_count       INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_token         UUID NULL,
    lease_expires_at    TIMESTAMPTZ NULL,
    last_error_code     TEXT NULL,
    last_error_detail   TEXT NULL,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_started_at     TIMESTAMPTZ NULL,
    migrated_at         TIMESTAMPTZ NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (status = 'running' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'running' AND lease_token IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (last_event_seq IS NULL OR last_event_seq <= observed_event_seq)
);

CREATE INDEX idx_board_yrs_migration_inventory_work
    ON board_yrs_migration_inventory(status, updated_at, board_id);
