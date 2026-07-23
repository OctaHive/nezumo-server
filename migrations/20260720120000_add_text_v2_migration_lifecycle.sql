-- Exclusive and recoverable conversion metadata for
-- structural collaborative text. The artifact row is intentionally retained
-- after success; it is the pre-migration recovery/export source.
ALTER TABLE board_yrs_heads DROP CONSTRAINT board_yrs_heads_state_check;
ALTER TABLE board_yrs_heads ADD CONSTRAINT board_yrs_heads_state_check
    CHECK (state IN ('legacy', 'activating', 'yrs_3a', 'migrating_text_v2', 'quarantined'));

ALTER TABLE board_yrs_updates DROP CONSTRAINT board_yrs_updates_source_check;
ALTER TABLE board_yrs_updates ADD CONSTRAINT board_yrs_updates_source_check
    CHECK (source IN ('bridge', 'client', 'migration'));

CREATE TABLE board_yrs_text_migrations (
    board_id              UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    migration_id          TEXT NOT NULL,
    status                TEXT NOT NULL
                              CHECK (status IN ('running', 'completed', 'quarantined')),
    lease_epoch           BIGINT NOT NULL CHECK (lease_epoch > 0),
    source_seq            BIGINT NOT NULL CHECK (source_seq >= 0),
    source_generation     BIGINT NOT NULL CHECK (source_generation > 0),
    source_schema_version INT NOT NULL,
    recovery_state_update BYTEA NOT NULL,
    recovery_state_vector BYTEA NOT NULL,
    recovery_sha256       BYTEA NOT NULL,
    migration_yupdate     BYTEA NULL,
    post_state_vector     BYTEA NULL,
    migrated_instances    BIGINT NULL,
    error_code            TEXT NULL,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at          TIMESTAMPTZ NULL,
    PRIMARY KEY (board_id, migration_id)
);
