-- Finalize canonical Yrs storage after every board has a complete schema-v2
-- snapshot and a schema-v2 journal tail. The guards use durable canonical data
-- rather than the disposable conversion inventory that this migration removes.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM boards b
        LEFT JOIN board_yrs_shadow_bases c ON c.board_id = b.id
        WHERE c.board_id IS NULL
           OR c.abandoned_at IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'cannot finalize Yrs storage: a board lacks an active canonical base';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM boards b
        LEFT JOIN board_yrs_heads h ON h.board_id = b.id
        WHERE h.board_id IS NULL
           OR h.state <> 'yrs_3a'
    ) THEN
        RAISE EXCEPTION 'cannot finalize Yrs storage: a canonical head is not ready';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM boards b
        WHERE NOT EXISTS (
            SELECT 1
            FROM board_yrs_text_migrations t
            WHERE t.board_id = b.id
              AND t.status = 'completed'
        )
    ) THEN
        RAISE EXCEPTION 'cannot finalize Yrs storage: structural text conversion is incomplete';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM boards b
        JOIN board_yrs_shadow_bases c ON c.board_id = b.id
        JOIN board_yrs_heads h ON h.board_id = b.id
        WHERE NOT EXISTS (
            SELECT 1
            FROM board_yrs_snapshots s
            WHERE s.board_id = b.id
              AND s.server_client_id = c.server_client_id
              AND s.base_generation = h.base_generation
              AND s.schema_version = 2
              AND s.last_event_seq <= h.processed_seq
        )
    ) THEN
        RAISE EXCEPTION 'cannot finalize Yrs storage: a board lacks a schema-v2 canonical snapshot';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM boards b
        JOIN board_yrs_shadow_bases c ON c.board_id = b.id
        JOIN board_yrs_heads h ON h.board_id = b.id
        CROSS JOIN LATERAL (
            SELECT s.last_event_seq
            FROM board_yrs_snapshots s
            WHERE s.board_id = b.id
              AND s.server_client_id = c.server_client_id
              AND s.base_generation = h.base_generation
              AND s.schema_version = 2
              AND s.last_event_seq <= h.processed_seq
            ORDER BY s.last_event_seq DESC
            LIMIT 1
        ) latest
        JOIN board_yrs_updates u
          ON u.board_id = b.id
         AND u.seq > latest.last_event_seq
         AND u.seq <= h.processed_seq
        WHERE u.schema_version <> 2
           OR u.base_generation <> h.base_generation
    ) THEN
        RAISE EXCEPTION 'cannot finalize Yrs storage: a canonical journal tail is incompatible';
    END IF;
END
$$;

-- Historical fleet checkpoints are ordinary canonical checkpoints now.
UPDATE board_yrs_snapshots
SET source = 'canonical_compaction'
WHERE source = 'legacy_fleet_migration';

ALTER TABLE board_yrs_snapshots
    DROP CONSTRAINT board_yrs_snapshots_source_check;
ALTER TABLE board_yrs_snapshots
    ADD CONSTRAINT board_yrs_snapshots_source_check
    CHECK (source = 'canonical_compaction');

-- Schema-conversion updates remain valid journal entries, but no runtime path
-- needs to distinguish them from other server-produced updates.
ALTER TABLE board_yrs_updates
    DROP CONSTRAINT board_yrs_updates_source_check;
UPDATE board_yrs_updates
SET source = 'server'
WHERE source IN ('bridge', 'migration');

ALTER TABLE board_yrs_updates
    ADD CONSTRAINT board_yrs_updates_source_check
    CHECK (source IN ('server', 'client'));
ALTER TABLE board_yrs_updates RENAME COLUMN legacy_event_id TO event_id;
ALTER TABLE board_yrs_updates RENAME COLUMN legacy_payload_hash TO payload_hash;

ALTER TABLE board_yrs_heads
    DROP CONSTRAINT board_yrs_heads_state_check;
UPDATE board_yrs_heads
SET state = 'ready'
WHERE state = 'yrs_3a';
ALTER TABLE board_yrs_heads
    ADD CONSTRAINT board_yrs_heads_state_check
    CHECK (state IN ('activating', 'ready', 'quarantined'));
ALTER TABLE board_yrs_heads DROP COLUMN cohort_epoch;

ALTER TABLE board_yrs_shadow_bases RENAME TO board_yrs_canonical_bases;
ALTER TABLE board_yrs_canonical_bases
    RENAME CONSTRAINT board_yrs_shadow_bases_pkey TO board_yrs_canonical_bases_pkey;
ALTER TABLE board_yrs_canonical_bases
    RENAME CONSTRAINT board_yrs_shadow_bases_board_id_fkey TO board_yrs_canonical_bases_board_id_fkey;
ALTER TABLE board_yrs_canonical_bases
    RENAME CONSTRAINT board_yrs_shadow_bases_base_seq_check TO board_yrs_canonical_bases_base_seq_check;
ALTER TABLE board_yrs_canonical_bases
    RENAME CONSTRAINT board_yrs_shadow_bases_update_encoding_check TO board_yrs_canonical_bases_update_encoding_check;
ALTER TABLE board_yrs_canonical_bases
    RENAME CONSTRAINT board_yrs_shadow_bases_server_client_id_check TO board_yrs_canonical_bases_server_client_id_check;
ALTER TABLE board_yrs_canonical_bases
    RENAME CONSTRAINT board_yrs_shadow_bases_base_generation_check TO board_yrs_canonical_bases_base_generation_check;
ALTER TABLE board_yrs_canonical_bases DROP COLUMN legacy_crdt_meta;

DROP TABLE board_yrs_text_migrations;
DROP TABLE board_yrs_migration_inventory;
