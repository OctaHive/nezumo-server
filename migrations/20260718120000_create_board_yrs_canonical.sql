-- Durable canonical Yrs journal and per-board processing watermark.
-- The event and its corresponding update are committed atomically.
--
-- `board_yrs_updates` is the DURABLE JOURNAL of canonical incremental updates: one
-- row per legacy commit that was bridged into the yrs Doc (the invariant "one
-- legacy commit → exactly one canonical update"). It is NOT the checkpoint base —
-- that stays in `board_yrs_shadow_bases`, which is only compaction/bootstrap base.
-- The event and its update row are written in one transaction; database
-- constraints — not the in-process mutex — are the correctness boundary.
--
-- `legacy_event_id` is the originating `board_events.id`, but WITHOUT a foreign key:
-- legacy events are GC'd after snapshots, while the canonical journal is compacted
-- on its own schedule and must survive event GC. Only `board_id` cascades.
CREATE TABLE board_yrs_updates (
    board_id            UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    seq                 BIGINT NOT NULL CHECK (seq >= 0),   -- == the legacy event seq (one pair)
    legacy_event_id     UUID NULL,                          -- board_events.id (no FK: events are GC'd)
    -- The originating client's idempotency id. For live commits this is the
    -- client-supplied `client_event_id`; for backfilled historical events (whose
    -- original client id was not retained) it is the reserved deterministic
    -- namespace `legacy-event:{legacy_event_id}` (live ids can never take that
    -- prefix). Deduplicates retries: one legacy command = one canonical update.
    client_event_id     TEXT NOT NULL,
    legacy_payload_hash  BYTEA NOT NULL,                    -- identity check on dedup race
    base_generation     BIGINT NOT NULL CHECK (base_generation > 0),
    schema_version      INT NOT NULL,
    yupdate             BYTEA NOT NULL,                     -- encode_state_as_update_v1 incremental diff
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (board_id, seq),                            -- => UNIQUE(board_id, seq)
    UNIQUE (board_id, client_event_id)                      -- => idempotency / dedup
);

CREATE INDEX idx_board_yrs_updates_board_seq ON board_yrs_updates(board_id, seq);

-- Durable per-board lifecycle and watermark for the canonical coordinator.
-- `processed_seq` means "every legacy event ≤ seq has been considered in order":
-- for a relevant board_commit exactly one verified update row exists (an allowed
-- empty update included), for an irrelevant event a no-op was explicitly confirmed.
-- It is advanced in the SAME transaction as the live pair / backfill row and is
-- never derived as MAX(update.seq).
--
-- `state`: legacy → activating → yrs_3a. `legacy`
-- boards behave exactly as before; only `activating`/`yrs_3a` route through the
-- coordinator. `writer_epoch` fences a superseded owner after failover; `cohort_epoch`
-- tracks cohort (re)assignment. A board with no row is implicitly `legacy`.
CREATE TABLE board_yrs_heads (
    board_id            UUID PRIMARY KEY REFERENCES boards(id) ON DELETE CASCADE,
    processed_seq       BIGINT NOT NULL DEFAULT 0 CHECK (processed_seq >= 0),
    base_generation     BIGINT NOT NULL CHECK (base_generation > 0),
    writer_epoch        BIGINT NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    cohort_epoch        BIGINT NOT NULL DEFAULT 0 CHECK (cohort_epoch >= 0),
    state               TEXT NOT NULL DEFAULT 'legacy'
                            CHECK (state IN ('legacy', 'activating', 'yrs_3a')),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
