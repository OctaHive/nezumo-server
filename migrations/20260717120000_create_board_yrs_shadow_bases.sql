-- Durable Yrs base storage, isolated from the replaceable JSON snapshot table.
-- built in parallel with BoardCrdt by the snapshot job. Kept in its OWN table (not
-- board_snapshots, which has a delete+insert lifecycle) so legacy snapshot writers
-- can never accidentally erase the last good yrs base. All columns are shadow-only;
-- This migration is additive and does not change the active write path.
--
-- server_client_id is drawn from the reserved SERVER band [2^52, 2^53) (ADR-11);
-- future client replicas use [1, 2^52) so server and client can never mint
-- different structures under the same (client_id, clock).
CREATE TABLE board_yrs_shadow_bases (
    board_id            UUID PRIMARY KEY REFERENCES boards(id) ON DELETE CASCADE,
    state_update        BYTEA NOT NULL,          -- encode_state_as_update_v1(default)
    state_vector        BYTEA NOT NULL,          -- encode_state_vector (future diff / integrity)
    legacy_crdt_meta    JSONB NOT NULL,          -- BoardCrdt checkpoint EXACTLY at base_seq (replay oracle)
    base_seq            BIGINT NOT NULL CHECK (base_seq >= 0),  -- last event seq folded into the base
    protocol_version    INT NOT NULL,
    schema_version      INT NOT NULL,
    min_writer_version  INT NOT NULL,
    update_encoding     TEXT NOT NULL CHECK (update_encoding = 'v1'),
    server_client_id    BIGINT NOT NULL CHECK (
                            server_client_id >= 4503599627370496 AND   -- 2^52
                            server_client_id <  9007199254740992       -- 2^53
                        ),
    base_generation     BIGINT NOT NULL DEFAULT 1 CHECK (base_generation > 0),
    abandoned_at        TIMESTAMPTZ NULL,        -- non-NULL => generation abandoned; excluded from GC watermark
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
