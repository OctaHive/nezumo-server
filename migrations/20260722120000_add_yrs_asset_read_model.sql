-- Freshness-fenced stable asset references projected from the
-- canonical Yrs document. Destructive GC may consume this model only when its
-- barrier exactly matches the durable canonical head.

CREATE TABLE IF NOT EXISTS board_yrs_asset_heads (
    board_id UUID PRIMARY KEY REFERENCES boards(id) ON DELETE CASCADE,
    last_event_seq BIGINT NOT NULL CHECK (last_event_seq >= 0),
    base_generation BIGINT NOT NULL CHECK (base_generation > 0),
    status TEXT NOT NULL CHECK (status IN ('ready', 'blocked')),
    blocker_code TEXT,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (status = 'ready' AND blocker_code IS NULL) OR
        (status = 'blocked' AND blocker_code IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS board_yrs_asset_refs (
    board_id UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    ref_kind TEXT NOT NULL CHECK (ref_kind IN ('object_key', 'pdf_doc')),
    stable_ref TEXT NOT NULL CHECK (stable_ref <> ''),
    last_event_seq BIGINT NOT NULL CHECK (last_event_seq >= 0),
    base_generation BIGINT NOT NULL CHECK (base_generation > 0),
    PRIMARY KEY (board_id, ref_kind, stable_ref)
);

CREATE INDEX IF NOT EXISTS idx_board_yrs_asset_heads_freshness
    ON board_yrs_asset_heads (status, projected_at);
