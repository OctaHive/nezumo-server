CREATE TABLE board_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    board_id UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (board_id, seq)
);

CREATE INDEX idx_board_events_board_seq ON board_events(board_id, seq);
CREATE INDEX idx_board_events_board_created_at ON board_events(board_id, created_at);
