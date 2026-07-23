-- Per-user, per-board camera (pan + zoom), so each user's view is restored on reopen.
CREATE TABLE IF NOT EXISTS board_view_state (
    user_id    UUID NOT NULL,
    board_id   UUID NOT NULL,
    x          DOUBLE PRECISION NOT NULL,
    y          DOUBLE PRECISION NOT NULL,
    zoom       DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, board_id)
);
