-- Per-board embed tokens: an unguessable secret that grants anonymous VIEW-ONLY
-- access to a board when supplied (header `X-Embed-Token` on REST, `embed_token`
-- query param on the realtime socket). Lets owners embed even PRIVATE boards in
-- a third-party <iframe> where the session cookie can't be sent (SameSite).
-- Revocable (delete the row) and optionally expiring.
CREATE TABLE IF NOT EXISTS board_embed_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    board_id    UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    token       TEXT NOT NULL UNIQUE,
    created_by  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_board_embed_tokens_board ON board_embed_tokens (board_id);
