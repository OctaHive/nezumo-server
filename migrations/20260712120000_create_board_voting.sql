-- Miro-style board voting: server-authoritative + persisted.
-- A session names a set of candidate objects, a per-participant vote budget, a
-- deadline, and an anonymity flag. Participants opt in (join/decline) and cast
-- votes on candidates. Ids are stored as SYNCED id strings (wire ids) so they
-- reconcile across web + desktop and survive reload.

CREATE TABLE IF NOT EXISTS board_voting_sessions (
    id                     UUID PRIMARY KEY,
    board_id               UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
    created_by             UUID NOT NULL,               -- no FK: anon/admin ids may not be in users
    candidate_ids          TEXT NOT NULL DEFAULT '[]',  -- JSON array of synced id strings
    votes_per_participant  INT  NOT NULL DEFAULT 1,
    anonymous              BOOLEAN NOT NULL DEFAULT TRUE,
    status                 TEXT NOT NULL DEFAULT 'active', -- 'active' | 'ended'
    ends_at                TIMESTAMPTZ NOT NULL,
    rev                    BIGINT NOT NULL DEFAULT 0,      -- monotonic; LWW on the wire
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- At most one active session per board.
CREATE UNIQUE INDEX IF NOT EXISTS board_voting_sessions_one_active
    ON board_voting_sessions (board_id) WHERE status = 'active';

CREATE INDEX IF NOT EXISTS board_voting_sessions_board_idx
    ON board_voting_sessions (board_id, created_at DESC);

CREATE TABLE IF NOT EXISTS board_voting_participants (
    id          UUID PRIMARY KEY,
    session_id  UUID NOT NULL REFERENCES board_voting_sessions(id) ON DELETE CASCADE,
    user_id     UUID NOT NULL,
    status      TEXT NOT NULL,                 -- 'joined' | 'declined'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, user_id)
);

CREATE TABLE IF NOT EXISTS board_votes (
    id            UUID PRIMARY KEY,
    session_id    UUID NOT NULL REFERENCES board_voting_sessions(id) ON DELETE CASCADE,
    voter_id      UUID NOT NULL,
    candidate_id  TEXT NOT NULL,               -- synced id string of the candidate object
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS board_votes_session_idx ON board_votes (session_id);
CREATE INDEX IF NOT EXISTS board_votes_session_voter_idx ON board_votes (session_id, voter_id);
