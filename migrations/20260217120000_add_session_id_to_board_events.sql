ALTER TABLE board_events ADD COLUMN session_id TEXT;
CREATE INDEX idx_board_events_user_session ON board_events(board_id, user_id, session_id);
