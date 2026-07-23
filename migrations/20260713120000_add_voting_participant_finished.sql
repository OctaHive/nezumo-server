-- Let a participant finish voting early (submit without spending every vote).
-- `done` in the organizer view = joined AND (finished OR used all votes).
ALTER TABLE board_voting_participants
    ADD COLUMN finished BOOLEAN NOT NULL DEFAULT FALSE;
