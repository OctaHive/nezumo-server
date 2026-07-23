-- Distinguish client-authored canonical updates from server-produced updates
-- updates without duplicating the binary payload in board_events.
ALTER TABLE board_yrs_updates
    ADD COLUMN protocol_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN source TEXT NOT NULL DEFAULT 'bridge'
        CHECK (source IN ('bridge', 'client')),
    ADD COLUMN writer_client_id BIGINT NULL
        CHECK (writer_client_id IS NULL OR (writer_client_id > 0 AND writer_client_id < 9007199254740992)),
    ADD COLUMN update_hash BYTEA NULL;

UPDATE board_yrs_updates
SET update_hash = decode(md5(yupdate), 'hex')
WHERE update_hash IS NULL;

ALTER TABLE board_yrs_updates
    ALTER COLUMN update_hash SET NOT NULL;
