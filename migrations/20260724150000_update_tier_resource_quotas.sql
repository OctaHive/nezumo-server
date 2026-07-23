-- Apply the product resource plans and represent an unlimited board count as
-- NULL rather than an arbitrary large number.
ALTER TABLE tiers
    DROP CONSTRAINT IF EXISTS tiers_max_owned_boards_positive,
    ALTER COLUMN max_owned_boards DROP NOT NULL;

UPDATE tiers
SET
    name = CASE level
        WHEN 3 THEN 'High'
        ELSE name
    END,
    max_owned_boards = CASE level
        WHEN 1 THEN 3
        WHEN 2 THEN 100
        WHEN 3 THEN NULL
        ELSE max_owned_boards
    END,
    max_upload_bytes = CASE level
        WHEN 1 THEN 10::BIGINT * 1024 * 1024
        WHEN 2 THEN 100::BIGINT * 1024 * 1024
        WHEN 3 THEN 500::BIGINT * 1024 * 1024
        ELSE max_upload_bytes
    END,
    max_storage_bytes = CASE level
        WHEN 1 THEN 250::BIGINT * 1024 * 1024
        WHEN 2 THEN 10::BIGINT * 1024 * 1024 * 1024
        WHEN 3 THEN 100::BIGINT * 1024 * 1024 * 1024
        ELSE max_storage_bytes
    END
WHERE level IN (1, 2, 3);

ALTER TABLE tiers
    ADD CONSTRAINT tiers_max_owned_boards_positive
        CHECK (max_owned_boards IS NULL OR max_owned_boards > 0);
