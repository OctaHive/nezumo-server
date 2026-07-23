-- Turn API request tiers into complete resource plans. Limits are deliberately
-- stored in PostgreSQL so every server instance enforces the same policy and
-- administrative clients can display the authoritative values.
ALTER TABLE tiers
    ADD COLUMN IF NOT EXISTS max_owned_boards INTEGER,
    ADD COLUMN IF NOT EXISTS max_upload_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS max_storage_bytes BIGINT;

UPDATE tiers
SET
    max_owned_boards = CASE level
        WHEN 1 THEN 10
        WHEN 2 THEN 100
        WHEN 3 THEN 1000
        ELSE 10
    END,
    max_upload_bytes = CASE level
        WHEN 1 THEN 20 * 1024 * 1024
        WHEN 2 THEN 50 * 1024 * 1024
        WHEN 3 THEN 200 * 1024 * 1024
        ELSE 20 * 1024 * 1024
    END,
    max_storage_bytes = CASE level
        WHEN 1 THEN 1::BIGINT * 1024 * 1024 * 1024
        WHEN 2 THEN 10::BIGINT * 1024 * 1024 * 1024
        WHEN 3 THEN 100::BIGINT * 1024 * 1024 * 1024
        ELSE 1::BIGINT * 1024 * 1024 * 1024
    END
WHERE max_owned_boards IS NULL
   OR max_upload_bytes IS NULL
   OR max_storage_bytes IS NULL;

ALTER TABLE tiers
    ALTER COLUMN max_owned_boards SET NOT NULL,
    ALTER COLUMN max_upload_bytes SET NOT NULL,
    ALTER COLUMN max_storage_bytes SET NOT NULL;

ALTER TABLE tiers
    ADD CONSTRAINT tiers_max_owned_boards_positive CHECK (max_owned_boards > 0),
    ADD CONSTRAINT tiers_max_upload_bytes_positive CHECK (max_upload_bytes > 0),
    ADD CONSTRAINT tiers_max_storage_bytes_positive CHECK (max_storage_bytes > 0),
    ADD CONSTRAINT tiers_upload_fits_storage CHECK (max_upload_bytes <= max_storage_bytes);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tiers_level_unique ON tiers(level);

-- Imported archives can contain storage objects that are not represented by a
-- board_files row (PDF pages and previews). Keep their byte total on the board
-- so deleting the board releases that quota automatically.
ALTER TABLE boards
    ADD COLUMN IF NOT EXISTS imported_storage_bytes BIGINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT boards_imported_storage_bytes_nonnegative
        CHECK (imported_storage_bytes >= 0);
