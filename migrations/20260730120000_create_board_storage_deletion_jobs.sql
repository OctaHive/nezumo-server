CREATE TABLE board_storage_deletion_jobs (
    board_id UUID PRIMARY KEY,
    object_prefix TEXT NOT NULL,
    preview_object_key TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cleanup_after TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '30 seconds'
);

CREATE INDEX idx_board_storage_deletion_jobs_pending
    ON board_storage_deletion_jobs (cleanup_after, updated_at);

CREATE OR REPLACE FUNCTION enqueue_board_storage_deletion()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO board_storage_deletion_jobs (board_id, object_prefix, preview_object_key)
    VALUES (OLD.id, 'boards/' || OLD.id::TEXT || '/', OLD.preview_object_key)
    ON CONFLICT (board_id) DO UPDATE
    SET object_prefix = EXCLUDED.object_prefix,
        preview_object_key = EXCLUDED.preview_object_key,
        attempts = 0,
        last_error = NULL,
        created_at = NOW(),
        updated_at = NOW(),
        cleanup_after = NOW() + INTERVAL '30 seconds';

    RETURN OLD;
END;
$$;

CREATE TRIGGER boards_enqueue_storage_deletion
AFTER DELETE ON boards
FOR EACH ROW
EXECUTE FUNCTION enqueue_board_storage_deletion();
