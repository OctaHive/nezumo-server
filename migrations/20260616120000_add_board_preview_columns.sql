-- Server-generated board preview (thumbnail) tracking.
-- preview_object_key: S3 object key for the rendered preview image
--   (e.g. "boards/{id}/preview.png"); NULL until the first preview is generated.
-- preview_generated_at: when the preview was last (re)generated; used by the
--   snapshot job to throttle regeneration and as a cache hint.
ALTER TABLE boards ADD COLUMN preview_object_key TEXT;
ALTER TABLE boards ADD COLUMN preview_generated_at TIMESTAMPTZ;
