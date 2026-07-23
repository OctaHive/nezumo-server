-- Per-project task-card status dictionary (preset defaults seeded on first
-- read, then user-extensible). Cards reference a status by id and also store a
-- denormalized label/color so the canvas renders without the dictionary.
CREATE TABLE IF NOT EXISTS project_statuses (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    label       TEXT NOT NULL,
    color       TEXT NOT NULL,
    position    INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_project_statuses_project
    ON project_statuses (project_id, position);
