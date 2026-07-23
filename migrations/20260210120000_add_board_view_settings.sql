ALTER TABLE boards
    ADD COLUMN grid_type TEXT NOT NULL DEFAULT 'lines',
    ADD COLUMN background_color TEXT NOT NULL DEFAULT '#f5f5f5';
