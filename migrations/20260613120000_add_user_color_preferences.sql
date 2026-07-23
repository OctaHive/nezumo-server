-- Per-user color picker preferences (recent + custom colors), stored as a JSON
-- string in TEXT (sqlx is built without the `json` feature). NULL = never set,
-- which the client uses to decide whether to migrate its localStorage cache up.
ALTER TABLE users ADD COLUMN color_preferences TEXT;
