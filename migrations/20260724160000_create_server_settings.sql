-- Runtime administrator settings. Only non-secret operational/product values
-- are stored here; credentials and bootstrap connectivity remain in env vars.
CREATE TABLE IF NOT EXISTS server_settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_by UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS server_settings_audit (
    id BIGSERIAL PRIMARY KEY,
    key TEXT NOT NULL,
    old_value JSONB,
    new_value JSONB NOT NULL,
    changed_by UUID REFERENCES users(id) ON DELETE SET NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_server_settings_audit_changed_at
    ON server_settings_audit(changed_at DESC);
