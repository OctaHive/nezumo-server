CREATE TABLE user_totp_enrollments (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    secret VARCHAR(64) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    attempts SMALLINT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX user_totp_enrollments_expires_at_idx
    ON user_totp_enrollments(expires_at);

ALTER TABLE login_challenges
    ADD COLUMN attempts SMALLINT NOT NULL DEFAULT 0 CHECK (attempts >= 0);
