CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    totp_secret VARCHAR(255),
    role_level INT NOT NULL DEFAULT 1,
    tier_level INT NOT NULL DEFAULT 1,
    creation_date DATE NOT NULL DEFAULT CURRENT_DATE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'active', -- 'active', 'disabled'
    CONSTRAINT unique_username UNIQUE (username)
);

CREATE TABLE pending_registrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    verification_code TEXT NOT NULL,
    verification_expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    verified_at TIMESTAMP WITH TIME ZONE,
    completion_token TEXT,
    completion_expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Insert the example 'user' into the users table with a conflict check for username
INSERT INTO users (username, email, password_hash, role_level, status)
VALUES
    ('user', 'user@test.com', '$argon2i$v=19$m=16,t=2,p=1$ZE1qUWd0U21vUUlIM0ltaQ$dowBmjU4oHtoPd355dXypQ', 1, 'active')
ON CONFLICT (username) DO NOTHING;

-- Insert the example 'admin' into the users table with a conflict check for username
INSERT INTO users (username, email, password_hash, role_level, status)
VALUES
    ('admin', 'admin@test.com', '$argon2i$v=19$m=16,t=2,p=1$ZE1qUWd0U21vUUlIM0ltaQ$dowBmjU4oHtoPd355dXypQ', 2, 'active')
ON CONFLICT (username) DO NOTHING;
