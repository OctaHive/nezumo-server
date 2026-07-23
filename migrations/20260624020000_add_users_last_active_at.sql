-- Track the last time each user was active (last realtime heartbeat ping or board
-- action), surfaced in the admin users list. Nullable: users with no activity since
-- this migration read as "never active". Live "active now" is derived separately
-- from the Redis session cache, not from this column.
ALTER TABLE users ADD COLUMN last_active_at TIMESTAMPTZ;
