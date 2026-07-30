ALTER TABLE usage
    DROP CONSTRAINT usage_user_id_fkey,
    ADD CONSTRAINT usage_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;

ALTER TABLE apikeys
    DROP CONSTRAINT apikeys_user_id_fkey,
    ADD CONSTRAINT apikeys_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
