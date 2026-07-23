-- board_events.user_id was `NOT NULL REFERENCES users(id) ON DELETE RESTRICT`,
-- but the app supports anonymous authorship: a guest who opens a board via an
-- edit link (link_access = 'editor') gets an editor role with a generated
-- anonymous UUID that has NO row in `users`. Every such commit therefore failed
-- the FK (`board_events_user_id_fkey`), so anonymous link-editors could not
-- write at all (surfaced at scale by the load test on board 2a3eabbd…).
--
-- Drop the FK so anonymous (and deleted-user) ids can be stored as-is. The
-- column stays NOT NULL — the anonymous UUID is kept for per-user attribution
-- (cursors / authorship). No code reads board_events via a join to users, so
-- this has no query ripple. We also lose ON DELETE RESTRICT, which means a user
-- can now be deleted without their event history blocking it (acceptable: the
-- events keep a dangling id and are GC'd by the snapshot job).
ALTER TABLE board_events
    DROP CONSTRAINT IF EXISTS board_events_user_id_fkey;
