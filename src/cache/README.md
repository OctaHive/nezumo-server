# Cache

This module provides the backend's Redis connection pool and cache helpers.
Redis currently stores ephemeral collaboration state such as active board
sessions, heartbeats, cursor positions, and anonymous display names. It is also
used directly by several handlers for rate limiting and other short-lived data.

The application creates one shared `deadpool_redis::Pool` during startup and
stores it in `AppState`. Functions in this module borrow that pool and acquire a
connection for each operation.

## Module layout

| File | Responsibility |
| --- | --- |
| [`connect.rs`](./connect.rs) | Builds the Redis URL, creates the connection pool, and verifies it with `PING`. |
| [`add.rs`](./add.rs) | Stores a non-empty string value under a non-empty key. |
| [`delete.rs`](./delete.rs) | Deletes a key and reports whether it existed. |
| [`sessions.rs`](./sessions.rs) | Manages board presence, heartbeats, cursors, anonymous names, and stale-session cleanup. |

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `CACHE_ENDPOINT` | yes | — | Redis hostname without a scheme, for example `cache` or `127.0.0.1`. |
| `CACHE_PORT` | no | `6379` | Redis port. |
| `CACHE_USERNAME` | no | empty | Redis ACL username. It is used only when a password is also set. |
| `CACHE_PASSWORD` | no | empty | Redis password. Password-only authentication is supported. |
| `CACHE_DB` | no | `0` | Logical Redis database number. |

The resulting connection URL has the form
`redis://[username:password@]host:port/db`. See
[`backend/.env.example`](../../.env.example) and the Docker Compose files for local configuration examples.

The backend treats Redis as a required startup dependency: failure to create a
pool connection or complete `PING` prevents the server from starting.

## Generic key-value usage

```rust
use crate::cache::{add::add_to_cache, delete::delete_from_cache};
use deadpool_redis::Pool;

async fn example(pool: &Pool) -> Result<(), String> {
    add_to_cache(pool, "example:key", "value").await?;
    let existed = delete_from_cache(pool, "example:key").await?;
    assert!(existed);
    Ok(())
}
```

`add_to_cache` uses Redis `SET` without an expiration. Prefer a direct Redis
command with an explicit TTL for temporary application data so abandoned keys
cannot accumulate indefinitely.

## Collaboration key schema

The helpers in `sessions.rs` use the following keys:

| Key | Redis type | TTL | Value |
| --- | --- | --- | --- |
| `board:{board_id}:sessions` | Set | 24 hours | Members formatted as `{user_id}:{session_id}`. |
| `board:{board_id}:heartbeat:{user_id}:{session_id}` | String | 60 seconds | The marker `1`. |
| `board:{board_id}:cursor:{user_id}` | String | 5 minutes | Coordinates formatted as `{x},{y}`. |
| `board:{board_id}:anon_name:{user_id}` | String | 24 hours | Anonymous user's display name. |

The session-set TTL is refreshed whenever a session is added. Heartbeats are
touched on connection and by realtime heartbeat messages. The cleanup job
scans `board:*:sessions`, removes members whose heartbeat expired, and also
removes their cursor and anonymous-name keys.

Typical presence flow:

1. Call `add_session` when a client connects.
2. Call `touch_session_heartbeat` on connection and every heartbeat tick.
3. Update transient presence data with `set_cursor_position` and
   `set_anon_display_name`.
4. On a clean disconnect, call `remove_session`, `remove_cursor`, and
   `remove_anon_display_name`.
5. Run `cleanup_stale_sessions` periodically to handle unclean disconnects.

`get_all_online_user_ids` unions session sets across boards and returns only
user IDs that parse as UUIDs, intentionally excluding anonymous/embed users.

## Operational notes

- Cache values are not a durable source of truth. Code must tolerate expiration
  and reconstruct collaboration state from live connections or persistent
  storage where appropriate.
- All helpers return string errors instead of retrying. Callers decide whether
  a Redis failure is fatal or whether an operation can degrade gracefully.
- `get_cursor_positions` performs one `GET` per user. Keep input lists bounded;
  use pipelining or a different Redis structure if this becomes a hot path for
  very large boards.
- Board discovery uses incremental `SCAN`, not blocking `KEYS`.
- Keep identifiers free of `:` where they are serialized into set members,
  because session values are parsed with the first colon as the delimiter.
- Do not store secrets or permanent business data in this cache database.
