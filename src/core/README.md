# Core

The `core` module contains the backend's process-level configuration helpers and
Axum application assembly. It connects required infrastructure, builds shared
application state, starts recurring jobs, and installs global middleware before
returning the router to `main.rs`.

Socket binding, graceful shutdown, and HTTP/TLS listener configuration remain
in [`../main.rs`](../main.rs). Route definitions and the `/api/v1` prefix are
owned by [`../routes/`](../routes/).

## Module layout

| File | Responsibility |
| --- | --- |
| [`config.rs`](./config.rs) | Reads required, optional, boolean, and numeric environment variables. |
| [`server.rs`](./server.rs) | Connects dependencies, creates `AppState`, starts jobs, builds routes, and applies middleware. |

## Configuration helpers

`config.rs` deliberately provides a small API rather than a typed application
configuration object:

| Function | Behaviour |
| --- | --- |
| `get_env(key)` | Returns the variable as a string and panics when it is absent. An existing empty value is returned unchanged. |
| `get_env_with_default(key, default)` | Uses the default only when the variable is absent. An empty value does not trigger the default. |
| `get_env_bool(key, default)` | Returns `true` only when the value equals `true`, ignoring ASCII case. Every other present value becomes `false`. |
| `get_env_u16(key, default)` | Parses an unsigned 16-bit integer and falls back for missing or invalid values. |
| `get_env_u64(key, default)` | Parses an unsigned 64-bit integer and falls back for missing or invalid values. |

Example:

```rust
use crate::core::config::{
    get_env,
    get_env_bool,
    get_env_u64,
    get_env_with_default,
};

let database_url = get_env("DATABASE_URL");
let environment = get_env_with_default("ENVIRONMENT", "development");
let tracing_enabled = get_env_bool("SERVER_TRACE_ENABLED", true);
let compaction_interval = get_env_u64("YRS_COMPACTION_INTERVAL_SECS", 300);
```

Use `get_env` for values without which the process cannot operate, such as
credentials or a database URL. Use a typed helper when malformed input can
safely fall back. Do not use `get_env_with_default` when an empty environment
value should also count as missing; validate the returned string explicitly.

Environment variables are loaded from `.env` near the start of `main`, before
any core function is called. Process-level variables take precedence according
to `dotenvy` semantics.

## Application assembly

`create_server` returns a fully configured `Router<()>`. Startup happens in the
following order:

1. connect to PostgreSQL;
2. run embedded SQLx migrations;
3. connect to S3-compatible object storage;
4. connect to Redis and verify it with `PING`;
5. create the SMTP transport and send its validation email;
6. create the in-memory realtime hub;
7. assemble the shared `AppState` inside an `Arc`;
8. start canonical Yrs compaction and stale-session cleanup jobs;
9. start the batched usage-record flusher;
10. build application routes;
11. apply tracing, Brotli compression, and CORS middleware.

PostgreSQL, storage, Redis, and SMTP are required dependencies. A connection,
migration, or SMTP validation failure causes `create_server` to panic, which
prevents the listener from starting with a partially usable application.

The shared state contains:

```text
AppState
├── database: sqlx::PgPool
├── storage: StorageState
├── cache: deadpool_redis::Pool
├── mail: MailerState
├── realtime: RealtimeHub
├── coordinators: CoordinatorRegistry
└── yrs_fanout: CanonicalFanout
```

See the subsystem documentation for connection-specific configuration:

- [Cache](../cache/README.md)
- [Storage](../storage/README.md)
- [Mail](../mail/README.md)
- [Background jobs](../jobs/README.md)

## Background tasks

The core starts four in-process task groups:

- canonical Yrs compaction at `YRS_COMPACTION_INTERVAL_SECS`, defaulting to 300 seconds;
- optional canonical journal and covered-event retention when
  `YRS_JOURNAL_GC_ENABLED=true`;
- stale Redis session cleanup every 30 seconds;
- usage-record batch persistence, currently flushed every 60 seconds.

These tasks share the same pools and state as request handlers. They are not
durable queued jobs and are started independently by every backend replica.
Review [the jobs README](../jobs/README.md) before changing intervals or scaling
the API horizontally.

## Middleware

### Request tracing

When `SERVER_TRACE_ENABLED` is `true` (the default), `create_server` adds an HTTP
trace layer around the application. Logging output is emitted through
`tracing`; `RUST_LOG` controls filtering at runtime.

The route builder also owns its own baseline `TraceLayer`. Keep that in mind
when changing tracing here to avoid duplicate request spans.

### Brotli compression

When `SERVER_COMPRESSION_ENABLED` is `true` (the default), responses are wrapped
in a Brotli compression layer. `SERVER_COMPRESSION_LEVEL` is read as a required
string and parsed as an integer; malformed values fall back to level 6.

The configured quality is passed directly to Tower HTTP. Keep it within the
documented Brotli range of 0–11. Higher values reduce response size at the cost
of substantially more CPU time.

### CORS

The following variables configure the global CORS layer:

| Variable | Meaning |
| --- | --- |
| `CORS_ALLOW_ORIGIN` | Comma-separated origins or `*`. |
| `CORS_ALLOW_METHODS` | Comma-separated HTTP methods. Invalid entries are ignored. |
| `CORS_ALLOW_HEADERS` | Comma-separated request header names. Invalid names abort startup. |
| `CORS_MAX_AGE` | Browser preflight cache lifetime in seconds; malformed values fall back to 3600. |
| `CORS_ALLOW_CREDENTIALS` | Enables credentialed cross-origin requests for an explicit origin list. |

`CORS_ALLOW_ORIGIN`, `CORS_ALLOW_METHODS`, `CORS_ALLOW_HEADERS`, and
`CORS_MAX_AGE` are retrieved with `get_env`, so they must exist even where the
subsequent parser has a fallback.

Wildcard origins cannot be combined with credentialed CORS. When
`CORS_ALLOW_ORIGIN` contains `*` and `CORS_ALLOW_CREDENTIALS=true`, the server
logs a warning and leaves credential support disabled. In production, prefer an
explicit allowlist of complete origins including scheme and port.

Middleware is applied after route construction. Axum layers wrap previously
installed services, so changing their order can affect which responses are
traced, compressed, or receive CORS headers.

## Listener and TLS boundary

`create_server` builds an application but does not open a socket. `main.rs` is
responsible for:

- reading `SERVER_IP` and `SERVER_PORT`;
- selecting HTTP or HTTPS with `SERVER_HTTPS_ENABLED`;
- loading certificate and private-key files;
- configuring HTTP/2 ALPN;
- binding the listener;
- handling `Ctrl+C` and `SIGTERM`.

This separation allows the router to be constructed independently of the
transport, which is useful for tests. TLS termination may also happen at a
reverse proxy; in that setup leave application HTTPS disabled and secure the
proxy-to-backend network appropriately.

## Adding core configuration

When introducing a new variable:

1. decide whether absence must abort startup or has a safe default;
2. use the narrowest existing helper, or add parsing and validation near the
   component that owns the setting;
3. document it in [`.env.example`](../../.env.example);
4. pass it through each relevant Docker Compose service;
5. update the owning subsystem README;
6. avoid printing credentials, tokens, private URLs, or other secrets.

Keep feature-specific configuration out of `server.rs` when it can be read by
the subsystem that owns the behaviour. The core should remain focused on
process assembly and truly global middleware.

## Verification

From `backend/`:

```bash
cargo fmt --check
SQLX_OFFLINE=true cargo check --locked
cargo test
```

Full startup verification requires PostgreSQL, Redis, S3-compatible storage,
and SMTP. The root [backend README](../../README.md) describes Docker-based and
host-based development workflows.
