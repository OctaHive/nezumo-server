# Database

This directory is the Nezumo PostgreSQL persistence layer. It owns connection
pool creation, schema migrations, and domain-specific SQL used by handlers and
background jobs.

Database modules accept a shared `sqlx::PgPool` for independent operations or a
`Transaction<Postgres>` where several writes must commit atomically. HTTP
authorization, request validation, and response shaping remain the
responsibility of handlers and models.

## Module layout

| Module | Responsibility |
| --- | --- |
| [`connect.rs`](./connect.rs) | PostgreSQL pool configuration, connection retries, and migrations. |
| [`users.rs`](./users.rs) | Users, account activation/deactivation, registration identity checks, credentials, profiles, TOTP, and password reset. |
| [`oauth_accounts.rs`](./oauth_accounts.rs) | External OAuth/OpenID identity links. |
| [`login_challenges.rs`](./login_challenges.rs) | Short-lived multi-step login challenges. |
| [`totp_enrollments.rs`](./totp_enrollments.rs) | Expiring setup state and atomic activation/deactivation for user-managed TOTP. |
| [`apikeys.rs`](./apikeys.rs) | Hashed API keys and their public metadata. |
| [`usage.rs`](./usage.rs) | Batched API usage records. |
| [`projects.rs`](./projects.rs) | Project CRUD, ownership/membership-aware listing, and favorites. |
| [`project_members.rs`](./project_members.rs) | Project membership, roles, and UI-facing member listing. |
| [`project_statuses.rs`](./project_statuses.rs) | Project-scoped task statuses and default seeding. |
| [`project_tags.rs`](./project_tags.rs) | Project-scoped tag dictionaries. |
| [`boards.rs`](./boards.rs) | Board CRUD, access-aware listings, configuration, and event sequence allocation. |
| [`board_members.rs`](./board_members.rs) | Explicit board membership and roles. |
| [`board_invite_links.rs`](./board_invite_links.rs) | Expiring role-bearing invitation links. |
| [`board_embed_tokens.rs`](./board_embed_tokens.rs) | View-only tokens for third-party embeds. |
| [`board_view.rs`](./board_view.rs) | Per-user board camera position and zoom. |
| [`board_files.rs`](./board_files.rs) | Metadata for board objects stored in S3-compatible storage. |
| [`events.rs`](./events.rs) | Ordered board events consumed by catch-up, diagnostics, and canonical retention. |
| [`snapshots.rs`](./snapshots.rs) | Legacy JSON snapshots retained for rollback and import compatibility. |
| [`voting.rs`](./voting.rs) | Server-authoritative board voting sessions and ballots. |

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DATABASE_URL` | yes | — | PostgreSQL connection URL beginning with `postgres://`. |
| `DATABASE_MAX_CONNECTIONS` | yes | `10` on parse failure | Maximum pool size. |
| `DATABASE_MIN_CONNECTIONS` | yes | `2` on parse failure | Connections kept available by the pool. |
| `DATABASE_CONNECT_RETRIES` | no | `10` | Maximum startup connection attempts. |
| `DATABASE_CONNECT_RETRY_DELAY_MS` | no | `1000` | Delay between attempts in milliseconds. |
| `DATABASE_ACQUIRE_TIMEOUT_SECS` | no | `15` | Maximum wait for a pooled connection. |
| `ENVIRONMENT` | no | `development` | Controls whether the backend runs migrations automatically. |

Although the min/max pool settings have parse fallbacks, they are read with the
required environment helper and therefore must exist. The pool uses a
five-minute idle timeout and tests connections before handing them to callers.

The connection URL is validated and logged in redacted form. Never log the raw
`DATABASE_URL` or interpolate it into user-visible errors.

See [`.env.example`](../../.env.example) for a complete configuration
template.

## Connection and startup

The backend creates one `PgPool` during server assembly and stores it in the
shared `AppState`. Connection failures are retried according to the settings
above. Exhausting all attempts prevents the API from starting.

```rust
use crate::database::connect::{connect_to_database, run_database_migrations};

let pool = connect_to_database().await?;
run_database_migrations(&pool).await?;
```

Most application code should receive `&PgPool` from `AppState` rather than
creating another pool.

## Migrations

Schema migrations live in [`migrations/`](../../migrations/) and are
ordered by their timestamp prefix. SQLx records applied versions in
`_sqlx_migrations`.

The startup behaviour depends on `ENVIRONMENT`:

- in development and staging, `run_database_migrations` loads `./migrations`
  and applies pending migrations;
- when `ENVIRONMENT=production`, the backend skips migrations entirely.

Production deployments must therefore run migrations as an explicit release
step before starting the new application version:

```bash
cd backend
sqlx migrate run
```

The process working directory matters because the migrator resolves
`./migrations` at runtime. If the directory is absent outside production, the
current implementation creates it; an empty directory then results in no schema
changes. Deploy migrations alongside the binary when automatic migrations are
expected.

### Adding a migration

With `sqlx-cli` installed:

```bash
cd backend
sqlx migrate add describe_the_change
```

Then:

1. write forward-only SQL that preserves existing data;
2. test it against a realistic development database;
3. update affected Rust models and queries;
4. refresh offline SQLx metadata;
5. verify both an empty-database install and an upgrade from the previous
   schema;
6. commit the migration, code, and `.sqlx` changes together.

Never edit a migration that may already have run in another environment. Add a
new migration to correct or extend it.

## SQLx query modes

The database layer uses both SQLx styles:

- `query!`, `query_as!`, and `query_scalar!` perform compile-time validation and
  depend on either a live schema or cached metadata in [`.sqlx`](../../.sqlx/);
- runtime `query`/`query_as` calls are used where offline metadata would be
  inconvenient or dynamic row mapping is required.

The release container builds with `SQLX_OFFLINE=true`. After changing a
compile-time checked query or its schema, connect to a migrated development
database and regenerate metadata:

```bash
cd backend
cargo sqlx prepare
```

Verify the same mode used by the image build:

```bash
SQLX_OFFLINE=true cargo check --locked
```

Do not replace compile-time checked queries merely to bypass stale metadata;
refresh the metadata or deliberately document why runtime mapping is preferable.

## Events and snapshots

Canonical collaborative state is persisted as an ordered event/Yrs journal.
`boards.rs::reserve_next_event_seq_tx` reserves the sequence in the same
transaction that persists the event and Yrs update, so the pair is atomic.

`yrs_canonical_bases.rs`, `yrs_heads.rs`, `yrs_updates.rs`, and
`yrs_snapshots.rs` own the canonical lifecycle and immutable binary
checkpoints. `snapshots.rs` remains only for rollback/import compatibility with
legacy JSON snapshots. Background compaction and optional journal retention
are documented in [the jobs documentation](../jobs/README.md).

Changes in these modules are concurrency-sensitive. Preserve sequence ordering,
transaction boundaries, reconnect behaviour, and compatibility with older
snapshot JSON. See [the jobs documentation](../jobs/README.md) for the
compaction lifecycle.

## Transactions and concurrency

Use a transaction when partial success would violate an invariant, for example
when several rows jointly represent one domain operation:

```rust
let mut tx = pool.begin().await?;

// Pass &mut tx to database helpers that participate in the operation.

tx.commit().await?;
```

Guidelines:

- keep transactions short and never hold them across network calls to Redis,
  S3, SMTP, OAuth providers, or the renderer;
- acquire locks in a consistent order;
- use unique constraints and `ON CONFLICT` for idempotent upserts;
- inspect affected-row counts when absence and deletion races matter;
- avoid unbounded result sets and N+1 queries on request hot paths;
- treat a pool timeout or serialization failure as an operational error, not as
  proof that a record does not exist.

## Storage boundaries

PostgreSQL stores metadata and stable object keys for uploaded board files. File
bytes are owned by S3-compatible storage, while Redis owns ephemeral presence.
Database rows and external objects cannot share one atomic transaction.

Flows that touch both systems must define their compensation and retry
behaviour. For example, orphan cleanup removes an S3 object before deleting its
`board_files` row; if storage deletion fails, the row remains so a later cycle
can retry. See [storage](../storage/README.md) and [cache](../cache/README.md).

## Adding or changing a query

1. Put the query in the module that owns the table or domain invariant.
2. Accept `&PgPool` for independent operations or an explicit transaction for
   caller-controlled atomic work.
3. Bind every value; never build SQL by concatenating user input.
4. Return domain models or a small purpose-specific row type.
5. Document public functions, unusual SQL, lock ordering, and expected absence.
6. Add indexes for new filtering, joining, or ordering patterns when needed.
7. Confirm authorization in the handler; a database helper name does not enforce
   caller permissions by itself.
8. Refresh `.sqlx` metadata for checked macros.
9. Test success, not-found, conflict, and rollback paths as applicable.

Avoid returning credential fields through broad `SELECT *` queries. Be
especially careful with password hashes, TOTP secrets, OAuth identifiers, API
key hashes, invite/embed tokens, and login/reset/verification codes.

## Verification

Run from `backend/` with a migrated development database where required:

```bash
cargo fmt --check
SQLX_OFFLINE=true cargo check --locked
cargo test
```

Useful migration commands:

```bash
sqlx migrate info
sqlx migrate run
```

The root [backend README](../../README.md) describes how to start PostgreSQL and
the other dependencies with Docker Compose.
