# Nezumo backend

The Nezumo backend is a Rust API for authentication, users, projects,
collaborative boards, realtime presence, file storage, and board exports. It is
built with Axum and Tokio and uses PostgreSQL, Redis, S3-compatible storage, and
SMTP as external services.

## Features

- JWT, cookie, API-key, OAuth/OIDC, and user-managed TOTP authentication flows;
- user registration, email verification, and password reset;
- projects, boards, membership, sharing, and access control;
- realtime board events and presence over WebSockets;
- canonical Yrs journal compaction into immutable binary checkpoints;
- image, audio, video, PDF, and generic file uploads;
- S3/MinIO-backed private object storage with presigned URLs;
- PNG, JPEG, SVG, and PDF board export;
- automatic board thumbnail generation;
- OpenAPI specification, Swagger UI, and dependency health checks;
- automatic cleanup of stale Redis sessions;
- opt-in, checkpoint-fenced pruning of old canonical updates and board events;
- opt-in, canonical-read-model-fenced deletion of orphaned board assets.

## Technology

| Area | Implementation |
| --- | --- |
| HTTP and WebSocket server | Axum, Tokio, Tower |
| Database | PostgreSQL, SQLx, migrations |
| Cache and presence | Redis, deadpool-redis |
| Object storage | AWS S3 SDK, MinIO-compatible API |
| Email | Lettre over SMTP |
| Authentication | JWT, Argon2, TOTP, OAuth 2.0/OpenID Connect |
| API documentation | Utoipa, Swagger UI |
| Board rendering | `nezumo-render`, wgpu, svg2pdf |

## Repository layout

```text
backend/
├── src/
│   ├── cache/          Redis pool and collaboration presence
│   ├── core/           Configuration and server assembly
│   ├── database/       SQLx queries and persistence helpers
│   ├── handlers/       HTTP and WebSocket request handlers
│   ├── jobs/           Background maintenance, previews, and exports
│   ├── mail/           SMTP transport and multipart messages
│   ├── middlewares/    Authentication and request middleware
│   ├── models/         API and database data structures
│   ├── routes/         Router and OpenAPI assembly
│   ├── state/          Board-state reconstruction
│   ├── storage/        S3-compatible object storage
│   └── main.rs         Process entry point and HTTP/TLS listener
├── migrations/         Versioned PostgreSQL schema migrations
├── .sqlx/              Offline SQLx query metadata
├── docker/             Dockerfile, Compose configs, and local service data
├── fonts/              Fonts used by SVG/PDF rendering
├── documentation/      Additional deployment and API notes
└── Cargo.toml
```

Detailed subsystem documentation:

- [Cache and realtime presence](src/cache/README.md)
- [Object storage](src/storage/README.md)
- [Email](src/mail/README.md)
- [Background jobs and rendering](src/jobs/README.md)
- [Authentication route builder](src/routes/README.md#authentication-route-builder)
- [Ubuntu installation notes](documentation/installation_ubuntu.md)

## Docker files

Docker configuration is kept in [`docker/`](docker/) so the repository root
contains only project-level files. The Compose files still use the repository
root as their build context, allowing the Dockerfile to copy the Rust sources,
SQLx metadata, migrations, fonts, and mail template into the image.

| File | Purpose |
| --- | --- |
| [`docker/Dockerfile`](docker/Dockerfile) | Multi-stage release build and minimal non-root runtime image |
| [`docker/Dockerfile.dockerignore`](docker/Dockerfile.dockerignore) | Build-context exclusions specific to the Dockerfile |
| [`docker/compose.yml`](docker/compose.yml) | Application with PostgreSQL, Redis, MinIO, and Mailpit |
| [`docker/compose.dev.yml`](docker/compose.dev.yml) | Development API container with live source mounting and `cargo watch` |
| [`docker/compose.loadbalanced.yml`](docker/compose.loadbalanced.yml) | Experimental two-instance layout behind HAProxy |

Persistent bind-mounted service data is created in subdirectories such as
`docker/db/`, `docker/cache/`, and `docker/storage/`. These runtime directories
are ignored by Git, while the configuration files in `docker/` are tracked.

Validate the files without starting containers:

```bash
docker compose -f docker/compose.yml config -q
docker compose -f docker/compose.dev.yml config -q
docker compose -f docker/compose.loadbalanced.yml config -q
docker build --check -f docker/Dockerfile .
```

The load-balanced Compose file is useful for exercising multiple API instances,
but it is not a complete production HA setup: its PostgreSQL and MinIO services
need a real replication and failover design before production use.

## Quick start with Docker

Requirements:

- Docker Engine;
- Docker Compose v2.

Create the local environment file and start the development stack:

```bash
cd backend
cp .env.example .env
docker compose -f docker/compose.dev.yml up
```

The development service installs its Rust tooling, runs migrations, and starts
the API through `cargo watch`. The Compose stack also starts PostgreSQL, Redis,
MinIO, and Mailpit.

Default local endpoints:

| Service | URL |
| --- | --- |
| API | `http://localhost:3000/api/v1` |
| Swagger UI | `http://localhost:3000/api/v1/docs` |
| OpenAPI JSON | `http://localhost:3000/api/v1/openapi.json` |
| Health check | `http://localhost:3000/api/v1/health` |
| Mailpit UI | `http://localhost:8025` |
| MinIO API | `http://localhost:9000` |
| MinIO console | `http://localhost:9001` |

Ports and credentials can be changed in `.env`. Do not use the example secrets
in a shared or production environment.

Stop the stack with:

```bash
docker compose -f docker/compose.dev.yml down
```

Add `--volumes` only when you intentionally want to remove local service data.

## Local development without the API container

Install:

- a Rust toolchain compatible with the lockfile;
- PostgreSQL;
- Redis;
- an S3-compatible service such as MinIO;
- an SMTP server or local Mailpit instance;
- `sqlx-cli` for manual migration and metadata workflows.

Start the dependency containers from `backend/`, then run the API on the host:

```bash
cp .env.example .env
docker compose -f docker/compose.yml up -d db cache storage mailpit
cargo run
```

Adjust `.env` endpoints from Docker service names to host-accessible addresses,
typically `localhost` or `127.0.0.1`. The server loads `.env` from its current
working directory and runs embedded SQLx migrations during startup.

The backend treats PostgreSQL, Redis, object storage, and SMTP as required
startup dependencies. It verifies each connection, and SMTP validation sends a
test message from `MAIL_FROM` to the same address.

### Mail template

Outbound application emails load `footer.html` from `MAIL_TEMPLATES_DIR`, which
defaults to `templates/mail`. When running directly from `backend/`, point it at
the source template unless your environment deploys templates separately:

```text
MAIL_TEMPLATES_DIR=src/mail
```

### Board renderer

The backend invokes a separately distributed `nezumo-render` executable. It is
a required runtime dependency for canonical Yrs update validation and is also
used for thumbnails and PNG, JPEG, and SVG rendering. PDF export uses its SVG
output before converting it with `svg2pdf`.

Renderer sources and build instructions are intentionally outside this backend
repository. Provision a renderer binary compatible with the backend's Yrs wire
protocol, place it on `PATH` or set its absolute path in
`PREVIEW_RENDERER_BIN`, and provide a working GPU or software Vulkan/GL
implementation. The server can start without the executable because renderer
workers are spawned lazily, but canonical board writes, thumbnails, and exports
will fail when first requested.

The supplied Docker image does not currently bundle `nezumo-render`; a
full-featured deployment must add it to the runtime image. Set `ASSET_BASE_URL`
so it can fetch board assets and `RENDER_FONTS_DIR` when fonts are not available
in the default `./fonts` directory. See
[the jobs documentation](src/jobs/README.md) for protocol, limits, and failure
behaviour.

## Configuration

Copy [.env.example](.env.example) and review every value before deployment. The
main configuration groups are:

| Group | Important variables |
| --- | --- |
| Server | `SERVER_IP`, `SERVER_PORT`, `SERVER_TRACE_ENABLED` |
| TLS | `SERVER_HTTPS_ENABLED`, `SERVER_HTTPS_HTTP2_ENABLED`, certificate and key paths |
| CORS and compression | `CORS_*`, `SERVER_COMPRESSION_*` |
| Authentication | `JWT_*`, OAuth/OIDC provider settings |
| PostgreSQL | `DATABASE_URL`, pool minimum and maximum |
| Redis | `CACHE_ENDPOINT`, `CACHE_PORT`, credentials, database number |
| Object storage | `STORAGE_*`, bucket names, public endpoint |
| SMTP | `MAIL_*`, `MAIL_TEMPLATES_DIR` |
| Support reports | `GITHUB_ISSUES_*`, support rate limits and attachment storage |
| Rendering and jobs | `YRS_COMPACTION_*`, `YRS_JOURNAL_*`, `PREVIEW_*`, `ASSET_BASE_URL`, `RENDER_FONTS_DIR` |

Production checklist:

- replace every sample password, access key, and JWT secret;
- restrict CORS to trusted frontend origins;
- use HTTPS either in the application or at a trusted reverse proxy;
- create required S3 buckets before accepting uploads;
- provide a browser-reachable `STORAGE_PUBLIC_ENDPOINT` when the internal S3
  endpoint is not externally resolvable;
- configure durable PostgreSQL, Redis, and object-storage volumes/backups;
- enable `YRS_JOURNAL_GC_ENABLED` only after canonical compaction is producing
  verified immutable checkpoints; retention remains disabled by default;
- ensure the mail footer and renderer assets exist inside the runtime image;
- keep certificate and private-key files outside version control;
- review resource limits for the renderer and background jobs.

## API and health checks

All application routes are mounted below `/api/v1`. Interactive documentation
is generated from the Rust route and schema definitions:

```text
GET /api/v1/docs
GET /api/v1/openapi.json
```

Use the health endpoint for container or load-balancer probes:

```bash
curl http://localhost:3000/api/v1/health
```

The health response includes checks for application dependencies and host
resources. A checked-in OpenAPI snapshot also exists at
[`documentation/openapi.json`](documentation/openapi.json), but the runtime
endpoint reflects the compiled server implementation.

Authenticated users can manage TOTP from the frontend security settings. The
server keeps a new setup pending until its first six-digit code is confirmed;
future password logins then require a short-lived `/login/totp` challenge. TOTP
is disabled by default, and disabling it requires a current authenticator code.

## Database migrations and SQLx

Migrations live in [`migrations/`](migrations/) and are applied automatically
when the server starts. To run them manually:

```bash
sqlx migrate run
```

The Docker release build sets `SQLX_OFFLINE=true` and compiles against metadata
in [`.sqlx/`](.sqlx/). After changing a compile-time checked query or the schema,
connect to a migrated development database and refresh the metadata:

```bash
cargo sqlx prepare
```

Commit the resulting `.sqlx` changes together with the query or migration.

## Quality checks

Run from `backend/`:

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
```

For an offline query check matching the container build:

```bash
SQLX_OFFLINE=true cargo check --locked
```

Some integration paths require live PostgreSQL, Redis, S3, SMTP, or renderer
services. Start the relevant Compose dependencies before exercising them.

## Continuous integration and releases

GitHub Actions runs formatting checks and tests for every pushed commit and
pull request. A SemVer tag such as `v0.1.0`, matching the version in
`Cargo.toml`, builds a versioned Linux release package and deploys it to a
configured production server over SSH.

See [GitHub Actions deployment](documentation/github_actions_deployment.md) for
the required GitHub secrets, one-time systemd setup, atomic deployment, and
rollback behaviour.

## Runtime model

Startup proceeds in dependency order: database connection and migrations,
object storage, Redis, SMTP validation, shared realtime state, background jobs,
and finally route construction. Failure of a required dependency aborts startup.

The process handles `Ctrl+C` and `SIGTERM` for graceful HTTP shutdown. Recurring
jobs are in-process Tokio tasks rather than durable queue workers. In a
multi-replica deployment every backend instance starts its own job loops; see
[the jobs documentation](src/jobs/README.md) before scaling horizontally.

Logs use `tracing`; configure verbosity with `RUST_LOG`. Avoid logging tokens,
SMTP credentials, signed storage URLs, verification codes, or uploaded content.

## License

Copyright © 2026 Roman Efremenko.

Nezumo backend is available under either:

- the [GNU Affero General Public License v3.0 only](LICENSE); or
- a separately negotiated [commercial license](COMMERCIAL-LICENSE.md).

The AGPL option permits commercial use when all of its conditions are met,
including the source-code obligations for modified versions offered over a
network. The commercial license is intended for proprietary use under
alternative terms. Contact [admin@nezumo.ru](mailto:admin@nezumo.ru).
