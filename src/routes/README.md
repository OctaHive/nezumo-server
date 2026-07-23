# Routes

This directory assembles Nezumo's Axum routes. Route modules map URL paths and
HTTP methods to handlers, select the authentication policy, apply route-specific
middleware and body limits, and are merged into the versioned API router.

Business logic belongs in [`../handlers/`](../handlers/); persistence belongs in
[`../database/`](../database/). Route modules should remain small and focused on
transport policy.

## URL structure

`routes/mod.rs` mounts the complete API under:

```text
/api/v1
```

Examples:

```text
GET  /api/v1/health
POST /api/v1/login
GET  /api/v1/me
GET  /api/v1/users/all
POST /api/v1/users/{id}/activate
POST /api/v1/users/{id}/deactivate
GET  /api/v1/projects
GET  /api/v1/projects/{id}/members
GET  /api/v1/boards/{id}
GET  /api/v1/docs
GET  /api/v1/openapi.json
```

Paths inside domain modules are relative to their parent nesting. For example,
`routes/projects.rs` registers `/{id}/members`, while `routes/mod.rs` nests the
result at `/projects`, producing `/api/v1/projects/{id}/members`.

The user-status actions are administrator-only. Deactivation updates both the
canonical `status` field and the legacy `disabled` flag; disabled users are
rejected by password, JWT, and OAuth authentication. An administrator cannot
deactivate their own account through these endpoints.

## Module layout

| Module | Responsibility |
| --- | --- |
| [`mod.rs`](./mod.rs) | Shared `AppState`, OpenAPI assembly, domain-router composition, `/api/v1` prefix, tracing, and fallback. |
| [`auth.rs`](./auth.rs) | Login, TOTP, logout, OAuth callbacks, and the current-user endpoint. |
| [`user.rs`](./user.rs) | Registration, reset, profiles, search, preferences, passwords, and account administration. |
| [`apikey.rs`](./apikey.rs) | API-key creation, listing, rotation, and revocation. |
| [`projects.rs`](./projects.rs) | Project CRUD, members, project boards, statuses, and tags. |
| [`boards.rs`](./boards.rs) | Boards, access, members, invites, embeds, uploads, views, voting, import, and export. |
| [`events.rs`](./events.rs) | Board commits, events, and snapshots. |
| [`realtime.rs`](./realtime.rs) | WebSocket collaboration and session presence. |
| [`support.rs`](./support.rs) | Support reports, feature requests, and signed attachment access. |
| [`link.rs`](./link.rs) | Server-side favicon resolution for board links. |
| [`usage.rs`](./usage.rs) | Usage summary endpoints. |
| [`health.rs`](./health.rs) | Public dependency and process health check. |
| [`homepage.rs`](./homepage.rs) | Public HTML landing page for the API root. |
| [`referencedata.rs`](./referencedata.rs) | Public country and language dictionaries. |

## Shared application state

Every stateful handler receives `Arc<AppState>`. The state is created once in
`core/server.rs` and contains:

```rust
pub struct AppState {
    pub database: sqlx::PgPool,
    pub storage: StorageState,
    pub cache: deadpool_redis::Pool,
    pub mail: MailerState,
    pub realtime: RealtimeHub,
}
```

Route builders clone the `Arc`, not the underlying pools or services. Do not
create new database, Redis, storage, or SMTP clients inside route modules.

## Authentication route builder

Most routes use
[`AuthenticatedRouteBuilder`](../wrappers/authentication_route_builder.rs),
which centralizes Axum routing and authentication middleware.

Import it with:

```rust
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;
```

### Authenticated methods

These methods require a valid identity and accept allowed numeric role levels:

```rust
.get(path, handler, allowed_roles)
.post(path, handler, allowed_roles)
.patch(path, handler, allowed_roles)
.delete(path, handler, allowed_roles)
```

Current role levels used by the routes are:

| Level | Meaning |
| --- | --- |
| `1` | Regular user |
| `2` | Administrator |

For example:

```rust
use axum::Router;
use std::sync::Arc;

use crate::handlers::{login::login, protected::protected};
use crate::routes::AppState;
use crate::wrappers::authentication_route_builder::AuthenticatedRouteBuilder;

pub fn create_example_routes(
    state: Arc<AppState>,
) -> Router<Arc<AppState>> {
    AuthenticatedRouteBuilder::new(state)
        .unauthenticated_post("/login", login)
        .get("/me", protected, vec![1, 2])
        .build()
}
```

The middleware verifies credentials, places the authenticated user in request
extensions, enforces allowed roles, and records authorized usage according to
the authentication implementation.

An empty role vector is not a documented synonym for “any authenticated user.”
Pass the explicit roles accepted by the endpoint.

### Optional authentication

Some board resources are readable by owners, members, public-link viewers, or
embed-token viewers. These methods attempt authentication but still call the
handler when no valid user session exists:

```rust
.maybe_authenticated_get(path, handler)
.maybe_authenticated_post(path, handler)
```

The handler must perform resource-level access checks and accept an optional
authenticated user. Optional authentication does not make a private resource
public by itself.

### Unauthenticated methods

Public endpoints use:

```rust
.unauthenticated_get(path, handler)
.unauthenticated_post(path, handler)
.unauthenticated_patch(path, handler)
.unauthenticated_delete(path, handler)
```

Use these only when the route is intentionally public or authenticates through
another mechanism, such as a signed attachment token. Public mutation endpoints
still need validation, abuse protection, and appropriate rate limiting.

## Resource authorization

Role middleware answers whether a user has a global application role. It does
not prove that the user owns a particular project, board, API key, or account.

Handlers must separately verify domain authorization, including:

- project ownership or project membership;
- board ownership, membership, visibility, link access, or embed token;
- API-key ownership;
- whether a user may update or delete the requested account;
- owner-only sharing and voting operations.

Never infer resource ownership merely because a route accepts role level 1.

## Direct Axum routers

Routes with unusual middleware composition may use Axum directly instead of the
wrapper. `support.rs`, for example, builds separate routers so it can apply:

- authenticated report submission with a 64 MiB body limit;
- unauthenticated JSON feature requests with a 64 KiB limit;
- public access to attachments protected by a signed URL token.

When bypassing `AuthenticatedRouteBuilder`, explicitly document authentication,
body limits, rate limiting, and state requirements near the route.

## Request body limits

Axum body limits should be as narrow as the endpoint permits:

- board uploads/imports currently use a 50 MiB limit in `boards.rs`;
- support reports use 64 MiB for optional media attachments;
- public feature requests use 64 KiB.

Do not raise a limit globally to accommodate one upload endpoint. Large bodies
consume memory and increase denial-of-service risk; apply limits to the smallest
possible router or route group.

## Route ordering and conflicts

Register literal paths before broad capture paths when they could overlap. For
example, user routes place `/current/preferences`, `/current/totp`, and
`/search` before `/{id}`. TOTP setup remains pending until the authenticated
user confirms a code generated from the returned provisioning URI.

When adding a route, check for conflicts involving:

- `/{id}` captures;
- nested routers;
- identical paths with different middleware expectations;
- trailing slash variants such as `/`;
- routes merged at the API root versus nested domain routes.

Use path parameter names that match the handler's Axum extractors and OpenAPI
declaration.

## OpenAPI and Swagger

`routes/mod.rs` derives the OpenAPI document with Utoipa and serves:

```text
/api/v1/docs
/api/v1/openapi.json
```

Adding a `#[utoipa::path]` attribute to a handler is not always sufficient. The
handler may also need to be listed in `ApiDoc`'s `paths`, and new standalone
schemas may need registration in `components(schemas(...))`.

When changing a route:

1. keep the Utoipa path and HTTP method aligned with the Axum registration;
2. document authentication and response status codes;
3. inspect the runtime OpenAPI output;
4. update `documentation/openapi.json` if the checked-in snapshot is retained.

The generated runtime document is authoritative; the checked-in JSON can become
stale.

## Global layers and fallback

After composing all domain routers, `routes/mod.rs`:

1. provides the shared state;
2. nests the router at `/api/v1`;
3. installs an HTTP tracing layer;
4. installs the global fallback error handler.

Additional global CORS, compression, and optional tracing configuration is
applied in `core/server.rs`. Axum layer order affects which requests and
responses middleware can observe, so review both locations before rearranging
layers.

## Adding a route

1. Implement request logic in the appropriate handler module.
2. Add or reuse typed request and response models.
3. Select authenticated, optional-auth, unauthenticated, or custom middleware
   explicitly.
4. Enforce resource ownership in the handler.
5. Register the route in the relevant domain router.
6. Add a new route module only when no existing domain owns the endpoint.
7. Merge or nest a new module in `routes/mod.rs`.
8. Apply a narrow body limit for uploads.
9. Update OpenAPI paths and schemas.
10. Test unauthorized, forbidden, not-found, invalid-input, and success cases.

## Verification

Run from `backend/`:

```bash
cargo fmt --check
SQLX_OFFLINE=true cargo check --locked
cargo test
```

For endpoint verification, start the dependency stack and inspect:

```bash
curl http://localhost:3000/api/v1/health
```

See the [handlers README](../handlers/README.md) if present for request logic,
the [core README](../core/README.md) for global middleware, and the root
[backend README](../../README.md) for development setup.
