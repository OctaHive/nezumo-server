# Models

This directory contains Nezumo's typed data contracts. The models describe
PostgreSQL rows, internal domain data, incoming request payloads, outgoing API
responses, validation rules, and OpenAPI schemas.

Models intentionally contain little orchestration logic. Handlers enforce
authorization and coordinate services, while modules under `database/` own SQL
operations.

## Module layout

| Module | Models |
| --- | --- |
| [`apikey.rs`](./apikey.rs) | API-key rows, creation/rotation payloads, and non-secret listing responses. |
| [`auth.rs`](./auth.rs) | JWT claims, login payloads, login challenges, and authentication errors. |
| [`board_files.rs`](./board_files.rs) | Board-file metadata, insert data, and upload responses. |
| [`board_invite_links.rs`](./board_invite_links.rs) | Invitation-link rows, requests, and responses. |
| [`boards.rs`](./boards.rs) | Boards, board members, access-aware listings, configuration, and state responses. |
| [`events.rs`](./events.rs) | Collaborative commits, persisted events, and snapshots. |
| [`projects.rs`](./projects.rs) | Projects, project membership, and mutation payloads. |
| [`project_statuses.rs`](./project_statuses.rs) | Project task statuses and default status definitions. |
| [`project_tags.rs`](./project_tags.rs) | Project tags and mutation payloads. |
| [`user.rs`](./user.rs) | User rows, profiles, registration, password reset, preferences, and search. |
| [`role.rs`](./role.rs) | User-role representation. |
| [`usage.rs`](./usage.rs) | Aggregated API usage responses. |
| [`health.rs`](./health.rs) | Health and host-resource responses. |
| [`documentation.rs`](./documentation.rs) | Generic OpenAPI success and error schemas. |
| [`error.rs`](./error.rs) | Standard serializable error body. |
| [`mod.rs`](./mod.rs) | Declares the model modules. |

## Model categories

Several categories may exist for the same domain object because database,
internal, request, and response shapes have different requirements.

### Database rows

Row models usually derive `sqlx::FromRow` and mirror a query result:

```rust
#[derive(Debug, FromRow)]
pub struct ProjectRow {
    pub id: Uuid,
    pub owner_id: Uuid,
    pub name: String,
    // ...
}
```

Keep field types aligned with PostgreSQL nullability and the columns selected by
`query_as!`. A row model may contain private data and must not automatically be
treated as an API response.

### Request payloads

Incoming JSON models derive `Deserialize`; payloads with input constraints also
derive `validator::Validate`:

```rust
#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UserRegisterBody {
    #[validate(email)]
    pub email: String,
}
```

Validation attributes check field syntax and bounds. They do not replace
authorization, uniqueness constraints, ownership checks, or domain operations
that require database state.

Use `#[serde(deny_unknown_fields)]` where silently accepting misspelled or stale
fields would be dangerous. Consider compatibility before adding it to an
existing public payload.

### Response schemas

Outgoing models derive `Serialize` and normally `utoipa::ToSchema`. Response
types should expose only fields intended for API consumers.

Do not serialize:

- password hashes;
- TOTP secrets except in a narrowly defined enrollment response;
- stored API-key hashes;
- login, verification, password-reset, invite, or embed secrets unless the
  endpoint is specifically responsible for issuing them;
- internal storage credentials or unrestricted signed URLs.

Sensitive fields on internal models should use `#[serde(skip)]`, but a dedicated
public response type is safer when the internal structure carries credentials.

### Internal domain models

Some models bridge database rows and API responses. Conversion implementations
make this boundary explicit:

```rust
impl From<UserRow> for User {
    fn from(row: UserRow) -> Self {
        // Explicit field mapping.
    }
}
```

Prefer explicit mapping over broad serialization of database rows. When a new
field is added, review every `From` implementation to decide deliberately
whether the field belongs in each destination type.

## Common derives

| Derive or trait | Purpose |
| --- | --- |
| `Serialize` | Converts response and internal values to JSON or other formats. |
| `Deserialize` | Parses request bodies and persisted JSON. |
| `FromRow` | Maps runtime SQLx rows into Rust structs. |
| `Validate` | Applies declarative input validation before handler logic. |
| `ToSchema` | Adds the type to generated OpenAPI schemas. |
| `Clone`, `Debug`, `Default` | Added only when required by consumers or useful semantics. |

Do not add derives mechanically. For example, serializing an internal credential
model or cloning a secret-bearing type increases the chance of accidental
exposure.

## Imports and usage

`models/mod.rs` exposes modules, not all types as flat re-exports. Import types
through their domain module:

```rust
use crate::models::auth::{AuthError, Claims, LoginData};
use crate::models::boards::{Board, BoardCreateBody};
use crate::models::user::{UserGetResponse, UserUpdateBody};
```

Avoid glob imports outside a tightly scoped domain implementation. Explicit
imports make it clear whether code is using a database row, internal model,
request body, or public response.

## Optional fields and update semantics

Request fields use different optional shapes intentionally:

- `T` — required value;
- `Option<T>` — omitted or supplied value;
- `Option<Option<T>>` — omitted, explicitly set to `null`, or supplied value.

The nested form is useful for PATCH-style updates where a nullable database
column must be clearable. Changing between these representations changes the
wire contract and update behaviour.

Serde attributes such as `default`, `skip`, `rename`, and `rename_all` are also
part of the public contract. Treat changes to them like API changes.

## OpenAPI integration

`ToSchema` makes a type eligible for the generated OpenAPI document, but it must
still be reachable from a documented handler or registered in the central
OpenAPI configuration under `routes/mod.rs`.

When changing a public model:

1. update field descriptions or schema annotations;
2. inspect the runtime document at `/api/v1/openapi.json`;
3. update the checked-in `documentation/openapi.json` if the project continues
   to maintain that snapshot;
4. verify frontend and external-client compatibility.

The runtime OpenAPI output generated from Rust is authoritative; a checked-in
snapshot can become stale.

## Adding or changing a model

1. Place it in the module that owns the domain concept.
2. Decide whether it represents a database row, internal value, request, or
   response; avoid one type serving all four roles when fields differ.
3. Add only the derives required for that role.
4. Document the type and any field whose meaning, units, lifetime, or security
   properties are not obvious.
5. Add validation for untrusted input, including length limits for strings and
   collections.
6. Update explicit conversions and struct literals.
7. Update SQL queries and migrations when persistence changes.
8. Regenerate SQLx offline metadata after modifying compile-time checked query
   shapes.
9. Review OpenAPI output and API compatibility.
10. Add serialization, validation, and conversion tests where behaviour is not
    trivial.

## Compatibility and security checklist

- Is the field safe to expose to every endpoint using this response type?
- Does nullability match both PostgreSQL and the JSON contract?
- Will older clients tolerate the new or removed field?
- Does validation impose reasonable size and format limits?
- Are dates and times represented consistently, including timezone semantics?
- Could `Debug` logging reveal credentials or personal data?
- Are defaults explicit and compatible with old persisted JSON?
- Do conversion implementations handle the field deliberately?
- Does OpenAPI describe the actual serialized representation?

## Verification

Run from `backend/`:

```bash
cargo fmt --check
SQLX_OFFLINE=true cargo check --locked
cargo test
```

Model changes that alter checked SQLx query results require a migrated
development database and refreshed metadata:

```bash
cargo sqlx prepare
```

See the [database README](../database/README.md) for query and migration rules,
and the root [backend README](../../README.md) for the development environment.
