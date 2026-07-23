# Utils

This directory contains small, reusable helpers shared by Nezumo handlers,
models, middleware, and route setup. Business logic and database access should
remain in their dedicated modules rather than being added here.

## Module layout

| File | Responsibility |
| --- | --- |
| [`auth.rs`](./auth.rs) | Password hashing and verification, JWT creation and validation, token extraction, API-key generation, and TOTP-related helpers. |
| [`global_error_handler.rs`](./global_error_handler.rs) | Returns the JSON `404 Not Found` response used as the router fallback. |
| [`process_image.rs`](./process_image.rs) | Crops uploaded images to a centered square, resizes them, and encodes them as WebP. |
| [`validate.rs`](./validate.rs) | Validators used by model derives for dates, usernames, passwords, birthdays, countries, and languages. |
| [`mod.rs`](./mod.rs) | Exposes the utility modules to the rest of the crate. |

## Authentication helpers

`auth.rs` uses Argon2id for password and API-key hashes. Hash verification is
offloaded with `spawn_blocking` so CPU-heavy Argon2 work does not block the
async runtime.

JWTs are signed with HS256 and have a 24-hour lifetime. Decoding validates the
configured issuer and audience and allows five minutes of clock skew. Tokens
can be read from either an `Authorization: Bearer <token>` header or the
configured cookie.

The authentication helpers use these environment variables:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `JWT_SECRET_KEY` | yes | — | Secret used to sign and verify JWTs. Use a strong production value. |
| `JWT_ISSUER` | yes | — | Expected token issuer. |
| `JWT_AUDIENCE` | yes | — | Expected token audience. |
| `JWT_COOKIE_NAME` | no | `auth_token` | Cookie inspected when extracting a JWT. |

See [`backend/.env.example`](../../.env.example) for configuration examples.

New TOTP enrollments use a Base32-encoded 160-bit secret with the standard
SHA-1, six-digit, 30-second profile used by common authenticator applications.
Enrollment is an authenticated setup-and-confirm flow under
`/users/current/totp`; TOTP remains disabled until the first code is verified.
Legacy eight-digit secrets remain verifiable temporarily so existing accounts
are not immediately locked out during migration.

## Image processing

`process_image` accepts uploaded bytes and target dimensions. It performs the
following work on a blocking worker thread:

1. Decode the input image.
2. Crop the largest centered square.
3. Resize to the requested width and height with the Lanczos3 filter.
4. Convert to RGB and encode WebP at quality `60`.

When its `debug` argument is `true`, processing timings are printed for every
stage. The helper currently powers profile-picture uploads.

```rust
use axum::body::Bytes;
use crate::utils::process_image::process_image;

async fn make_avatar(upload: Bytes) -> Result<Bytes, String> {
    process_image(upload, 300, 300, false).await
}
```

## Validation

The validators return `validator::ValidationError` and are referenced from
model field attributes. Important rules include:

- future dates use the `YYYY-MM-DD` format;
- usernames accept ASCII letters, digits, `_`, and `-`;
- passwords require at least eight characters and uppercase, lowercase,
  numeric, and supported special characters;
- birthdays must not be in the future or more than 120 years old;
- country and language values must exist in `crate::referencedata`.

Changing a validator can affect request deserialization throughout the API, so
update the corresponding models and API documentation together.

## Adding utilities

- Keep helpers independent of handlers and application state when practical.
- Add the module to [`mod.rs`](./mod.rs) when creating a new file.
- Document public functions with Rustdoc, including failure behavior.
- Add focused unit tests for parsing, validation, cryptography wrappers, and
  image transformations.
