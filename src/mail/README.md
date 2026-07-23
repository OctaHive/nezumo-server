# Mail

This module configures the backend's asynchronous SMTP transport and sends
multipart email messages. It is currently used for registration verification
and password-reset emails.

The application creates one shared [`MailerState`](./mod.rs) during startup and
stores it in `AppState`. The state contains a reusable `lettre` SMTP transport
and the configured sender address.

## Module layout

| File | Responsibility |
| --- | --- |
| [`connect.rs`](./connect.rs) | Builds the SMTP transport, selects the security mode, and validates the configuration by sending a startup test email. |
| [`send.rs`](./send.rs) | Builds and sends plain-text/HTML multipart messages with the shared footer. |
| [`footer.html`](./footer.html) | HTML footer source that must be available in the configured runtime template directory. |
| [`mod.rs`](./mod.rs) | Exposes the submodules and defines `MailerState`. |

## Configuration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `MAIL_SERVER` | yes | — | SMTP server hostname, for example `smtp.example.com` or `mailpit`. |
| `MAIL_PORT` | no | `587` | SMTP port. Invalid numeric values also fall back to `587`. |
| `MAIL_USER` | yes | — | SMTP username. May be empty only when the server does not require authentication. |
| `MAIL_PASS` | yes | — | SMTP password. May be empty only when the server does not require authentication. |
| `MAIL_FROM` | yes | empty | Sender mailbox. It must be a valid email address and is also the recipient of the startup test message. |
| `MAIL_SECURITY` | no | `starttls` | Connection mode: `none`, `starttls`, or `implicit`. |
| `MAIL_TEMPLATES_DIR` | no | `templates/mail` | Runtime directory containing `footer.html`. |

`MAIL_USER` and `MAIL_PASS` are read through the required environment helper,
so both variables must exist even when their values are empty. Authentication
is disabled only when both are empty. A password without a username is still
passed to the SMTP transport; a username without a password is treated as
configured credentials with an empty password.

See [`backend/.env.example`](../../.env.example) and the Docker Compose files in
`backend/` for environment examples.

## Security modes

| Mode | Typical port | Behaviour |
| --- | --- | --- |
| `none` | `1025` | Plain SMTP. Intended for local services such as Mailpit; do not use over untrusted networks. |
| `starttls` | `587` | Connects with STARTTLS and upgrades the connection before authentication. |
| `implicit` | `465` | Establishes TLS when the connection is opened. |

The selected port is not inferred from the mode. Configure `MAIL_PORT`
explicitly when it differs from `587`.

## Runtime template

`send_mail` reads `${MAIL_TEMPLATES_DIR}/footer.html` on its first call and
caches the contents for the lifetime of the process. Ensure the file is copied
or mounted at that location in every runtime environment. For example, when
running from the `backend` directory without a separate template deployment:

```text
MAIL_TEMPLATES_DIR=src/mail
```

Changes to `footer.html` are not picked up until the backend process restarts.
If the file is absent or unreadable, sending fails before contacting SMTP.

## Usage

```rust
use crate::mail::{send::send_mail, MailerState};

async fn send_welcome_mail(mail: &MailerState, recipient: &str) -> Result<(), String> {
    send_mail(
        mail,
        recipient,
        "Welcome to Nezumo",
        "Your account is ready.",
    )
    .await
    .map_err(|error| error.to_string())
}
```

The supplied `body` is treated as plain text:

- the text alternative contains the body plus an HTML-to-text version of the
  footer;
- the HTML alternative escapes the body before wrapping it in a paragraph, so
  user-controlled text cannot inject markup;
- the footer itself is trusted HTML and is inserted without escaping.

New email flows should pass unformatted text to `body`. Add intentional shared
markup to the footer template or introduce a dedicated, safely rendered
template rather than passing HTML through `body`.

## Startup and failure behaviour

`connect_to_mail` sends an `SMTP Test` message from `MAIL_FROM` to `MAIL_FROM`
on every backend startup. The server treats a failure to build or send this
message as fatal and does not finish starting. This verifies delivery early,
but it also means that:

- the sender mailbox must be accepted as a recipient;
- deployments generate one test message per backend process start;
- an SMTP outage can prevent the application from starting.

`send_mail` does not retry failed deliveries or enqueue messages. Callers
receive an error immediately and decide how it affects the request. Avoid
logging credentials, verification codes, reset codes, or full message bodies
when handling these errors.
