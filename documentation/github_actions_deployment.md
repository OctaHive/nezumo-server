# GitHub Actions: tests and production deployment

The repository contains two workflows:

- `CI` runs `cargo fmt --check` and `cargo test --locked` for every pushed
  commit and pull request;
- `Release backend` builds an Ubuntu 24.04 x86-64 release package and deploys
  it when a `v*` tag matching the package version in `Cargo.toml` is pushed.

For example, Cargo version `0.27.3` must be released with tag `v0.27.3`, and
produces `nezumo-0.27.3-x86_64-unknown-linux-gnu.tar.gz`. The commit SHA remains
available inside the package as `REVISION`, while `VERSION` contains the Cargo
package version.

The release also contains migrations, fonts, and the runtime mail template.
Deployments are stored under `/opt/nezumo/releases` by default. During
activation the workflow compares the packaged migrations with the production
database, stops `nezumo.service`, applies pending migrations, installs the
executable at `/opt/nezumo/bin/nezumo` as `nezumo:nezumo` with mode `0755`,
switches the `/opt/nezumo/current` runtime-resource symlink, and starts the
service. If the service does not become active, the previous binary and
resources are restored and started again.

## One-time server setup

The example below uses separate `deploy` and `nezumo` users. Adjust names and
paths if the server already has deployment conventions.

```bash
sudo useradd --create-home --shell /bin/bash deploy
sudo useradd --system --home /opt/nezumo --shell /usr/sbin/nologin nezumo
sudo install -d -o deploy -g nezumo -m 0750 /opt/nezumo
sudo install -d -o deploy -g nezumo -m 0750 /opt/nezumo/releases
sudo install -d -o deploy -g nezumo -m 0755 /opt/nezumo/bin
sudo install -d -o root -g nezumo -m 0750 /etc/nezumo
sudo install -o root -g nezumo -m 0640 /dev/null /etc/nezumo/nezumo.env
```

Put the production environment variables in `/etc/nezumo/nezumo.env`. At
minimum, review `ENVIRONMENT=production`, database, Redis, S3, SMTP, JWT, CORS,
renderer, and TLS settings. The file is read by systemd and must not be stored
in GitHub or in the release package.

The backend process itself intentionally skips automatic migrations when
`ENVIRONMENT=production`. Deployment runs SQLx CLI on the deployment machine
and connects directly to production PostgreSQL through
`DEPLOY_DATABASE_URL`. Production does not need Rust or SQLx CLI installed.
Install a compatible SQLx CLI on the developer machine:

```bash
cargo install sqlx-cli --version 0.8.6 --locked \
  --no-default-features --features postgres
```

If it is not on the local `PATH`, set `DEPLOY_SQLX_BIN` to its local absolute
path. The deployment extracts migrations from the exact package being
deployed, runs `sqlx migrate info`, uploads the package, stops the backend over
SSH, and runs the idempotent local `sqlx migrate run` against the remote
database. A checksum mismatch or migration failure starts the previous backend
without replacing its binary.

SQL migrations are forward-only and are not automatically reverted if the new
backend later fails its startup check. Production migrations should therefore
remain backward-compatible with the immediately preceding backend version.

Install and enable the unit:

```bash
sudo cp deploy/nezumo.service /etc/systemd/system/nezumo.service
sudo systemctl daemon-reload
sudo systemctl enable nezumo.service
```

The local configuration in this repository currently connects as `root`, so
the deployment script does not invoke `sudo` remotely. If a dedicated `deploy`
user is used instead, it owns `/opt/nezumo/bin` and needs narrowly scoped
passwordless permission for systemd plus ownership/mode changes. Create
`/etc/sudoers.d/nezumo-deploy` with `visudo`:

```text
Cmnd_Alias NEZUMO_SYSTEMD = /usr/bin/systemctl stop nezumo.service, /usr/bin/systemctl start nezumo.service, /usr/bin/systemctl is-active --quiet nezumo.service
Cmnd_Alias NEZUMO_BINARY = /usr/bin/chown nezumo\:nezumo /opt/nezumo/bin/.nezumo.next.*, /usr/bin/chown nezumo\:nezumo /opt/nezumo/bin/nezumo, /usr/bin/chmod 0755 /opt/nezumo/bin/.nezumo.next.*, /usr/bin/chmod 0755 /opt/nezumo/bin/nezumo
deploy ALL=(root) NOPASSWD: NEZUMO_SYSTEMD, NEZUMO_BINARY
```

Add the public half of a dedicated deployment SSH key to
`/home/deploy/.ssh/authorized_keys`. Keep the private half for the GitHub
environment secret described below.

The binary is built on Ubuntu 24.04 and targets
`x86_64-unknown-linux-gnu`. Use an x86-64 Ubuntu 24.04 server, or change the
runner/target and install compatible runtime libraries. The host must also
provide runtime tools used by enabled features, including `mutool` and the
separately distributed `nezumo-render`.

## GitHub production environment

In the repository settings, create an environment named `production`. Add
approval rules if releases must be confirmed manually.

Add these environment secrets:

| Secret | Value |
| --- | --- |
| `DEPLOY_HOST` | Server DNS name or IP address |
| `DEPLOY_USER` | SSH user, for example `deploy` |
| `SSH_PRIVATE_KEY` | Private key dedicated to deployments |
| `SSH_KNOWN_HOSTS` | Pinned server host key from `ssh-keyscan -H HOST` |
| `DEPLOY_DATABASE_URL` | Production PostgreSQL URL; required unless migrations are skipped |

Add these optional environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DEPLOY_PORT` | `22` | SSH port |
| `DEPLOY_PATH` | `/opt/nezumo` | Root of release directories |
| `DEPLOY_SERVICES` | `nezumo.service` | Comma-separated systemd units to stop/start |
| `DEPLOY_BINARY_PATH` | `/opt/nezumo/bin/nezumo` | Stable executable path used by systemd |
| `DEPLOY_OWNER` | `nezumo` | Executable owner |
| `DEPLOY_GROUP` | `nezumo` | Executable group |
| `DEPLOY_BINARY_MODE` | `0755` | Executable permissions |
| `DEPLOY_MIGRATIONS` | `auto` | `auto` compares/applies migrations; `skip` disables this |
| `DEPLOY_SQLX_BIN` | `sqlx` | Local SQLx CLI command or absolute path |

For local manual deployment, keep `DEPLOY_DATABASE_URL` only in the ignored
`.env.deploy` file with mode `0600`. Do not add it to the tracked
`manual-deploy.env.example`.

When multiple units are listed in `DEPLOY_SERVICES`, allow the exact combined
`systemctl stop`, `systemctl start`, and `systemctl is-active --quiet` commands
in sudoers.

## Creating a release

First update the package version in `Cargo.toml` and `Cargo.lock`. Then create
the matching immutable tag. The current repository version is `0.1.0`, so its
release command is:

```bash
git tag v0.1.0
git push origin v0.1.0
```

If the tag and Cargo version differ, the release job stops before tests, build,
or deployment. The workflow keeps the package as a GitHub Actions artifact for
30 days. Previous server releases are retained, allowing a manual rollback by
repointing `/opt/nezumo/current` and restarting the service.

## Local build without publishing

Docker can build the same type of glibc-based x86-64 Linux package locally,
including on an Apple Silicon Mac:

```bash
./scripts/build-linux-release.sh
```

The result is written to `dist/`, for example:

```text
dist/nezumo-0.1.0-x86_64-unknown-linux-gnu.tar.gz
dist/nezumo-0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256
```

This command does not create a Git tag, upload an artifact, or connect to the
server. `BUILD_OUTPUT_DIR` can override the output directory.

## Manual deployment from the local machine

Copy the environment template and fill in the SSH endpoint and key paths:

```bash
cp deploy/manual-deploy.env.example .env.deploy
chmod 600 .env.deploy
```

Then build and deploy in one command:

```bash
./scripts/manual-deploy.sh
```

The script reads `.env.deploy` by default, builds the Linux archive, uploads it,
checks/applies its migrations locally against production PostgreSQL, and reuses
the same systemd activation and rollback mechanism as GitHub Actions. It does
not create a tag or publish a GitHub release, and it does not require SQLx CLI
on the production server.

To deploy an archive that has already been built:

```bash
DEPLOY_ARCHIVE="$PWD/dist/nezumo-0.1.0-x86_64-unknown-linux-gnu.tar.gz" \
  ./scripts/manual-deploy.sh
```

Set `DEPLOY_ENV_FILE` to use another environment file. All values can also be
exported directly instead of creating `.env.deploy`. A dedicated SSH key
through `DEPLOY_SSH_KEY` is preferred. For servers that only allow password
authentication, omit `DEPLOY_SSH_KEY` and set `DEPLOY_PASSWORD`; the script
passes it through `SSH_ASKPASS` instead of a command-line argument. Keep the
environment file at mode `0600`.
