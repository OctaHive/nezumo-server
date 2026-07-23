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
Deployments are stored under `/opt/nezumo/releases` by default. The
`/opt/nezumo/current` symlink is switched atomically. If a configured systemd
service does not become active after restart, the symlink is restored to the
previous release and the services are restarted again.

## One-time server setup

The example below uses separate `deploy` and `nezumo` users. Adjust names and
paths if the server already has deployment conventions.

```bash
sudo useradd --create-home --shell /bin/bash deploy
sudo useradd --system --home /opt/nezumo --shell /usr/sbin/nologin nezumo
sudo install -d -o deploy -g nezumo -m 0750 /opt/nezumo
sudo install -d -o deploy -g nezumo -m 0750 /opt/nezumo/releases
sudo install -d -o root -g nezumo -m 0750 /etc/nezumo
sudo install -o root -g nezumo -m 0640 /dev/null /etc/nezumo/nezumo.env
```

Put the production environment variables in `/etc/nezumo/nezumo.env`. At
minimum, review `ENVIRONMENT=production`, database, Redis, S3, SMTP, JWT, CORS,
renderer, and TLS settings. The file is read by systemd and must not be stored
in GitHub or in the release package.

The backend intentionally skips automatic migrations when
`ENVIRONMENT=production`. If a release contains schema changes, apply the
checked-in migrations through the existing production migration procedure
before pushing its release tag. The deployment workflow does not read database
credentials and does not run migrations.

Install and enable the unit:

```bash
sudo cp deploy/nezumo.service /etc/systemd/system/nezumo.service
sudo systemctl daemon-reload
sudo systemctl enable nezumo.service
```

The deployment user needs passwordless permission only for the systemd
operations used by the deployment script. Create `/etc/sudoers.d/nezumo-deploy`
with `visudo`:

```text
deploy ALL=(root) NOPASSWD: /usr/bin/systemctl restart nezumo.service, /usr/bin/systemctl is-active --quiet nezumo.service
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

Add these optional environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `DEPLOY_PORT` | `22` | SSH port |
| `DEPLOY_PATH` | `/opt/nezumo` | Root of release directories |
| `DEPLOY_SERVICES` | `nezumo.service` | Comma-separated systemd units to restart |

When multiple units are listed in `DEPLOY_SERVICES`, allow the exact combined
`systemctl restart` and `systemctl is-active --quiet` commands in sudoers.

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
