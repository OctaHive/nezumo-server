#!/usr/bin/env bash

# Builds (unless DEPLOY_ARCHIVE is set) and manually deploys a package over SSH.
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
deploy_env_file="${DEPLOY_ENV_FILE:-${repo_root}/.env.deploy}"

if [[ -f "${deploy_env_file}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${deploy_env_file}"
  set +a
fi

required_variables=(DEPLOY_HOST DEPLOY_USER)
for variable_name in "${required_variables[@]}"; do
  if [[ -z "${!variable_name:-}" ]]; then
    echo "Required environment variable is not set: ${variable_name}" >&2
    exit 1
  fi
done

deploy_port="${DEPLOY_PORT:-22}"
deploy_path="${DEPLOY_PATH:-/opt/nezumo}"
deploy_services="${DEPLOY_SERVICES:-nezumo.service}"
deploy_binary_path="${DEPLOY_BINARY_PATH:-${deploy_path}/bin/nezumo}"
deploy_owner="${DEPLOY_OWNER:-nezumo}"
deploy_group="${DEPLOY_GROUP:-nezumo}"
deploy_binary_mode="${DEPLOY_BINARY_MODE:-0755}"
deploy_migrations="${DEPLOY_MIGRATIONS:-auto}"
deploy_sqlx_bin="${DEPLOY_SQLX_BIN:-sqlx}"
deploy_database_url="${DEPLOY_DATABASE_URL:-}"
known_hosts_file="${DEPLOY_KNOWN_HOSTS:-${HOME}/.ssh/known_hosts}"

if [[ "${deploy_migrations}" != auto && "${deploy_migrations}" != skip ]]; then
  echo "DEPLOY_MIGRATIONS must be auto or skip" >&2
  exit 1
fi

ssh_auth_options=()
if [[ -n "${DEPLOY_SSH_KEY:-}" ]]; then
  if [[ ! -f "${DEPLOY_SSH_KEY}" ]]; then
    echo "SSH private key does not exist: ${DEPLOY_SSH_KEY}" >&2
    exit 1
  fi
  ssh_auth_options=(-i "${DEPLOY_SSH_KEY}" -o BatchMode=yes)
elif [[ -n "${DEPLOY_PASSWORD:-}" ]]; then
  askpass_script="${repo_root}/scripts/ssh-askpass.sh"
  if [[ ! -x "${askpass_script}" ]]; then
    echo "SSH askpass helper is not executable: ${askpass_script}" >&2
    exit 1
  fi
  export SSH_ASKPASS="${askpass_script}"
  export SSH_ASKPASS_REQUIRE=force
  export DISPLAY="${DISPLAY:-nezumo-deploy}"
  ssh_auth_options=(
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
    -o NumberOfPasswordPrompts=1
  )
else
  echo "Set either DEPLOY_SSH_KEY or DEPLOY_PASSWORD" >&2
  exit 1
fi

if [[ ! "${deploy_port}" =~ ^[0-9]+$ ]]; then
  echo "DEPLOY_PORT must be numeric" >&2
  exit 1
fi

if [[ ! -f "${known_hosts_file}" ]]; then
  echo "SSH known_hosts file does not exist: ${known_hosts_file}" >&2
  exit 1
fi

for command_name in base64 scp ssh tar; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command is not installed: ${command_name}" >&2
    exit 1
  fi
done

if [[ "${deploy_migrations}" == auto ]]; then
  if [[ -z "${deploy_database_url}" ]]; then
    echo "DEPLOY_DATABASE_URL is required when DEPLOY_MIGRATIONS=auto" >&2
    exit 1
  fi

  if [[ "${deploy_sqlx_bin}" == */* ]]; then
    if [[ ! -x "${deploy_sqlx_bin}" ]]; then
      echo "Local SQLx CLI is not executable: ${deploy_sqlx_bin}" >&2
      exit 1
    fi
  elif resolved_sqlx="$(command -v "${deploy_sqlx_bin}" 2>/dev/null)"; then
    deploy_sqlx_bin="${resolved_sqlx}"
  elif [[ -x "${HOME}/.cargo/bin/${deploy_sqlx_bin}" ]]; then
    deploy_sqlx_bin="${HOME}/.cargo/bin/${deploy_sqlx_bin}"
  else
    echo "Local SQLx CLI is not installed: ${deploy_sqlx_bin}" >&2
    exit 1
  fi
fi

archive="${DEPLOY_ARCHIVE:-}"
if [[ -z "${archive}" ]]; then
  "${repo_root}/scripts/build-linux-release.sh"

  package_version="$(
    awk '
      /^\[package\]$/ { in_package = 1; next }
      /^\[/ && in_package { exit }
      in_package && /^version = "/ {
        value = $0
        sub(/^version = "/, "", value)
        sub(/"$/, "", value)
        print value
        exit
      }
    ' "${repo_root}/Cargo.toml"
  )"
  archive="${BUILD_OUTPUT_DIR:-${repo_root}/dist}/nezumo-${package_version}-x86_64-unknown-linux-gnu.tar.gz"
fi

if [[ ! -f "${archive}" ]]; then
  echo "Deployment archive does not exist: ${archive}" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  archive_sha="$(sha256sum "${archive}" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  archive_sha="$(shasum -a 256 "${archive}" | awk '{print $1}')"
else
  echo "Either sha256sum or shasum is required" >&2
  exit 1
fi

package_version="$(tar -xOf "${archive}" ./VERSION | tr -d '\r\n')"
if [[ ! "${package_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?([+][0-9A-Za-z.-]+)?$ ]]; then
  echo "The package contains an invalid VERSION value" >&2
  exit 1
fi

release_name="${DEPLOY_RELEASE_NAME:-manual-v${package_version}-$(date -u +%Y%m%d%H%M%S)}"
if [[ ! "${release_name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "DEPLOY_RELEASE_NAME contains unsupported characters" >&2
  exit 1
fi

remote_archive="/tmp/nezumo-manual-${package_version}-$$.tar.gz"
target="${DEPLOY_USER}@${DEPLOY_HOST}"
ssh_options=(
  "${ssh_auth_options[@]}"
  -p "${deploy_port}"
  -o StrictHostKeyChecking=yes
  -o "UserKnownHostsFile=${known_hosts_file}"
)

IFS=',' read -r -a raw_services <<< "${deploy_services}"
services=()
remote_service_arguments=""
for raw_service in "${raw_services[@]}"; do
  service="${raw_service#"${raw_service%%[![:space:]]*}"}"
  service="${service%"${service##*[![:space:]]}"}"
  if [[ ! "${service}" =~ ^[A-Za-z0-9_.@-]+\.service$ ]]; then
    echo "Invalid systemd service name: ${service}" >&2
    exit 1
  fi
  services+=("${service}")
  remote_service_arguments+=" '${service}'"
done

if [[ "${#services[@]}" -eq 0 ]]; then
  echo "At least one systemd service is required" >&2
  exit 1
fi

remote_systemctl() {
  local action="$1"
  if [[ "${action}" != stop && "${action}" != start ]]; then
    echo "Unsupported remote systemctl action: ${action}" >&2
    return 1
  fi

  # Service names are restricted to a safe systemd-unit character set above.
  # shellcheck disable=SC2029
  ssh "${ssh_options[@]}" "${target}" \
    "if [ \"\$(id -u)\" -eq 0 ]; then systemctl '${action}'${remote_service_arguments}; else sudo systemctl '${action}'${remote_service_arguments}; fi"
}

migration_temp_dir=""
service_stopped_for_migration=false

cleanup() {
  local status="$?"
  trap - EXIT

  if [[ "${service_stopped_for_migration}" == true ]]; then
    echo "Deployment did not complete; starting the previous backend..." >&2
    remote_systemctl start || true
  fi

  if [[ -n "${migration_temp_dir}" ]]; then
    rm -rf -- "${migration_temp_dir}"
  fi

  exit "${status}"
}
trap cleanup EXIT

if [[ "${deploy_migrations}" == auto ]]; then
  migration_temp_dir="$(mktemp -d)"
  tar -xzf "${archive}" -C "${migration_temp_dir}" ./migrations

  echo "Comparing packaged migrations with the production database..." >&2
  DATABASE_URL="${deploy_database_url}" \
    "${deploy_sqlx_bin}" migrate info \
    --source "${migration_temp_dir}/migrations" \
    --no-dotenv
fi

echo "Uploading ${archive} to ${target}..." >&2
scp \
  "${ssh_auth_options[@]}" \
  -P "${deploy_port}" \
  -o StrictHostKeyChecking=yes \
  -o "UserKnownHostsFile=${known_hosts_file}" \
  "${archive}" "${target}:${remote_archive}"

if [[ "${deploy_migrations}" == auto ]]; then
  echo "Stopping ${deploy_services} before applying production migrations..." >&2
  remote_systemctl stop
  service_stopped_for_migration=true

  echo "Applying pending production migrations from the local machine..." >&2
  DATABASE_URL="${deploy_database_url}" \
    "${deploy_sqlx_bin}" migrate run \
    --source "${migration_temp_dir}/migrations" \
    --no-dotenv
fi

encode_base64() {
  printf '%s' "$1" | base64 | tr -d '\r\n'
}

deploy_path_b64="$(encode_base64 "${deploy_path}")"
deploy_services_b64="$(encode_base64 "${deploy_services}")"
release_name_b64="$(encode_base64 "${release_name}")"
deploy_binary_path_b64="$(encode_base64 "${deploy_binary_path}")"
deploy_owner_b64="$(encode_base64 "${deploy_owner}")"
deploy_group_b64="$(encode_base64 "${deploy_group}")"

echo "Stopping ${deploy_services}, installing ${deploy_binary_path}, and starting the service..." >&2
# These client-side values contain only fixed paths, digits, hex, or base64.
# shellcheck disable=SC2029
ssh "${ssh_options[@]}" "${target}" \
  "bash -s -- '${remote_archive}' '${deploy_path_b64}' '${deploy_services_b64}' '${release_name_b64}' '${archive_sha}' '${deploy_binary_path_b64}' '${deploy_owner_b64}' '${deploy_group_b64}' '${deploy_binary_mode}'" \
  < "${repo_root}/scripts/deploy-release.sh"

service_stopped_for_migration=false
echo "Manual deployment completed: ${release_name}" >&2
