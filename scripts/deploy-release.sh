#!/usr/bin/env bash

# Runs on the target Linux server. GitHub Actions or manual deployment streams
# this file over SSH.
set -Eeuo pipefail

if [[ "$#" -ne 9 ]]; then
  echo "Usage: deploy-release.sh ARCHIVE DEPLOY_PATH_B64 SERVICES_B64 RELEASE_B64 SHA256 BINARY_PATH_B64 OWNER_B64 GROUP_B64 MODE" >&2
  exit 2
fi

archive="$1"
install_root="$(printf '%s' "$2" | base64 --decode)"
services_csv="$(printf '%s' "$3" | base64 --decode)"
release_name="$(printf '%s' "$4" | base64 --decode)"
expected_sha="$5"
binary_path="$(printf '%s' "$6" | base64 --decode)"
binary_owner="$(printf '%s' "$7" | base64 --decode)"
binary_group="$(printf '%s' "$8" | base64 --decode)"
binary_mode="$9"

if [[ "${EUID}" -eq 0 ]]; then
  elevate=()
else
  elevate=(sudo)
fi

if [[ ! -f "${archive}" ]]; then
  echo "Release archive does not exist: ${archive}" >&2
  exit 1
fi

if [[ "${install_root}" != /* || "${install_root}" == "/" ]]; then
  echo "DEPLOY_PATH must be an absolute directory other than /" >&2
  exit 1
fi

if [[ "${binary_path}" != "${install_root}/"* || "${binary_path}" == */ ]]; then
  echo "DEPLOY_BINARY_PATH must be a file below DEPLOY_PATH" >&2
  exit 1
fi

if [[ ! "${release_name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid release name: ${release_name}" >&2
  exit 1
fi

if [[ ! "${binary_owner}" =~ ^[A-Za-z_][A-Za-z0-9_.-]*[$]?$ ]]; then
  echo "Invalid binary owner: ${binary_owner}" >&2
  exit 1
fi

if [[ ! "${binary_group}" =~ ^[A-Za-z_][A-Za-z0-9_.-]*[$]?$ ]]; then
  echo "Invalid binary group: ${binary_group}" >&2
  exit 1
fi

if [[ ! "${binary_mode}" =~ ^0?[0-7]{3}$ ]]; then
  echo "DEPLOY_BINARY_MODE must be an octal mode such as 0755" >&2
  exit 1
fi
if [[ "${#binary_mode}" -eq 3 ]]; then
  binary_mode="0${binary_mode}"
fi

if ! getent passwd "${binary_owner}" >/dev/null; then
  echo "Binary owner does not exist on the server: ${binary_owner}" >&2
  exit 1
fi
if ! getent group "${binary_group}" >/dev/null; then
  echo "Binary group does not exist on the server: ${binary_group}" >&2
  exit 1
fi

if [[ ! "${expected_sha}" =~ ^[a-f0-9]{64}$ ]]; then
  echo "Invalid SHA-256 value" >&2
  exit 1
fi

actual_sha="$(sha256sum "${archive}" | awk '{print $1}')"
if [[ "${actual_sha}" != "${expected_sha}" ]]; then
  echo "Release archive checksum mismatch" >&2
  exit 1
fi

IFS=',' read -r -a raw_services <<< "${services_csv}"
services=()
for raw_service in "${raw_services[@]}"; do
  service="${raw_service#"${raw_service%%[![:space:]]*}"}"
  service="${service%"${service##*[![:space:]]}"}"
  if [[ ! "${service}" =~ ^[A-Za-z0-9_.@-]+\.service$ ]]; then
    echo "Invalid systemd service name: ${service}" >&2
    exit 1
  fi
  services+=("${service}")
done

if [[ "${#services[@]}" -eq 0 ]]; then
  echo "At least one systemd service is required" >&2
  exit 1
fi

releases_dir="${install_root}/releases"
release_dir="${releases_dir}/${release_name}"
current_link="${install_root}/current"
binary_dir="$(dirname "${binary_path}")"
staging_dir="${releases_dir}/.${release_name}.tmp.$$"
next_link="${install_root}/.current.next.$$"
next_binary="${binary_dir}/.nezumo.next.$$"
backup_binary="${binary_dir}/.nezumo.backup.$$"
old_target=""
needs_recovery=false
binary_replaced=false
binary_had_previous=false
current_switched=false

cleanup() {
  rm -rf -- "${staging_dir}"
  rm -f -- "${next_link}" "${next_binary}" "${backup_binary}" "${archive}"
}

recover_previous_release() {
  local original_status="$1"
  trap - ERR

  if [[ "${needs_recovery}" == true ]]; then
    echo "Deployment failed; restoring the previous binary and runtime resources" >&2
    "${elevate[@]}" systemctl stop "${services[@]}" || true

    if [[ "${binary_replaced}" == true ]]; then
      if [[ "${binary_had_previous}" == true && -f "${backup_binary}" ]]; then
        mv -f -- "${backup_binary}" "${binary_path}" || true
        "${elevate[@]}" chown "${binary_owner}:${binary_group}" "${binary_path}" || true
        "${elevate[@]}" chmod "${binary_mode}" "${binary_path}" || true
      else
        rm -f -- "${binary_path}" || true
      fi
    fi

    if [[ "${current_switched}" == true ]]; then
      if [[ -n "${old_target}" && -d "${old_target}" ]]; then
        ln -s -- "${old_target}" "${next_link}" || true
        mv -Tf -- "${next_link}" "${current_link}" || true
      else
        rm -f -- "${current_link}" || true
      fi
    fi

    "${elevate[@]}" systemctl start "${services[@]}" || true
    "${elevate[@]}" systemctl is-active --quiet "${services[@]}" || true
  fi

  exit "${original_status}"
}

trap cleanup EXIT
trap 'recover_previous_release $?' ERR

mkdir -p -- "${releases_dir}"

if [[ ! -d "${release_dir}" ]]; then
  mkdir -- "${staging_dir}"
  tar -xzf "${archive}" -C "${staging_dir}"

  if [[ ! -x "${staging_dir}/nezumo" ]]; then
    echo "The release package does not contain an executable nezumo binary" >&2
    exit 1
  fi
  if [[ ! -f "${staging_dir}/templates/mail/footer.html" ]]; then
    echo "The release package does not contain the mail template" >&2
    exit 1
  fi

  chmod 0755 "${staging_dir}"
  mv -- "${staging_dir}" "${release_dir}"
fi

if [[ -L "${current_link}" ]]; then
  old_target="$(readlink "${current_link}")"
  if [[ "${old_target}" != /* ]]; then
    old_target="${install_root}/${old_target}"
  fi
fi

install -d -m 0755 -- "${binary_dir}"

# Keep the old process running until the package has been fully validated.
# From this point onward any error triggers recovery and starts the old service.
needs_recovery=true
"${elevate[@]}" systemctl stop "${services[@]}"

if [[ -f "${binary_path}" ]]; then
  cp -- "${binary_path}" "${backup_binary}"
  binary_had_previous=true
fi

install -m "${binary_mode}" -- "${release_dir}/nezumo" "${next_binary}"
"${elevate[@]}" chown "${binary_owner}:${binary_group}" "${next_binary}"
"${elevate[@]}" chmod "${binary_mode}" "${next_binary}"
mv -f -- "${next_binary}" "${binary_path}"
binary_replaced=true

ln -s -- "${release_dir}" "${next_link}"
mv -Tf -- "${next_link}" "${current_link}"
current_switched=true

"${elevate[@]}" systemctl start "${services[@]}"
"${elevate[@]}" systemctl is-active --quiet "${services[@]}"
needs_recovery=false

rm -f -- "${backup_binary}"
echo "Release ${release_name} deployed successfully"
