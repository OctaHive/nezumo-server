#!/usr/bin/env bash

# Runs on the target Linux server. GitHub Actions streams this file over SSH.
set -Eeuo pipefail

if [[ "$#" -ne 5 ]]; then
  echo "Usage: deploy-release.sh ARCHIVE DEPLOY_PATH_B64 SERVICES_B64 RELEASE_B64 SHA256" >&2
  exit 2
fi

archive="$1"
install_root="$(printf '%s' "$2" | base64 --decode)"
services_csv="$(printf '%s' "$3" | base64 --decode)"
release_name="$(printf '%s' "$4" | base64 --decode)"
expected_sha="$5"

if [[ ! -f "${archive}" ]]; then
  echo "Release archive does not exist: ${archive}" >&2
  exit 1
fi

if [[ "${install_root}" != /* || "${install_root}" == "/" ]]; then
  echo "DEPLOY_PATH must be an absolute directory other than /" >&2
  exit 1
fi

if [[ ! "${release_name}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid release name: ${release_name}" >&2
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
staging_dir="${releases_dir}/.${release_name}.tmp.$$"
next_link="${install_root}/.current.next.$$"
old_target=""

cleanup() {
  rm -rf -- "${staging_dir}"
  rm -f -- "${next_link}" "${archive}"
}
trap cleanup EXIT

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

ln -s -- "${release_dir}" "${next_link}"
mv -Tf -- "${next_link}" "${current_link}"

if sudo systemctl restart "${services[@]}" \
  && sudo systemctl is-active --quiet "${services[@]}"; then
  echo "Release ${release_name} deployed successfully"
  exit 0
fi

echo "One or more services failed after deployment; rolling back" >&2
if [[ -n "${old_target}" && -d "${old_target}" ]]; then
  ln -s -- "${old_target}" "${next_link}"
  mv -Tf -- "${next_link}" "${current_link}"
  sudo systemctl restart "${services[@]}" || true
else
  rm -f -- "${current_link}"
fi

exit 1
