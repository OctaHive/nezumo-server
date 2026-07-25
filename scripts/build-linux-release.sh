#!/usr/bin/env bash

# Builds a deployable x86-64 Linux package locally without publishing it.
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${BUILD_OUTPUT_DIR:-${repo_root}/dist}"
build_platform="${BUILD_PLATFORM:-linux/amd64}"

for command_name in cargo docker awk git; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Required command is not installed: ${command_name}" >&2
    exit 1
  fi
done

if [[ "${build_platform}" != "linux/amd64" ]]; then
  echo "BUILD_PLATFORM must be linux/amd64 for the configured production package" >&2
  exit 1
fi

(
  cd "${repo_root}"
  cargo metadata --locked --no-deps --format-version 1 >/dev/null
)

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

if [[ ! "${package_version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?([+][0-9A-Za-z.-]+)?$ ]]; then
  echo "Could not read a valid package version from Cargo.toml" >&2
  exit 1
fi

source_revision="$(git -C "${repo_root}" rev-parse HEAD)"
if [[ -n "$(git -C "${repo_root}" status --porcelain)" ]]; then
  source_revision="${source_revision}-dirty"
fi

staging_dir="$(mktemp -d)"
cleanup() {
  rm -rf -- "${staging_dir}"
}
trap cleanup EXIT

echo "Building Nezumo ${package_version} for ${build_platform}..." >&2
docker build \
  --platform "${build_platform}" \
  --file "${repo_root}/deploy/Dockerfile.release" \
  --target artifact \
  --build-arg "PACKAGE_VERSION=${package_version}" \
  --build-arg "SOURCE_REVISION=${source_revision}" \
  --output "type=local,dest=${staging_dir}" \
  "${repo_root}"

archive_name="nezumo-${package_version}-x86_64-unknown-linux-gnu.tar.gz"
install -d -m 0755 "${output_dir}"
install -m 0644 "${staging_dir}/${archive_name}" "${output_dir}/${archive_name}"
install -m 0644 "${staging_dir}/${archive_name}.sha256" "${output_dir}/${archive_name}.sha256"

echo "Linux package created: ${output_dir}/${archive_name}" >&2
printf '%s\n' "${output_dir}/${archive_name}"
