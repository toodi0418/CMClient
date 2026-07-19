#!/usr/bin/env bash

set -euo pipefail

SYFT_VERSION="1.42.3"
SYFT_ARCHIVE="syft_${SYFT_VERSION}_linux_amd64.tar.gz"
SYFT_SHA256="0d6be741479eddd2c8644a288990c04f3df0d609bbc1599a005532a9dff63509"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "SBOM_TOOL_UNSUPPORTED_PLATFORM" >&2
  exit 1
fi

destination="${1:-$PWD/.sbom-tools}"
work_directory="$(mktemp -d)"
trap 'rm -rf "$work_directory"' EXIT

mkdir -p "$destination"
curl --proto '=https' --tlsv1.2 \
  --fail --silent --show-error --location \
  "https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/${SYFT_ARCHIVE}" \
  --output "$work_directory/$SYFT_ARCHIVE"
printf '%s  %s\n' "$SYFT_SHA256" "$SYFT_ARCHIVE" |
  (cd "$work_directory" && sha256sum --check --strict -)
tar --extract --gzip --file "$work_directory/$SYFT_ARCHIVE" \
  --directory "$work_directory" syft
install -m 0755 "$work_directory/syft" "$destination/syft"
"$destination/syft" version
