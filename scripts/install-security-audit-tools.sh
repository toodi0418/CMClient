#!/usr/bin/env bash

set -euo pipefail

GITLEAKS_VERSION="8.30.1"
CARGO_AUDIT_VERSION="0.22.2"
ACTIONLINT_VERSION="1.7.12"

GITLEAKS_ARCHIVE="gitleaks_${GITLEAKS_VERSION}_linux_x64.tar.gz"
GITLEAKS_SHA256="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
CARGO_AUDIT_ARCHIVE="cargo-audit-x86_64-unknown-linux-musl-v${CARGO_AUDIT_VERSION}.tgz"
CARGO_AUDIT_DIRECTORY="cargo-audit-x86_64-unknown-linux-musl-v${CARGO_AUDIT_VERSION}"
CARGO_AUDIT_SHA256="7fb9497f8594b389e5fce5ef9b92db08432996895b2e0c5a0167a69ed445c428"
ACTIONLINT_ARCHIVE="actionlint_${ACTIONLINT_VERSION}_linux_amd64.tar.gz"
ACTIONLINT_SHA256="8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8"

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "SECURITY_AUDIT_TOOLS_UNSUPPORTED_PLATFORM" >&2
  exit 1
fi

destination="${1:-$PWD/.security-tools}"
work_directory="$(mktemp -d)"
trap 'rm -rf "$work_directory"' EXIT

download_and_verify() {
  local url="$1"
  local archive="$2"
  local sha256="$3"

  curl --proto '=https' --tlsv1.2 \
    --fail --silent --show-error --location \
    "$url" \
    --output "$work_directory/$archive"
  printf '%s  %s\n' "$sha256" "$archive" |
    (cd "$work_directory" && sha256sum --check --strict -)
}

mkdir -p "$destination"

download_and_verify \
  "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/${ACTIONLINT_ARCHIVE}" \
  "$ACTIONLINT_ARCHIVE" \
  "$ACTIONLINT_SHA256"
tar --extract --gzip --file "$work_directory/$ACTIONLINT_ARCHIVE" \
  --directory "$work_directory" actionlint
install -m 0755 "$work_directory/actionlint" "$destination/actionlint"

download_and_verify \
  "https://github.com/gitleaks/gitleaks/releases/download/v${GITLEAKS_VERSION}/${GITLEAKS_ARCHIVE}" \
  "$GITLEAKS_ARCHIVE" \
  "$GITLEAKS_SHA256"
tar --extract --gzip --file "$work_directory/$GITLEAKS_ARCHIVE" \
  --directory "$work_directory" gitleaks
install -m 0755 "$work_directory/gitleaks" "$destination/gitleaks"

download_and_verify \
  "https://github.com/rustsec/rustsec/releases/download/cargo-audit/v${CARGO_AUDIT_VERSION}/${CARGO_AUDIT_ARCHIVE}" \
  "$CARGO_AUDIT_ARCHIVE" \
  "$CARGO_AUDIT_SHA256"
tar --extract --gzip --file "$work_directory/$CARGO_AUDIT_ARCHIVE" \
  --directory "$work_directory" \
  --strip-components=1 \
  "$CARGO_AUDIT_DIRECTORY/cargo-audit"
install -m 0755 "$work_directory/cargo-audit" "$destination/cargo-audit"

"$destination/actionlint" -version
"$destination/gitleaks" version
"$destination/cargo-audit" --version
