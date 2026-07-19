#!/usr/bin/env bash

set -euo pipefail

VALIDATOR_VERSION="2.0.0-alpha-1-20251018"

case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    VALIDATOR_ARCH="x86_64"
    VALIDATOR_SHA256="b10c8d39a0a917432af185afc92f1cd54b7f68aa70deda927acacf38ded84990"
    ;;
  Linux:aarch64)
    VALIDATOR_ARCH="aarch64"
    VALIDATOR_SHA256="79ca9d7b97ffbfb87838659cf7ffa35aa6956226c45bb038c323cda6843a49d4"
    ;;
  *)
    echo "APPIMAGE_VALIDATOR_UNSUPPORTED_PLATFORM" >&2
    exit 1
    ;;
esac

VALIDATOR_ASSET="validate-${VALIDATOR_ARCH}.AppImage"
destination="${1:-$PWD/.appimage-tools}"
work_directory="$(mktemp -d)"
trap 'rm -rf "$work_directory"' EXIT

mkdir -p "$destination"
curl --proto '=https' --tlsv1.2 \
  --fail --silent --show-error --location \
  "https://github.com/AppImageCommunity/AppImageUpdate/releases/download/${VALIDATOR_VERSION}/${VALIDATOR_ASSET}" \
  --output "$work_directory/$VALIDATOR_ASSET"
printf '%s  %s\n' "$VALIDATOR_SHA256" "$VALIDATOR_ASSET" |
  (cd "$work_directory" && sha256sum --check --strict -)
install -m 0755 "$work_directory/$VALIDATOR_ASSET" "$destination/validate"
