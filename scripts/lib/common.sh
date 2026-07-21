#!/usr/bin/env bash
set -euo pipefail

export GIT_TERMINAL_PROMPT="${GIT_TERMINAL_PROMPT:-0}"
export GCM_INTERACTIVE="${GCM_INTERACTIVE:-Never}"

DEFAULT_WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ ! -f "$DEFAULT_WORKSPACE_ROOT/state/TASKS.json" ]] &&
  [[ -f "$DEFAULT_WORKSPACE_ROOT/../../state/TASKS.json" ]]; then
  DEFAULT_WORKSPACE_ROOT="$(cd "$DEFAULT_WORKSPACE_ROOT/../.." && pwd)"
fi
WORKSPACE_ROOT="${CMCLIENT_WORKSPACE_ROOT:-$DEFAULT_WORKSPACE_ROOT}"

if [[ -f "$WORKSPACE_ROOT/workspace.env" ]]; then
  # shellcheck disable=SC1091
  source "$WORKSPACE_ROOT/workspace.env"
fi

CMCLIENT_REPO_URL="${CMCLIENT_REPO_URL:-https://github.com/toodi0418/CMClient.git}"
CMCLIENT_BRANCH="${CMCLIENT_BRANCH:-dev}"
CMCLIENT_REMOTE="${CMCLIENT_REMOTE:-origin}"
CMCLIENT_REPO_DIR="${CMCLIENT_REPO_DIR:-repository/CMClient}"
CMCLIENT_AUTO_PUSH="${CMCLIENT_AUTO_PUSH:-1}"
CMCLIENT_VERIFY_MODE="${CMCLIENT_VERIFY_MODE:-auto}"
CMCLIENT_PYTHON3="${CMCLIENT_PYTHON3:-python3}"
export CMCLIENT_PYTHON3
CMCLIENT_CAMPAIGN_ROOT="${CMCLIENT_CAMPAIGN_ROOT:-}"
CMCLIENT_VERIFY_REPO_DIR="${CMCLIENT_VERIFY_REPO_DIR:-}"
CMCLIENT_WINDOWS_CAMPAIGN_TOOLCHAIN="${CMCLIENT_WINDOWS_CAMPAIGN_TOOLCHAIN:-0}"
CMCLIENT_WINDOWS_TOOLCHAIN_ROOT="${CMCLIENT_WINDOWS_TOOLCHAIN_ROOT:-$CMCLIENT_CAMPAIGN_ROOT}"

if [[ "$CMCLIENT_REPO_DIR" = /* ]]; then
  REPO_DIR="$CMCLIENT_REPO_DIR"
else
  REPO_DIR="$WORKSPACE_ROOT/$CMCLIENT_REPO_DIR"
fi

if [[ -z "$CMCLIENT_VERIFY_REPO_DIR" ]]; then
  VERIFY_REPO_DIR="$REPO_DIR"
elif [[ "$CMCLIENT_VERIFY_REPO_DIR" = /* ]]; then
  VERIFY_REPO_DIR="$CMCLIENT_VERIFY_REPO_DIR"
else
  VERIFY_REPO_DIR="$WORKSPACE_ROOT/$CMCLIENT_VERIFY_REPO_DIR"
fi

log() { printf '[cm2-workspace] %s\n' "$*"; }
warn() { printf '[cm2-workspace] WARNING: %s\n' "$*" >&2; }
die() { printf '[cm2-workspace] ERROR: %s\n' "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "缺少必要命令：$1"
}

require_python3() {
  [[ -x "$CMCLIENT_PYTHON3" ]] || command -v "$CMCLIENT_PYTHON3" >/dev/null 2>&1 ||
    die "缺少必要命令：Python 3（CMCLIENT_PYTHON3=$CMCLIENT_PYTHON3）"
}

activate_windows_campaign_toolchain() {
  [[ "$CMCLIENT_WINDOWS_CAMPAIGN_TOOLCHAIN" == "1" ]] || return 0
  [[ "${OS:-}" == "Windows_NT" ]] || return 0
  [[ -n "$CMCLIENT_CAMPAIGN_ROOT" ]] || die "CMCLIENT_CAMPAIGN_ROOT is required"
  command -v cygpath >/dev/null 2>&1 || die "cygpath is required on Windows"

  local campaign_windows toolchain_windows sysroot
  local msvc_root sdk_include_root sdk_lib_root
  local msvc_short sdk_include_short sdk_lib_short
  local -a msvc_roots sdk_include_roots sdk_lib_roots
  campaign_windows="$(cygpath -m "$CMCLIENT_CAMPAIGN_ROOT")"
  campaign_windows="${campaign_windows%/}"
  toolchain_windows="$(cygpath -m "$CMCLIENT_WINDOWS_TOOLCHAIN_ROOT")"
  toolchain_windows="${toolchain_windows%/}"
  sysroot="$campaign_windows/dependencies/windows-msvc-winsysroot"
  mapfile -t msvc_roots < <(printf '%s\n' "$sysroot"/VC/Tools/MSVC/*)
  mapfile -t sdk_include_roots < <(printf '%s\n' "$sysroot"/Windows\ Kits/10/Include/*)
  mapfile -t sdk_lib_roots < <(printf '%s\n' "$sysroot"/Windows\ Kits/10/Lib/*)
  [[ "${#msvc_roots[@]}" -eq 1 && "${#sdk_include_roots[@]}" -eq 1 && "${#sdk_lib_roots[@]}" -eq 1 ]] ||
    die "Campaign Windows MSVC winsysroot version layout is invalid"
  msvc_root="${msvc_roots[0]}"
  sdk_include_root="${sdk_include_roots[0]}"
  sdk_lib_root="${sdk_lib_roots[0]}"
  [[ -f "$sdk_include_root/um/Windows.h" && -f "$sdk_lib_root/um/x64/kernel32.Lib" && -f "$msvc_root/lib/x64/libcmt.lib" ]] ||
    die "Windows MSVC sysroot is missing from the configured toolchain root"
  msvc_short="$(cygpath -d "$msvc_root")"
  sdk_include_short="$(cygpath -d "$sdk_include_root")"
  sdk_lib_short="$(cygpath -d "$sdk_lib_root")"
  msvc_short="${msvc_short//\\//}"
  sdk_include_short="${sdk_include_short//\\//}"
  sdk_lib_short="${sdk_lib_short//\\//}"
  [[ "$msvc_short$sdk_include_short$sdk_lib_short" != *[[:space:]]* ]] ||
    die "Campaign Windows MSVC sysroot requires whitespace-free short paths"
  mkdir -p \
    "$CMCLIENT_CAMPAIGN_ROOT/tmp" \
    "$CMCLIENT_CAMPAIGN_ROOT/cache/npm" \
    "$CMCLIENT_CAMPAIGN_ROOT/dependencies/cargo"

  export PATH="$CMCLIENT_WINDOWS_TOOLCHAIN_ROOT/tooling/node:$CMCLIENT_WINDOWS_TOOLCHAIN_ROOT/tooling/pnpm:$CMCLIENT_CAMPAIGN_ROOT/dependencies/cargo/bin:/c/msys64/ucrt64/bin:$CMCLIENT_WINDOWS_TOOLCHAIN_ROOT/tooling/cargo/bin:$PATH:/c/msys64/usr/bin"
  export TEMP="$campaign_windows/tmp"
  export TMP="$campaign_windows/tmp"
  export TMPDIR="$campaign_windows/tmp"
  export CI="true"
  export npm_config_cache="$campaign_windows/cache/npm"
  export npm_config_store_dir="$campaign_windows/cache/pnpm-store"
  export npm_config_virtual_store_dir="$campaign_windows/dependencies/pnpm-virtual-store"
  export npm_config_virtual_store_dir_max_length="60"
  export npm_config_confirm_modules_purge="false"
  export XDG_CACHE_HOME="$campaign_windows/cache"
  export CARGO_HOME="$campaign_windows/dependencies/cargo"
  export CARGO_CACHE_AUTO_CLEAN_FREQUENCY="never"
  export CARGO_NET_OFFLINE="true"
  export RUSTUP_HOME="$toolchain_windows/tooling/rustup"
  export RUSTUP_TOOLCHAIN="1.96.0-x86_64-pc-windows-gnu"
  export CARGO_TARGET_DIR="$campaign_windows/build/cargo-msvc"
  export CARGO_BUILD_TARGET="x86_64-pc-windows-msvc"
  export CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_LINKER="lld-link"
  export CC_x86_64_pc_windows_msvc="clang-cl"
  export CXX_x86_64_pc_windows_msvc="clang-cl"
  export AR_x86_64_pc_windows_msvc="llvm-lib"
  export TARGET_CC="clang-cl"
  export TARGET_CXX="clang-cl"
  export TARGET_AR="llvm-lib"
  export MSYS2_ENV_CONV_EXCL="CFLAGS_x86_64_pc_windows_msvc;CXXFLAGS_x86_64_pc_windows_msvc;BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_msvc"
  export INCLUDE="$msvc_root/include;$sdk_include_root/ucrt;$sdk_include_root/shared;$sdk_include_root/um;$sdk_include_root/winrt;$sdk_include_root/cppwinrt"
  export LIB="$msvc_root/lib/x64;$sdk_lib_root/ucrt/x64;$sdk_lib_root/um/x64"
  export CFLAGS_x86_64_pc_windows_msvc="/imsvc $msvc_short/include /imsvc $sdk_include_short/ucrt /imsvc $sdk_include_short/shared /imsvc $sdk_include_short/um /imsvc $sdk_include_short/winrt /imsvc $sdk_include_short/cppwinrt"
  export CXXFLAGS_x86_64_pc_windows_msvc="$CFLAGS_x86_64_pc_windows_msvc"
  export BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_msvc="--target=x86_64-pc-windows-msvc -isystem$msvc_short/include -isystem$sdk_include_short/ucrt -isystem$sdk_include_short/shared -isystem$sdk_include_short/um -isystem$sdk_include_short/winrt -isystem$sdk_include_short/cppwinrt"
  unset RUSTFLAGS
  export CARGO_TARGET_X86_64_PC_WINDOWS_MSVC_RUSTFLAGS="-C linker-flavor=lld-link -Lnative=$msvc_short/lib/x64 -Lnative=$sdk_lib_short/ucrt/x64 -Lnative=$sdk_lib_short/um/x64 -C link-arg=-defaultlib:oldnames"
  export PLAYWRIGHT_BROWSERS_PATH="$toolchain_windows/browsers/playwright"
}

require_repo() {
  [[ "$(git -C "$REPO_DIR" rev-parse --is-inside-work-tree 2>/dev/null || true)" == "true" ]] ||
    die "Repository 尚未建立：$REPO_DIR，請先執行 scripts/bootstrap.sh"
}

require_dev_branch() {
  local branch
  branch="$(git -C "$REPO_DIR" branch --show-current)"
  [[ "$branch" == "$CMCLIENT_BRANCH" ]] || die "目前分支是 '$branch'，必須是 '$CMCLIENT_BRANCH'"
  [[ "$branch" != "main" ]] || die "禁止在 main 執行自動 checkpoint"
}
