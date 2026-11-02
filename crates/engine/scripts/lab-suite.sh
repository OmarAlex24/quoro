#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# lab-suite.sh [--work-dir DIR] [--only vmm|net|ch|boot|image]...
#
# Prepare every input the `#[ignore]`d KVM lab tests of the engine need and run
# them as root on a Debian 13 x86_64 host with /dev/kvm and passwordless sudo:
#
#   crates/engine/scripts/lab-suite.sh
#
# The script downloads and verifies the pinned Cloud Hypervisor and guest
# kernel, builds the static musl `quoro-init`, builds the two probe root
# filesystems, resolves the `alpine:3.22` digest from Docker Hub and then runs
# each suite with `--ignored --test-threads=1`. It never installs packages and
# never writes outside the work directory and the cargo target directory.

set -euo pipefail

# Debian keeps mkfs.ext4, nft and ip in the sbin directories, which a normal
# user's PATH does not list.
PATH="$PATH:/usr/sbin:/sbin"
export PATH

DEFAULT_SUITES=(vmm net image boot ch)
ALPINE_REPOSITORY="library/alpine"
ALPINE_TAG="3.22"

PROBE_INIT='#!/bin/busybox sh
/bin/busybox poweroff -f'
PB_INIT='#!/bin/busybox sh
exec /bin/busybox sleep 2147483647'

SCRIPT_DIR=""
REPO_ROOT=""
WORK=""
MKFS=""
BUSYBOX=""
ALPINE_DIGEST=""
DNS=""
INIT_BIN=""
TARGET_DIR=""
FAILED=0
SUITES=()
RESULTS=()
BASE_ENV=()
SUITE_ENV=()

die() {
  printf 'lab-suite: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
lab-suite.sh: prepare and run the engine's KVM lab suites on a Debian 13
x86_64 host with /dev/kvm and passwordless sudo.

Usage:
  crates/engine/scripts/lab-suite.sh [--work-dir DIR] [--only SUITE]...

Options:
  --work-dir DIR   where artifacts, probe images and scratch state live
                   (default: ${XDG_CACHE_HOME:-$HOME/.cache}/quoro-engine-lab)
  --only SUITE     run only this suite; repeatable; the default order is
                   vmm net image boot ch
  -h, --help       print this help

Suites:
  vmm    ch::vmm              VMM process layer (probe root filesystems)
  net    ch::net              bridge, TAP and network namespace lifecycle
  image  image::live_tests    live Docker Hub image conversion
  boot   boot_lab             image pipeline booted end to end
  ch     ch_lab               engine integration suite

Environment honoured by this script (the QUORO_ENGINE_* variables are set for
each suite; export one to override what the script would choose):

  XDG_CACHE_HOME            base of the default work directory
  QUORO_ENGINE_ALPINE       digest (sha256:...) of alpine:3.22; resolved live
                            from Docker Hub when unset
  QUORO_ENGINE_DNS          guest DNS servers; defaults to the host's first
                            non-loopback nameserver
  CARGO_TARGET_DIR          honoured for the init build, the test build and
                            the suites
  CARGO_BUILD_JOBS          build parallelism (default 2)

Environment set per suite:

  QUORO_ENGINE_LAB=1                 every suite
  QUORO_ENGINE_ARTIFACTS             WORK/artifacts (verified pinned files)
  QUORO_ENGINE_INIT                  the built static musl quoro-init
  QUORO_ENGINE_PROBE_DIR             WORK/probe (probe.ext4, pb.ext4)
  QUORO_ENGINE_STATE                 WORK/state
  QUORO_ENGINE_SCRATCH               WORK/scratch
  QUORO_ENGINE_NET_TESTS=1           image suite
  QUORO_IMAGE_TEST_ROOT              WORK/images (image suite)
  QUORO_PRESERVE_OWNERSHIP=1         image suite
  QUORO_MKFS                         path of mkfs.ext4
EOF
}

add_suite() {
  local suite="$1" existing
  case "$suite" in
    vmm | net | image | boot | ch) ;;
    *) die "unknown suite: $suite (choose vmm, net, image, boot or ch)" ;;
  esac
  for existing in "${SUITES[@]}"; do
    if [[ "$existing" == "$suite" ]]; then
      return
    fi
  done
  SUITES+=("$suite")
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --work-dir)
        [[ $# -ge 2 ]] || die '--work-dir needs a directory'
        WORK="$2"
        shift 2
        ;;
      --work-dir=*)
        WORK="${1#*=}"
        shift
        ;;
      --only)
        [[ $# -ge 2 ]] || die '--only needs a suite name'
        add_suite "$2"
        shift 2
        ;;
      --only=*)
        add_suite "${1#*=}"
        shift
        ;;
      -h | --help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1 (see --help)"
        ;;
    esac
  done
  if ((${#SUITES[@]} == 0)); then
    SUITES=("${DEFAULT_SUITES[@]}")
  fi
}

preflight() {
  [[ "$(uname -s)" == "Linux" ]] ||
    die "this script only runs on Linux (the lab tests need KVM)"
  [[ "$(uname -m)" == "x86_64" ]] ||
    die "this script only supports x86_64: pins.txt has no artifacts for $(uname -m)"
  [[ -e /dev/kvm ]] ||
    die '/dev/kvm is missing: enable KVM/VT-x on the host (kvm_intel or kvm_amd)'
  if ! sudo -n true 2>/dev/null; then
    die 'passwordless sudo is required: the lab suites run as root'
  fi

  local -a missing=()
  local spec tool pkg kind
  for spec in \
    'cargo|install Rust with rustup (https://rustup.rs)' \
    'rustup|install Rust with rustup (https://rustup.rs)' \
    'curl|apt-get install curl' \
    'sha256sum|apt-get install coreutils' \
    'python3|apt-get install python3' \
    'mkfs.ext4|apt-get install e2fsprogs' \
    'nft|apt-get install nftables' \
    'ip|apt-get install iproute2' \
    'busybox|apt-get install busybox-static'; do
    tool="${spec%%|*}"
    pkg="${spec#*|}"
    if ! command -v "$tool" >/dev/null 2>&1; then
      missing+=("$tool: $pkg")
    fi
  done
  if ((${#missing[@]} > 0)); then
    printf 'lab-suite: missing required tools:\n' >&2
    printf '  %s\n' "${missing[@]}" >&2
    die 'install the packages above and retry (this script never installs anything)'
  fi

  MKFS="$(command -v mkfs.ext4)"
  BUSYBOX="$(command -v busybox)"
  if ! "$BUSYBOX" --help >/dev/null 2>&1; then
    die "$BUSYBOX does not run: install busybox-static"
  fi
  if command -v file >/dev/null 2>&1; then
    kind="$(file -b -- "$BUSYBOX")"
    if [[ "$kind" != *statically\ linked* ]]; then
      die "$BUSYBOX is not static: apt-get install busybox-static"
    fi
  elif command -v ldd >/dev/null 2>&1; then
    if ldd -- "$BUSYBOX" >/dev/null 2>&1; then
      die "$BUSYBOX is dynamically linked: apt-get install busybox-static"
    fi
  fi

  if ! command -v musl-gcc >/dev/null 2>&1; then
    printf 'lab-suite: note: musl-gcc not found; apt-get install musl-tools if the\n' >&2
    printf 'lab-suite:       x86_64-unknown-linux-musl build fails to link\n' >&2
  fi
}

setup_work_dir() {
  if [[ -z "$WORK" ]]; then
    WORK="${XDG_CACHE_HOME:-$HOME/.cache}/quoro-engine-lab"
  fi
  mkdir -p -- "$WORK"
  WORK="$(cd -- "$WORK" && pwd)"
  mkdir -p -- "$WORK"/{artifacts,probe,state,scratch,images}
  printf 'lab-suite: work directory: %s\n' "$WORK"
}

resolve_target_dir() {
  if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    case "$CARGO_TARGET_DIR" in
      /*) TARGET_DIR="$CARGO_TARGET_DIR" ;;
      *) TARGET_DIR="$REPO_ROOT/$CARGO_TARGET_DIR" ;;
    esac
  else
    TARGET_DIR="$REPO_ROOT/target"
  fi
}

fetch_artifacts() {
  printf 'lab-suite: fetching and verifying the pinned artifacts\n'
  bash -- "$SCRIPT_DIR/fetch-pinned.sh" "$WORK/artifacts"
}

build_init() {
  printf 'lab-suite: building quoro-init for x86_64-unknown-linux-musl\n'
  rustup target add x86_64-unknown-linux-musl
  nice -n 10 env CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" \
    cargo build --release --locked --target x86_64-unknown-linux-musl \
    -p quoro-init --bin quoro-init
  INIT_BIN="$TARGET_DIR/x86_64-unknown-linux-musl/release/quoro-init"
  [[ -x "$INIT_BIN" ]] || die "the musl quoro-init is missing at $INIT_BIN"
}

stage_probe_image() {
  local image="$1" label="$2" init="$3"
  local stage
  stage="$(mktemp -d -- "$WORK/probe/.stage.XXXXXX")"
  mkdir -p -- "$stage/bin" "$stage/quoro" "$stage/dev" "$stage/proc" "$stage/sys"
  cp -- "$BUSYBOX" "$stage/bin/busybox"
  chmod 0755 -- "$stage/bin/busybox"
  printf '%s\n' "$init" >"$stage/quoro/init"
  chmod 0755 -- "$stage/quoro/init"
  "$MKFS" -q -F -d "$stage" -L "$label" "$WORK/probe/$image" 32M
  rm -rf -- "$stage"
}

build_probes() {
  local probe_dir="$WORK/probe"
  local stamp="$probe_dir/busybox.sha256"
  local sha
  sha="$(sha256sum -- "$BUSYBOX" | awk '{print $1}')"
  if [[ -f "$probe_dir/probe.ext4" && -f "$probe_dir/pb.ext4" && -f "$stamp" ]]; then
    if [[ "$(cat -- "$stamp")" == "$sha" ]]; then
      printf 'lab-suite: probe images are current\n'
      return
    fi
  fi
  printf 'lab-suite: building the probe images\n'
  stage_probe_image probe.ext4 quoro-probe "$PROBE_INIT"
  stage_probe_image pb.ext4 quoro-pb "$PB_INIT"
  printf '%s\n' "$sha" >"$stamp"
}

fetch_alpine_digest() {
  local token_json token headers digest
  printf 'lab-suite: resolving %s:%s from Docker Hub\n' "$ALPINE_REPOSITORY" "$ALPINE_TAG" >&2
  token_json="$(curl --fail --location --retry 3 --silent --show-error \
    "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${ALPINE_REPOSITORY}:pull")" ||
    die 'the Docker Hub token request failed'
  token="$(printf '%s' "$token_json" | python3 -I -c '
import json
import sys

data = json.load(sys.stdin)
print(data.get("token") or data.get("access_token") or "")
')" || die 'the Docker Hub token response did not parse'
  [[ -n "$token" ]] || die 'the Docker Hub token response carried no token'
  headers="$(curl --fail --silent --show-error --head --retry 3 \
    --header 'Accept: application/vnd.oci.image.index.v1+json' \
    --header 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
    --header 'Accept: application/vnd.oci.image.manifest.v1+json' \
    --header 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    --header "Authorization: Bearer ${token}" \
    "https://registry-1.docker.io/v2/${ALPINE_REPOSITORY}/manifests/${ALPINE_TAG}" |
    tr -d '\r')" ||
    die 'the Docker Hub manifest request failed'
  digest="$(printf '%s\n' "$headers" | tr '[:upper:]' '[:lower:]' |
    sed -n 's/^docker-content-digest:[[:space:]]*//p' | tail -n 1 | tr -d '[:space:]')"
  [[ -n "$digest" ]] || die 'the manifest response had no docker-content-digest header'
  printf '%s\n' "$digest"
}

resolve_alpine() {
  local digest="${QUORO_ENGINE_ALPINE:-}"
  if [[ -z "$digest" ]]; then
    digest="$(fetch_alpine_digest)"
  fi
  if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    die "not a sha256:... digest of alpine:3.22: ${digest:-<empty>}"
  fi
  ALPINE_DIGEST="$digest"
  printf 'lab-suite: alpine:%s digest: %s\n' "$ALPINE_TAG" "$ALPINE_DIGEST"
}

resolve_dns() {
  if [[ -n "${QUORO_ENGINE_DNS:-}" ]]; then
    DNS="$QUORO_ENGINE_DNS"
    return
  fi
  local file server
  for file in /etc/resolv.conf /run/systemd/resolve/resolv.conf; do
    [[ -r "$file" ]] || continue
    server="$(awk '
      $1 == "nameserver" &&
      $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ &&
      $2 !~ /^127\./ { print $2; exit }
    ' "$file")"
    if [[ -n "$server" ]]; then
      DNS="$server"
      return
    fi
  done
  printf 'lab-suite: no non-loopback DNS server found; QUORO_ENGINE_DNS stays unset\n' >&2
}

build_tests() {
  printf 'lab-suite: building the engine test binaries as %s\n' "$(id -un)"
  nice -n 10 env CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" \
    cargo test --no-run --locked -p quoro-engine --tests --lib
}

base_env() {
  BASE_ENV=(
    "QUORO_ENGINE_LAB=1"
    "QUORO_ENGINE_ARTIFACTS=$WORK/artifacts"
    "QUORO_ENGINE_INIT=$INIT_BIN"
    "QUORO_ENGINE_ALPINE=$ALPINE_DIGEST"
    "QUORO_MKFS=$MKFS"
  )
  if [[ -n "$DNS" ]]; then
    BASE_ENV+=("QUORO_ENGINE_DNS=$DNS")
  fi
  if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    BASE_ENV+=("CARGO_TARGET_DIR=$TARGET_DIR")
  fi
  # sudo may reset HOME, and root's own rustup has no toolchain: name the
  # caller's toolchain and registry explicitly.
  BASE_ENV+=(
    "CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}"
    "RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup}"
  )
}

run_suite() {
  local label="$1"
  shift
  printf '\n==> lab suite: %s\n' "$label"
  if sudo -E env "PATH=$PATH" "${BASE_ENV[@]}" "${SUITE_ENV[@]}" \
    cargo test --locked -p quoro-engine "$@" -- --ignored --test-threads=1; then
    RESULTS+=("PASS $label")
    printf 'lab-suite: %s PASS\n' "$label"
  else
    RESULTS+=("FAIL $label")
    FAILED=1
    printf 'lab-suite: %s FAIL\n' "$label" >&2
  fi
}

run_vmm() {
  SUITE_ENV=("QUORO_ENGINE_PROBE_DIR=$WORK/probe")
  run_suite vmm --lib ch::vmm
}

run_net() {
  SUITE_ENV=()
  run_suite net --lib ch::net
}

run_image() {
  SUITE_ENV=(
    "QUORO_ENGINE_NET_TESTS=1"
    "QUORO_IMAGE_TEST_ROOT=$WORK/images"
    "QUORO_PRESERVE_OWNERSHIP=1"
  )
  run_suite image --lib image::live_tests
}

run_boot() {
  SUITE_ENV=("QUORO_ENGINE_SCRATCH=$WORK/scratch")
  run_suite boot --test boot_lab
}

run_ch() {
  SUITE_ENV=("QUORO_ENGINE_STATE=$WORK/state")
  run_suite ch --test ch_lab
}

run_suites() {
  local suite
  for suite in "${SUITES[@]}"; do
    case "$suite" in
      vmm) run_vmm ;;
      net) run_net ;;
      image) run_image ;;
      boot) run_boot ;;
      ch) run_ch ;;
      *) die "unknown suite: $suite" ;;
    esac
  done
}

restore_ownership() {
  local owner
  owner="$(id -u):$(id -g)"
  printf '\nlab-suite: restoring ownership of %s and %s\n' "$TARGET_DIR" "$WORK"
  if [[ -d "$TARGET_DIR" ]]; then
    sudo chown -R -- "$owner" "$TARGET_DIR"
  fi
  sudo chown -R -- "$owner" "$WORK"
}

report() {
  printf '\nlab-suite: summary\n'
  local line
  for line in "${RESULTS[@]}"; do
    printf '  %s\n' "$line"
  done
  printf '\nTo free space: (cd %s && cargo clean) and rm -rf %s\n' \
    "$REPO_ROOT" "$WORK"
  if ((FAILED != 0)); then
    printf 'lab-suite: some suites failed\n' >&2
    return 1
  fi
  printf 'lab-suite: every requested suite passed\n'
}

main() {
  SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
  [[ -f "$REPO_ROOT/Cargo.toml" ]] ||
    die "cannot find the repository root above $SCRIPT_DIR"
  [[ -x "$SCRIPT_DIR/fetch-pinned.sh" || -f "$SCRIPT_DIR/fetch-pinned.sh" ]] ||
    die "cannot find $SCRIPT_DIR/fetch-pinned.sh"

  parse_args "$@"
  preflight
  setup_work_dir
  cd -- "$REPO_ROOT"
  resolve_target_dir

  fetch_artifacts
  build_init
  build_probes
  resolve_alpine
  resolve_dns
  build_tests
  base_env
  run_suites
  restore_ownership
  report
}

main "$@"
