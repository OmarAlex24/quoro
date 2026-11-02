#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# fetch-pinned.sh <dest-dir>: materialise the artifacts pinned in ../pins.txt
# for this machine's architecture in <dest-dir>.
#
# Every file is verified against its pinned sha256: a missing file is
# downloaded to <file>.part, verified and renamed into place; an existing file
# is verified on every run, and a mismatch deletes it and fails. The engine
# never runs an artifact this script did not check.

set -euo pipefail

# File name of the pin that is executed, so it needs the executable bit.
readonly EXECUTABLE_PIN="cloud-hypervisor"

die() {
  printf 'fetch-pinned: %s\n' "$*" >&2
  exit 1
}

# Checks <file> against <sha256> with `sha256sum -c`, which prints the file and
# OK/FAILED. Returns non-zero on a mismatch.
verify_pin() {
  local file="$1" sha256="$2"
  local output
  if output="$(printf '%s  %s\n' "$sha256" "$file" | sha256sum --check - 2>&1)"; then
    printf '%s\n' "$output"
    return 0
  fi
  printf '%s\n' "$output"
  return 1
}

main() {
  local dest="${1-}"
  [[ -n "$dest" ]] || die 'usage: fetch-pinned.sh <dest-dir>'

  local script_dir pins_file
  script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
  pins_file="$script_dir/../pins.txt"
  [[ -r "$pins_file" ]] || die "cannot read $pins_file"

  local arch
  arch="$(uname -m)"
  mkdir -p -- "$dest"

  local name version pin_arch file url sha256
  local found=0
  while read -r name version pin_arch file url sha256; do
    case "$name" in
      '' | '#'*) continue ;;
    esac
    [[ "$pin_arch" == "$arch" ]] || continue
    found=$((found + 1))

    local target="$dest/$file"
    local part="$target.part"

    if [[ -f "$target" ]]; then
      printf 'fetch-pinned: verifying %s %s\n' "$name" "$version"
      if ! verify_pin "$target" "$sha256"; then
        rm -f -- "$target"
        die "$target did not match its pin and was removed"
      fi
    else
      printf 'fetch-pinned: downloading %s %s\n' "$name" "$version"
      rm -f -- "$part"
      curl --fail --location --retry 3 --silent --show-error \
        --output "$part" "$url" || die "download failed: $url"
      if ! verify_pin "$part" "$sha256"; then
        rm -f -- "$part"
        die "$name $version did not match its pin; the download was discarded"
      fi
      mv -- "$part" "$target"
    fi

    if [[ "$name" == "$EXECUTABLE_PIN" ]]; then
      chmod 0755 -- "$target"
    fi
  done <"$pins_file"

  [[ "$found" -gt 0 ]] || die "no pins for $arch in $pins_file"
  printf 'fetch-pinned: %d artifact(s) verified in %s\n' "$found" "$dest"
}

main "$@"
