#!/usr/bin/env bash
# Build the spike init + oci2rootfs and turn an image into an ext4 root disk.
#
#   ./build-rootfs.sh <image-ref> <out.ext4> [extra oci2rootfs args...]
#
# Runs on the lab VM (needs root via sudo, e2fsprogs and the musl target).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <image-ref> <out.ext4> [extra oci2rootfs args...]" >&2
  exit 2
fi

image_ref="$1"
out="$2"
shift 2

# oci2rootfs runs from $here, so make paths absolute before we cd.
case "$out" in
  /*) ;;
  *) out="$PWD/$out" ;;
esac

if [ ! -f "$here/init/Cargo.toml" ]; then
  echo "error: $here/init/Cargo.toml is missing." >&2
  echo "       The quoro-init crate (owned by the init worker) is not in this tree yet," >&2
  echo "       so there is no init binary to put at /quoro/init." >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  if [ -f "$HOME/.cargo/env" ]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
  fi
fi
if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo not found (looked on PATH and in ~/.cargo/env)." >&2
  exit 1
fi

cd "$here"

echo "==> building quoro-init (release, x86_64-unknown-linux-musl)"
cargo build --release --target x86_64-unknown-linux-musl -p quoro-init

echo "==> building oci2rootfs (release)"
cargo build --release -p oci2rootfs

init_bin="$here/target/x86_64-unknown-linux-musl/release/quoro-init"
tool_bin="$here/target/release/oci2rootfs"

if [ ! -x "$init_bin" ]; then
  echo "error: expected init binary at $init_bin" >&2
  exit 1
fi
if [ ! -x "$tool_bin" ]; then
  echo "error: expected oci2rootfs binary at $tool_bin" >&2
  exit 1
fi

echo "==> converting $image_ref -> $out"
sudo "$tool_bin" \
  --image "$image_ref" \
  --init "$init_bin" \
  --out "$out" \
  "$@"

sudo chown "$(id -u):$(id -g)" "$out"
echo "==> done: $out"
