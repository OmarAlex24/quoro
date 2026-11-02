#!/bin/sh
# Build the release artifact for one version: a static musl binary tarball and
# its SHA256SUMS. The build is reproducible: every input that can leak into
# the bytes (clock, paths, locale, archive metadata, toolchain flags) is
# pinned here. See packaging/README.md and ADR 0027.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/build-release.sh [VERSION]

Builds dist/quoro-VERSION-x86_64-unknown-linux-musl.tar.gz and dist/SHA256SUMS
from the current checkout. VERSION defaults to the version of the Cargo
package named by QUORO_PACKAGE (default: quoro).

Environment:
  QUORO_PACKAGE      Cargo package to build (default: quoro)
  QUORO_ALLOW_DIRTY  set to 1 to build from a dirty tree (not a release build)
  SOURCE_DATE_EPOCH  build timestamp; defaults to the last commit time
  CARGO_HOME         defaults to $HOME/.cargo
EOF
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

note() {
	printf '%s\n' "$*" >&2
}

case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
esac

if [ "$#" -gt 1 ]; then
	usage >&2
	exit 2
fi

# Always build from the repository root, never from the caller's directory:
# the remap-path-prefix values and the archive contents depend on it.
cd "$(dirname "$0")/.." || die "cannot enter the repository root"

export LC_ALL=C
export TZ=UTC
umask 022

QUORO_PACKAGE="${QUORO_PACKAGE:-quoro}"
[ -n "$QUORO_PACKAGE" ] || die "QUORO_PACKAGE is empty"

if [ "$#" -eq 1 ]; then
	version=$1
else
	# Read the package version without pulling in dependencies or the network.
	# python3 -I ignores PYTHON* environment variables and user site-packages,
	# so the parse cannot be influenced by the caller's environment.
	metadata=$(mktemp) || die "cannot create a temporary file"
	if ! cargo metadata --format-version 1 --no-deps --locked >"$metadata"; then
		rm -f "$metadata"
		die "cargo metadata failed"
	fi
	version=$(
		python3 -I -c '
import json
import sys

package = sys.argv[1]
metadata = json.load(sys.stdin)
matches = [p for p in metadata["packages"] if p["name"] == package]
if not matches:
    sys.exit("package not found in cargo metadata: " + package)
print(matches[0]["version"])
' "$QUORO_PACKAGE" <"$metadata"
	) || {
		rm -f "$metadata"
		die "cannot read the version of package $QUORO_PACKAGE (pass VERSION explicitly)"
	}
	rm -f "$metadata"
fi

[ -n "$version" ] || die "VERSION is empty"
case "$version" in
*/* | *[!A-Za-z0-9.+_-]*)
	die "VERSION contains characters that are unsafe in a file name: $version"
	;;
esac

# A release must be built from exactly the tree that the tag points at.
status=$(git status --porcelain) || die "cannot read the git status"
if [ -n "$status" ]; then
	if [ "${QUORO_ALLOW_DIRTY:-0}" = "1" ]; then
		note "warning: building from a dirty tree; the result is NOT a release build"
		printf '%s\n' "$status" >&2
	else
		note "error: the working tree is dirty; commit or stash first"
		printf '%s\n' "$status" >&2
		note "set QUORO_ALLOW_DIRTY=1 to build anyway (not a release build)"
		exit 1
	fi
fi

# Fixed timestamp for the archive metadata and for anything that honours
# SOURCE_DATE_EPOCH.
if [ -z "${SOURCE_DATE_EPOCH:-}" ]; then
	SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) || die "cannot read the commit timestamp"
fi
case "$SOURCE_DATE_EPOCH" in
'' | *[!0-9]*) die "SOURCE_DATE_EPOCH must be a non-negative integer" ;;
esac
export SOURCE_DATE_EPOCH

# One pinned toolchain, taken from rust-toolchain.toml, for local builds and CI.
channel=$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)
[ -n "$channel" ] || die "cannot read channel from rust-toolchain.toml"

rustup toolchain install "$channel" --profile minimal
rustup target add --toolchain "$channel" x86_64-unknown-linux-musl
rustc +"$channel" -vV

# Deterministic rustc/cargo behaviour.
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export CARGO_HOME
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_LTO=true
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_PROFILE_RELEASE_STRIP=symbols
export CARGO_PROFILE_RELEASE_DEBUG=0
export CARGO_PROFILE_RELEASE_PANIC=unwind
export CARGO_TARGET_DIR="$PWD/target/release-build"

# Overwrite RUSTFLAGS instead of appending: an inherited value would silently
# change the binary. rust-lld ships with the toolchain, so the host linker is
# not part of the build. Remapping the build paths keeps absolute paths out of
# panic messages, debug info and embedded strings.
RUSTFLAGS="-C linker=rust-lld -C target-feature=+crt-static --remap-path-prefix=$PWD=/build/quoro --remap-path-prefix=$CARGO_HOME=/build/cargo"
export RUSTFLAGS
unset CARGO_BUILD_RUSTFLAGS
unset RUSTC_WRAPPER
# Not named by the contract, but a workspace wrapper would change the compile
# commands just like RUSTC_WRAPPER does.
unset RUSTC_WORKSPACE_WRAPPER

# C code (a bundled SQLite later on) is compiled by the host C compiler, so
# pin it to musl-gcc and strip the build paths from its debug info as well.
if command -v musl-gcc >/dev/null 2>&1; then
	CC_x86_64_unknown_linux_musl=musl-gcc
	CFLAGS_x86_64_unknown_linux_musl="-ffile-prefix-map=$PWD=/build/quoro -ffile-prefix-map=$CARGO_HOME=/build/cargo"
	export CC_x86_64_unknown_linux_musl CFLAGS_x86_64_unknown_linux_musl
else
	note "note: musl-gcc not found; C dependencies would fall back to the default compiler"
fi

cargo +"$channel" build --release --locked --target x86_64-unknown-linux-musl \
	-p "$QUORO_PACKAGE" --bin quoro

binary="$CARGO_TARGET_DIR/x86_64-unknown-linux-musl/release/quoro"
[ -f "$binary" ] || die "build succeeded but $binary is missing"
[ -f LICENSE ] || die "LICENSE is missing from the repository root"
[ -f packaging/systemd/quoro.service ] ||
	die "packaging/systemd/quoro.service is missing; the release needs the systemd unit"

name="quoro-$version-x86_64-unknown-linux-musl"

stage=$(mktemp -d) || die "cannot create a staging directory"
cleanup() {
	rm -rf "$stage"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$stage/$name"
install -m 0755 "$binary" "$stage/$name/quoro"
install -m 0644 LICENSE "$stage/$name/LICENSE"
install -m 0644 packaging/systemd/quoro.service "$stage/$name/quoro.service"

# Normalise every staged timestamp, including symlinks and the directory
# entries themselves, because tar records all of them.
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/quoro"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/LICENSE"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/quoro.service"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage"

if ! tar --version 2>/dev/null | grep -q 'GNU tar'; then
	die "GNU tar is required for a reproducible archive (BSD tar is not supported)"
fi

rm -rf dist
mkdir -p dist

tarball="$name.tar.gz"
# --sort=name, fixed mtime, fixed ownership and a normalised mode mask make the
# archive independent of the filesystem; gzip -n drops the name and timestamp.
# The archive is written to a file first so that a tar failure is not hidden by
# the gzip exit status (POSIX sh has no pipefail).
archive=$(mktemp) || die "cannot create a temporary file"
tar --sort=name \
	--format=gnu \
	--mtime="@$SOURCE_DATE_EPOCH" \
	--owner=0 \
	--group=0 \
	--numeric-owner \
	--mode='u+rwX,go+rX,go-w' \
	-C "$stage" \
	-cf "$archive" "$name"
gzip -9 -n -c "$archive" >"dist/$tarball"
rm -f "$archive"

(
	cd dist
	sha256sum "$tarball" >SHA256SUMS
)

printf 'built: dist/%s\n' "$tarball"
printf 'sha256(binary): %s\n' "$(sha256sum "$binary")"
printf 'sha256(tarball): %s\n' "$(sha256sum "dist/$tarball")"
