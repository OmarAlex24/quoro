#!/bin/sh
# Build the release artifact for one version: a static musl binary tarball and
# its SHA256SUMS. The build is reproducible: every input that can leak into
# the bytes (clock, paths, locale, archive metadata, toolchain flags) is
# pinned here, and the build environment itself (C compiler, binutils, musl)
# is pinned by the release build image. Release builds run this script through
# packaging/build-in-container.sh. See packaging/README.md, ADR 0027 and
# ADR 0090.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/build-release.sh [VERSION]
       packaging/build-release.sh --print-version

Builds dist/quoro-VERSION-x86_64-unknown-linux-musl.tar.gz and dist/SHA256SUMS
from the current checkout. VERSION defaults to the version of the Cargo
package named by QUORO_PACKAGE (default: quoro). --print-version prints that
default on stdout and exits without building.

A release build runs inside the release build image: use
packaging/build-in-container.sh, which takes the same arguments. Run directly
on a host, the result works but does not match the published bytes.

Environment:
  QUORO_PACKAGE      Cargo package to build (default: quoro)
  QUORO_ALLOW_DIRTY  set to 1 to build from a dirty tree (not a release build)
  QUORO_SKIP_PANEL   set to 1 to skip rebuilding the panel (not a release build);
                     panel/dist must already exist
  SOURCE_DATE_EPOCH  build timestamp; defaults to the last commit time
  CARGO_HOME         defaults to $HOME/.cargo

The panel is built before cargo because rust-embed compiles panel/dist into the
binary. The panel toolchain is pinned in packaging/node.txt: put that Node.js
first on PATH (packaging/fetch-node.sh <dir> prints its bin directory).
EOF
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

note() {
	printf '%s\n' "$*" >&2
}

print_version=0
case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
--print-version)
	print_version=1
	shift
	;;
-*)
	usage >&2
	exit 2
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

if [ "$print_version" = "1" ] && [ "$#" -ne 0 ]; then
	usage >&2
	exit 2
fi

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

if [ "$print_version" = "1" ]; then
	printf '%s\n' "$version"
	exit 0
fi

# The release build image (packaging/container/Containerfile) pins the C
# compiler, binutils and musl, which reach the binary through C dependencies.
# Outside it the build still works, but its bytes depend on the host.
release_image=0
if [ -f /etc/quoro-release-build ]; then
	release_image=1
	note "build environment: $(cat /etc/quoro-release-build) ${QUORO_BUILD_IMAGE:-}"
else
	note "warning: not running in the release build image; the result will NOT match the published release (use packaging/build-in-container.sh)"
fi

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

# The panel is compiled into the binary by rust-embed, so its bytes are part of
# the release. Build it first, with the Node.js pinned in packaging/node.txt:
# npm comes with that Node, and a different Node produces different panel bytes
# (ADR 0027). The dirty-tree check above runs before this, which is safe because
# panel/dist, panel/node_modules and panel/src/api/generated are gitignored.
node_pin_arch=$(uname -m) || die "cannot determine the machine architecture"
node_pins=$(
	awk -v arch="$node_pin_arch" '
		NF && $1 !~ /^#/ && $2 == arch { print $1 }
	' packaging/node.txt
) || die "cannot read packaging/node.txt"
[ -n "$node_pins" ] ||
	die "packaging/node.txt has no Node.js pin for $node_pin_arch"
[ "$(printf '%s\n' "$node_pins" | wc -l | tr -d ' ')" -eq 1 ] ||
	die "more than one Node.js pin for $node_pin_arch in packaging/node.txt"
node_pin=$node_pins

if [ "${QUORO_SKIP_PANEL:-0}" = "1" ]; then
	note "warning: QUORO_SKIP_PANEL=1: the panel was not rebuilt; this is NOT a release build"
	[ -f panel/dist/index.html ] ||
		die "QUORO_SKIP_PANEL=1 but panel/dist/index.html is missing; build the panel first or unset QUORO_SKIP_PANEL"
else
	command -v node >/dev/null 2>&1 ||
		die "node is not on PATH; run packaging/fetch-node.sh <dir> and put <dir>/bin first on PATH (the panel needs Node $node_pin)"
	node_version=$(node --version) || die "cannot run node --version"
	[ "$node_version" = "$node_pin" ] ||
		die "node $node_version is on PATH but packaging/node.txt pins $node_pin for $node_pin_arch; run packaging/fetch-node.sh <dir> and put <dir>/bin first on PATH"
	command -v npm >/dev/null 2>&1 ||
		die "npm is not on PATH; it ships with the pinned Node.js"
	[ -f panel/package-lock.json ] ||
		die "panel/package-lock.json is missing; npm ci needs it"

	note "building the panel with Node $node_version"
	rm -rf panel/dist
	(
		cd panel &&
			npm ci --no-audit --no-fund &&
			npm run build
	) || die "the panel build failed (npm ci + npm run build in panel/)"
	[ -f panel/dist/index.html ] ||
		die "the panel build did not produce panel/dist/index.html"
	note "panel built into panel/dist"
fi

# One pinned toolchain, taken from rust-toolchain.toml, for local builds and CI.
channel=$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)
[ -n "$channel" ] || die "cannot read channel from rust-toolchain.toml"

# The release build image already carries the toolchain and the musl target;
# install them only when they are missing (a host build), so the image's
# toolchain directory is never written to.
if ! rustup toolchain list | grep -q "^$channel-"; then
	rustup toolchain install "$channel" --profile minimal
fi
if ! rustup target list --installed --toolchain "$channel" | grep -qx x86_64-unknown-linux-musl; then
	rustup target add --toolchain "$channel" x86_64-unknown-linux-musl
fi
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

# C code (ring, aws-lc, the bundled SQLite) is compiled by the C compiler of
# the build environment, so pin it to musl-gcc and strip the build paths from
# its debug info as well. The release build image always has musl-gcc.
if [ "$release_image" = "1" ] && ! command -v musl-gcc >/dev/null 2>&1; then
	die "musl-gcc is missing from the release build image"
fi
if command -v musl-gcc >/dev/null 2>&1; then
	CC_x86_64_unknown_linux_musl=musl-gcc
	CFLAGS_x86_64_unknown_linux_musl="-ffile-prefix-map=$PWD=/build/quoro -ffile-prefix-map=$CARGO_HOME=/build/cargo"
	export CC_x86_64_unknown_linux_musl CFLAGS_x86_64_unknown_linux_musl
else
	note "note: musl-gcc not found; C dependencies would fall back to the default compiler"
fi

# Both binaries ship: quoro is the service, quoro-init is PID 1 inside every
# microVM. One invocation so they share the exact same environment.
cargo +"$channel" build --release --locked --target x86_64-unknown-linux-musl \
	-p "$QUORO_PACKAGE" --bin quoro \
	-p quoro-init --bin quoro-init

binary="$CARGO_TARGET_DIR/x86_64-unknown-linux-musl/release/quoro"
init_binary="$CARGO_TARGET_DIR/x86_64-unknown-linux-musl/release/quoro-init"
[ -f "$binary" ] || die "build succeeded but $binary is missing"
[ -f "$init_binary" ] || die "build succeeded but $init_binary is missing"
[ -f LICENSE ] || die "LICENSE is missing from the repository root"
[ -f packaging/systemd/quoro.service ] ||
	die "packaging/systemd/quoro.service is missing; the release needs the systemd unit"
[ -f packaging/systemd/quoro-node.service ] ||
	die "packaging/systemd/quoro-node.service is missing; the release needs the node agent's systemd unit"
# install.sh fetches Cloud Hypervisor and the guest kernel and verifies them
# against these pins, so the engine's pinned versions travel with the release.
[ -f crates/engine/pins.txt ] ||
	die "crates/engine/pins.txt is missing; the release needs the engine pins"
# install.sh stages the cloudflared connector from these pins the same way, so
# the tunnel binary ships verified against the attested tarball too (ADR 0024,
# ADR 0055).
[ -f crates/ingress/pins.txt ] ||
	die "crates/ingress/pins.txt is missing; the release needs the ingress pins"

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
install -m 0755 "$init_binary" "$stage/$name/quoro-init"
install -m 0644 LICENSE "$stage/$name/LICENSE"
install -m 0644 packaging/systemd/quoro.service "$stage/$name/quoro.service"
install -m 0644 packaging/systemd/quoro-node.service "$stage/$name/quoro-node.service"
install -m 0644 crates/engine/pins.txt "$stage/$name/pins.txt"
install -m 0644 crates/ingress/pins.txt "$stage/$name/ingress-pins.txt"

# Normalise every staged timestamp, including symlinks and the directory
# entries themselves, because tar records all of them.
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/quoro"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/quoro-init"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/LICENSE"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/quoro.service"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/quoro-node.service"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/pins.txt"
touch -h -d "@$SOURCE_DATE_EPOCH" "$stage/$name/ingress-pins.txt"
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
printf 'sha256(quoro-init): %s\n' "$(sha256sum "$init_binary")"
printf 'sha256(tarball): %s\n' "$(sha256sum "dist/$tarball")"
