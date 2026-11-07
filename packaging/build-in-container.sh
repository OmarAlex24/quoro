#!/bin/sh
# Run packaging/build-release.sh inside the release build image (ADR 0090).
#
# The host C compiler used to leak into the quoro binary (C code from ring,
# aws-lc and SQLite is compiled by musl-gcc), so a build on another
# distribution produced different bytes. Here the whole build environment comes
# from packaging/container/Containerfile, where every input is pinned, and the
# checkout and cargo home sit at the same paths on every host. The release
# workflow and packaging/reproduce-release.sh both build through this script.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/build-in-container.sh [VERSION | --print-version]

Builds the release build image from packaging/container/Containerfile (podman
or docker; the layer cache makes a rebuild cheap) and runs
packaging/build-release.sh inside it on this checkout. The arguments are passed
to build-release.sh. The results land in dist/ as with a host build.

Environment:
  QUORO_CONTAINER_ENGINE  podman or docker (default: podman if installed,
                          otherwise docker)
  QUORO_PACKAGE, QUORO_ALLOW_DIRTY, QUORO_SKIP_PANEL, SOURCE_DATE_EPOCH
                          passed to build-release.sh when set
  QUORO_DEV_WARM_POLICY   set to 1 for a test-lab dev build only, never a
                          release: enables the dev-only dev-warm-policy
                          feature (ADR 0223); passed to build-release.sh

The image is tagged quoro-release-build:<hash of its inputs> and kept, so the
next build reuses it; the tag is printed on stderr. Downloads (crates, npm) are
cached in target/release-container/, which cargo clean removes.
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

cd "$(dirname "$0")/.." || die "cannot enter the repository root"
repo=$(pwd -P) || die "cannot resolve the repository root"
case "$repo" in
*:*) die "the checkout path contains ':', which a container volume cannot mount: $repo" ;;
esac

engine=${QUORO_CONTAINER_ENGINE:-}
if [ -z "$engine" ]; then
	if command -v podman >/dev/null 2>&1; then
		engine=podman
	elif command -v docker >/dev/null 2>&1; then
		engine=docker
	else
		die "neither podman nor docker is installed; the release build runs in a container (ADR 0090)"
	fi
fi
case "$engine" in
podman | docker) ;;
*) die "QUORO_CONTAINER_ENGINE must be podman or docker, not: $engine" ;;
esac
command -v "$engine" >/dev/null 2>&1 || die "$engine is not installed"

if command -v sha256sum >/dev/null 2>&1; then
	sha256_stdin() { sha256sum | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
	sha256_stdin() { shasum -a 256 | awk '{print $1}'; }
else
	die "sha256sum or shasum is required"
fi

# The image is built from a context holding only its declared inputs, so
# nothing else in the checkout (a large target/, local files) can reach it.
# It lives under target/ (gitignored) so it never dirties the tree and never
# touches the system temporary directory.
state="$repo/target/release-container"
context="$state/context"
cache="$state/cache"
rm -rf "$context"
mkdir -p "$context" "$cache/cargo" "$cache/home" ||
	die "cannot create $state"
for input in packaging/container/Containerfile rust-toolchain.toml \
	packaging/node.txt packaging/fetch-node.sh; do
	cp "$input" "$context/${input##*/}" ||
		die "cannot stage $input in the image build context"
done

inputs_hash=$(
	cd "$context" &&
		cat Containerfile rust-toolchain.toml node.txt fetch-node.sh | sha256_stdin
) || die "cannot hash the image inputs"
image="quoro-release-build:$(printf '%s' "$inputs_hash" | cut -c1-16)"

note "building the release build image $image with $engine"
# podman prints build progress on stdout; keep stdout for build-release.sh
# (--print-version is read from it).
"$engine" build --platform linux/amd64 -t "$image" -f "$context/Containerfile" "$context" >&2 ||
	die "cannot build the release build image"
rm -rf "$context"

# Run as the invoking user so that target/, dist/ and panel/ stay owned by
# them. Rootless podman maps the user with keep-id; docker (and podman run as
# root) take an explicit --user. Rootless docker maps container root to the
# invoking user, so it runs as root inside.
uid=$(id -u) || die "cannot read the user id"
gid=$(id -g) || die "cannot read the group id"
user_opts=
if [ "$engine" = podman ] && [ "$uid" != 0 ]; then
	user_opts="--userns=keep-id"
elif [ "$engine" = docker ] &&
	"$engine" info --format '{{.SecurityOptions}}' 2>/dev/null | grep -q rootless; then
	user_opts="--user=0:0"
else
	user_opts="--user=$uid:$gid"
fi

# Only the build-release.sh variables are passed through; nothing else from
# the host environment reaches the build.
env_opts=
for var in QUORO_PACKAGE QUORO_ALLOW_DIRTY QUORO_SKIP_PANEL QUORO_DEV_WARM_POLICY SOURCE_DATE_EPOCH; do
	eval "is_set=\${$var+x}"
	# shellcheck disable=SC2154 # assigned by the eval above
	if [ -n "$is_set" ]; then
		env_opts="$env_opts --env=$var"
	fi
done

note "running packaging/build-release.sh $* in $image"
# The paths inside the container are fixed: build-release.sh remaps them, and
# keeping them identical on every host means even an unremapped path is the
# same bytes. The checkout is owned by the host user, which git may see as a
# different owner inside the container; safe.directory is set for this one
# path only. :z relabels the mounts on SELinux hosts and is ignored elsewhere.
# shellcheck disable=SC2086 # user_opts and env_opts are word lists on purpose
exec "$engine" run --rm --platform linux/amd64 \
	$user_opts \
	--volume "$repo:/build/quoro:z" \
	--volume "$cache:/build/cache:z" \
	--workdir /build/quoro \
	--env HOME=/build/cache/home \
	--env CARGO_HOME=/build/cache/cargo \
	--env QUORO_BUILD_IMAGE="$image" \
	--env GIT_CONFIG_COUNT=1 \
	--env GIT_CONFIG_KEY_0=safe.directory \
	--env GIT_CONFIG_VALUE_0=/build/quoro \
	$env_opts \
	"$image" \
	/build/quoro/packaging/build-release.sh "$@"
