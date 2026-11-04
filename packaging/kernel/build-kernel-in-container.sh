#!/bin/sh
# Run packaging/kernel/build-kernel.sh inside the guest kernel build image
# (packaging/kernel/Containerfile, ADR 0152). The guest kernel workflow and
# packaging/kernel/reproduce-kernel.sh both build through this script, so the
# kernel's bytes come from pinned inputs only, at the same paths on every host.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/kernel/build-kernel-in-container.sh [--print-tag]

Builds the guest kernel build image from packaging/kernel/Containerfile
(podman or docker; the layer cache makes a rebuild cheap) and runs
packaging/kernel/build-kernel.sh inside it on this checkout. The results land
in dist/kernel/.

--print-tag prints the release tag of the recipe in packaging/kernel/kernel.txt
(guest-kernel-<VERSION>-<REVISION>) and exits without building.

Environment:
  QUORO_CONTAINER_ENGINE  podman or docker (default: podman if installed,
                          otherwise docker)
  QUORO_KERNEL_JOBS       parallel make jobs inside the container (default:
                          all CPUs); it does not change the output

The image is tagged quoro-kernel-build:<hash of its inputs> and kept, so the
next build reuses it. The source tarball and the build tree live in
target/kernel-build/ (about 2 GB), which cargo clean removes.
EOF
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

note() {
	printf '%s\n' "$*" >&2
}

print_tag=0
case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
--print-tag) print_tag=1 ;;
'') ;;
*)
	usage >&2
	exit 2
	;;
esac

cd "$(dirname "$0")/../.." || die "cannot enter the repository root"
repo=$(pwd -P) || die "cannot resolve the repository root"

recipe_value() {
	awk -F= -v k="$1" '$1 == k { sub(/^[^=]*=/, ""); print; found = 1 } END { exit !found }' \
		packaging/kernel/kernel.txt || die "packaging/kernel/kernel.txt has no $1"
}

if [ "$print_tag" = 1 ]; then
	version=$(recipe_value VERSION)
	revision=$(recipe_value REVISION)
	tag="guest-kernel-$version-$revision"
	case "$tag" in
	*[!A-Za-z0-9.-]*) die "unexpected characters in the tag: $tag" ;;
	esac
	printf '%s\n' "$tag"
	exit 0
fi

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
		die "neither podman nor docker is installed; the kernel build runs in a container (ADR 0152)"
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

# The image context holds only the Containerfile; the work directory (tarball
# cache and build tree) is mounted at a fixed path outside the checkout's
# mount, so the tree is never inside a git repository.
state="$repo/target/kernel-build"
context="$state/context"
work="$state/work"
rm -rf "$context"
mkdir -p "$context" "$work" || die "cannot create $state"
cp packaging/kernel/Containerfile "$context/Containerfile" ||
	die "cannot stage the Containerfile"

inputs_hash=$(sha256_stdin <"$context/Containerfile") || die "cannot hash the image inputs"
image="quoro-kernel-build:$(printf '%s' "$inputs_hash" | cut -c1-16)"

note "building the guest kernel build image $image with $engine"
"$engine" build --platform linux/amd64 -t "$image" -f "$context/Containerfile" "$context" >&2 ||
	die "cannot build the guest kernel build image"
rm -rf "$context"

# Run as the invoking user so dist/ and target/ stay owned by them (same rules
# as packaging/build-in-container.sh).
uid=$(id -u) || die "cannot read the user id"
gid=$(id -g) || die "cannot read the group id"
if [ "$engine" = podman ] && [ "$uid" != 0 ]; then
	user_opts="--userns=keep-id"
elif [ "$engine" = docker ] &&
	"$engine" info --format '{{.SecurityOptions}}' 2>/dev/null | grep -q rootless; then
	user_opts="--user=0:0"
else
	user_opts="--user=$uid:$gid"
fi

env_opts=
if [ -n "${QUORO_KERNEL_JOBS+x}" ]; then
	env_opts="--env=QUORO_KERNEL_JOBS"
fi

note "running packaging/kernel/build-kernel.sh in $image"
# shellcheck disable=SC2086 # user_opts and env_opts are word lists on purpose
exec "$engine" run --rm --platform linux/amd64 \
	$user_opts \
	--volume "$repo:/build/quoro:z" \
	--volume "$work:/build/work:z" \
	--workdir /build/quoro \
	--env HOME=/build/work \
	$env_opts \
	"$image" \
	/build/quoro/packaging/kernel/build-kernel.sh
