#!/bin/sh
# Rebuild a published guest kernel (or a dry-run build) from its source and
# compare the sha256 of vmlinux-x86_64 and config-x86_64 with the published
# SHA256SUMS. The kernel is reproducible when this prints REPRODUCIBLE.
#
# The rebuild runs in the guest kernel build image through the clone's own
# packaging/kernel/build-kernel-in-container.sh, so an old tag is rebuilt with
# the recipe and image it was released with. It needs podman or docker.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/kernel/reproduce-kernel.sh <tag> [--sums <path-or-https-url>]
       packaging/kernel/reproduce-kernel.sh --ref <commit-or-branch> --sums <path-or-https-url>

Fetches the source into a fresh temporary directory, rebuilds the guest kernel
there and compares vmlinux-x86_64 and config-x86_64 with the published
SHA256SUMS.

<tag> is a guest kernel tag, for example guest-kernel-7.2.15-q1. Its
SHA256SUMS defaults to the one attached to the GitHub release.

--ref rebuilds a commit (the full 40-character sha) or a branch, for example
the source of a workflow_dispatch dry run; --sums is then required (the
SHA256SUMS from the dry run's workflow artifact).

The work directory is created under TMPDIR (default /tmp) and needs about
4 GB free; /tmp is often a small tmpfs, so point TMPDIR at a disk.

Environment:
  TMPDIR                  where the work directory is created
  QUORO_REPO_URL          repository to fetch (default: https://github.com/OmarAlex24/quoro)
  QUORO_KEEP              set to 1 to keep the work directory (its path is printed)
  QUORO_CONTAINER_ENGINE  podman or docker (default: podman if installed)
  QUORO_KERNEL_JOBS       parallel make jobs (default: all CPUs)
EOF
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

[ "$#" -ge 1 ] || {
	usage >&2
	exit 2
}

tag=
ref=
sums=
case "$1" in
-h | --help)
	usage
	exit 0
	;;
--ref)
	[ "$#" -ge 2 ] || die "--ref needs a commit sha or a branch name"
	ref=$2
	shift 2
	;;
-*)
	usage >&2
	exit 2
	;;
*)
	tag=$1
	shift
	;;
esac

while [ "$#" -gt 0 ]; do
	case "$1" in
	--sums)
		[ "$#" -ge 2 ] || die "--sums needs a path or https URL"
		sums=$2
		shift 2
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
done

if [ -n "$tag" ]; then
	case "$tag" in
	guest-kernel-*) ;;
	*) die "a guest kernel tag starts with guest-kernel-: $tag" ;;
	esac
	case "$tag" in
	*[!A-Za-z0-9.-]*) die "the tag contains characters that are unsafe: $tag" ;;
	esac
	fetch_ref="refs/tags/$tag"
	label=$tag
	sums_source=${sums:-"https://github.com/OmarAlex24/quoro/releases/download/$tag/SHA256SUMS"}
else
	[ -n "$ref" ] || die "the ref is empty"
	[ -n "$sums" ] || die "--ref needs --sums <path-or-https-url> (the dry run's SHA256SUMS)"
	case "$ref" in
	*[!0-9a-f]*) is_sha=0 ;;
	*) [ "${#ref}" -eq 40 ] && is_sha=1 || is_sha=0 ;;
	esac
	if [ "$is_sha" = "1" ]; then
		fetch_ref=$ref
	else
		case "$ref" in
		-* | */ | /* | *..* | *//* | *[!A-Za-z0-9._/-]*)
			die "the ref must be a full 40-character commit sha or a branch name: $ref"
			;;
		esac
		fetch_ref="refs/heads/$ref"
	fi
	label=$ref
	sums_source=$sums
fi

repo_url=${QUORO_REPO_URL:-https://github.com/OmarAlex24/quoro}
[ -n "$repo_url" ] || die "QUORO_REPO_URL is empty"

keep=${QUORO_KEEP:-0}
work=$(mktemp -d) || die "cannot create a temporary directory"
# shellcheck disable=SC2329 # invoked through trap below
cleanup() {
	if [ "$keep" = "1" ]; then
		printf 'work directory kept at %s\n' "$work" >&2
	else
		# The build tree may hold files the container created; they belong to
		# the invoking user (build-kernel-in-container.sh runs as that user).
		rm -rf "$work"
	fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

need_kb=$((4 * 1024 * 1024))
free_kb=$(df -Pk "$work" | awk 'NR == 2 { print $4 }') ||
	die "cannot read the free space in $work"
case "$free_kb" in
'' | *[!0-9]*) die "cannot read the free space in $work" ;;
esac
[ "$free_kb" -ge "$need_kb" ] ||
	die "$work has $((free_kb / 1024)) MB free; the rebuild needs about 4 GB (set TMPDIR to a disk)"

# The published sums first: a typo in the tag fails before a long build.
published="$work/SHA256SUMS"
case "$sums_source" in
https://*)
	curl --fail --location --retry 3 --silent --show-error --proto '=https' \
		--output "$published" "$sums_source" || die "cannot download $sums_source"
	;;
*)
	[ -r "$sums_source" ] || die "cannot read $sums_source"
	cp "$sums_source" "$published"
	;;
esac

expected() {
	awk -v f="$1" '$2 == f || $2 == "*" f { print $1; found = 1 } END { exit !found }' "$published" ||
		die "$sums_source has no line for $1"
}
want_vmlinux=$(expected vmlinux-x86_64)
want_config=$(expected config-x86_64)

printf 'fetching %s from %s\n' "$label" "$repo_url" >&2
git init -q "$work/src"
git -C "$work/src" fetch -q --depth=1 "$repo_url" "$fetch_ref" || die "cannot fetch $fetch_ref"
git -C "$work/src" -c advice.detachedHead=false checkout -q FETCH_HEAD || die "cannot check out $fetch_ref"

if [ -n "$tag" ]; then
	recipe_tag=$("$work/src/packaging/kernel/build-kernel-in-container.sh" --print-tag) ||
		die "cannot read the recipe of $tag"
	[ "$recipe_tag" = "$tag" ] || die "the recipe at $tag builds $recipe_tag"
fi

"$work/src/packaging/kernel/build-kernel-in-container.sh" || die "the rebuild failed"

sha256_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	else
		shasum -a 256 "$1" | awk '{print $1}'
	fi
}
got_vmlinux=$(sha256_of "$work/src/dist/kernel/vmlinux-x86_64")
got_config=$(sha256_of "$work/src/dist/kernel/config-x86_64")

printf 'vmlinux-x86_64 published %s\n' "$want_vmlinux"
printf 'vmlinux-x86_64 rebuilt   %s\n' "$got_vmlinux"
printf 'config-x86_64  published %s\n' "$want_config"
printf 'config-x86_64  rebuilt   %s\n' "$got_config"
if [ "$want_vmlinux" = "$got_vmlinux" ] && [ "$want_config" = "$got_config" ]; then
	printf 'REPRODUCIBLE\n'
	exit 0
fi
printf 'MISMATCH\n'
exit 1
