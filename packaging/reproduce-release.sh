#!/bin/sh
# Rebuild a published release (or a dry-run artifact) from its source and
# compare the tarball hash with the published SHA256SUMS. A build is
# reproducible when this prints REPRODUCIBLE; anything else is a MISMATCH or a
# broken environment.
#
# The rebuild runs in the release build image (ADR 0090) through the clone's
# own packaging/build-in-container.sh, so an old tag is rebuilt with the image
# definition it was released with. It needs podman or docker.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/reproduce-release.sh <tag> [--sums <path-or-https-url>]
       packaging/reproduce-release.sh --ref <commit-or-branch> --sums <path-or-https-url>

Fetches the source into a fresh temporary directory, rebuilds the release
artifact there in the release build image and compares its sha256 with the
published SHA256SUMS.

<tag> is a release tag, with or without the leading v (v0.1.0 or 0.1.0). Its
SHA256SUMS defaults to the one attached to the GitHub release.

--ref rebuilds a commit (the full 40-character sha) or a branch instead of a
tag, for example the source of a workflow_dispatch dry run (ADR 0082). The
version is then the Cargo version, as in the dry run, and --sums is required:
pass the SHA256SUMS from the dry run's workflow artifact.

Environment:
  QUORO_REPO_URL          repository to fetch (default: https://github.com/OmarAlex24/quoro)
  QUORO_KEEP              set to 1 to keep the work directory (its path is printed)
  QUORO_CONTAINER_ENGINE  podman or docker (default: podman if installed)

The release build image stays in the container engine's image store for the
next run; its tag is printed so it can be removed.
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
	*/* | *[!A-Za-z0-9.+_v-]*) die "the tag contains characters that are unsafe: $tag" ;;
	esac
	version=${tag#v}
	[ -n "$version" ] || die "the tag has no version after the leading v"
	# Release tags always carry the leading v.
	tag="v$version"
	fetch_ref="refs/tags/$tag"
	label=$tag
	sums_source=${sums:-"https://github.com/OmarAlex24/quoro/releases/download/$tag/SHA256SUMS"}
else
	[ -n "$ref" ] || die "the ref is empty"
	[ -n "$sums" ] || die "--ref needs --sums <path-or-https-url> (the dry run's SHA256SUMS)"
	# A full commit sha, or a branch name. Short shas cannot be fetched, and a
	# name is kept to a safe alphabet because it ends up on a git command line.
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
	version=
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
		rm -rf "$work"
	fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Read the checksum file first: a bad path or URL should fail before a long
# build. HTTPS only, so a redirect or a plain-http URL cannot downgrade the
# transport.
sums_file="$work/SHA256SUMS"
case "$sums_source" in
https://*)
	curl -fsSL --proto '=https' --tlsv1.2 -o "$sums_file" "$sums_source" ||
		die "cannot download $sums_source"
	;;
http://* | ftp://* | file://*)
	die "only https URLs or local paths are accepted for --sums"
	;;
*)
	[ -f "$sums_source" ] || die "no such checksum file: $sums_source"
	cp "$sums_source" "$sums_file" || die "cannot read $sums_source"
	;;
esac

src="$work/quoro"
printf 'fetching %s at %s\n' "$repo_url" "$label" >&2
git init -q "$src" || die "cannot create $src"
git -C "$src" fetch -q --depth 1 "$repo_url" "$fetch_ref" ||
	die "cannot fetch $label from $repo_url"
git -C "$src" -c advice.detachedHead=false checkout -q --detach FETCH_HEAD ||
	die "cannot check out $label"
commit=$(git -C "$src" rev-parse HEAD) || die "cannot read the fetched commit"
if [ -n "$ref" ] && [ "$is_sha" = "1" ] && [ "$commit" != "$ref" ]; then
	die "fetched commit $commit, expected $ref"
fi
printf 'source commit: %s\n' "$commit" >&2

[ -x "$src/packaging/build-in-container.sh" ] ||
	die "$label has no packaging/build-in-container.sh; it predates the release build image (ADR 0090)"

printf 'rebuilding %s in the release build image\n' "$label" >&2
if [ -n "$version" ]; then
	"$src/packaging/build-in-container.sh" "$version"
else
	"$src/packaging/build-in-container.sh"
fi

# Exactly one tarball: its name carries the version the build used.
set -- "$src"/dist/quoro-*-x86_64-unknown-linux-musl.tar.gz
[ "$#" -eq 1 ] && [ -f "$1" ] ||
	die "the rebuild did not produce exactly one dist/quoro-*-x86_64-unknown-linux-musl.tar.gz"
built=$1
tarball=${built##*/}

expected=$(
	awk -v file="$tarball" '
		{
			name = $2
			sub(/^\*/, "", name)
			if (name == file) {
				print tolower($1)
				found = 1
				exit
			}
		}
		END {
			if (!found) {
				exit 1
			}
		}
	' "$sums_file"
) || die "SHA256SUMS has no entry for $tarball"

actual=$(sha256sum "$built") || die "cannot hash $built"
actual=${actual%% *}

printf 'artifact:             %s\n' "$tarball"
printf 'source commit:        %s\n' "$commit"
printf 'expected (published): %s\n' "$expected"
printf 'actual   (rebuilt):   %s\n' "$actual"

if [ "$actual" = "$expected" ]; then
	printf 'REPRODUCIBLE\n'
	exit 0
fi

printf 'MISMATCH\n' >&2
exit 1
