#!/bin/sh
# Rebuild a published release from its tag and compare the tarball hash with
# the published SHA256SUMS. A release is reproducible when this prints
# REPRODUCIBLE; anything else is a MISMATCH or a broken environment.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/reproduce-release.sh <tag> [--sums <path-or-https-url>]

Clones the repository at <tag> into a fresh temporary directory, rebuilds the
release artifact there and compares its sha256 with the published SHA256SUMS.

The tag may be written with or without the leading v (v0.1.0 or 0.1.0).

Environment:
  QUORO_REPO_URL  repository to clone (default: https://github.com/OmarAlex24/quoro)
  QUORO_KEEP      set to 1 to keep the clone directory (its path is printed)
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
case "$1" in
-h | --help)
	usage
	exit 0
	;;
esac

tag=$1
shift
[ -n "$tag" ] || die "the tag is empty"
case "$tag" in
*/* | *[!A-Za-z0-9.+_v-]*) die "the tag contains characters that are unsafe: $tag" ;;
esac

sums=
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

version=${tag#v}
# Release tags always carry the leading v.
tag="v$version"
[ -n "$version" ] || die "the tag has no version after the leading v"

repo_url=${QUORO_REPO_URL:-https://github.com/OmarAlex24/quoro}
[ -n "$repo_url" ] || die "QUORO_REPO_URL is empty"
sums_source=${sums:-"https://github.com/OmarAlex24/quoro/releases/download/$tag/SHA256SUMS"}

keep=${QUORO_KEEP:-0}
work=$(mktemp -d) || die "cannot create a temporary directory"
# shellcheck disable=SC2329 # invoked through trap below
cleanup() {
	if [ "$keep" = "1" ]; then
		printf 'clone kept at %s\n' "$work" >&2
	else
		rm -rf "$work"
	fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

printf 'cloning %s at %s\n' "$repo_url" "$tag" >&2
git clone --depth 1 --branch "$tag" "$repo_url" "$work/quoro" ||
	die "cannot clone $repo_url at $tag"

printf 'rebuilding %s from the clone\n' "$version" >&2
"$work/quoro/packaging/build-release.sh" "$version"

tarball="quoro-$version-x86_64-unknown-linux-musl.tar.gz"
built="$work/quoro/dist/$tarball"
[ -f "$built" ] || die "the rebuild did not produce $tarball"

# Fetch the published checksums. HTTPS only, so a redirect or a plain-http URL
# cannot downgrade the transport.
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

printf 'expected (published): %s\n' "$expected"
printf 'actual   (rebuilt):   %s\n' "$actual"

if [ "$actual" = "$expected" ]; then
	printf 'REPRODUCIBLE\n'
	exit 0
fi

printf 'MISMATCH\n' >&2
exit 1
