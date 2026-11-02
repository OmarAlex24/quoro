#!/bin/sh
# fetch-node.sh <dest-dir>: materialise the Node.js pinned in node.txt for this
# machine's architecture in <dest-dir> and print its bin directory on stdout.
#
# Node is a build input, not a runtime dependency: the panel is compiled into
# the quoro binary with rust-embed (ADR 0027). The tarball is downloaded from
# nodejs.org, verified against the pinned sha256 and only then extracted, so a
# bad or partial download never reaches the build.
#
# An extracted tree is reused when its stamp file still holds the pinned
# sha256; anything else is discarded and fetched again.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/fetch-node.sh <dest-dir>

Downloads the Node.js pinned in packaging/node.txt for this machine's
architecture into <dest-dir>/node-<version>, verifies its sha256 and prints the
resulting bin directory on stdout (its last line). Add that directory to PATH
before packaging/build-release.sh.

Environment: none. The pin file next to this script is the only input.
EOF
}

die() {
	printf 'fetch-node: %s\n' "$*" >&2
	exit 1
}

note() {
	printf 'fetch-node: %s\n' "$*" >&2
}

case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
esac

[ "$#" -eq 1 ] || {
	usage >&2
	exit 2
}
dest=$1
[ -n "$dest" ] || die "the destination directory is empty"

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd) ||
	die "cannot resolve the script directory"
pins_file="$script_dir/node.txt"
[ -f "$pins_file" ] || die "cannot read $pins_file"

command -v curl >/dev/null 2>&1 ||
	die "curl is required to download Node.js"
command -v tar >/dev/null 2>&1 ||
	die "tar is required to extract Node.js"

if command -v sha256sum >/dev/null 2>&1; then
	sha256_of() {
		sha256sum "$1" | awk '{print $1}'
	}
elif command -v shasum >/dev/null 2>&1; then
	sha256_of() {
		shasum -a 256 "$1" | awk '{print $1}'
	}
else
	die "sha256sum or shasum is required to verify the download"
fi

arch=$(uname -m) || die "cannot determine the machine architecture"

# Exactly one pin may match: two lines for the same arch would make the choice
# depend on file order, so refuse instead of guessing.
found=0
pin_version=
pin_file=
pin_url=
pin_sha256=
while IFS= read -r line || [ -n "$line" ]; do
	case "$line" in
	'' | '#'*) continue ;;
	esac
	# Word splitting on purpose: the pin format is whitespace separated. This
	# clobbers the script's positional parameters, so `dest` was saved above
	# and nothing below reads them.
	# shellcheck disable=SC2086
	set -- $line
	[ "$#" -eq 5 ] ||
		die "malformed line in $pins_file (want 5 fields, got $#): $line"
	version=$1 pin_arch=$2 file=$3 url=$4 sha256=$5

	# Version, file name and hash are copied into paths and checks below, so
	# their alphabets are fixed here rather than trusted.
	case "$version" in
	v[0-9]*.[0-9]*.[0-9]*) ;;
	*) die "pin version is not vN.N.N: $version" ;;
	esac
	case "${version#v}" in
	*[!0-9.]*) die "pin version has invalid characters: $version" ;;
	esac
	case "$file" in
	'' | *[!0-9A-Za-z._+-]*) die "pin file name has invalid characters: $file" ;;
	esac
	case "$sha256" in
	*[!0123456789abcdef]*) die "pin sha256 is not lowercase hex: $sha256" ;;
	esac
	[ "${#sha256}" -eq 64 ] || die "pin sha256 is not 64 hex characters: $sha256"
	# The URL must be the exact nodejs.org dist URL for this version and file:
	# a pin can only point at the official distribution over HTTPS.
	[ "$url" = "https://nodejs.org/dist/$version/$file" ] ||
		die "pin url is not https://nodejs.org/dist/$version/$file: $url"

	[ "$pin_arch" = "$arch" ] || continue
	found=$((found + 1))
	pin_version=$version
	pin_file=$file
	pin_url=$url
	pin_sha256=$sha256
done <"$pins_file"

[ "$found" -gt 0 ] || die "no Node.js pin for architecture $arch in $pins_file"
[ "$found" -eq 1 ] || die "more than one Node.js pin for architecture $arch in $pins_file"

mkdir -p -- "$dest" || die "cannot create $dest"
dest=$(CDPATH='' cd -- "$dest" && pwd) || die "cannot enter $dest"

node_dir="$dest/node-$pin_version"
bin_dir="$node_dir/bin"
stamp="$node_dir/.quoro-node.sha256"

# Reuse an extraction this script (or the same pin) already verified. The
# stamp is what makes reuse safe: a directory without it is not trusted, even
# if it looks complete.
if [ -f "$stamp" ] && [ -x "$bin_dir/node" ]; then
	stamp_sha=$(cat "$stamp" 2>/dev/null) || stamp_sha=
	if [ "$stamp_sha" = "$pin_sha256" ]; then
		note "reusing verified Node.js $pin_version in $node_dir"
		printf '%s\n' "$bin_dir"
		exit 0
	fi
	note "stamp in $node_dir does not match the pin; extracting again"
fi

part="$dest/node-$pin_version.tar.xz.part"
extract="$dest/node-$pin_version.extract"
rm -rf -- "$extract"
rm -f -- "$part"

note "downloading Node.js $pin_version from $pin_url"
curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "$part" "$pin_url" || {
	rm -f -- "$part"
	die "download failed: $pin_url"
}

actual=$(sha256_of "$part") || {
	rm -f -- "$part"
	die "cannot hash the download"
}
if [ "$actual" != "$pin_sha256" ]; then
	rm -f -- "$part"
	die "sha256 mismatch for $pin_file (expected $pin_sha256, got $actual); the download was discarded"
fi
note "sha256 ok: $pin_file"

# Extract into a scratch directory next to the target, so an interrupted
# extraction never leaves a half tree that could be mistaken for a complete one.
mkdir -p -- "$extract" || die "cannot create $extract"
tar -xJf "$part" -C "$extract" --strip-components=1 ||
	die "cannot extract $pin_file"
[ -x "$extract/bin/node" ] ||
	die "$pin_file did not contain bin/node"
[ -x "$extract/bin/npm" ] ||
	die "$pin_file did not contain bin/npm; the panel build needs it"

printf '%s\n' "$pin_sha256" >"$extract/.quoro-node.sha256" ||
	die "cannot write the stamp file"

rm -rf -- "$node_dir"
mv -- "$extract" "$node_dir" || die "cannot move the extracted tree into $node_dir"
rm -f -- "$part"

note "extracted Node.js $pin_version into $node_dir"
printf '%s\n' "$bin_dir"
