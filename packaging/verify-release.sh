#!/bin/sh
# Verify a downloaded release tarball: its checksum line in the adjacent
# SHA256SUMS file and its GitHub artifact attestation (Sigstore, SLSA build
# provenance). Prints VERIFIED only when both checks pass.
#
# This is the human/testing tool, not the installer: the installer must do its
# own verification before it writes anything to disk.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/verify-release.sh <tarball> <tag> [--bundle <file>]

Verifies <tarball> against the SHA256SUMS file next to it (when present) and
against its GitHub artifact attestation. <tag> is the release tag the artifact
must have been built from, for example v0.1.0.

The attestation bundle defaults to <tarball>.sigstore.jsonl.

Environment:
  QUORO_VERIFY_REPO             repository that signed the release
                                (default: OmarAlex24/quoro)
  QUORO_VERIFY_SIGNER_WORKFLOW  workflow that signed the release
                                (default: OmarAlex24/quoro/.github/workflows/release.yml)
  QUORO_VERIFY_SOURCE_REF       git ref recorded in the attestation
                                (default: refs/tags/<tag>)
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

[ "$#" -ge 2 ] || {
	usage >&2
	exit 2
}
tarball=$1
tag=$2
shift 2
[ -n "$tarball" ] || die "the tarball path is empty"
[ -n "$tag" ] || die "the tag is empty"

bundle=
while [ "$#" -gt 0 ]; do
	case "$1" in
	--bundle)
		[ "$#" -ge 2 ] || die "--bundle needs a file"
		bundle=$2
		shift 2
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
done

[ -f "$tarball" ] || die "no such file: $tarball"
bundle=${bundle:-"$tarball.sigstore.jsonl"}
[ -f "$bundle" ] || die "no attestation bundle at $bundle (pass --bundle)"

command -v gh >/dev/null 2>&1 ||
	die "gh is required to verify attestations; install it from https://cli.github.com"

repo=${QUORO_VERIFY_REPO:-OmarAlex24/quoro}
signer=${QUORO_VERIFY_SIGNER_WORKFLOW:-OmarAlex24/quoro/.github/workflows/release.yml}
source_ref=${QUORO_VERIFY_SOURCE_REF:-"refs/tags/$tag"}
[ -n "$repo" ] || die "QUORO_VERIFY_REPO is empty"
[ -n "$signer" ] || die "QUORO_VERIFY_SIGNER_WORKFLOW is empty"
[ -n "$source_ref" ] || die "QUORO_VERIFY_SOURCE_REF is empty"

dir=$(dirname "$tarball")
base=$(basename "$tarball")

# (a) Checksum. --ignore-missing would silently pass when the file has no line
# for this tarball, so the line is required explicitly first.
if [ -f "$dir/SHA256SUMS" ]; then
	awk -v file="$base" '
		{
			name = $2
			sub(/^\*/, "", name)
			if (name == file) {
				found = 1
				exit
			}
		}
		END {
			if (!found) {
				exit 1
			}
		}
	' "$dir/SHA256SUMS" || die "SHA256SUMS has no entry for $base"

	(
		cd "$dir"
		sha256sum -c --ignore-missing SHA256SUMS
	) || die "checksum mismatch for $base"
	printf 'checksum: ok\n'
else
	printf 'checksum: skipped (no SHA256SUMS next to %s)\n' "$base" >&2
fi

# (b) Attestation: binds these bytes to a workflow run in this repository, on
# this tag, on a GitHub-hosted runner.
gh attestation verify "$tarball" \
	--bundle "$bundle" \
	--repo "$repo" \
	--signer-workflow "$signer" \
	--source-ref "$source_ref" \
	--deny-self-hosted-runners || die "attestation verification failed"

printf 'attestation: ok (%s from %s at %s)\n' "$repo" "$signer" "$source_ref"
printf 'VERIFIED\n'
