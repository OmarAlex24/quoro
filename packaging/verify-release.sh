#!/bin/sh
# Verify a downloaded release tarball: its checksum line in the adjacent
# SHA256SUMS file and its GitHub artifact attestation (Sigstore, SLSA build
# provenance). Prints VERIFIED only when both checks pass.
#
# A workflow_dispatch dry run (ADR 0082) builds the same artifact from a branch
# and attests it against that branch ref. It is not a release, so it is verified
# with --dry-run-ref instead of a tag.
#
# This is the human/testing tool, not the installer: the installer must do its
# own verification before it writes anything to disk.
set -eu

usage() {
	cat <<'EOF'
usage: packaging/verify-release.sh <tarball> <tag> [--bundle <file>]
       packaging/verify-release.sh <tarball> --dry-run-ref <ref> [--bundle <file>]

Verifies <tarball> against the SHA256SUMS file next to it (when present) and
against its GitHub artifact attestation. <tag> is the release tag the artifact
must have been built from, for example v0.1.0.

With --dry-run-ref <ref> the artifact is treated as a workflow_dispatch dry run
(not a release) and must have been attested from <ref>, which must be
refs/heads/<branch>. This cannot be combined with QUORO_VERIFY_SOURCE_REF.

The attestation bundle defaults to <tarball>.sigstore.jsonl.

Environment:
  QUORO_VERIFY_REPO             repository that signed the release
                                (default: OmarAlex24/quoro)
  QUORO_VERIFY_SIGNER_WORKFLOW  workflow that signed the release
                                (default: OmarAlex24/quoro/.github/workflows/release.yml)
  QUORO_VERIFY_SOURCE_REF       git ref recorded in the attestation
                                (default: refs/tags/<tag>); incompatible with
                                --dry-run-ref
EOF
}

die() {
	printf 'error: %s\n' "$*" >&2
	exit 1
}

note() {
	printf '%s\n' "$*" >&2
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
shift

# The second positional argument is either a release tag or --dry-run-ref <ref>.
tag=
dry_run_ref=
case "$1" in
--dry-run-ref)
	[ "$#" -ge 2 ] || die "--dry-run-ref needs a ref"
	[ -n "$2" ] || die "--dry-run-ref needs a ref"
	dry_run_ref=$2
	shift 2
	;;
*)
	tag=$1
	shift
	;;
esac
[ -n "$tarball" ] || die "the tarball path is empty"
[ -n "$tag" ] || [ -n "$dry_run_ref" ] || die "the tag is empty"

bundle=
while [ "$#" -gt 0 ]; do
	case "$1" in
	--bundle)
		[ "$#" -ge 2 ] || die "--bundle needs a file"
		bundle=$2
		shift 2
		;;
	--dry-run-ref)
		die "--dry-run-ref must be the second argument, before --bundle"
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
done

if [ -n "$dry_run_ref" ]; then
	[ -z "${QUORO_VERIFY_SOURCE_REF:-}" ] ||
		die "QUORO_VERIFY_SOURCE_REF and --dry-run-ref cannot be combined"

	# Only a branch can be dry-run dispatched; a tag cannot, because that would
	# be a release. The branch alphabet is fixed so the value cannot smuggle
	# options or ref syntax into the gh command.
	case "$dry_run_ref" in
	refs/heads/*) branch=${dry_run_ref#refs/heads/} ;;
	*) die "--dry-run-ref must be refs/heads/<branch>, got: $dry_run_ref" ;;
	esac
	[ -n "$branch" ] || die "--dry-run-ref has an empty branch name"
	case "$branch" in
	*//*) die "--dry-run-ref branch must not contain '//': $branch" ;;
	/* | */) die "--dry-run-ref branch must not start or end with '/': $branch" ;;
	*..*) die "--dry-run-ref branch must not contain '..': $branch" ;;
	*[!A-Za-z0-9._/-]*) die "--dry-run-ref branch has invalid characters: $branch" ;;
	esac
	note "warning: verifying a workflow_dispatch dry run built from $dry_run_ref; this is not a release"
fi

[ -f "$tarball" ] || die "no such file: $tarball"
bundle=${bundle:-"$tarball.sigstore.jsonl"}
[ -f "$bundle" ] || die "no attestation bundle at $bundle (pass --bundle)"

command -v gh >/dev/null 2>&1 ||
	die "gh is required to verify attestations; install it from https://cli.github.com"

repo=${QUORO_VERIFY_REPO:-OmarAlex24/quoro}
signer=${QUORO_VERIFY_SIGNER_WORKFLOW:-OmarAlex24/quoro/.github/workflows/release.yml}
if [ -n "$dry_run_ref" ]; then
	source_ref=$dry_run_ref
else
	source_ref=${QUORO_VERIFY_SOURCE_REF:-"refs/tags/$tag"}
fi
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
# this tag (or branch, for a dry run), on a GitHub-hosted runner.
gh attestation verify "$tarball" \
	--bundle "$bundle" \
	--repo "$repo" \
	--signer-workflow "$signer" \
	--source-ref "$source_ref" \
	--deny-self-hosted-runners || die "attestation verification failed"

printf 'attestation: ok (%s from %s at %s)\n' "$repo" "$signer" "$source_ref"
if [ -n "$dry_run_ref" ]; then
	printf 'VERIFIED (dry run from %s, not a release)\n' "$dry_run_ref"
else
	printf 'VERIFIED\n'
fi
