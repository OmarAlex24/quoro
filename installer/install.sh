#!/bin/sh
# Quoro installer.
#
# Intended invocation:
#
#   curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo sh
#
# What this script trusts, in order:
#   1. TLS to github.com for this script and for the release artifacts.
#   2. SHA256SUMS from the release, which proves only that the download is the
#      file that was published (integrity, not provenance).
#   3. A GitHub artifact attestation (keyless Sigstore) for the tarball, issued
#      by the release workflow at ref refs/tags/v$VERSION. It is verified with
#      a pinned GitHub CLI binary whose own sha256 is pinned below.
#   4. The engine runtime (Cloud Hypervisor and the guest kernel) against the
#      sha256 pins in pins.txt, read from the tarball once the checks above
#      passed. Upstream publishes no signatures for them, so the pins inside
#      the attested tarball are the integrity anchor: TLS to GitHub -> attested
#      tarball -> pins.txt -> sha256.
#
# The repository, signer workflow, source ref and GitHub CLI pins are constants
# on purpose: no environment variable can redirect them. The single, opt-in
# exception is QUORO_INSTALL_DRY_RUN_REF, which replaces only the attested
# source ref, only for a workflow_dispatch dry-run build, and only together
# with QUORO_INSTALL_FROM. Bumping any of them is a reviewed change (ADR 0027).
#
# Every statement that does work lives inside a function; the only top-level
# statement is the final `main "$@"`, so a truncated download executes nothing.
#
# Environment (all optional):
#   QUORO_VERSION                           version to install instead of the latest
#   QUORO_INSTALL_FROM=<dir>                install from local artifacts (testing
#                                           before a release exists)
#   QUORO_INSTALL_DRY_RUN_REF=refs/heads/<branch>
#                                           verify a workflow_dispatch dry-run build
#                                           from that branch instead of the release
#                                           tag; only honored together with
#                                           QUORO_INSTALL_FROM, and never together
#                                           with the insecure flag
#   QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1 skip Sigstore verification; only honored
#                                           together with QUORO_INSTALL_FROM
#   QUORO_NO_START=1                        install files, do not start the service

set -eu
umask 022

# --- Pinned trust anchors (reviewed constants, not configurable) -------------

QUORO_REPO='OmarAlex24/quoro'
QUORO_SIGNER_WORKFLOW='OmarAlex24/quoro/.github/workflows/release.yml'
QUORO_GH_VERSION='2.103.3'
QUORO_GH_SHA256='4c806978ffce1cec70853302a56d70ac6da4f19bae7adca4e1a8ab00633dd91d'
QUORO_GH_ARCHIVE="gh_${QUORO_GH_VERSION}_linux_amd64"
QUORO_GH_URL="https://github.com/cli/cli/releases/download/v${QUORO_GH_VERSION}/${QUORO_GH_ARCHIVE}.tar.gz"

# --- State filled in by the steps below -------------------------------------

TARGET=''
VERSION=''
WORK_DIR=''
TARBALL=''
CHECKSUMS=''
BUNDLE=''
ARTIFACT_ROOT=''
BINARY_CHANGED=0
UNIT_CHANGED=0
ENGINE_CHANGED=0

# --- Output helpers ---------------------------------------------------------

log() {
    printf '%s\n' "quoro-install: $*" >&2
}

die() {
    printf '%s\n' "quoro-install: error: $*" >&2
    exit 1
}

cleanup() {
    if [ -n "${WORK_DIR:-}" ] && [ -d "$WORK_DIR" ]; then
        rm -rf "$WORK_DIR"
    fi
}

# --- Preconditions ----------------------------------------------------------

is_digits() {
    case $1 in
        '' | *[!0-9]*) return 1 ;;
    esac
    return 0
}

# Accepts X.Y.Z optionally followed by a pre-release/build tail that starts with
# '-' or '.' (the ^v?[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.-]+)?$ shape).
validate_version() {
    _ver=$1
    if [ -z "$_ver" ]; then
        return 1
    fi
    case $_ver in
        *[!0-9A-Za-z.-]*) return 1 ;;
    esac
    _rest=${_ver%%.*}
    [ "$_rest" != "$_ver" ] || return 1
    is_digits "$_rest" || return 1
    _rest=${_ver#*.}
    _patch=${_rest%%.*}
    [ "$_patch" != "$_rest" ] || return 1
    is_digits "$_patch" || return 1
    _rest=${_rest#*.}
    _patch=${_rest%%[-.]*}
    is_digits "$_patch" || return 1
    _suffix=${_rest#"$_patch"}
    if [ -n "$_suffix" ]; then
        case $_suffix in
            -?* | .?*) : ;;
            *) return 1 ;;
        esac
        case $_suffix in
            *[!0-9A-Za-z.-]*) return 1 ;;
        esac
    fi
    return 0
}

validate_dry_run_ref() {
    _ref=$1
    case $_ref in
        refs/heads/?*) : ;;
        *) return 1 ;;
    esac
    _branch=${_ref#refs/heads/}
    case $_branch in
        *[!0-9A-Za-z._/-]*) return 1 ;;
    esac
    case $_branch in
        *..* | *//* | /* | */) return 1 ;;
    esac
    return 0
}

# Looks for a host tool in the usual sbin/bin directories instead of relying on
# PATH: `curl | sudo sh` runs with the caller's PATH, and sudo's secure_path may
# differ from the one a login shell uses.
has_host_tool() {
    _tool_dir=''
    for _tool_dir in /usr/sbin /sbin /usr/bin /bin; do
        if [ -x "$_tool_dir/$1" ]; then
            return 0
        fi
    done
    return 1
}

check_preconditions() {
    _os=$(uname -s) || die 'could not detect the operating system'
    [ "$_os" = 'Linux' ] || die "this installer supports Linux only (found $_os)"

    if [ "$(id -u)" -ne 0 ]; then
        die 'run as root: curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo sh'
    fi

    [ -d /run/systemd/system ] || die 'systemd is not running on this host (/run/systemd/system is missing)'

    if [ "${QUORO_INSTALL_INSECURE_SKIP_SIGNATURE:-}" = '1' ] && [ -z "${QUORO_INSTALL_FROM:-}" ]; then
        die 'QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1 is only honored together with QUORO_INSTALL_FROM; refusing'
    fi

    if [ -n "${QUORO_INSTALL_DRY_RUN_REF:-}" ]; then
        if [ -z "${QUORO_INSTALL_FROM:-}" ]; then
            die 'QUORO_INSTALL_DRY_RUN_REF is only honored together with QUORO_INSTALL_FROM; refusing'
        fi
        if [ "${QUORO_INSTALL_INSECURE_SKIP_SIGNATURE:-}" = '1' ]; then
            die 'QUORO_INSTALL_DRY_RUN_REF cannot be combined with QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1; refusing'
        fi
        if ! validate_dry_run_ref "$QUORO_INSTALL_DRY_RUN_REF"; then
            die "invalid QUORO_INSTALL_DRY_RUN_REF '$QUORO_INSTALL_DRY_RUN_REF': expected refs/heads/<branch>"
        fi
    fi

    _machine=$(uname -m) || die 'could not detect the machine architecture'
    case $_machine in
        x86_64 | amd64) TARGET='x86_64-unknown-linux-musl' ;;
        *) die "no release for $_machine yet; only x86_64-unknown-linux-musl is published" ;;
    esac

    _missing=''
    for _tool in curl tar gzip sha256sum install mktemp cmp systemctl grep; do
        command -v "$_tool" >/dev/null 2>&1 || _missing="$_missing $_tool"
    done
    [ -z "$_missing" ] || die "missing required tools:$_missing"

    # The engine needs e2fsprogs (mkfs.ext4 for image conversion), iproute2 (ip)
    # and nftables (nft). They are installed later, after every verification
    # passed, because installing packages changes the host; here only the
    # presence of a way to install them is checked.
    if ! command -v apt-get >/dev/null 2>&1; then
        _missing=''
        for _tool in mkfs.ext4 ip nft; do
            if ! has_host_tool "$_tool"; then
                _missing="$_missing $_tool"
            fi
        done
        [ -z "$_missing" ] ||
            die "missing host tools:$_missing and no apt-get on this host; install the packages e2fsprogs, iproute2 and nftables"
    fi

    if [ ! -e /dev/kvm ]; then
        log 'warning: this host has no /dev/kvm; quoro.service will refuse to start the cloud-hypervisor engine (ADR 0009)'
    fi

    log "host checks passed (Linux, root, systemd, $TARGET)"
}

# --- Version ----------------------------------------------------------------

# Prints the version carried by the single
# quoro-<version>-x86_64-unknown-linux-musl.tar.gz in $1; fails otherwise.
version_from_install_dir() {
    _dir=$1
    _count=0
    _name=''
    for _file in "$_dir"/quoro-*-"$TARGET".tar.gz; do
        [ -f "$_file" ] || continue
        _count=$((_count + 1))
        _name=${_file##*/}
    done
    [ "$_count" -eq 1 ] || return 1
    _name=${_name#quoro-}
    _name=${_name%-"$TARGET".tar.gz}
    printf '%s\n' "$_name"
}

resolve_version() {
    _source='QUORO_VERSION'
    if [ -n "${QUORO_VERSION:-}" ]; then
        VERSION=$QUORO_VERSION
    elif [ -n "${QUORO_INSTALL_FROM:-}" ]; then
        _source="the tarball name in $QUORO_INSTALL_FROM"
        VERSION=$(version_from_install_dir "$QUORO_INSTALL_FROM") ||
            die "expected exactly one quoro-<version>-${TARGET}.tar.gz in $QUORO_INSTALL_FROM"
    else
        _source='the latest GitHub release'
        _url=$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${QUORO_REPO}/releases/latest") ||
            die 'could not resolve the latest release from GitHub; set QUORO_VERSION to pin a version'
        case $_url in
            */releases/tag/*) VERSION=${_url##*/} ;;
            *) die "no published release found in $QUORO_REPO yet; set QUORO_VERSION to install a specific version" ;;
        esac
    fi

    case $VERSION in
        v*) VERSION=${VERSION#v} ;;
        V*) VERSION=${VERSION#V} ;;
    esac

    if ! validate_version "$VERSION"; then
        die "invalid version '$VERSION' from $_source: expected something like 0.1.0 or 0.1.0-rc1"
    fi
    log "version $VERSION (from $_source)"
}

# --- Artifacts --------------------------------------------------------------

download() {
    curl -fsSL --proto '=https' --tlsv1.2 --retry 3 -o "$2" "$1"
}

fetch_artifacts() {
    _dl="$WORK_DIR/dl"
    install -d -m 0700 "$_dl" || die "could not create $_dl"

    TARBALL="quoro-${VERSION}-${TARGET}.tar.gz"
    CHECKSUMS='SHA256SUMS'
    BUNDLE="${TARBALL}.sigstore.jsonl"

    if [ -n "${QUORO_INSTALL_FROM:-}" ]; then
        _src=$QUORO_INSTALL_FROM
        [ -d "$_src" ] || die "QUORO_INSTALL_FROM=$_src is not a directory"
        log "using local artifacts from $_src"
        for _name in "$TARBALL" "$CHECKSUMS" "$BUNDLE"; do
            if [ -f "$_src/$_name" ]; then
                cp "$_src/$_name" "$_dl/$_name" || die "could not copy $_name from $_src"
            else
                log "warning: $_src/$_name is missing"
            fi
        done
    else
        _base="https://github.com/${QUORO_REPO}/releases/download/v${VERSION}"
        log "downloading v$VERSION for $TARGET"
        download "$_base/$TARBALL" "$_dl/$TARBALL" || die "could not download $TARBALL"
        download "$_base/$CHECKSUMS" "$_dl/$CHECKSUMS" || die "could not download $CHECKSUMS"
        if ! download "$_base/$BUNDLE" "$_dl/$BUNDLE"; then
            log "warning: could not download $BUNDLE"
            rm -f "$_dl/$BUNDLE"
        fi
    fi

    [ -f "$_dl/$TARBALL" ] || die "missing $TARBALL: nothing to install"
    [ -f "$_dl/$CHECKSUMS" ] || die "missing $CHECKSUMS: cannot verify $TARBALL"
}

# --- Verification -----------------------------------------------------------

verify_checksum() {
    _sums="$WORK_DIR/dl/$CHECKSUMS"
    _hash=''
    _found=0
    while IFS= read -r _line; do
        case $_line in
            *' '*) : ;;
            *) continue ;;
        esac
        _name=${_line#* }
        _name=${_name# }
        _name=${_name#\*}
        if [ "$_name" = "$TARBALL" ]; then
            _hash=${_line%% *}
            _found=1
            break
        fi
    done < "$_sums"

    [ "$_found" -eq 1 ] || die "$CHECKSUMS has no entry for $TARBALL"
    [ "${#_hash}" -eq 64 ] || die "malformed sha256 for $TARBALL in $CHECKSUMS"
    case $_hash in
        *[!0-9a-fA-F]*) die "malformed sha256 for $TARBALL in $CHECKSUMS" ;;
    esac

    printf '%s  %s\n' "$_hash" "$TARBALL" > "$WORK_DIR/dl/verify.sha256"
    if ! (cd "$WORK_DIR/dl" && sha256sum -c verify.sha256) >/dev/null 2>&1; then
        die "checksum verification failed for $TARBALL: the download does not match $CHECKSUMS"
    fi
    log "checksum verified (sha256 $_hash)"
}

signature_failed() {
    log "error: $*"
    die 'signature verification failed, nothing was installed'
}

insecure_warning() {
    printf '%s\n' \
        '' \
        'quoro-install: ############################################################' \
        'quoro-install: WARNING: SIGNATURE VERIFICATION IS DISABLED' \
        'quoro-install: QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1 is set, so the' \
        'quoro-install: tarball was only checked against SHA256SUMS. Nothing proves' \
        'quoro-install: it was built by OmarAlex24/quoro.' \
        'quoro-install: This mode exists to test local artifacts before a release' \
        'quoro-install: exists. Never use it for a real installation.' \
        'quoro-install: ############################################################' \
        '' >&2
}

dry_run_warning() {
    printf '%s\n' \
        '' \
        'quoro-install: ############################################################' \
        'quoro-install: WARNING: DRY-RUN BUILD, NOT A RELEASE' \
        'quoro-install: QUORO_INSTALL_DRY_RUN_REF is set, so this installs a' \
        'quoro-install: workflow_dispatch build from the branch' \
        "quoro-install:   ${QUORO_INSTALL_DRY_RUN_REF#refs/heads/}" \
        "quoro-install: (attested at $QUORO_INSTALL_DRY_RUN_REF)" \
        'quoro-install: instead of a release. The tarball still passes the Sigstore' \
        'quoro-install: verification, but no release was made from this commit.' \
        'quoro-install: This mode exists to verify the installer against a dry-run' \
        'quoro-install: build. Never use it for a real installation.' \
        'quoro-install: ############################################################' \
        '' >&2
}

verify_signature() {
    _source_ref="refs/tags/v$VERSION"
    if [ -n "${QUORO_INSTALL_DRY_RUN_REF:-}" ]; then
        if [ -z "${QUORO_INSTALL_FROM:-}" ]; then
            die 'QUORO_INSTALL_DRY_RUN_REF is only honored together with QUORO_INSTALL_FROM; refusing'
        fi
        if [ "${QUORO_INSTALL_INSECURE_SKIP_SIGNATURE:-}" = '1' ]; then
            die 'QUORO_INSTALL_DRY_RUN_REF cannot be combined with QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1; refusing'
        fi
        if ! validate_dry_run_ref "$QUORO_INSTALL_DRY_RUN_REF"; then
            die "invalid QUORO_INSTALL_DRY_RUN_REF '$QUORO_INSTALL_DRY_RUN_REF': expected refs/heads/<branch>"
        fi
        dry_run_warning
        _source_ref=$QUORO_INSTALL_DRY_RUN_REF
    fi

    if [ "${QUORO_INSTALL_INSECURE_SKIP_SIGNATURE:-}" = '1' ]; then
        if [ -z "${QUORO_INSTALL_FROM:-}" ]; then
            die 'QUORO_INSTALL_INSECURE_SKIP_SIGNATURE=1 is only honored together with QUORO_INSTALL_FROM; refusing'
        fi
        insecure_warning
        return 0
    fi

    _bundle="$WORK_DIR/dl/$BUNDLE"
    if [ ! -f "$_bundle" ]; then
        signature_failed "missing $BUNDLE; cannot prove the artifact was built by $QUORO_SIGNER_WORKFLOW"
    fi

    log "fetching the pinned GitHub CLI $QUORO_GH_VERSION"
    _gh_tar="$WORK_DIR/gh.tar.gz"
    if ! download "$QUORO_GH_URL" "$_gh_tar"; then
        signature_failed 'could not download the pinned GitHub CLI archive'
    fi

    _gh_sha=$(sha256sum "$_gh_tar") || signature_failed 'could not hash the pinned GitHub CLI archive'
    _gh_sha=${_gh_sha%% *}
    if [ "$_gh_sha" != "$QUORO_GH_SHA256" ]; then
        signature_failed "pinned GitHub CLI archive sha256 mismatch (got $_gh_sha)"
    fi

    _gh="$WORK_DIR/$QUORO_GH_ARCHIVE/bin/gh"
    if ! tar -xzf "$_gh_tar" -C "$WORK_DIR" "${QUORO_GH_ARCHIVE}/bin/gh"; then
        signature_failed 'could not extract gh from the pinned archive'
    fi
    [ -f "$_gh" ] || signature_failed 'the pinned GitHub CLI archive does not contain bin/gh'
    chmod 0755 "$_gh" || signature_failed 'could not make the pinned gh executable'

    install -d -m 0700 "$WORK_DIR/gh-config" || signature_failed 'could not create a gh config directory'

    log "verifying the Sigstore attestation for $TARBALL"
    _status=0
    (
        unset GH_TOKEN GITHUB_TOKEN GH_HOST
        GH_CONFIG_DIR="$WORK_DIR/gh-config" \
            GH_NO_UPDATE_NOTIFIER=1 \
            GH_PROMPT_DISABLED=1 \
            "$_gh" attestation verify "$WORK_DIR/dl/$TARBALL" \
            --bundle "$_bundle" \
            --repo "$QUORO_REPO" \
            --signer-workflow "$QUORO_SIGNER_WORKFLOW" \
            --source-ref "$_source_ref" \
            --deny-self-hosted-runners
    ) || _status=$?
    [ "$_status" -eq 0 ] || signature_failed 'gh attestation verify rejected the tarball'

    log "signature verified: keyless Sigstore attestation from $QUORO_SIGNER_WORKFLOW at $_source_ref"
}

# --- Extraction -------------------------------------------------------------

extract_artifacts() {
    _top="quoro-${VERSION}-${TARGET}"
    _tarball="$WORK_DIR/dl/$TARBALL"

    if ! tar -tzf "$_tarball" > "$WORK_DIR/entries.txt"; then
        die "could not read $TARBALL"
    fi
    [ -s "$WORK_DIR/entries.txt" ] || die "$TARBALL is empty"

    while IFS= read -r _entry; do
        case $_entry in
            '') die 'refusing an archive with an empty entry name' ;;
            /*) die "refusing an archive with an absolute path: $_entry" ;;
        esac
        case $_entry in
            "$_top" | "$_top"/*) : ;;
            *) die "refusing an archive entry outside $_top/: $_entry" ;;
        esac
        case "/${_entry}/" in
            */../*) die "refusing an archive entry with a '..' component: $_entry" ;;
        esac
    done < "$WORK_DIR/entries.txt"

    install -d -m 0755 "$WORK_DIR/x" || die 'could not create the extraction directory'
    if ! tar -xzf "$_tarball" -C "$WORK_DIR/x" --no-same-owner; then
        die "could not extract $TARBALL"
    fi

    ARTIFACT_ROOT="$WORK_DIR/x/$_top"
    for _f in quoro quoro.service quoro-init pins.txt; do
        if [ ! -f "$ARTIFACT_ROOT/$_f" ] || [ -h "$ARTIFACT_ROOT/$_f" ]; then
            die "$TARBALL does not contain $_top/$_f as a regular file"
        fi
    done
    log 'archive contents checked and extracted'
}

# --- Engine runtime ---------------------------------------------------------

file_sha256() {
    _sum=$(sha256sum "$1") || return 1
    printf '%s\n' "${_sum%% *}"
}

# Copies an engine artifact from $1 to $2 and tells whether its sha256 is $3.
# A symlink is never accepted: the engine checks the file it will execute.
copy_if_pinned() {
    [ -f "$1" ] && [ ! -h "$1" ] || return 1
    cp "$1" "$2" || return 1
    _actual=$(file_sha256 "$2") || return 1
    [ "$_actual" = "$3" ]
}

# Reads the sha256 pins shipped in the attested tarball and stages every
# artifact for this architecture in $WORK_DIR/runtime, verified against its pin.
# Runs before anything is written outside the temporary directory.
fetch_engine_runtime() {
    _pins="$ARTIFACT_ROOT/pins.txt"
    _runtime="$WORK_DIR/runtime"
    _list="$WORK_DIR/runtime.list"
    install -d -m 0700 "$_runtime" || die 'could not create the engine runtime staging directory'
    : > "$_list" || die 'could not create the engine runtime list'

    _machine=$(uname -m) || die 'could not detect the machine architecture'
    case $_machine in
        amd64) _machine='x86_64' ;;
    esac

    _line_no=0
    _have_hypervisor=0
    _have_kernel=0

    while read -r _name _version _arch _file _url _sha _extra; do
        _line_no=$((_line_no + 1))
        case $_name in
            '' | '#'*) continue ;;
        esac
        if [ -z "$_sha" ] || [ -n "$_extra" ]; then
            die "pins.txt line $_line_no: expected 6 fields (name version arch file url sha256)"
        fi
        [ "$_arch" = "$_machine" ] || continue

        case $_file in
            '' | . | .. | *[!A-Za-z0-9._-]*)
                die "pins.txt line $_line_no: unsafe file name '$_file'"
                ;;
        esac
        if [ "${#_sha}" -ne 64 ]; then
            die "pins.txt line $_line_no: sha256 must be 64 lowercase hex digits"
        fi
        case $_sha in
            *[!0-9a-f]*) die "pins.txt line $_line_no: sha256 must be 64 lowercase hex digits" ;;
        esac
        case $_url in
            https://github.com/cloud-hypervisor/*) : ;;
            *) die "pins.txt line $_line_no: url must start with https://github.com/cloud-hypervisor/" ;;
        esac

        _dest="$_runtime/$_file"
        _source=''

        if [ -n "${QUORO_INSTALL_FROM:-}" ]; then
            if copy_if_pinned "$QUORO_INSTALL_FROM/$_file" "$_dest" "$_sha"; then
                _source="QUORO_INSTALL_FROM=$QUORO_INSTALL_FROM"
            else
                rm -f "$_dest"
                if [ -e "$QUORO_INSTALL_FROM/$_file" ]; then
                    log "warning: $QUORO_INSTALL_FROM/$_file does not match the $_name pin (or is not a regular file); ignoring it"
                fi
            fi
        fi

        if [ -z "$_source" ] && [ -e "/usr/local/lib/quoro/$_file" ]; then
            if copy_if_pinned "/usr/local/lib/quoro/$_file" "$_dest" "$_sha"; then
                _source="the existing /usr/local/lib/quoro"
            else
                rm -f "$_dest"
                log "warning: the installed /usr/local/lib/quoro/$_file does not match the $_name pin (or is not a regular file); ignoring it"
            fi
        fi

        if [ -z "$_source" ]; then
            log "downloading $_name $_version from $_url"
            if ! download "$_url" "$_dest"; then
                signature_failed "could not download $_file for $_name $_version"
            fi
            _actual=$(file_sha256 "$_dest") || signature_failed "could not hash the downloaded $_file"
            if [ "$_actual" != "$_sha" ]; then
                signature_failed "$_file did not match its pin (expected $_sha, got $_actual), nothing was installed"
            fi
            _source=$_url
        fi

        log "engine runtime verified: $_name $_version ($_file from $_source, sha256 $_sha)"
        printf '%s %s %s %s\n' "$_name" "$_version" "$_file" "$_sha" >> "$_list" ||
            die 'could not record the verified engine runtime'

        case $_name in
            cloud-hypervisor) _have_hypervisor=$((_have_hypervisor + 1)) ;;
            guest-kernel) _have_kernel=$((_have_kernel + 1)) ;;
        esac
    done < "$_pins"

    [ "$_have_hypervisor" -ge 1 ] || die "pins.txt has no cloud-hypervisor pin for $_machine"
    [ "$_have_kernel" -ge 1 ] || die "pins.txt has no guest-kernel pin for $_machine"
    log "engine runtime for $_machine is staged and verified"
}

# --- Installation -----------------------------------------------------------

# Installs the host packages the engine needs. Runs only after every
# verification passed, because it changes the host.
install_host_packages() {
    _missing=''
    for _tool in mkfs.ext4 ip nft; do
        if ! has_host_tool "$_tool"; then
            _missing="$_missing $_tool"
        fi
    done
    if [ -z "$_missing" ]; then
        log 'host tools present: mkfs.ext4, ip, nft'
        return 0
    fi

    log "host tools missing:$_missing"
    if ! command -v apt-get >/dev/null 2>&1; then
        die "missing host tools:$_missing; apt-get is not available, install the packages e2fsprogs, iproute2 and nftables"
    fi

    log 'installing the host packages e2fsprogs, iproute2 and nftables'
    DEBIAN_FRONTEND=noninteractive apt-get update -q || die 'apt-get update failed'
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends e2fsprogs iproute2 nftables ||
        die 'apt-get install failed for e2fsprogs, iproute2 and nftables'

    _missing=''
    for _tool in mkfs.ext4 ip nft; do
        if ! has_host_tool "$_tool"; then
            _missing="$_missing $_tool"
        fi
    done
    [ -z "$_missing" ] || die "still missing host tools after installing e2fsprogs, iproute2 and nftables:$_missing"
    log 'host packages installed'
}

install_dirs() {
    # Older installs created a quoro system user that ran the service. The
    # engine manages a bridge, TAP devices and nftables rules and converts
    # images with ownership preserved, so the service runs as root now.
    if id -u quoro >/dev/null 2>&1; then
        log 'user quoro exists from an older install; it is no longer used (the service runs as root)'
    fi

    _data='/var/lib/quoro'
    if [ -d "$_data" ]; then
        # Older installs owned it by quoro; move it to root and close it down.
        chown root:root "$_data" || die "could not change the owner of $_data to root:root"
        chmod 0700 "$_data" || die "could not set the mode of $_data to 0700"
        log "kept $_data (root:root 0700; older installs had it owned by quoro)"
    else
        install -d -m 0700 -o root -g root "$_data" || die "could not create $_data"
        log "created $_data (root:root 0700)"
    fi

    install -d -m 0755 /etc/quoro || die 'could not create /etc/quoro'
    log 'config directory /etc/quoro is in place'
}

config_has_engine() {
    [ -r "$1" ] || return 1
    grep -q '^[[:space:]]*engine[[:space:]]*=' "$1" 2>/dev/null
}

install_config() {
    _conf='/etc/quoro/quoro.toml'
    if [ -f "$_conf" ]; then
        log "kept existing $_conf"
        if ! config_has_engine "$_conf"; then
            log 'note: the existing config has no engine line, so quoro runs the fake engine; add engine = "cloud-hypervisor" to use the hypervisor (the defaults are in installer/README.md)'
        fi
        return 0
    fi

    _tmp="$WORK_DIR/quoro.toml"
    printf '%s\n' \
        '# Quoro configuration (ADR 0054 host layout).' \
        '# Edit this file and restart quoro: systemctl restart quoro' \
        '' \
        '# One TLS listener for API, panel and MCP. Ingress keeps 80/443.' \
        'listen = "0.0.0.0:8443"' \
        '' \
        '# SQLite database, cluster key files (0600) and engine state.' \
        'data_dir = "/var/lib/quoro"' \
        '' \
        '# Engine: "cloud-hypervisor" runs one microVM per app; "fake" runs nothing' \
        '# and exists for development only.' \
        'engine = "cloud-hypervisor"' \
        '' \
        '# Engine settings. The values below are the defaults, and the block is' \
        '# commented out; uncomment a line only to change it. Guest DNS defaults' \
        "# to this host's non-loopback resolvers; set dns to override them." \
        '# [cloud_hypervisor]' \
        '# artifacts_dir = "/usr/local/lib/quoro"' \
        '# init_binary = "/usr/local/lib/quoro/quoro-init"' \
        '# state_dir = "/var/lib/quoro/engine"' \
        '# bridge = "quoro0"' \
        '# subnet = "10.42.0.0/24"' \
        '# dns = ["192.0.2.53"]' > "$_tmp"

    install -m 0640 -o root -g root "$_tmp" "$_conf" || die "could not create $_conf"
    log "wrote $_conf"
}

install_binary() {
    _dest='/usr/local/bin/quoro'
    _new='/usr/local/bin/quoro.new'

    BINARY_CHANGED=1
    if [ -f "$_dest" ] && cmp -s "$ARTIFACT_ROOT/quoro" "$_dest"; then
        BINARY_CHANGED=0
    fi

    install -m 0755 -o root -g root "$ARTIFACT_ROOT/quoro" "$_new" || die 'could not stage the quoro binary'
    mv -f "$_new" "$_dest" || die 'could not install the quoro binary'

    if [ "$BINARY_CHANGED" -eq 1 ]; then
        log "installed $_dest"
    else
        log "kept $_dest (unchanged)"
    fi
}

install_engine_runtime() {
    _lib='/usr/local/lib/quoro'
    install -d -m 0755 -o root -g root "$_lib" || die "could not create $_lib"

    ENGINE_CHANGED=0
    while read -r _name _version _file _sha; do
        [ -n "$_file" ] || die 'malformed entry in the engine runtime list'
        _src="$WORK_DIR/runtime/$_file"
        _dst="$_lib/$_file"

        # The staged copy was verified already; hash it again so a file swapped
        # between verification and installation is still caught.
        _actual=$(file_sha256 "$_src") || die "could not hash $_src"
        [ "$_actual" = "$_sha" ] || die "$_file changed since it was verified; refusing to install it"

        _mode=0644
        case $_name in
            cloud-hypervisor) _mode=0755 ;;
        esac

        _changed=1
        if [ -f "$_dst" ] && [ ! -h "$_dst" ] && cmp -s "$_src" "$_dst"; then
            _changed=0
        fi

        install -m "$_mode" -o root -g root "$_src" "$_dst.new" || die "could not stage $_dst"
        mv -f "$_dst.new" "$_dst" || die "could not install $_dst"

        if [ "$_changed" -eq 1 ]; then
            ENGINE_CHANGED=1
            log "installed $_dst ($_name $_version)"
        else
            log "kept $_dst ($_name $_version, unchanged)"
        fi
    done < "$WORK_DIR/runtime.list"

    _init_src="$ARTIFACT_ROOT/quoro-init"
    _init_dst="$_lib/quoro-init"
    _changed=1
    if [ -f "$_init_dst" ] && [ ! -h "$_init_dst" ] && cmp -s "$_init_src" "$_init_dst"; then
        _changed=0
    fi
    install -m 0755 -o root -g root "$_init_src" "$_init_dst.new" || die "could not stage $_init_dst"
    mv -f "$_init_dst.new" "$_init_dst" || die "could not install $_init_dst"
    if [ "$_changed" -eq 1 ]; then
        ENGINE_CHANGED=1
        log "installed $_init_dst"
    else
        log "kept $_init_dst (unchanged)"
    fi
}

install_unit() {
    _unit='/etc/systemd/system/quoro.service'

    UNIT_CHANGED=1
    if [ -f "$_unit" ] && cmp -s "$ARTIFACT_ROOT/quoro.service" "$_unit"; then
        UNIT_CHANGED=0
    fi

    install -m 0644 -o root -g root "$ARTIFACT_ROOT/quoro.service" "$_unit" || die "could not install $_unit"
    systemctl daemon-reload || die 'systemctl daemon-reload failed'
    if ! systemctl enable quoro.service >/dev/null 2>&1; then
        die 'systemctl enable quoro.service failed'
    fi

    if [ "$UNIT_CHANGED" -eq 1 ]; then
        log "installed $_unit and enabled quoro.service"
    else
        log "kept $_unit (unchanged) and made sure quoro.service is enabled"
    fi
}

start_service() {
    if [ "${QUORO_NO_START:-}" = '1' ]; then
        log 'QUORO_NO_START=1: not starting quoro.service'
        return 0
    fi

    if [ "$BINARY_CHANGED" -eq 1 ] || [ "$UNIT_CHANGED" -eq 1 ] || [ "$ENGINE_CHANGED" -eq 1 ] ||
        ! systemctl is-active --quiet quoro.service; then
        log 'starting quoro.service'
        # A failing restart is reported through the state check below, so the
        # operator always gets the same journal hint.
        if ! systemctl restart quoro.service; then
            log 'systemctl restart quoro.service reported an error'
        fi
    else
        log 'quoro.service already active, left running'
    fi

    # Type=simple counts as active as soon as the process runs, and quoro serve
    # checks the host (KVM, CPU flags, kernel, artifacts) after that. Require
    # five consecutive active seconds, so a refusal (exit 78) is reported here.
    _i=0
    _up=0
    while [ "$_i" -lt 20 ]; do
        if systemctl is-active --quiet quoro.service; then
            _up=$((_up + 1))
            if [ "$_up" -ge 5 ]; then
                log 'quoro.service is active'
                return 0
            fi
        else
            _up=0
        fi
        sleep 1
        _i=$((_i + 1))
    done

    log 'quoro.service did not stay active; recent journal:'
    journalctl -u quoro -n 50 --no-pager >&2 || true
    die 'quoro.service failed to start; check the journal: journalctl -u quoro -n 50 --no-pager'
}

# --- Report -----------------------------------------------------------------

report() {
    _version_out=''
    if _version_out=$(/usr/local/bin/quoro version 2>/dev/null); then
        log "installed version: $_version_out"
    elif _version_out=$(/usr/local/bin/quoro --version 2>/dev/null); then
        log "installed version: $_version_out"
    else
        die 'the installed quoro binary does not report a version (quoro version and quoro --version both failed)'
    fi

    _token=''
    if [ "${QUORO_NO_START:-}" = '1' ]; then
        log 'service not started, so no setup token was issued'
    elif _token=$(/usr/local/bin/quoro setup-token 2>/dev/null); then
        if [ -n "$_token" ]; then
            printf '\nSetup token for the first-run /setup form:\n\n    %s\n\n' "$_token"
        else
            log 'warning: quoro setup-token printed nothing'
        fi
    else
        log 'warning: could not read a setup token; run "sudo quoro setup-token" yourself'
    fi

    cat <<'EOF'
Next steps

  1. Open https://<host>:8443/ in a browser (<host> is this machine's address).
     The certificate is self-signed, so accept the browser warning once.
  2. Paste the setup token into the setup form within the hour.

The setup token is single use and expires one hour after it is issued. Print
it again with:

    sudo quoro setup-token

Back up the cluster key files under /var/lib/quoro separately from the SQLite
database (ADR 0050). A database copy alone does not contain them, and losing
the data-encryption key loses every stored secret.
EOF
}

# --- Entry point ------------------------------------------------------------

main() {
    check_preconditions
    resolve_version

    WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/quoro-install.XXXXXX") || die 'could not create a temporary directory'
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    log "installing quoro $VERSION for $TARGET"
    fetch_artifacts
    verify_checksum
    verify_signature
    extract_artifacts
    fetch_engine_runtime

    install_host_packages
    install_dirs
    install_config
    install_binary
    install_engine_runtime
    install_unit
    start_service
    report
}

main "$@"
