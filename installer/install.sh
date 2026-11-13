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
#   5. The cloudflared connector, the same way and against the same kind of
#      pins, in ingress-pins.txt: TLS to GitHub -> attested tarball ->
#      ingress-pins.txt -> sha256 (ADR 0024, ADR 0055).
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
#   QUORO_NO_START=1                        install files, do not start the services
#   QUORO_ROLE=primary|node|edge            what this host runs (default: primary).
#                                           A primary runs the unprivileged control
#                                           plane and its own node agent; a node runs
#                                           only the node agent and joins a primary
#                                           (ADR 0089); an edge runs the proxy and
#                                           the mesh, starts no VM and needs no KVM
#                                           (ADR 0143).
#   QUORO_JOIN_URL=<https url>              the primary's URL, for QUORO_ROLE=node
#                                           or edge.
#   QUORO_JOIN_TOKEN=<token>                a join token from the primary, for
#                                           QUORO_ROLE=node or edge. Never logged.
#   QUORO_JOIN_FINGERPRINT=sha256:<hex>     the primary's TLS fingerprint, for
#                                           QUORO_ROLE=node or edge. Not needed when
#                                           this host is already enrolled.

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
ROLE=''
WORK_DIR=''
TARBALL=''
CHECKSUMS=''
BUNDLE=''
ARTIFACT_ROOT=''
BINARY_CHANGED=0
UNIT_CHANGED=0
NODE_UNIT_CHANGED=0
ENGINE_CHANGED=0
INGRESS_CHANGED=0
# The version of the quoro binary this run replaces; empty on a first install.
PREVIOUS_VERSION=''

# Where the node agent keeps its identity, local store and engine state. It is a
# sibling of the control plane's data directory, never inside it, so the two can
# belong to different users (ADR 0089).
NODE_DATA_DIR='/var/lib/quoro-node'

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

# --- Roles ------------------------------------------------------------------

# Whether this host already holds a complete node enrollment (ADR 0089): the
# identity, the session credential and the private-key seed.
node_is_enrolled() {
    [ -f "$NODE_DATA_DIR/identity.json" ] &&
        [ -f "$NODE_DATA_DIR/credential" ] &&
        [ -f "$NODE_DATA_DIR/node-ed25519.seed" ]
}

# Reads QUORO_ROLE. A node runs only the node agent, an edge runs the agent
# without an engine, and a primary runs the control plane and its own agent.
resolve_role() {
    ROLE=${QUORO_ROLE:-primary}
    case $ROLE in
        primary | node | edge) : ;;
        *) die "invalid QUORO_ROLE '${QUORO_ROLE}': expected primary, node or edge" ;;
    esac
}

# A node or an edge enrolls with a join token, so it needs the primary's URL,
# its token and its TLS fingerprint. Checking them here means a missing one
# fails before any file is written; the token itself is never printed, logged
# or traced.
check_node_join_credentials() {
    if node_is_enrolled; then
        log "this host is already enrolled ($NODE_DATA_DIR); no join token needed"
        return 0
    fi

    _missing=''
    [ -n "${QUORO_JOIN_URL:-}" ] || _missing="$_missing QUORO_JOIN_URL"
    [ -n "${QUORO_JOIN_TOKEN:-}" ] || _missing="$_missing QUORO_JOIN_TOKEN"
    [ -n "${QUORO_JOIN_FINGERPRINT:-}" ] || _missing="$_missing QUORO_JOIN_FINGERPRINT"
    [ -z "$_missing" ] ||
        die "QUORO_ROLE=$ROLE needs$_missing to enroll this host; create a join token on the primary first"

    case ${QUORO_JOIN_URL} in
        https://?*) : ;;
        *) die 'QUORO_JOIN_URL must start with https:// and name the primary, for example https://quoro.example.com:8443' ;;
    esac
}

# A host that already ran a control plane keeps its database and its TLS
# material there. Turning it into a node or an edge would leave that data
# orphaned with no API to serve it, so refuse instead of installing over it.
check_node_host_is_not_a_primary() {
    if [ -e /etc/systemd/system/quoro.service ] || [ -e /var/lib/quoro/quoro.db ]; then
        die 'this host runs a Quoro control plane (found /etc/systemd/system/quoro.service or /var/lib/quoro/quoro.db); installing it as a node or edge would orphan that data. Uninstall the control plane first, then install it with QUORO_ROLE=node or QUORO_ROLE=edge'
    fi
}

# The mirror case: a node or an edge runs no control plane, and the role is not
# remembered between runs. Re-running the installer there without QUORO_ROLE
# would install and start a second control plane next to the agent, so refuse
# and name the role to pass instead.
check_primary_host_is_not_a_node() {
    if node_is_enrolled && [ ! -e /etc/systemd/system/quoro.service ] && [ ! -e /var/lib/quoro/quoro.db ]; then
        die "this host is enrolled as a node or an edge (found $NODE_DATA_DIR/identity.json) and runs no control plane; re-run the installer with the role it was installed with, QUORO_ROLE=node or QUORO_ROLE=edge"
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

    resolve_role

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
    # and nftables (nft); Git builds need git, which fetches the app's commit on
    # the node (ADR 0118). They are installed later, after every verification
    # passed, because installing packages changes the host; here only the
    # presence of a way to install them is checked. An edge runs no engine but
    # needs ip and nft for the mesh (ADR 0144), so it gets the same packages.
    if ! command -v apt-get >/dev/null 2>&1; then
        _missing=''
        for _tool in mkfs.ext4 ip nft git; do
            if ! has_host_tool "$_tool"; then
                _missing="$_missing $_tool"
            fi
        done
        [ -z "$_missing" ] ||
            die "missing host tools:$_missing and no apt-get on this host; install the packages e2fsprogs, iproute2, nftables and git"
    fi

    case $ROLE in
        primary)
            check_primary_host_is_not_a_node
            # The control plane runs as this system user; useradd creates it.
            has_host_tool useradd || die 'useradd is missing; the primary needs it to create the quoro-api system user'
            ;;
        node | edge)
            check_node_join_credentials
            check_node_host_is_not_a_primary
            ;;
    esac

    # An edge needs no KVM (ADR 0143), so the warning names only the roles that
    # run the engine.
    if [ "$ROLE" != 'edge' ] && [ ! -e /dev/kvm ]; then
        log 'warning: this host has no /dev/kvm; quoro-node.service will refuse to start the cloud-hypervisor engine (ADR 0009)'
    fi

    log "host checks passed (Linux, root, systemd, $TARGET, role $ROLE)"
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
    for _f in quoro quoro.service quoro-node.service quoro-init pins.txt; do
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

# Sets MACHINE_ARCH to the architecture name the pins files use.
resolve_pin_arch() {
    if ! MACHINE_ARCH=$(uname -m); then
        die 'could not detect the machine architecture'
    fi
    case $MACHINE_ARCH in
        amd64) MACHINE_ARCH='x86_64' ;;
    esac
}

# Rejects a pin line whose file name could escape the staging directory, whose
# sha256 is not 64 lowercase hex digits, or whose URL is not on the pinned
# upstream. $1 is the pins file name for the message, $2 the line number, $3 the
# pinned file, $4 its sha256, $5 its URL and $6 the URL prefix that upstream
# publishes its releases under.
validate_pin_fields() {
    _pin_label=$1
    _pin_line_no=$2
    _pin_file=$3
    _pin_sha=$4
    _pin_url=$5
    _pin_url_prefix=$6

    case $_pin_file in
        '' | . | .. | *[!A-Za-z0-9._-]*)
            die "$_pin_label line $_pin_line_no: unsafe file name '$_pin_file'"
            ;;
    esac
    if [ "${#_pin_sha}" -ne 64 ]; then
        die "$_pin_label line $_pin_line_no: sha256 must be 64 lowercase hex digits"
    fi
    case $_pin_sha in
        *[!0-9a-f]*) die "$_pin_label line $_pin_line_no: sha256 must be 64 lowercase hex digits" ;;
    esac
    case $_pin_url in
        "$_pin_url_prefix"*) : ;;
        *) die "$_pin_label line $_pin_line_no: url must start with $_pin_url_prefix" ;;
    esac
}

# Stages one pinned artifact at $1/$2 after checking it against the sha256 in
# $3. It tries QUORO_INSTALL_FROM first, then the installed /usr/local/lib/quoro,
# then the URL in $4. A local candidate that does not match is ignored with a
# warning and the next source is tried; a download that does not match is fatal.
# $5 and $6 name the artifact and its version for the log. On success the source
# that matched is left in STAGED_SOURCE. It is a variable and not stdout so a
# failure dies in this shell rather than in a command substitution.
stage_pinned_artifact() {
    _stage_dir=$1
    _stage_file=$2
    _stage_sha=$3
    _stage_url=$4
    _stage_name=$5
    _stage_version=$6

    STAGED_SOURCE=''
    _stage_dest="$_stage_dir/$_stage_file"

    if [ -n "${QUORO_INSTALL_FROM:-}" ]; then
        if copy_if_pinned "$QUORO_INSTALL_FROM/$_stage_file" "$_stage_dest" "$_stage_sha"; then
            STAGED_SOURCE="QUORO_INSTALL_FROM=$QUORO_INSTALL_FROM"
        else
            rm -f "$_stage_dest"
            if [ -e "$QUORO_INSTALL_FROM/$_stage_file" ]; then
                log "warning: $QUORO_INSTALL_FROM/$_stage_file does not match the $_stage_name pin (or is not a regular file); ignoring it"
            fi
        fi
    fi

    if [ -z "$STAGED_SOURCE" ] && [ -e "/usr/local/lib/quoro/$_stage_file" ]; then
        if copy_if_pinned "/usr/local/lib/quoro/$_stage_file" "$_stage_dest" "$_stage_sha"; then
            STAGED_SOURCE='the existing /usr/local/lib/quoro'
        else
            rm -f "$_stage_dest"
            log "warning: the installed /usr/local/lib/quoro/$_stage_file does not match the $_stage_name pin (or is not a regular file); ignoring it"
        fi
    fi

    if [ -z "$STAGED_SOURCE" ]; then
        log "downloading $_stage_name $_stage_version from $_stage_url"
        if ! download "$_stage_url" "$_stage_dest"; then
            signature_failed "could not download $_stage_file for $_stage_name $_stage_version"
        fi
        _stage_actual=$(file_sha256 "$_stage_dest") || signature_failed "could not hash the downloaded $_stage_file"
        if [ "$_stage_actual" != "$_stage_sha" ]; then
            signature_failed "$_stage_file did not match its pin (expected $_stage_sha, got $_stage_actual), nothing was installed"
        fi
        STAGED_SOURCE=$_stage_url
    fi
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

    resolve_pin_arch
    _machine=$MACHINE_ARCH

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

        # Cloud Hypervisor comes from its own releases. The guest kernel comes
        # from Quoro's guest-kernel-* releases (ADR 0152); a tarball from
        # before them still pins Cloud Hypervisor's kernel build.
        _prefix='https://github.com/cloud-hypervisor/'
        case $_name:$_url in
            "guest-kernel:https://github.com/${QUORO_REPO}/releases/download/guest-kernel-"*)
                _prefix="https://github.com/${QUORO_REPO}/releases/download/guest-kernel-"
                ;;
        esac
        validate_pin_fields 'pins.txt' "$_line_no" "$_file" "$_sha" "$_url" "$_prefix"

        stage_pinned_artifact "$_runtime" "$_file" "$_sha" "$_url" "$_name" "$_version"
        _source=$STAGED_SOURCE

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

# Stages the cloudflared connector the node agent runs to serve app hostnames
# through Cloudflare Tunnel (ADR 0024, ADR 0055), from the ingress pins in the
# same attested tarball. An older tarball has no ingress pins file: the install
# then continues without a connector and says so, because the rest of the host
# is still usable and a node can be updated later.
fetch_ingress_runtime() {
    _pins="$ARTIFACT_ROOT/ingress-pins.txt"
    if [ ! -f "$_pins" ] || [ -h "$_pins" ]; then
        log "warning: $TARBALL has no ingress-pins.txt (older release); cloudflared is not installed, so the node agent cannot run the Cloudflare Tunnel connector"
        return 0
    fi

    _ingress="$WORK_DIR/ingress"
    _list="$WORK_DIR/ingress.list"
    install -d -m 0700 "$_ingress" || die 'could not create the connector staging directory'
    : > "$_list" || die 'could not create the connector list'

    resolve_pin_arch
    _machine=$MACHINE_ARCH

    _line_no=0
    _have_connector=0

    while read -r _name _version _arch _file _url _sha _extra; do
        _line_no=$((_line_no + 1))
        case $_name in
            '' | '#'*) continue ;;
        esac
        if [ -z "$_sha" ] || [ -n "$_extra" ]; then
            die "ingress-pins.txt line $_line_no: expected 6 fields (name version arch file url sha256)"
        fi
        [ "$_arch" = "$_machine" ] || continue
        if [ "$_name" != 'cloudflared' ]; then
            die "ingress-pins.txt line $_line_no: unknown artifact '$_name'; only cloudflared is pinned here"
        fi

        validate_pin_fields 'ingress-pins.txt' "$_line_no" "$_file" "$_sha" "$_url" \
            'https://github.com/cloudflare/cloudflared/releases/download/'

        stage_pinned_artifact "$_ingress" "$_file" "$_sha" "$_url" "$_name" "$_version"
        _source=$STAGED_SOURCE

        log "connector verified: $_name $_version ($_file from $_source, sha256 $_sha)"
        printf '%s %s %s %s\n' "$_name" "$_version" "$_file" "$_sha" >> "$_list" ||
            die 'could not record the verified connector'

        _have_connector=$((_have_connector + 1))
    done < "$_pins"

    [ "$_have_connector" -ge 1 ] ||
        die "ingress-pins.txt has no cloudflared pin for $_machine"
    log "cloudflared for $_machine is staged and verified"
}

# --- Installation -----------------------------------------------------------

# Installs the host packages the engine needs. Runs only after every
# verification passed, because it changes the host.
install_host_packages() {
    _missing=''
    for _tool in mkfs.ext4 ip nft git; do
        if ! has_host_tool "$_tool"; then
            _missing="$_missing $_tool"
        fi
    done
    if [ -z "$_missing" ]; then
        log 'host tools present: mkfs.ext4, ip, nft, git'
        return 0
    fi

    log "host tools missing:$_missing"
    if ! command -v apt-get >/dev/null 2>&1; then
        die "missing host tools:$_missing; apt-get is not available, install the packages e2fsprogs, iproute2, nftables and git"
    fi

    log 'installing the host packages e2fsprogs, iproute2, nftables and git'
    DEBIAN_FRONTEND=noninteractive apt-get update -q || die 'apt-get update failed'
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends e2fsprogs iproute2 nftables git ||
        die 'apt-get install failed for e2fsprogs, iproute2, nftables and git'

    _missing=''
    for _tool in mkfs.ext4 ip nft git; do
        if ! has_host_tool "$_tool"; then
            _missing="$_missing $_tool"
        fi
    done
    [ -z "$_missing" ] || die "still missing host tools after installing e2fsprogs, iproute2, nftables and git:$_missing"
    log 'host packages installed'
}

# Installs xfsprogs when it is missing. Optional: on a data directory without
# reflink the engine keeps volumes in a loop-mounted XFS pool so that volume
# snapshots need no pause (ADR 0156); without mkfs.xfs it falls back to
# pause_copy, so a failure here is a warning, never fatal.
install_optional_packages() {
    if has_host_tool mkfs.xfs && has_host_tool xfs_growfs; then
        log 'host tools present: mkfs.xfs, xfs_growfs'
        return 0
    fi
    if ! command -v apt-get >/dev/null 2>&1; then
        log 'warning: mkfs.xfs is missing and apt-get is not available; install xfsprogs, or volume snapshots on a filesystem without reflink pause the app (ADR 0156)'
        return 0
    fi
    log 'installing the optional host package xfsprogs (volume pool, ADR 0156)'
    if DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends xfsprogs ||
        { DEBIAN_FRONTEND=noninteractive apt-get update -q &&
            DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends xfsprogs; }; then
        log 'xfsprogs installed'
    else
        log 'warning: apt-get install failed for xfsprogs; volume snapshots on a filesystem without reflink pause the app (ADR 0156)'
    fi
}

install_dirs() {
    # Older installs created a quoro system user that ran the service. The
    # engine manages a bridge, TAP devices and nftables rules and converts
    # images with ownership preserved, so the node agent runs as root now.
    if id -u quoro >/dev/null 2>&1; then
        log 'user quoro exists from an older install; it is no longer used'
    fi

    # The node agent's own directory belongs to root on every role: only
    # quoro-node.service, which runs as root, reads it.
    if [ -d "$NODE_DATA_DIR" ]; then
        chown root:root "$NODE_DATA_DIR" || die "could not change the owner of $NODE_DATA_DIR to root:root"
        chmod 0700 "$NODE_DATA_DIR" || die "could not set the mode of $NODE_DATA_DIR to 0700"
        log "kept $NODE_DATA_DIR (root:root 0700)"
    else
        install -d -m 0700 -o root -g root "$NODE_DATA_DIR" || die "could not create $NODE_DATA_DIR"
        log "created $NODE_DATA_DIR (root:root 0700)"
    fi

    install -d -m 0755 /etc/quoro || die 'could not create /etc/quoro'
    log 'config directory /etc/quoro is in place'
}

# The control plane's system user (ADR 0089). It never gets a home, a shell or a
# password: it exists so the API and panel run without privileges, and so the
# data directory has an owner that is not root and not a human.
install_api_user() {
    _user='quoro-api'

    if id -u "$_user" >/dev/null 2>&1; then
        _uid=$(id -u "$_user") || die "could not read the uid of $_user"
        if [ "$_uid" -ge 1000 ]; then
            die "user $_user exists with uid $_uid, which is a login account; refusing to hand /var/lib/quoro to a human account. Remove it and re-run: userdel $_user"
        fi
        log "user $_user exists (uid $_uid); left as is"
        return 0
    fi

    # Prefer the nologin shell; the first one present is used.
    _shell='/usr/sbin/nologin'
    if [ ! -x "$_shell" ]; then
        _shell='/sbin/nologin'
    fi
    if [ ! -x "$_shell" ]; then
        _shell='/bin/false'
    fi

    useradd --system --user-group --no-create-home --home-dir /var/lib/quoro --shell "$_shell" "$_user" ||
        die "could not create the system user $_user"
    log "created the system user $_user (no home, $_shell)"
}

# /var/lib/quoro belongs to the unprivileged control plane, with one exception:
# engine/ is the M1 engine state, written by the root service of ADR 0088. The
# control plane must not read it, so it stays root-owned.
install_primary_data_dir() {
    _data='/var/lib/quoro'

    if [ ! -d "$_data" ]; then
        install -d -m 0700 -o quoro-api -g quoro-api "$_data" || die "could not create $_data"
        log "created $_data (quoro-api:quoro-api 0700)"
        return 0
    fi

    if [ -d "$_data/engine" ]; then
        log "$_data/engine is the M1 engine state and stays root-owned; the node agent keeps its engine state in $NODE_DATA_DIR/engine from now on, and VMs created by M1 are not adopted"
    fi

    # A service still running as root holds the database open and can recreate a
    # file under root after the ownership changed, so stop it first. It starts
    # again as quoro.service later in this run.
    if systemctl is-active --quiet quoro.service 2>/dev/null &&
        [ -n "$(find "$_data" -maxdepth 0 ! -user quoro-api 2>/dev/null)" ]; then
        systemctl stop quoro.service || die 'could not stop quoro.service before changing the owner of /var/lib/quoro'
        log 'stopped quoro.service before handing /var/lib/quoro to quoro-api'
    fi

    # On an upgrade from M1 everything here is root-owned. Hand it to the API
    # user except engine/, which keeps root.
    find "$_data" -mindepth 1 -maxdepth 1 ! -name engine -exec chown -R quoro-api:quoro-api {} + ||
        die "could not change the owner of $_data to quoro-api:quoro-api"
    chown quoro-api:quoro-api "$_data" || die "could not change the owner of $_data to quoro-api:quoro-api"
    chmod 0700 "$_data" || die "could not set the mode of $_data to 0700"
    log "kept $_data (quoro-api:quoro-api 0700)"
}

config_has_engine() {
    [ -r "$1" ] || return 1
    grep -q '^[[:space:]]*engine[[:space:]]*=' "$1" 2>/dev/null
}

config_has_node_table() {
    [ -r "$1" ] || return 1
    grep -q '^[[:space:]]*\[node\][[:space:]]*$' "$1" 2>/dev/null
}

config_has_node_local_false() {
    [ -r "$1" ] || return 1
    grep -q '^[[:space:]]*local[[:space:]]*=[[:space:]]*false[[:space:]]*$' "$1" 2>/dev/null
}

install_config() {
    if [ "$ROLE" = 'primary' ]; then
        install_primary_config
    else
        install_node_config
    fi
}

install_primary_config() {
    _conf='/etc/quoro/quoro.toml'
    if [ -f "$_conf" ]; then
        log "kept existing $_conf"
        if ! config_has_engine "$_conf"; then
            log 'note: the existing config has no engine line, so quoro runs the fake engine; add engine = "cloud-hypervisor" to use the hypervisor (the defaults are in installer/README.md)'
        fi

        # Without this table, quoro serve would also try to run the node agent
        # in its own unprivileged process and refuse (ADR 0089).
        if ! config_has_node_table "$_conf"; then
            printf '\n[node]\n# The node agent runs as quoro-node.service (ADR 0089).\nlocal = false\n' >> "$_conf" ||
                die "could not append the [node] table to $_conf"
            log "appended the [node] table (local = false) to $_conf"
        elif ! config_has_node_local_false "$_conf"; then
            log "warning: $_conf has a [node] table without 'local = false'; quoro serve would also try to run the node agent unprivileged and refuse. Set local = false in that table"
        fi

        # The API must be able to read the config and only root may write it.
        chown root:quoro-api "$_conf" || die "could not change the owner of $_conf to root:quoro-api"
        chmod 0640 "$_conf" || die "could not set the mode of $_conf to 0640"
        return 0
    fi

    _tmp="$WORK_DIR/quoro.toml"
    printf '%s\n' \
        '# Quoro configuration (ADR 0054 host layout, ADR 0089 privilege split).' \
        '# Edit this file and restart quoro: systemctl restart quoro quoro-node' \
        '' \
        '# One TLS listener for API, panel and MCP. Ingress keeps 80/443.' \
        'listen = "0.0.0.0:8443"' \
        '' \
        '# SQLite database, cluster key files (0600) and the local join file.' \
        'data_dir = "/var/lib/quoro"' \
        '' \
        '# Engine: "cloud-hypervisor" runs one microVM per app; "fake" runs nothing' \
        '# and exists for development only.' \
        'engine = "cloud-hypervisor"' \
        '' \
        '# Base URL nodes dial, for example "https://quoro.example.com:8443".' \
        '# Unset, it defaults to the Host header of the request that created the' \
        '# join token, which is right for most installs.' \
        '# public_url = "https://<this host>:8443"' \
        '' \
        '# Engine settings. The values below are the defaults, and the block is' \
        '# commented out; uncomment a line only to change it. Guest DNS defaults' \
        "# to this host's non-loopback resolvers; set dns to override them." \
        '# [cloud_hypervisor]' \
        '# artifacts_dir = "/usr/local/lib/quoro"' \
        '# init_binary = "/usr/local/lib/quoro/quoro-init"' \
        '# state_dir = "/var/lib/quoro-node/engine"' \
        '# bridge = "quoro0"' \
        '# subnet = "10.42.0.0/24"' \
        '# dns = ["192.0.2.53"]' \
        '' \
        '[node]' \
        '# The node agent runs as quoro-node.service, separate from the' \
        '# unprivileged control plane (ADR 0089).' \
        'local = false' > "$_tmp"

    install -m 0640 -o root -g quoro-api "$_tmp" "$_conf" || die "could not create $_conf"
    log "wrote $_conf (root:quoro-api 0640)"
}

install_node_config() {
    _conf='/etc/quoro/quoro.toml'
    if [ -f "$_conf" ]; then
        log "kept existing $_conf"
        return 0
    fi

    _tmp="$WORK_DIR/quoro.toml"
    if [ "$ROLE" = 'edge' ]; then
        _purpose='# This host is an edge: it runs the proxy and the mesh for its
# primary, starts no microVM and runs no control plane (ADR 0143). Edit this
# file and restart with:'
    else
        _purpose='# This host is a node: it runs microVMs for its primary and runs no
# control plane. Edit this file and restart with:'
    fi
    printf '%s\n' \
        '# Quoro configuration (ADR 0089 privilege split).' \
        "$_purpose" \
        '#   systemctl restart quoro-node' \
        '' \
        '# Engine: "cloud-hypervisor" runs one microVM per app; "fake" runs nothing' \
        '# and exists for development only. An edge starts no VM, so the agent' \
        '# ignores this line and runs without an engine.' \
        'engine = "cloud-hypervisor"' \
        '' \
        '# Engine settings. The values below are the defaults, and the block is' \
        '# commented out; uncomment a line only to change it. Guest DNS defaults' \
        "# to this host's non-loopback resolvers; set dns to override them." \
        '# [cloud_hypervisor]' \
        '# artifacts_dir = "/usr/local/lib/quoro"' \
        '# init_binary = "/usr/local/lib/quoro/quoro-init"' \
        '# state_dir = "/var/lib/quoro-node/engine"' \
        '# bridge = "quoro0"' \
        '# subnet = "10.42.0.0/24"' \
        '# dns = ["192.0.2.53"]' \
        '' \
        '[node]' \
        '# The identity, local store and engine state of the agent on this host.' \
        '# data_dir = "/var/lib/quoro-node"' > "$_tmp"

    install -m 0640 -o root -g root "$_tmp" "$_conf" || die "could not create $_conf"
    log "wrote $_conf (root:root 0640)"
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

# Installs the verified connector. The node agent runs it from this exact path,
# so it gets the fixed name cloudflared whatever the upstream asset is called.
# An older tarball carried no ingress pins, so an empty list is not an error.
install_ingress_runtime() {
    if [ ! -s "$WORK_DIR/ingress.list" ]; then
        return 0
    fi

    _lib='/usr/local/lib/quoro'
    install -d -m 0755 -o root -g root "$_lib" || die "could not create $_lib"

    INGRESS_CHANGED=0
    while read -r _name _version _file _sha; do
        [ -n "$_file" ] || die 'malformed entry in the connector list'
        _src="$WORK_DIR/ingress/$_file"
        _dst="$_lib/cloudflared"

        # Hash the staged copy again so a file swapped between verification and
        # installation is still caught.
        _actual=$(file_sha256 "$_src") || die "could not hash $_src"
        [ "$_actual" = "$_sha" ] || die "$_file changed since it was verified; refusing to install it"

        _changed=1
        if [ -f "$_dst" ] && [ ! -h "$_dst" ] && cmp -s "$_src" "$_dst"; then
            _changed=0
        fi

        install -m 0755 -o root -g root "$_src" "$_dst.new" || die "could not stage $_dst"
        mv -f "$_dst.new" "$_dst" || die "could not install $_dst"

        if [ "$_changed" -eq 1 ]; then
            INGRESS_CHANGED=1
            log "installed $_dst ($_name $_version)"
        else
            log "kept $_dst ($_name $_version, unchanged)"
        fi
    done < "$WORK_DIR/ingress.list"
}

# Installs one unit from the tarball and enables it. $1 is the unit name without
# its .service suffix; the result is recorded for the start decision below.
install_unit() {
    _name=$1
    _unit="/etc/systemd/system/${_name}.service"

    _changed=1
    if [ -f "$_unit" ] && cmp -s "$ARTIFACT_ROOT/${_name}.service" "$_unit"; then
        _changed=0
    fi

    install -m 0644 -o root -g root "$ARTIFACT_ROOT/${_name}.service" "$_unit" || die "could not install $_unit"
    systemctl daemon-reload || die 'systemctl daemon-reload failed'
    if ! systemctl enable "${_name}.service" >/dev/null 2>&1; then
        die "systemctl enable ${_name}.service failed"
    fi

    if [ "$_changed" -eq 1 ]; then
        log "installed $_unit and enabled ${_name}.service"
    else
        log "kept $_unit (unchanged) and made sure ${_name}.service is enabled"
    fi

    case $_name in
        quoro) UNIT_CHANGED=$_changed ;;
        quoro-node) NODE_UNIT_CHANGED=$_changed ;;
    esac
}

# A primary runs the control plane and its own node agent, a node and an edge
# run the agent alone (ADR 0089, ADR 0143). The control plane goes first so its
# unit is enabled first.
install_units() {
    if [ "$ROLE" = 'primary' ]; then
        install_unit 'quoro'
    fi
    install_unit 'quoro-node'
}

# Enrolls this host with its primary, before the unit starts. The token is only
# passed as an argument: quoro join prints the node id or its own error, never
# the token. An edge passes --role edge, which the agent persists (ADR 0143).
enroll_node() {
    if node_is_enrolled; then
        log "this host is already enrolled ($NODE_DATA_DIR); not joining again"
        return 0
    fi

    log "enrolling this host with $QUORO_JOIN_URL as a $ROLE"
    _status=0
    if [ "$ROLE" = 'edge' ]; then
        _out=$(/usr/local/bin/quoro join "$QUORO_JOIN_URL" \
            --token "$QUORO_JOIN_TOKEN" \
            --fingerprint "$QUORO_JOIN_FINGERPRINT" \
            --role edge 2> "$WORK_DIR/join.err") || _status=$?
    else
        _out=$(/usr/local/bin/quoro join "$QUORO_JOIN_URL" \
            --token "$QUORO_JOIN_TOKEN" \
            --fingerprint "$QUORO_JOIN_FINGERPRINT" 2> "$WORK_DIR/join.err") || _status=$?
    fi
    if [ "$_status" -ne 0 ]; then
        cat "$WORK_DIR/join.err" >&2 || true
        # A voter's enrollment while the voter set changes is refused with a
        # 409 whose detail names the voters, and the refusal leaves the token
        # unused (node-agent-protocol.md sections 2.3 and 8.10 rule 2).
        if grep -q 'HTTP 409: .*voter' "$WORK_DIR/join.err" 2>/dev/null; then
            die 'enrollment refused until the voter set settles; the join token was not used, so run the installer again with the same QUORO_JOIN_TOKEN once the voters have caught up'
        fi
        die 'enrollment failed; a join token is single use and expires, so create a new one on the primary and pass it in QUORO_JOIN_TOKEN'
    fi
    log "enrolled: $_out"
}

# Waits until $1 has been active for five consecutive seconds. Type=simple
# counts as active as soon as the process runs, and quoro checks the host after
# that, so a refusal (exit 78) is reported here instead of a moment later.
wait_until_active() {
    _unit=$1

    _i=0
    _up=0
    while [ "$_i" -lt 20 ]; do
        if systemctl is-active --quiet "$_unit"; then
            _up=$((_up + 1))
            if [ "$_up" -ge 5 ]; then
                log "$_unit is active"
                return 0
            fi
        else
            _up=0
        fi
        sleep 1
        _i=$((_i + 1))
    done

    log "$_unit did not stay active; recent journal:"
    journalctl -u "$_unit" -n 50 --no-pager >&2 || true
    die "$_unit failed to start; check the journal: journalctl -u $_unit -n 50 --no-pager"
}

# Restarts $1 when the binary, the engine runtime, the connector or its own unit
# changed, or when it is not running, then waits for it to stay active. $2 says
# whether the unit file changed.
start_unit() {
    _unit=$1
    _unit_changed=$2

    if [ "$BINARY_CHANGED" -eq 0 ] && [ "$ENGINE_CHANGED" -eq 0 ] &&
        [ "$INGRESS_CHANGED" -eq 0 ] &&
        [ "$_unit_changed" -eq 0 ] && systemctl is-active --quiet "$_unit"; then
        log "$_unit already active, left running"
        return 0
    fi

    log "starting $_unit"
    # A failing restart is reported through the state check below, so the
    # operator always gets the same journal hint.
    if ! systemctl restart "$_unit"; then
        log "systemctl restart $_unit reported an error"
    fi
    wait_until_active "$_unit"
}

start_services() {
    if [ "${QUORO_NO_START:-}" = '1' ]; then
        log 'QUORO_NO_START=1: not starting any unit'
        return 0
    fi

    # quoro.service first: on a primary it writes the local join file that
    # quoro-node.service reads to enroll itself, so the order is required.
    if [ "$ROLE" = 'primary' ]; then
        start_unit 'quoro.service' "$UNIT_CHANGED"
        start_unit 'quoro-node.service' "$NODE_UNIT_CHANGED"
    else
        start_unit 'quoro-node.service' "$NODE_UNIT_CHANGED"
    fi
}

# --- Report -----------------------------------------------------------------

# An install over an existing quoro binary is an upgrade (or a re-run of the
# same version). Its version is read before the binary is replaced, so the
# report can say what changed and skip the first-run steps.
read_previous_version() {
    _bin='/usr/local/bin/quoro'
    [ -x "$_bin" ] || return 0
    _out=''
    if _out=$("$_bin" version 2>/dev/null) || _out=$("$_bin" --version 2>/dev/null); then
        PREVIOUS_VERSION=${_out#quoro }
        PREVIOUS_VERSION=${PREVIOUS_VERSION#v}
    fi
    if [ -z "$PREVIOUS_VERSION" ]; then
        PREVIOUS_VERSION='an earlier version'
    fi
}

# "upgraded from 0.1.1 to 0.2.0", or the same for a re-run of one version.
describe_upgrade() {
    if [ "$PREVIOUS_VERSION" = "$VERSION" ]; then
        printf 'reinstalled %s over the same version' "$VERSION"
    else
        printf 'upgraded from %s to %s' "$PREVIOUS_VERSION" "$VERSION"
    fi
}

print_installed_version() {
    _version_out=''
    if _version_out=$(/usr/local/bin/quoro version 2>/dev/null); then
        log "installed version: $_version_out"
    elif _version_out=$(/usr/local/bin/quoro --version 2>/dev/null); then
        log "installed version: $_version_out"
    else
        die 'the installed quoro binary does not report a version (quoro version and quoro --version both failed)'
    fi
}

# Reads node_id out of the enrolled identity. No jq on a fresh host, so a sed
# reads the one JSON member needed.
node_id_from_identity() {
    sed -n 's/.*"node_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$NODE_DATA_DIR/identity.json" 2>/dev/null |
        head -n 1
}

# The primary's TLS fingerprint is the value a node pins, so printing it here
# saves the operator a journal read. quoro serve logs it at start.
print_fingerprint() {
    _fp=''
    if _line=$(journalctl -u quoro -n 200 --no-pager 2>/dev/null | grep 'tls fingerprint:' | tail -n 1); then
        _fp=${_line##*tls fingerprint: }
        _fp=${_fp%% *}
    fi

    case $_fp in
        sha256:*)
            printf '\nThis host'"'"'s TLS fingerprint (QUORO_JOIN_FINGERPRINT on a node):\n\n    %s\n\n' "$_fp"
            ;;
        *)
            log 'could not read the TLS fingerprint from the journal; see journalctl -u quoro'
            ;;
    esac
}

report_primary() {
    _token=''
    _err="$WORK_DIR/setup-token.err"
    if [ "${QUORO_NO_START:-}" = '1' ]; then
        if [ -n "$PREVIOUS_VERSION" ]; then
            report_primary_upgrade "$(describe_upgrade); services not started"
            return 0
        fi
        log 'services not started, so no setup token was issued'
    elif _token=$(/usr/local/bin/quoro setup-token 2>"$_err"); then
        if [ -n "$_token" ]; then
            printf '\nSetup token for the first-run /setup form:\n\n    %s\n\n' "$_token"
        else
            log 'warning: quoro setup-token printed nothing'
        fi
    elif grep -q 'an owner already exists' "$_err" 2>/dev/null; then
        # The cluster is set up: there is no token to issue and no first-run
        # step to repeat.
        if [ -n "$PREVIOUS_VERSION" ]; then
            report_primary_upgrade "$(describe_upgrade); the cluster is already set up"
            return 0
        fi
        log 'the cluster is already set up; no setup token is needed'
    else
        log 'warning: could not read a setup token; run "sudo quoro setup-token" yourself'
    fi

    print_fingerprint

    cat <<'EOF'
Next steps

  1. Open https://<host>:8443/ in a browser (<host> is this machine's address).
     The certificate is self-signed, so accept the browser warning once.
  2. Paste the setup token into the setup form within the hour.
  3. To add another host as a node, create a join token in the panel or with
     POST /api/v1/nodes/join-tokens, then run on that host:

         curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh \
           | sudo QUORO_ROLE=node QUORO_JOIN_URL=<this host's https URL> \
                  QUORO_JOIN_TOKEN=<token> \
                  QUORO_JOIN_FINGERPRINT=<fingerprint above> sh

     The join token is single use and expires, so pass it right after creating
     it. Its QUORO_JOIN_FINGERPRINT is the value printed above.

The setup token is single use and expires one hour after it is issued. Print
it again with:

    sudo quoro setup-token

Back up the cluster key files under /var/lib/quoro separately from the SQLite
database (ADR 0050). A database copy alone does not contain them, and losing
the data-encryption key loses every stored secret.
EOF
}

# A primary upgraded over an earlier version: no setup token, no first-run
# steps, only what an upgrade asks for next. $1 is the line to log.
report_primary_upgrade() {
    log "$1"
    print_fingerprint
    cat <<EOF
Next steps

  Upgrade every node and edge of this cluster to the same version: re-run the
  installer on each host with its role and this version, for example

      curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh \\
        | sudo QUORO_ROLE=node QUORO_VERSION=$VERSION sh

  and QUORO_ROLE=edge on an edge. An enrolled host needs no join token. See
  "Upgrading" in installer/README.md.
EOF
}

report_node() {
    _node_id=$(node_id_from_identity)
    if [ -n "$_node_id" ]; then
        log "node id: $_node_id"
    else
        log "warning: could not read the node id from $NODE_DATA_DIR/identity.json"
    fi
    if [ -n "$PREVIOUS_VERSION" ]; then
        report_agent_upgrade
        return 0
    fi

    cat <<'EOF'
Next steps

  This host is a node: it runs microVMs for its primary and serves no API or
  panel of its own. When the agent runs, the node shows as connected on the
  primary's Nodes page.

  The agent keeps its VMs running while it is stopped or restarted
  (KillMode=process): manage and stop them from the primary. Read the agent's
  journal with:

      journalctl -u quoro-node
EOF
}

report_edge() {
    _node_id=$(node_id_from_identity)
    if [ -n "$_node_id" ]; then
        log "edge id: $_node_id"
    else
        log "warning: could not read the edge id from $NODE_DATA_DIR/identity.json"
    fi
    if [ -n "$PREVIOUS_VERSION" ]; then
        report_agent_upgrade
        return 0
    fi

    cat <<'EOF'
Next steps

  This host is an edge: it runs the proxy and the mesh for its primary and
  starts no microVM, so it needs no /dev/kvm. It serves no API or panel of its
  own; when the agent runs, the edge shows as connected on the primary's Nodes
  page.

  Read the agent's journal with:

      journalctl -u quoro-node
EOF
}

# A node or an edge that ran an earlier version: the agent kept its identity
# and was restarted on the new binary.
report_agent_upgrade() {
    log "$(describe_upgrade)"
    cat <<'EOF'
Next steps

  Nothing else to do on this host: it kept its enrollment, and the agent runs
  the new version. Upgrade the primary first, then every node and edge to the
  same version (see "Upgrading" in installer/README.md). Read the agent's
  journal with:

      journalctl -u quoro-node
EOF
}

report() {
    print_installed_version
    case $ROLE in
        primary) report_primary ;;
        edge) report_edge ;;
        *) report_node ;;
    esac
}

# --- Entry point ------------------------------------------------------------

main() {
    check_preconditions
    resolve_version

    WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/quoro-install.XXXXXX") || die 'could not create a temporary directory'
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    read_previous_version
    if [ -n "$PREVIOUS_VERSION" ]; then
        log "found quoro $PREVIOUS_VERSION; installing $VERSION for $TARGET (role $ROLE) over it"
    else
        log "installing quoro $VERSION for $TARGET (role $ROLE)"
    fi
    fetch_artifacts
    verify_checksum
    verify_signature
    extract_artifacts
    fetch_engine_runtime
    fetch_ingress_runtime

    # Every role that runs the agent gets the host packages: an edge runs no
    # engine, but the mesh needs ip and nft (ADR 0144).
    install_host_packages
    install_optional_packages
    install_dirs
    if [ "$ROLE" = 'primary' ]; then
        install_api_user
        install_primary_data_dir
    fi
    install_config
    install_binary
    install_engine_runtime
    install_ingress_runtime
    # Enrollment writes identity files only; it starts nothing, so it also runs
    # with QUORO_NO_START=1. It needs the binary and the config in place.
    if [ "$ROLE" != 'primary' ]; then
        enroll_node
    fi
    install_units
    start_services
    report
}

main "$@"
