#!/usr/bin/env bash
#
# run.sh - boot a Cloud Hypervisor microVM from an ext4 rootfs built by
# oci2rootfs and check the Quoro spike markers on the serial console.
#
# Usage: ./run.sh <rootfs.ext4> [--timeout 60]
#
# Runs unprivileged on a Linux KVM host as a user in the `kvm` group. The VMM
# and kernel are cached under ${QUORO_SPIKE_CACHE:-$HOME/.cache/quoro-spike} and
# their sha256 is verified on *every* run, including cached files.
#
# About the digests below: they are GitHub's release-asset digests (the sha256
# GitHub itself lists for each uploaded asset). The upstream projects publish no
# signature and no SUMS file for these assets, so this digest comparison is the
# strongest check available to us. If upstream ever starts publishing
# SUMS/signatures, this script should switch to those.
#
# Exit status: 0 only when the boot marker was seen and the app exited code=0.
set -euo pipefail

CH_VERSION="v54.0"
CH_URL="https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download/${CH_VERSION}/cloud-hypervisor-static"
CH_SHA256="4f4f98a3d661a40f3e5272627cc895a1b2e525f2f6bedf5f895f9cd21ba5c65b"

KERNEL_URL="https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v7.2.8-20261009/vmlinux-x86_64"
KERNEL_SHA256="edfeb8a8a6c04ff2979e9f1c700feaa29b8f564f952261834ef0b7d2e673a853"

BOOT_MARKER="QUORO_SPIKE_BOOT_OK"
EXIT_MARKER="QUORO_SPIKE_APP_EXIT"

usage() {
    echo "usage: $0 <rootfs.ext4> [--timeout 60]" >&2
    exit 2
}

die() {
    echo "run.sh: $*" >&2
    exit 1
}

ROOTFS=""
TIMEOUT=60

while [ "$#" -gt 0 ]; do
    case "$1" in
        --timeout)
            [ "$#" -ge 2 ] || usage
            TIMEOUT="$2"
            shift 2
            ;;
        --timeout=*)
            TIMEOUT="${1#--timeout=}"
            shift
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo "run.sh: unknown option: $1" >&2
            usage
            ;;
        *)
            [ -z "$ROOTFS" ] || usage
            ROOTFS="$1"
            shift
            ;;
    esac
done

[ -n "$ROOTFS" ] || usage
case "$TIMEOUT" in
    ''|*[!0-9]*) die "--timeout must be a whole number of seconds, got '$TIMEOUT'" ;;
esac
[ "$TIMEOUT" -gt 0 ] || die "--timeout must be positive"

[ -e "$ROOTFS" ] || die "rootfs not found: $ROOTFS"
[ -f "$ROOTFS" ] || die "rootfs is not a regular file: $ROOTFS"
[ -r "$ROOTFS" ] || die "rootfs is not readable: $ROOTFS"
ROOTFS="$(cd "$(dirname "$ROOTFS")" && pwd)/$(basename "$ROOTFS")"

# ---------------------------------------------------------------------------
# 1. Cached, verified VMM + kernel
# ---------------------------------------------------------------------------

CACHE="${QUORO_SPIKE_CACHE:-$HOME/.cache/quoro-spike}"
mkdir -p "$CACHE"

download() { # url dest name
    local url="$1" dest="$2" name="$3"
    echo "run.sh: downloading $name" >&2
    if command -v curl >/dev/null 2>&1; then
        curl -fL --retry 3 --retry-delay 2 -o "$dest.part" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$dest.part" "$url"
    else
        die "need curl or wget to download $name"
    fi
    mv "$dest.part" "$dest"
}

verify_sha256() { # sha256 path name
    local sha="$1" path="$2" name="$3"
    if ! printf '%s  %s\n' "$sha" "$path" | sha256sum -c --status; then
        rm -f "$path"
        die "sha256 mismatch for $name ($path); refusing to run an unverified binary"
    fi
}

fetch_verified() { # url sha256 name
    local url="$1" sha="$2" name="$3"
    local dest="$CACHE/$name"
    if [ ! -f "$dest" ]; then
        download "$url" "$dest" "$name"
    fi
    verify_sha256 "$sha" "$dest" "$name"
    printf '%s\n' "$dest"
}

command -v sha256sum >/dev/null 2>&1 || die "sha256sum not found (coreutils)"

VMM="$(fetch_verified "$CH_URL" "$CH_SHA256" cloud-hypervisor-static)"
KERNEL="$(fetch_verified "$KERNEL_URL" "$KERNEL_SHA256" vmlinux-x86_64)"
chmod +x "$VMM"

# ---------------------------------------------------------------------------
# 2. KVM access
# ---------------------------------------------------------------------------

[ -e /dev/kvm ] || die "/dev/kvm is missing: this host does not expose KVM (nested virt off?)"
[ -r /dev/kvm ] || die "/dev/kvm is not readable by $(id -un); add the user to the kvm group"
[ -w /dev/kvm ] || die "/dev/kvm is not writable by $(id -un); add the user to the kvm group"

# ---------------------------------------------------------------------------
# 3. Per-run directory (the rootfs is used in place, never copied)
# ---------------------------------------------------------------------------

RUN="$(mktemp -d "${TMPDIR:-/tmp}/quoro-spike.XXXXXX")"
SERIAL="$RUN/serial.log"
CONSOLE="$RUN/console.log"
API_SOCK="$RUN/api.sock"

VMM_PID=""
cleanup() {
    if [ -n "$VMM_PID" ] && kill -0 "$VMM_PID" 2>/dev/null; then
        kill "$VMM_PID" 2>/dev/null || true
        wait "$VMM_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 4. Launch
# ---------------------------------------------------------------------------
#
# Flags are for cloud-hypervisor v54; `--serial file=...` is the kernel console
# (ttyS0) and `--console file=...` is the virtio-console (hvc0) the init writes
# the app's output to. `init=/quoro/init` overrides the rootfs' own init, and
# `panic=-1` makes a kernel panic reboot instead of hanging (we kill the VMM on
# timeout either way).
#
# Adaptation from the original sketch, checked against
# `cloud-hypervisor-static --help` on v54: `--disk path=...` is rejected with
# "Image type required for disk", so the disk says `image_type=raw` explicitly
# (the pipeline produces a raw ext4 filesystem image, not qcow2). If a future
# version renames a flag, check `--help` and adjust here.

T0="$(date +%s%N)"
deadline_ns=$(( T0 + TIMEOUT * 1000000000 ))

echo "run.sh: rootfs=$ROOTFS" >&2
echo "run.sh: run_dir=$RUN" >&2

"$VMM" \
    --api-socket "path=$API_SOCK" \
    --kernel "$KERNEL" \
    --cmdline "console=ttyS0 root=/dev/vda rw init=/quoro/init panic=-1" \
    --disk "path=$ROOTFS,image_type=raw" \
    --cpus boot=1 \
    --memory "size=256M" \
    --serial "file=$SERIAL" \
    --console "file=$CONSOLE" &
VMM_PID=$!

# ---------------------------------------------------------------------------
# 5. Wait for the boot marker, then for the VMM to power off
# ---------------------------------------------------------------------------

T1=""
while :; do
    if [ -f "$SERIAL" ] && grep -q "$BOOT_MARKER" "$SERIAL"; then
        T1="$(date +%s%N)"
        break
    fi
    if ! kill -0 "$VMM_PID" 2>/dev/null; then
        break # VMM died before the marker: fall through to the failure report
    fi
    if [ "$(date +%s%N)" -ge "$deadline_ns" ]; then
        break
    fi
    sleep 0.005
done

TIMED_OUT=0
while kill -0 "$VMM_PID" 2>/dev/null; do
    if [ "$(date +%s%N)" -ge "$deadline_ns" ]; then
        echo "run.sh: timeout after ${TIMEOUT}s waiting for the guest to power off; killing the VMM" >&2
        kill "$VMM_PID" 2>/dev/null || true
        TIMED_OUT=1
        break
    fi
    sleep 0.01
done
wait "$VMM_PID" 2>/dev/null || true
VMM_PID=""
T2="$(date +%s%N)"

# ---------------------------------------------------------------------------
# 6. Report
# ---------------------------------------------------------------------------

echo "--- serial ($SERIAL) ---"
[ -f "$SERIAL" ] && cat "$SERIAL" || echo "(no serial log; the VMM never opened its console)"
echo "--- console ($CONSOLE) ---"
[ -f "$CONSOLE" ] && cat "$CONSOLE" || echo "(no console log)"

BOOT_MARKER_MS=""
[ -n "$T1" ] && BOOT_MARKER_MS=$(( (T1 - T0) / 1000000 ))

GUEST_UPTIME_MS=""
if [ -f "$SERIAL" ]; then
    GUEST_UPTIME_MS="$(sed -n "s/.*${BOOT_MARKER} uptime_ms=\([0-9][0-9]*\).*/\1/p" "$SERIAL" | tail -n 1)"
fi

APP_EXIT=""
if [ -f "$SERIAL" ]; then
    APP_EXIT="$(sed -n "s/.*${EXIT_MARKER} \(code=[0-9][0-9]*\|signal=[0-9][0-9]*\).*/\1/p" "$SERIAL" | tail -n 1)"
fi

TOTAL_MS=$(( (T2 - T0) / 1000000 ))

echo "--- summary ---"
echo "boot_marker_ms=${BOOT_MARKER_MS:-<none>}"
echo "guest_uptime_ms=${GUEST_UPTIME_MS:-<none>}"
echo "app_exit=${APP_EXIT:-<none>}"
echo "total_ms=$TOTAL_MS"
echo "run_dir=$RUN"

if [ -n "$T1" ] && [ "$APP_EXIT" = "code=0" ] && [ "$TIMED_OUT" -eq 0 ]; then
    exit 0
fi

if [ -z "$T1" ]; then
    echo "run.sh: FAILED: boot marker $BOOT_MARKER never appeared" >&2
else
    echo "run.sh: FAILED: app_exit=${APP_EXIT:-<none>} timed_out=$TIMED_OUT" >&2
fi
exit 1
