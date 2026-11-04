#!/bin/sh
# Build Quoro's guest kernel (ADR 0152) from packaging/kernel/kernel.txt.
#
# Runs inside the guest kernel build image (packaging/kernel/Containerfile)
# through packaging/kernel/build-kernel-in-container.sh, which mounts the
# checkout at /build/quoro and a work directory at /build/work. It never runs
# on a bare host: the host compiler would change the bytes.
#
# Steps: download the upstream tarball and check its sha256 and kernel.org's
# signature, unpack it, apply the vendored Cloud Hypervisor patch, run
# "make ch_defconfig", require the options Quoro's guests need, build vmlinux
# with a fixed build identity, and write dist/kernel/:
#
#   vmlinux-x86_64   the kernel Cloud Hypervisor boots (PVH ELF)
#   config-x86_64    the resolved .config it was built with
#   SHA256SUMS       sha256 of both
set -eu

usage() {
	cat <<'EOF'
usage: packaging/kernel/build-kernel.sh

Run through packaging/kernel/build-kernel-in-container.sh, not directly.

Environment:
  QUORO_KERNEL_JOBS  parallel make jobs (default: the number of CPUs). The
                     job count does not change the output.
EOF
}

die() {
	printf 'build-kernel: %s\n' "$*" >&2
	exit 1
}

note() {
	printf 'build-kernel: %s\n' "$*" >&2
}

case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
'') ;;
*)
	usage >&2
	exit 2
	;;
esac

[ -f /etc/quoro-kernel-build ] ||
	die "not inside the guest kernel build image; use packaging/kernel/build-kernel-in-container.sh"

readonly CHECKOUT=/build/quoro
readonly WORK=/build/work
readonly HERE="$CHECKOUT/packaging/kernel"
readonly RECIPE="$HERE/kernel.txt"
readonly OUT="$CHECKOUT/dist/kernel"

[ -r "$RECIPE" ] || die "cannot read $RECIPE"
[ -d "$WORK" ] || die "$WORK is not mounted"

# Options the guests depend on. The defconfig enables them today; the build
# fails if a recipe change ever drops one.
#   virtio transport and devices: PCI and MMIO transports, block (root and
#     data disks), net, console, entropy, vsock (terminal exec, ADR 0113)
#   PVH: Cloud Hypervisor's direct boot of an ELF vmlinux
#   serial 8250 console: the console Quoro reads (ADR 0116)
#   ext4 (root disks), overlayfs and cgroup v2 controllers plus namespaces and
#     seccomp (BuildKit in the builder VM), devtmpfs, tmpfs, initrd
REQUIRED='CONFIG_64BIT
CONFIG_PVH
CONFIG_VIRTIO
CONFIG_VIRTIO_PCI
CONFIG_VIRTIO_MMIO
CONFIG_VIRTIO_BLK
CONFIG_VIRTIO_NET
CONFIG_VIRTIO_CONSOLE
CONFIG_HW_RANDOM_VIRTIO
CONFIG_VSOCKETS
CONFIG_VIRTIO_VSOCKETS
CONFIG_SERIAL_8250
CONFIG_SERIAL_8250_CONSOLE
CONFIG_EXT4_FS
CONFIG_OVERLAY_FS
CONFIG_TMPFS
CONFIG_DEVTMPFS
CONFIG_BLK_DEV_INITRD
CONFIG_CGROUPS
CONFIG_MEMCG
CONFIG_CGROUP_PIDS
CONFIG_CGROUP_SCHED
CONFIG_CPUSETS
CONFIG_CGROUP_FREEZER
CONFIG_CGROUP_DEVICE
CONFIG_NAMESPACES
CONFIG_USER_NS
CONFIG_PID_NS
CONFIG_NET_NS
CONFIG_IPC_NS
CONFIG_UTS_NS
CONFIG_SECCOMP
CONFIG_SECCOMP_FILTER
CONFIG_UNIX
CONFIG_INET'

# Options that must stay off: they would make the build depend on the host or
# on randomness, or load code the image does not carry.
FORBIDDEN='CONFIG_MODULES
CONFIG_RANDSTRUCT_FULL
CONFIG_RANDSTRUCT_PERFORMANCE
CONFIG_GCC_PLUGINS
CONFIG_IKHEADERS
CONFIG_DEBUG_INFO_BTF
CONFIG_LOCALVERSION_AUTO'

value() {
	awk -F= -v k="$1" '$1 == k { sub(/^[^=]*=/, ""); print; found = 1 } END { exit !found }' "$RECIPE" ||
		die "kernel.txt has no $1"
}

version=$(value VERSION)
tarball_url=$(value TARBALL_URL)
tarball_sha256=$(value TARBALL_SHA256)
signature_url=$(value SIGNATURE_URL)
signer=$(value SIGNER_FINGERPRINT)
key_sha256=$(value SIGNING_KEY_SHA256)
patch="$HERE/$(value PATCH)"
defconfig=$(value DEFCONFIG)
build_timestamp=$(value KBUILD_BUILD_TIMESTAMP)
build_user=$(value KBUILD_BUILD_USER)
build_host=$(value KBUILD_BUILD_HOST)
build_version=$(value KBUILD_BUILD_VERSION)

case "$version" in
'' | *[!0-9.]*) die "unexpected VERSION: $version" ;;
esac
case "$defconfig" in
'' | */* | *[!A-Za-z0-9_]*) die "unexpected DEFCONFIG: $defconfig" ;;
esac
[ -r "$patch" ] || die "cannot read $patch"

jobs=${QUORO_KERNEL_JOBS:-$(nproc)}
case "$jobs" in
'' | *[!0-9]* | 0) die "QUORO_KERNEL_JOBS must be a positive integer, not: $jobs" ;;
esac

# Nothing from the host environment that kbuild reads may reach the build.
unset KCFLAGS KCPPFLAGS KAFLAGS KRUSTFLAGS KBUILD_EXTRA_SYMBOLS KBUILD_OUTPUT \
	KCONFIG_CONFIG CROSS_COMPILE ARCH CC LD CFLAGS LDFLAGS LOCALVERSION \
	SOURCE_DATE_EPOCH 2>/dev/null || true
export LC_ALL=C TZ=UTC
umask 022

check_sha256() {
	printf '%s  %s\n' "$2" "$1" | sha256sum --check --quiet - ||
		die "$1: sha256 does not match $2"
}

# 1. The upstream tarball, its sha256 and its signature. Downloads are cached
# in the work directory and checked on every run.
dl="$WORK/dl"
mkdir -p "$dl"
tarball="$dl/${tarball_url##*/}"
signature="$dl/${signature_url##*/}"
fetch() {
	[ -f "$2" ] && return 0
	note "downloading $1"
	curl --fail --location --retry 3 --silent --show-error --proto '=https' \
		--tlsv1.2 --output "$2.part" "$1" || die "download failed: $1"
	mv "$2.part" "$2"
}
fetch "$tarball_url" "$tarball"
fetch "$signature_url" "$signature"
check_sha256 "$tarball" "$tarball_sha256"
note "sha256 OK: ${tarball##*/}"

key="$HERE/keys/stable-signing-key.asc"
check_sha256 "$key" "$key_sha256"
gnupg="$WORK/gnupg"
rm -rf "$gnupg"
mkdir -m 0700 "$gnupg"
GNUPGHOME="$gnupg" gpg --batch --quiet --dearmor --output "$gnupg/keyring.gpg" "$key" ||
	die "cannot read $key"
# kernel.org signs the uncompressed tar. VALIDSIG's last field is the primary
# key fingerprint, which must be the pinned one.
status=$(xz -dc "$tarball" |
	GNUPGHOME="$gnupg" gpgv --status-fd 1 --keyring "$gnupg/keyring.gpg" "$signature" - 2>/dev/null) ||
	die "${tarball##*/}: the kernel.org signature does not verify"
printf '%s\n' "$status" | awk -v fpr="$signer" '$2 == "VALIDSIG" && $NF == fpr { ok = 1 } END { exit !ok }' ||
	die "${tarball##*/}: not signed by $signer"
rm -rf "$gnupg"
note "signature OK: ${tarball##*/} signed by $signer"

# 2. A fresh source tree at a fixed path, with the vendored patch applied
# exactly (git apply refuses fuzz).
src="$WORK/linux-$version"
rm -rf "$src"
tar -xJf "$tarball" -C "$WORK" --no-same-owner
[ -f "$src/Makefile" ] || die "${tarball##*/} did not unpack to $src"
# The tree is not a git repository, so git apply works like patch(1) and
# setlocalversion adds nothing to the release string.
(cd "$src" && git apply --whitespace=nowarn "$patch") || die "the patch does not apply to $version"
note "applied ${patch#"$CHECKOUT"/}"

kmake() {
	make -C "$src" -j"$jobs" ARCH=x86_64 \
		KBUILD_BUILD_TIMESTAMP="$build_timestamp" \
		KBUILD_BUILD_USER="$build_user" \
		KBUILD_BUILD_HOST="$build_host" \
		KBUILD_BUILD_VERSION="$build_version" \
		"$@"
}

# 3. The configuration.
kmake "$defconfig" >/dev/null
for option in $REQUIRED; do
	grep -qx "$option=y" "$src/.config" || die "$option is not built in (=y) after $defconfig"
done
for option in $FORBIDDEN; do
	if grep -q "^$option=" "$src/.config"; then
		die "$option must not be set"
	fi
done
note "config OK: $(printf '%s\n' "$REQUIRED" | wc -l) required options built in"
note "resolved .config vs arch/x86/configs/$defconfig (scripts/diffconfig):"
python3 "$src/scripts/diffconfig" "$src/arch/x86/configs/$defconfig" "$src/.config" >&2 ||
	die "diffconfig failed"

# 4. The kernel.
note "building vmlinux $version with $jobs jobs"
kmake vmlinux

release=$(kmake -s kernelrelease)
[ "$release" = "$version" ] || die "kernelrelease is $release, expected $version"

rm -rf "$OUT"
mkdir -p "$OUT"
cp "$src/vmlinux" "$OUT/vmlinux-x86_64"
cp "$src/.config" "$OUT/config-x86_64"
(cd "$OUT" && sha256sum vmlinux-x86_64 config-x86_64 >SHA256SUMS)
note "wrote $OUT:"
cat "$OUT/SHA256SUMS" >&2
