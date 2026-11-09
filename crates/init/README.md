# `quoro-init`

PID 1 inside every Quoro microVM, and the two formats it shares with the
engine: the configuration the engine writes before boot and the status record
the guest writes back. The contract is
[`docs/contracts/engine-trait.md`](../../docs/contracts/engine-trait.md)
section 6; decision D-0061 chose a purpose-built Rust init over systemd or
BusyBox init.

The crate has two halves:

- `src/config.rs` — the pure format library (`quoro_init`), no dependencies,
  builds on any platform. The engine links it to write configs and read
  statuses; the guest links it to read configs and write statuses.
- `src/guest/` — the Linux-only PID 1 (`quoro-init`), built for
  `x86_64-unknown-linux-musl` so the binary is static.

Everything is safe Rust: the workspace forbids `unsafe`, so the syscalls `std`
does not wrap (mount, chroot, reboot, sethostname, rtnetlink, waiting on any
child) go through `rustix`.

## The initramfs and the kernel command line

The engine builds a tiny initramfs that holds only `/init` (this binary), `/dev`
and `/dev/console` (the kernel's built-in initramfs already provides the last
one). The kernel command line is:

```
console=ttyS0 loglevel=4 panic=-1 rdinit=/init
```

The disks are virtio-blk, and they are found by their serial number, never by
probe order:

| serial        | filesystem | use                                     |
| ------------- | ---------- | --------------------------------------- |
| `quoro-root`  | ext4       | read-only image, the overlay's `lowerdir` |
| `quoro-data`  | ext4       | writable, the overlay's `upperdir` and `workdir` |
| `quoro-config`| raw        | config region + status region (below)   |

## The config disk

One raw disk, `CONFIG_DISK_LEN` = 1 MiB + 4 KiB bytes:

```
offset 0                 config region, CONFIG_REGION_LEN = 1 MiB
offset 1 MiB             status region, STATUS_REGION_LEN = 4 KiB
```

Both regions are NUL-padded. A region is read up to its first NUL byte, so a
shorter file with zero padding reads the same as a full-size one. All-zero (or
empty) status means "the guest has not reported yet".

### Configuration grammar

UTF-8 text. Every line ends with `\n`. No comments, no blank lines. Lines
appear in exactly the encoding order below, and values are escaped: `\` becomes
`\\` and newline becomes `\n`. On decode any other `\x` escape, a trailing lone
backslash, an unknown key, a missing or duplicated single key, a partial
`net.*` group, a NUL, or invalid UTF-8 is an error naming the line number.

```
quoro-init-config v1
arg=<value>                  1 or more, in order; args[0] is non-empty
env=<NAME>=<value>           0 or more, in order; split at the first '=' after the key
user=<value>                 0 or 1
workdir=<value>              0 or 1; absolute
hostname=<value>             0 or 1; 1..=64 chars of [A-Za-z0-9.-], no leading/trailing '-' or '.'
console=virtio|serial|none   exactly 1
net.mac=02:00:00:00:00:01    net.* is all three or none
net.address=10.42.1.10/24    prefix 0..=32
net.gateway=10.42.1.1
dns=1.1.1.1                  0 or more
stop_timeout_ms=<u64>        exactly 1, at most 86_400_000 (one day)
```

Semantics:

- `arg` — the app's argv. `args[0]` is the entrypoint; when it has no `/` it is
  searched in `PATH`.
- `env` — the app's whole environment, in order. Names are non-empty and hold
  neither `=` nor NUL; no value holds a NUL.
- `user` — `name`, `uid`, `user:group`, `uid:gid` (each side a name or a
  number). Resolved against the image's `/etc/passwd` and `/etc/group`. Absent
  means root (`0:0`).
- `workdir` — the app's working directory, created (0755) if missing. Absent
  means `/`.
- `console` — where the app's tagged output goes: `/dev/hvc0` (virtio-console),
  `/dev/ttyS0`, or nowhere.
- `net` — one IPv4 interface, found by MAC. `dns` becomes `/etc/resolv.conf`.

Encoding validates with the same rules as decoding and errors when the result
would not fit the 1 MiB region. `GuestConfig::encode` returns the text without
padding; the caller writes it at offset 0 of a `CONFIG_DISK_LEN` file.

### Configuration version 3 (M5a)

A configuration with an exec port is written with the header
`quoro-init-config v3`. It carries the same lines as v2, in the same order,
then one line per attached volume, in the order of `VmSpec::volumes`:

```
quoro-init-config v3
...                              every v2 line, including exec_port
volume=quoro-vol-0:/var/lib/app/data   0 or more, in order
```

The value is `<serial>:<mount_path>`, split at the first `:`. Rules:

- `serial` matches `quoro-vol-[0-9]+`: the prefix, then one or more ASCII
  digits (no sign).
- `mount_path` is absolute, at most 255 bytes, holds no `:`, no empty, `.` or
  `..` segment and no trailing `/`, and is not `/` nor `/proc`, `/sys` or
  `/dev` (or anything under them). `/sysx` and `/procfs/a` are ordinary paths.
- Serials are unique, and mount paths are unique and not nested in one another.

`volume=` is valid only in v3 and only after `exec_port`, and a v3
configuration requires `exec_port` like v2. A configuration with volumes and no
exec port does not encode. The init still decodes v1 and v2, where a `volume=`
line is an unknown key. The encoder writes v3 whenever `exec_port` is set, with
or without volumes, and never writes v2.

### Status grammar

Written at `STATUS_OFFSET`, NUL-terminated, at most `STATUS_REGION_LEN`:

```
quoro-init-status v1
exit=<i32>
fatal=<escaped message>      only for AppStatus::Fatal
```

`Exited { code }` is the app's exit status, or `128 + signal` when it was
killed. `Fatal { code, message }` is a boot failure: `127` when the entrypoint
was missing or not executable, `1` otherwise. `encode_status` truncates a long
fatal message on a character boundary so the record fits; `decode_status`
returns `None` for an all-zero region.

## Boot sequence

1. **Diagnostics.** `/dev/ttyS0`, falling back to `/dev/console` until devtmpfs
   exists. Every line is `quoro-init: <text>`.
2. **Early mounts.** devtmpfs on `/dev`, proc on `/proc`, sysfs on `/sys`.
3. **Disks.** Poll `/sys/block/vd*/serial` for up to 5 s (every 50 ms) until
   the three serials appear, then map each to `/dev/vdX`. The config disk is
   opened read-write first so every later failure can be reported; its first
   1 MiB is read and decoded. A missing disk is a FATAL.
4. **Root.** Mount the root image ext4 read-only at `/quoro/lower`, the data
   disk ext4 (`NOATIME`) at `/quoro/data`, create
   `/quoro/data/root/{upper,work}`, and mount the overlay at `/newroot`:
   `lowerdir=/quoro/lower,upperdir=/quoro/data/root/upper,workdir=/quoro/data/root/work`.
   Because the data disk survives stop/start, writes to `/` survive until the
   VM is deleted.
5. **Switch root.** Move `/dev`, `/proc` and `/sys` under `/newroot`,
   `chdir("/newroot")`, `mount_move(".", "/")`, `chroot(".")`, `chdir("/")`.
6. **Inside the new root.** devpts on `/dev/pts`
   (`newinstance,ptmxmode=0666,mode=0620`), tmpfs on `/dev/shm` (1777), tmpfs on
   `/run` when the image has that directory, `cgroup2` on `/sys/fs/cgroup`
   (`nosuid,nodev,noexec`) when the image has that directory, `/tmp` created
   (1777) when missing, and the `/dev/fd`, `/dev/stdin`, `/dev/stdout`,
   `/dev/stderr` symlinks when missing. A cgroup2 mount that fails — an old
   kernel without cgroup v2, or something already mounted there — is a
   diagnostic line, never a boot failure; BuildKit needs it, an app does not.
   Then the volumes of a v3 configuration (M5a): each disk was already found by
   its serial before the switch, and is mounted `ext4`, writable, with
   `nosuid,nodev`, at its mount path inside the app's root; a missing mount
   directory is created (0755) with its parents. A volume that cannot be found
   or mounted is a FATAL.
7. **Hostname and files.** `sethostname` when configured; `/etc/hosts`
   (`127.0.0.1 localhost`, `::1 localhost`, plus `<address> <hostname>` when
   both are configured); `/etc/resolv.conf` when `dns` is non-empty. Files are
   unlinked before they are re-created, so a symlink in the image is never
   written through.
8. **Network** (when `net` is set), over rtnetlink: loopback up, the interface
   whose `/sys/class/net/<if>/address` matches `net.mac` (up to 5 s), its
   address as `IFA_LOCAL` + `IFA_ADDRESS`, `IFF_UP`, and the default route via
   the gateway. Every request asks for an acknowledgement; `EEXIST` is not an
   error.
9. **User.** Resolved like runc: a name gives the passwd uid, primary gid and
   home; a numeric uid uses its passwd entry when there is one and gid 0 when
   there is not; `user:group` resolves each side as a name or a number; a name
   that does not exist is a FATAL. Absent means `0:0`. `HOME` comes from
   passwd (or `/`).
10. **Program.** `args[0]`, searched in `PATH` (from the config env, default
    `/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`) when it has
    no `/`; it must be a regular file with an execute bit, or the boot is a
    FATAL with code 127. The working directory is created (0755) if missing.
    When the configuration is v2 and carries `exec_port`, the exec listener is
    bound here, after the switch into the app's root and before the spawn
    (below).
11. **Spawn.** `std::process::Command` with `arg0`, the config env plus `HOME`
    and `PATH` defaults, the working directory, the resolved uid/gid, its own
    process group, `/dev/null` on stdin and pipes on stdout/stderr.
12. **Supervise.** One thread per pipe tags lines onto the log console; a
    thread watches the ACPI power button. The main thread waits for any child,
    reaping orphans, until the app's pid exits.
13. **Report.** Write the status record (see above); the engine reads it back
    after the VMM exits.
14. **Tear down.** SIGKILL the app's process group, wait at most 2 s for the
    output threads. Then send `SIGTERM` to every remaining process, reap for up
    to 1 s, send `SIGKILL` to whatever is left and reap for up to 1 s again. The
    init is PID 1 and is never signalled.
15. **Clean the disks.** `sync()`, then read `/proc/self/mounts` and, for every
    mount whose source is a device under `/dev/` (a work disk the app mounted on
    `/mnt/work`, or an attached volume, for example), last mounted first:
    `umount2(target, 0)`, or a read-only remount (`MS_REMOUNT | MS_RDONLY`,
    retried with `MS_BIND` for a bind mount) when the unmount finds it busy.
16. **Power off.** `sync()`, `reboot(PowerOff)`. If the reboot returns, the
    process sleeps forever.

A FATAL at any point logs `quoro-init: FATAL <message>`, writes
`AppStatus::Fatal` when the config disk is known, runs the same teardown as an
app exit, and powers off the same way.

## Disks at power-off

The engine reads a work disk after the VMM exits, so the init leaves it clean: a
disk that was unmounted, or remounted read-only after a `sync()`, needs no
journal recovery on the node. Steps 14 to 16 run on both endings, after the
status record is written.

Only mounts backed by a device node under `/dev/` are touched, and each is tried
in reverse mount order so the last mounted goes first. Pseudo filesystems —
`/dev`, `/proc`, `/sys`, `/run`, `/dev/shm`, `/dev/pts`, `cgroup2` — and the
overlay are never unmounted. Every attempt is bounded: a busy mount answers
`EBUSY` immediately, there is no lazy unmount and nothing waits on a writer, so
a stuck disk is a logged line and the VM powers off anyway. The overlay's upper
disk usually cannot be released while the root is in use, which is expected.

## Console protocol

Two consoles, two protocols:

- **Diagnostics** go to `/dev/ttyS0` (the engine's `--serial` file), one line
  per event: `quoro-init: <text>`. This is where the boot sequence and FATAL
  messages are visible.
- **App output** goes to the console from the config: `/dev/hvc0`
  (`console=virtio`, the engine's `--console` file), `/dev/ttyS0`
  (`console=serial`), or nowhere (`console=none`). Each line the app writes is
  emitted as `out <line>\n` or `err <line>\n`, one `write_all` per record under
  a shared mutex so the two streams never interleave mid-line. Bytes are passed
  through unchanged (no UTF-8 validation); the init clears `OPOST` on the
  console tty so the kernel does not turn `\n` into `\r\n`. A line longer
  than 16384 bytes is emitted as consecutive 16384-byte records, each tagged; a trailing partial
  line is flushed at EOF. If the console cannot be opened, the init warns on
  the diagnostics console and discards the output.

The init never writes anything of its own onto the app's console. The engine
reads the status record from the config disk after the VMM exits, not from the
console.

## Exec sessions (M4)

The web terminal reaches the app's context over virtio-vsock
([`engine-trait.md`](../../docs/contracts/engine-trait.md) section 6, ADR 0113;
the spike is [`m4-terminal-and-console.md`](../../docs/research/m4-terminal-and-console.md)).
A configuration is v2 or v3 exactly when it carries `exec_port`; a v1
configuration starts no listener. v3 adds the volumes above; the encoder never
writes v2 any more, and every VM with an exec port is written as v3.

The listener binds `VMADDR_CID_ANY:<exec_port>` inside the app's root, after
the switch, the network and the user, and before the app starts. It runs for
the VM's lifetime. A bind failure is one `quoro-init:` line on the serial
console and nothing else: the app still boots, only the terminal is missing.
The listener never writes to the app's stdout or to its log console;
diagnostics go only to `/dev/ttyS0`.

One connection is one session, and the first frame must be `OPEN` (JSON
`{"argv":[...],"user":null|"...","rows":R,"cols":C}`) or `SYNC` (below). Any
other first frame, a malformed or oversized frame, or an unknown type answers
`ERROR` when the connection still allows it and closes. At most 16 sessions,
PTY and sync sessions together, run at once; the next connection is refused
with `ERROR` "too many sessions".

**Sync sessions (ADR 0282).** The engine flushes a volume's filesystem before
it snapshots the volume file on the host, so the snapshot holds what the app
wrote without `fsync` (`engine-trait.md` sections 3.16 and 6). The first and
only frame is `SYNC` (type 5, JSON `{"serials":["quoro-vol-0",...]}`: one to 64
distinct volume serials, no other member). For each serial in order, the init
opens that volume's mount path from the configuration, at that moment, and
calls `syncfs` on it. It then answers `SYNCED` (type 132, no payload) and
closes. A serial the configuration lacks, a mount path it cannot open, or a
`syncfs` that fails answers `ERROR` with the reason and closes. A sync session
starts no program, allocates no PTY, reads no frame after the `SYNC`, and
writes nothing to the app's console. A volume whose sync fails is logged on
the serial console only the first time, or when its reason changes, until a
sync of that volume succeeds, so a sync every few seconds cannot push the boot
output out of the serial ring. A malformed `SYNC` is answered with `ERROR` and
not logged. A `SYNC` after an `OPEN`
ends that PTY session with `ERROR`. An init older than ADR 0282 answers a
`SYNC` with `ERROR` "the first frame must be open, got type 5", which the engine
reads as an init that cannot flush.

Per session:

- A PTY is allocated (`rustix::pty`), its initial window size set on the
  master, and the slave opened with `NOCTTY` so PID 1 never acquires it.
- The program is started through a self re-exec: `/proc/self/exe
  --quoro-exec-helper <program> <arg0> <args...>` with the PTY slave on stdin,
  stdout and stderr. `pre_exec` is `unsafe` and the workspace forbids it, so
  the helper (`guest::exec::helper`) calls `setsid`, makes fd 0 the controlling
  terminal (`TIOCSCTTY`) and `exec`s the program with the requested `argv[0]`.
  If that `exec` fails it prints `quoro-init: exec <program>: <error>` on the
  PTY and exits 127, which reaches the host as `OUTPUT` then `EXIT 127`.
- The environment is the app's (the config's, plus the `HOME` and `PATH`
  defaults it would get) with `TERM=xterm-256color` replacing any other `TERM`;
  the working directory is the app's; the user is the app's unless the open
  names another, which is resolved with `user::resolve` and a failure is an
  `ERROR` "no such user ...".
- `argv[0]` without `/` is searched in the app's `PATH` (the config's, or the
  default above). A program the image lacks is **not** an error of the open:
  the session starts, prints `quoro-init: <argv0>: not found` on its terminal
  and sends `EXIT 127`.
- PTY output becomes `OUTPUT` frames of at most 64 KiB; `INPUT` is written to
  the master; `RESIZE` calls `tcsetwinsize`, so the kernel sends `SIGWINCH` to
  the foreground group. Writes to the connection block, so a host that stops
  reading stalls the PTY and the program rather than growing a buffer in the
  init.
- `CLOSE`, or the host dropping the connection, sends `SIGHUP` to the session's
  process group and stops relaying. When the program exits, the init drains
  what the PTY still holds (until `EIO`), sends `EXIT` (`128 + signal` when a
  signal ended it) and closes the connection.

PID 1's `supervise` loop reaps every child, so a session never calls `waitpid`:
it registers the helper's pid in a small process-wide table and waits on a
channel that `supervise` and `reap_remaining` feed with `exec::reaped`. A
status that arrives before the session registers is kept in the same table, so
a program that exits immediately cannot lose its exit code.

The helper relies on `/proc/self/exe` still being executable after
`switch_root`. The boot moves the overlay over the initramfs root and `chroot`s
without unmounting the old root (there is no `pivot_root`), and `/proc` is
carried into the new root; the magic link resolves to the binary's inode on
that still-mounted root, not to a path in the new root.

## Known gaps

- **Supplementary groups.** The app runs with no supplementary groups: when a
  uid is set, `std` calls `setgroups(0)` before `setuid`. The groups listed for
  the user in the image's `/etc/group` are not added, because adding them needs
  `CommandExt::groups` (unstable) or a raw `setgroups`, and the workspace
  forbids `unsafe`. `uid` and `gid` are set correctly. Exec sessions inherit
  the same limitation, and they skip `uid`/`gid` entirely when the resolved
  identity is already the init's own.
- **Exec drain timeout.** After the program exits, a session waits at most 5 s
  for the PTY reader to reach `EIO` (a background process that still holds the
  slave can delay it). A host that stopped reading blocks the output thread, so
  after that budget the connection is shut down and the `EXIT` frame is lost;
  a host that reads sees it normally.
- **Timestamps.** The guest never sets the clock (no `CLOCK_REALTIME` from the
  host); app output and files carry the kernel's default time.
- **No `pivot_root`.** The boot moves the overlay over the initramfs root and
  `chroot`s, which leaves the old rootfs unreachable but still mounted. It is a
  few pages of ramfs, and PID 1 dies with the VM.
- **One interface, IPv4 only.** `net.*` describes a single virtio-net device
  with a static address, one gateway and IPv4 DNS servers. No DHCP, no IPv6, no
  routes beyond the default.
- **Power button only.** There is no signal handling for `SIGTERM` from the
  host (a VM stop is a VMM exit, not a guest signal), and the init does not
  watch for `SIGPWR`. The engine's forced stop is the fallback when the power
  button device is absent.
- **Exit code range.** `AppStatus::Exited` carries an `i32`; an app killed by a
  signal reports `128 + signal`, never the raw wait status.
- **`/etc/hosts` and `/etc/resolv.conf` are overwritten.** An image that ships
  its own copies loses them on every boot; the engine's configuration is the
  source of truth.
- **No fsck.** A data disk that needs journal recovery fails to mount; the
  engine is expected to hand the guest a clean ext4. The init unmounts or
  remounts read-only every block-device mount before power-off, but the overlay's
  upper disk and anything else still in use when the app exits stay mounted, so
  the node must not read a disk while its VM is still running.
