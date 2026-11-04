// SPDX-License-Identifier: AGPL-3.0-only
//! End-to-end proof of the image pipeline and the guest boot files: convert
//! `alpine:3.22` with [`ImageStore`], build the initramfs, config disk and data
//! disk with [`quoro_engine::boot`], boot them under the pinned Cloud
//! Hypervisor through its command line, and check the tagged app output and
//! the status record the init writes back.
//!
//! `#[ignore]`d: it needs `/dev/kvm`, root, network access to Docker Hub and
//! the pinned artifacts. Run with `crates/engine/scripts/lab-suite.sh` (it
//! fetches the pinned artifacts, builds `quoro-init` and the probe images,
//! resolves the alpine digest and runs every lab suite as root); the variables
//! below are what it sets.
//!
//! - `QUORO_ENGINE_LAB=1`: the lab switch every test checks.
//! - `QUORO_ENGINE_ARTIFACTS`: directory with the pinned Cloud Hypervisor and
//!   guest kernel under their pinned file names (`scripts/fetch-pinned.sh`).
//! - `QUORO_ENGINE_INIT`: the static musl `quoro-init` binary.
//! - `QUORO_ENGINE_ALPINE`: the digest (`sha256:…`) of `alpine:3.22`.
//! - `QUORO_ENGINE_SCRATCH`: optional work directory (default under `/tmp`);
//!   it is wiped first.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use quoro_engine::boot;
use quoro_engine::image::{ImageStore, ImageStoreConfig};
use quoro_engine::pins;
use quoro_engine::{ImageDigest, ImageRef};
use quoro_init::{AppStatus, GuestConfig, LogConsole};

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set")))
}

fn pinned(artifacts: &Path, name: &str) -> PathBuf {
    let pin = pins::get(name).expect("pin");
    let path = artifacts.join(&pin.file);
    pins::verify(&path, &pin).expect("pinned artifact verifies");
    path
}

#[tokio::test]
#[ignore = "needs KVM, root and the lab artifacts; see the module docs"]
async fn alpine_boots_from_the_pipeline_and_reports_its_exit() {
    assert_eq!(
        std::env::var("QUORO_ENGINE_LAB").as_deref(),
        Ok("1"),
        "set QUORO_ENGINE_LAB=1 to run the lab suite"
    );
    let artifacts = env_path("QUORO_ENGINE_ARTIFACTS");
    let init = env_path("QUORO_ENGINE_INIT");
    let digest = std::env::var("QUORO_ENGINE_ALPINE").expect("QUORO_ENGINE_ALPINE is not set");
    let scratch = std::env::var("QUORO_ENGINE_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/quoro-boot-lab"));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch");

    let vmm = pinned(&artifacts, pins::CLOUD_HYPERVISOR);
    let kernel = pinned(&artifacts, pins::GUEST_KERNEL);
    let mkfs = PathBuf::from("/usr/sbin/mkfs.ext4");

    // The image pipeline, twice: the second call must come from the cache.
    let store = ImageStore::new(ImageStoreConfig {
        root: scratch.join("images"),
        mkfs_ext4: mkfs.clone(),
        preserve_ownership: true,
    })
    .expect("store");
    let image = ImageRef {
        reference: format!("docker.io/library/alpine@{digest}"),
        digest: ImageDigest(digest.clone()),
    };
    let prepared = store.prepare(&image).await.expect("prepare alpine");
    let again = store.prepare(&image).await.expect("prepare from cache");
    assert_eq!(prepared.rootfs_path, again.rootfs_path);
    assert!(prepared.rootfs_path.starts_with(scratch.join("images")));
    assert!(
        prepared.config.cmd.iter().any(|arg| arg.contains("sh")),
        "alpine's Cmd is /bin/sh: {:?}",
        prepared.config.cmd
    );

    // The boot files.
    let initramfs = scratch.join("initramfs.cpio");
    let config_disk = scratch.join("config.img");
    let data_disk = scratch.join("data.img");
    boot::write_initramfs(&init, &initramfs).expect("initramfs");
    let guest = GuestConfig {
        args: vec![
            "/bin/sh".into(),
            "-c".into(),
            "echo hello from alpine; echo to stderr >&2; id -u; \
             echo persisted > /var/marker; exit 7"
                .into(),
        ],
        env: prepared.config.env.clone(),
        user: None,
        workdir: None,
        hostname: Some("boot-lab".into()),
        log_console: LogConsole::Virtio,
        net: None,
        dns: Vec::new(),
        stop_timeout_ms: 5_000,
        exec_port: None,
        volumes: Vec::new(),
    };
    boot::write_config_disk(&config_disk, &guest).expect("config disk");
    boot::create_data_disk(&mkfs, &data_disk, 1).expect("data disk");
    assert_eq!(boot::read_status(&config_disk).expect("status"), None);
    let vm = Vm {
        scratch: scratch.clone(),
        vmm,
        kernel,
        initramfs,
        rootfs: prepared.rootfs_path.clone(),
        data: data_disk,
        config: config_disk.clone(),
    };

    // First boot: the app runs as root and exits 7.
    let console = vm.boot("first");
    assert!(console.contains("out hello from alpine\n"), "{console}");
    assert!(console.contains("err to stderr\n"), "{console}");
    assert!(console.contains("out 0\n"), "{console}");
    assert_eq!(
        boot::read_status(&config_disk).expect("status"),
        Some(AppStatus::Exited { code: 7 })
    );

    // Second boot: as `nobody` (resolved from the image's /etc/passwd), and the
    // write to / survived on the data disk.
    boot::reset_status(&config_disk).expect("reset status");
    let guest = GuestConfig {
        args: vec![
            "/bin/sh".into(),
            "-c".into(),
            "id -u; cat /var/marker".into(),
        ],
        user: Some("nobody".into()),
        ..guest
    };
    boot::write_config_disk(&config_disk, &guest).expect("config disk");
    let console = vm.boot("second");
    assert!(console.contains("out 65534\n"), "{console}");
    assert!(console.contains("out persisted\n"), "{console}");
    assert_eq!(
        boot::read_status(&config_disk).expect("status"),
        Some(AppStatus::Exited { code: 0 })
    );

    // A missing entrypoint is a boot failure with code 127.
    boot::reset_status(&config_disk).expect("reset status");
    let guest = GuestConfig {
        args: vec!["/no/such/program".into()],
        user: None,
        ..guest
    };
    boot::write_config_disk(&config_disk, &guest).expect("config disk");
    vm.boot("third");
    assert!(matches!(
        boot::read_status(&config_disk).expect("status"),
        Some(AppStatus::Fatal { code: 127, .. })
    ));

    store.remove(&prepared.digest).await.expect("remove image");
    assert!(!prepared.rootfs_path.exists());
    let _ = std::fs::remove_dir_all(&scratch);
}

/// The files of one test VM.
struct Vm {
    scratch: PathBuf,
    vmm: PathBuf,
    kernel: PathBuf,
    initramfs: PathBuf,
    rootfs: PathBuf,
    data: PathBuf,
    config: PathBuf,
}

impl Vm {
    /// Boots once and waits for the guest to power off; returns the app
    /// console.
    fn boot(&self, label: &str) -> String {
        let Self {
            scratch,
            vmm,
            kernel,
            initramfs,
            rootfs,
            data,
            config,
        } = self;
        let console = scratch.join(format!("{label}-console.log"));
        let serial = scratch.join(format!("{label}-serial.log"));
        let mut child = Command::new(vmm)
            .arg("--kernel")
            .arg(kernel)
            .arg("--initramfs")
            .arg(initramfs)
            .arg("--cmdline")
            .arg(boot::kernel_cmdline())
            .args(["--cpus", "boot=1", "--memory", "size=256M"])
            .arg("--disk")
            .arg(format!(
                "path={},image_type=raw,readonly=on,serial={}",
                rootfs.display(),
                quoro_init::SERIAL_ROOT
            ))
            .arg(format!(
                "path={},image_type=raw,serial={}",
                data.display(),
                quoro_init::SERIAL_DATA
            ))
            .arg(format!(
                "path={},image_type=raw,serial={}",
                config.display(),
                quoro_init::SERIAL_CONFIG
            ))
            .arg("--console")
            .arg(format!("file={}", console.display()))
            .arg("--serial")
            .arg(format!("file={}", serial.display()))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn cloud-hypervisor");

        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().expect("wait") {
                break status;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                let serial = std::fs::read_to_string(&serial).unwrap_or_default();
                panic!("{label} boot did not power off within 60 s; serial:\n{serial}");
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let serial = std::fs::read_to_string(&serial).unwrap_or_default();
        assert!(
            status.success(),
            "{label}: VMM exited {status}; serial:\n{serial}"
        );
        std::fs::read_to_string(&console).unwrap_or_default()
    }
}
