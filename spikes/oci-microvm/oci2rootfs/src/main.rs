// SPDX-License-Identifier: AGPL-3.0-only
//! `oci2rootfs` — pull an OCI image from a registry and write it, plus the
//! Quoro init, into an ext4 root disk. Throwaway spike (issue #10).

use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::task::{Context as TaskContext, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use oci_client::client::{linux_amd64_resolver, ClientConfig};
use oci_client::manifest::OciDescriptor;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use oci2rootfs::{
    apparent_size, apply_layer, compression_for, parse_image_config, render_config, resolve_user,
    sha256_file, ConfigSpec, Ident, WhiteoutStats,
};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWrite;

const USAGE: &str = "\
usage: oci2rootfs --image <ref> --init <quoro-init> --out <rootfs.ext4> [options]

  --image <ref>      image reference, e.g. docker.io/library/alpine:3.22
  --init <path>      quoro-init binary copied to /quoro/init
  --out <path>       ext4 image to create (overwritten)
  --size-mb <n>      fixed image size in MiB (default: staging*1.3 + 64)
  --arg <arg>        override the image Entrypoint+Cmd (repeatable, ordered)
  --hostname <name>  hostname for /quoro/config
  --work-dir <dir>   scratch dir for blobs and the staging tree
  --keep-work        keep the work dir after a successful run
  -h, --help         show this help
";

const MI_B: f64 = 1024.0 * 1024.0;

struct Cli {
    image: String,
    init: PathBuf,
    out: PathBuf,
    size_mb: Option<u64>,
    args: Vec<String>,
    hostname: Option<String>,
    work_dir: Option<PathBuf>,
    keep_work: bool,
}

fn parse_args() -> Result<Cli> {
    let mut argv = std::env::args().skip(1);
    let mut image = None;
    let mut init = None;
    let mut out = None;
    let mut size_mb = None;
    let mut args = Vec::new();
    let mut hostname = None;
    let mut work_dir = None;
    let mut keep_work = false;

    while let Some(arg) = argv.next() {
        let mut value = |flag: &str| -> Result<String> {
            argv.next()
                .with_context(|| format!("{flag} requires a value\n\n{USAGE}"))
        };
        match arg.as_str() {
            "--image" => image = Some(value("--image")?),
            "--init" => init = Some(PathBuf::from(value("--init")?)),
            "--out" => out = Some(PathBuf::from(value("--out")?)),
            "--size-mb" => {
                let raw = value("--size-mb")?;
                let parsed: u64 = raw
                    .parse()
                    .with_context(|| format!("--size-mb expects an integer, got {raw:?}"))?;
                if parsed == 0 {
                    bail!("--size-mb must be greater than zero");
                }
                size_mb = Some(parsed);
            }
            "--arg" => args.push(value("--arg")?),
            "--hostname" => hostname = Some(value("--hostname")?),
            "--work-dir" => work_dir = Some(PathBuf::from(value("--work-dir")?)),
            "--keep-work" => keep_work = true,
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }

    Ok(Cli {
        image: image.context("--image is required")?,
        init: init.context("--init is required")?,
        out: out.context("--out is required")?,
        size_mb,
        args,
        hostname,
        work_dir,
        keep_work,
    })
}

/// `mkfs.ext4 -d` preserves ownership, so we must be root.
fn ensure_root() -> Result<()> {
    let uid = fs::metadata("/proc/self")
        .context("reading /proc/self to check the effective uid")?
        .uid();
    if uid != 0 {
        bail!("must run as root (uid {uid}); mkfs.ext4 -d has to preserve image ownership");
    }
    Ok(())
}

/// Async writer that hashes bytes as they are written, so every blob digest is
/// verified by us and not only by the client.
struct HashingWriter {
    file: File,
    hasher: Sha256,
    bytes: u64,
}

impl HashingWriter {
    fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).with_context(|| format!("creating {}", path.display()))?;
        Ok(Self { file, hasher: Sha256::new(), bytes: 0 })
    }

    fn finish(self) -> (String, u64) {
        let digest = format!("sha256:{}", oci2rootfs::hex(&self.hasher.finalize()));
        (digest, self.bytes)
    }
}

impl AsyncWrite for HashingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.file.write(buf) {
            Ok(0) if !buf.is_empty() => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write while saving a blob",
            ))),
            Ok(written) => {
                this.hasher.update(&buf[..written]);
                this.bytes += written as u64;
                Poll::Ready(Ok(written))
            }
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Poll::Ready(this.file.flush())
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Poll::Ready(this.file.flush())
    }
}

/// Pull one blob into `dest` and verify its digest against the descriptor.
async fn pull_blob_verified(
    client: &Client,
    reference: &Reference,
    descriptor: &OciDescriptor,
    dest: &Path,
) -> Result<u64> {
    let algorithm = descriptor
        .digest
        .split_once(':')
        .map(|(algorithm, _)| algorithm)
        .unwrap_or("");
    if algorithm != "sha256" {
        bail!(
            "blob {dest:?} has descriptor digest {:?}; only sha256 digests are supported",
            descriptor.digest
        );
    }

    let mut writer = HashingWriter::create(dest)?;
    client
        .pull_blob(reference, descriptor, &mut writer)
        .await
        .with_context(|| format!("pulling blob {} to {}", descriptor.digest, dest.display()))?;
    let (digest, bytes) = writer.finish();

    if !digest.eq_ignore_ascii_case(&descriptor.digest) {
        bail!(
            "digest mismatch for blob written to {}: manifest says {}, we computed {}",
            dest.display(),
            descriptor.digest,
            digest
        );
    }
    if descriptor.size >= 0 && bytes != descriptor.size as u64 {
        bail!(
            "size mismatch for blob {}: manifest says {} bytes, we downloaded {}",
            descriptor.digest,
            descriptor.size,
            bytes
        );
    }
    Ok(bytes)
}

fn prepare_work_dir(cli: &Cli) -> Result<PathBuf> {
    let dir = match &cli.work_dir {
        Some(dir) => dir.clone(),
        None => {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("reading the clock")?
                .subsec_nanos();
            PathBuf::from("/var/tmp").join(format!("oci2rootfs-{}-{}", std::process::id(), nanos))
        }
    };
    if dir.exists() {
        let mut entries = fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))?;
        if entries.next().is_some() {
            bail!(
                "--work-dir {} already exists and is not empty; refusing to touch it",
                dir.display()
            );
        }
    } else {
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    fs::create_dir_all(dir.join("layers"))?;
    fs::create_dir_all(dir.join("rootfs"))?;
    Ok(dir)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting mode {mode:o} on {}", path.display()))
}

fn ensure_dir(path: &Path, mode: u32) -> Result<()> {
    if !path.is_dir() {
        fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    }
    set_mode(path, mode)
}

fn install_init(staging: &Path, init: &Path) -> Result<()> {
    let quoro = staging.join("quoro");
    ensure_dir(&quoro, 0o755)?;
    let dest = quoro.join("init");
    fs::copy(init, &dest).with_context(|| {
        format!("copying init {} to {}", init.display(), dest.display())
    })?;
    set_mode(&dest, 0o755)?;
    std::os::unix::fs::chown(&dest, Some(0), Some(0))
        .with_context(|| format!("chown 0:0 {}", dest.display()))?;
    Ok(())
}

fn write_config(staging: &Path, spec: &ConfigSpec) -> Result<()> {
    let quoro = staging.join("quoro");
    ensure_dir(&quoro, 0o755)?;
    let rendered = render_config(spec)?;
    let path = quoro.join("config");
    fs::write(&path, rendered).with_context(|| format!("writing {}", path.display()))?;
    set_mode(&path, 0o644)?;
    std::os::unix::fs::chown(&path, Some(0), Some(0))
        .with_context(|| format!("chown 0:0 {}", path.display()))?;
    Ok(())
}

fn ensure_runtime_dirs(staging: &Path) -> Result<()> {
    for dir in ["proc", "sys", "dev", "run"] {
        ensure_dir(&staging.join(dir), 0o755)?;
    }
    ensure_dir(&staging.join("tmp"), 0o1777)
}

fn make_ext4(staging: &Path, out: &Path, size_mb: u64) -> Result<()> {
    let size_bytes = size_mb * 1024 * 1024;
    {
        let file = File::create(out).with_context(|| format!("creating {}", out.display()))?;
        file.set_len(size_bytes)
            .with_context(|| format!("sizing {} to {} bytes", out.display(), size_bytes))?;
    }
    let status = Command::new("mkfs.ext4")
        .arg("-F")
        .arg("-q")
        .arg("-L")
        .arg("quoro-root")
        .arg("-d")
        .arg(staging)
        .arg(out)
        .status()
        .context("running mkfs.ext4 (is e2fsprogs installed?)")?;
    if !status.success() {
        bail!("mkfs.ext4 failed with {status}");
    }
    Ok(())
}

async fn convert(cli: &Cli, work: &Path) -> Result<()> {
    let staging = work.join("rootfs");
    let client = Client::new(ClientConfig {
        platform_resolver: Some(Box::new(linux_amd64_resolver)),
        ..Default::default()
    });
    let reference = Reference::try_from(cli.image.as_str())
        .with_context(|| format!("parsing image reference {:?}", cli.image))?;

    // ---- pull ------------------------------------------------------------
    let pull_started = Instant::now();

    let (manifest, manifest_digest, index_digest) = client
        .pull_image_manifest_and_list_digest(&reference, &RegistryAuth::Anonymous)
        .await
        .with_context(|| format!("resolving manifest for {}", cli.image))?;

    // Reject unsupported layers before downloading gigabytes.
    for layer in &manifest.layers {
        compression_for(&layer.media_type)?;
    }

    eprintln!("oci2rootfs: fetching config blob {}", manifest.config.digest);
    let config_path = work.join("config.json");
    pull_blob_verified(&client, &reference, &manifest.config, &config_path).await?;
    let config_json =
        fs::read_to_string(&config_path).context("reading the downloaded image config")?;
    let image_config = parse_image_config(&config_json)?;

    let mut layer_paths = Vec::with_capacity(manifest.layers.len());
    let mut compressed_bytes: u64 = 0;
    for (index, layer) in manifest.layers.iter().enumerate() {
        let path = work.join("layers").join(format!("layer-{index:03}.tar"));
        eprintln!(
            "oci2rootfs: fetching layer {}/{} {} ({} bytes)",
            index + 1,
            manifest.layers.len(),
            layer.digest,
            layer.size
        );
        compressed_bytes += pull_blob_verified(&client, &reference, layer, &path).await?;
        layer_paths.push((path, compression_for(&layer.media_type)?));
    }
    let pull_ms = pull_started.elapsed().as_millis();

    // ---- image config -> argv/env/identity -------------------------------
    let args: Vec<String> = if cli.args.is_empty() {
        let mut argv = image_config.entrypoint.clone();
        argv.extend(image_config.cmd.iter().cloned());
        argv
    } else {
        cli.args.clone()
    };
    if args.is_empty() {
        bail!(
            "image {} sets neither Entrypoint nor Cmd; pass --arg to choose argv",
            cli.image
        );
    }

    // ---- apply layers ----------------------------------------------------
    let apply_started = Instant::now();
    let mut stats = WhiteoutStats::default();
    for (index, (path, compression)) in layer_paths.iter().enumerate() {
        eprintln!("oci2rootfs: applying layer {}/{}", index + 1, layer_paths.len());
        let layer_stats = apply_layer(&staging, path, *compression)
            .with_context(|| format!("applying layer {}", path.display()))?;
        stats.whiteouts += layer_stats.whiteouts;
        stats.opaques += layer_stats.opaques;
    }
    let apply_ms = apply_started.elapsed().as_millis();

    let ident: Ident = resolve_user(
        &image_config.user,
        &read_optional(&staging.join("etc/passwd"))?,
        &read_optional(&staging.join("etc/group"))?,
    )
    .with_context(|| format!("resolving image User {:?}", image_config.user))?;

    let workdir = if image_config.working_dir.is_empty() {
        "/".to_string()
    } else {
        image_config.working_dir.clone()
    };

    ensure_runtime_dirs(&staging)?;
    install_init(&staging, &cli.init)?;
    write_config(
        &staging,
        &ConfigSpec {
            image_ref: cli.image.clone(),
            manifest_digest: manifest_digest.clone(),
            args: args.clone(),
            env: image_config.env.clone(),
            uid: ident.uid,
            gid: ident.gid,
            workdir: workdir.clone(),
            hostname: cli.hostname.clone(),
        },
    )?;

    // ---- size + mkfs -----------------------------------------------------
    let apparent = apparent_size(&staging)?;
    let apparent_mib = apparent as f64 / MI_B;
    let size_mb = cli
        .size_mb
        .unwrap_or_else(|| (apparent_mib * 1.3 + 64.0).ceil() as u64);

    let mkfs_started = Instant::now();
    make_ext4(&staging, &cli.out, size_mb)?;
    let mkfs_ms = mkfs_started.elapsed().as_millis();

    // ---- summary ---------------------------------------------------------
    println!("image           {}", cli.image);
    if let Some(index_digest) = &index_digest {
        println!("index manifest  {index_digest}");
    }
    println!("manifest        {manifest_digest}");
    println!(
        "layers          {} (compressed {:.2} MiB)",
        manifest.layers.len(),
        compressed_bytes as f64 / MI_B
    );
    println!(
        "whiteouts       {} (opaque dirs {})",
        stats.whiteouts, stats.opaques
    );
    println!(
        "rootfs          {} (apparent staging {:.2} MiB)",
        cli.out.display(),
        apparent_mib
    );
    println!("rootfs size     {size_mb} MiB");
    println!("argv            {args:?}");
    println!("user            {}:{}", ident.uid, ident.gid);
    println!("workdir         {workdir}");
    println!(
        "timing          pull {pull_ms} ms, apply {apply_ms} ms, mkfs {mkfs_ms} ms"
    );
    println!("sha256          {}", sha256_file(&cli.out)?);
    Ok(())
}

fn read_optional(path: &Path) -> Result<String> {
    match fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(String::new()),
        Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("oci2rootfs: error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = parse_args()?;
    ensure_root()?;
    if !cli.init.is_file() {
        bail!("--init {} does not exist or is not a file", cli.init.display());
    }

    let work = prepare_work_dir(&cli)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;

    let result = runtime.block_on(convert(&cli, &work));
    match result {
        Ok(()) => {
            if cli.keep_work {
                eprintln!("oci2rootfs: work dir kept at {}", work.display());
            } else {
                fs::remove_dir_all(&work)
                    .with_context(|| format!("cleaning up work dir {}", work.display()))?;
            }
            Ok(())
        }
        Err(err) => {
            eprintln!("oci2rootfs: work dir kept for debugging at {}", work.display());
            Err(err)
        }
    }
}
