//! Testable building blocks of `oci2rootfs`.
//!
//! Everything here is plain filesystem work: applying OCI layers with whiteout
//! handling, resolving an image `User` string against `/etc/passwd` and
//! `/etc/group`, and rendering `/quoro/config`. The network/pull side lives in
//! `main.rs`.

use std::fs::{self, File};
use std::io::{self, BufReader, Read};
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};

/// Uncompressed OCI layer (a tar archive).
pub const OCI_LAYER_TAR: &str = "application/vnd.oci.image.layer.v1.tar";
/// gzip-compressed OCI layer.
pub const OCI_LAYER_TAR_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
/// Uncompressed Docker layer (a tar archive).
pub const DOCKER_LAYER_TAR: &str = "application/vnd.docker.image.rootfs.diff.tar";
/// gzip-compressed Docker layer.
pub const DOCKER_LAYER_TAR_GZIP: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";

/// How a layer blob is compressed on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
}

/// Map a layer media type to its compression.
///
/// Anything else (zstd, foreign/non-distributable layers, ...) is a hard error
/// that names the offending media type, so the operator knows exactly which
/// layer we cannot handle.
pub fn compression_for(media_type: &str) -> Result<Compression> {
    // Some registries append parameters (`; charset=utf-8`); ignore them.
    let base = media_type.split(';').next().unwrap_or(media_type).trim();
    match base {
        OCI_LAYER_TAR | DOCKER_LAYER_TAR => Ok(Compression::None),
        OCI_LAYER_TAR_GZIP | DOCKER_LAYER_TAR_GZIP => Ok(Compression::Gzip),
        other => bail!(
            "unsupported layer media type {other:?}; supported: {OCI_LAYER_TAR}, \
             {OCI_LAYER_TAR_GZIP}, {DOCKER_LAYER_TAR}, {DOCKER_LAYER_TAR_GZIP}"
        ),
    }
}

/// Counters for one applied layer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WhiteoutStats {
    /// `.wh.<name>` entries applied.
    pub whiteouts: usize,
    /// `.wh..wh..opq` entries applied.
    pub opaques: usize,
}

/// Normalised, relative components of a tar entry path.
///
/// Mirrors what `tar`'s own unpacking does with leading `/` and `.` (they are
/// dropped) but refuses `..` outright instead of silently skipping the entry.
fn safe_components(path: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            Component::Normal(part) => {
                let name = part.to_string_lossy().into_owned();
                if !name.is_empty() {
                    out.push(name);
                }
            }
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => bail!(
                "tar entry path {:?} contains '..'; refusing to apply it",
                path.display()
            ),
        }
    }
    Ok(out)
}

/// Resolve `components` under `root_canon`, following symlinked parent
/// directories only when they stay inside the staging root.
///
/// The final component is never followed: callers delete/inspect it with
/// `symlink_metadata`.
fn resolve_entry_target(
    root_canon: &Path,
    components: &[String],
) -> Result<PathBuf> {
    let mut cur = root_canon.to_path_buf();
    for (idx, name) in components.iter().enumerate() {
        let is_last = idx + 1 == components.len();
        let next = cur.join(name);
        if is_last {
            return Ok(next);
        }
        match fs::symlink_metadata(&next) {
            Ok(meta) if meta.file_type().is_symlink() => {
                let resolved = fs::canonicalize(&next).with_context(|| {
                    format!("resolving symlinked directory {} in the staging tree", next.display())
                })?;
                if !resolved.starts_with(root_canon) {
                    bail!(
                        "layer entry {:?} escapes the rootfs through symlink {}",
                        components.join("/"),
                        next.display()
                    );
                }
                cur = resolved;
            }
            Ok(_) => cur = next,
            Err(err) if err.kind() == io::ErrorKind::NotFound => cur = next,
            Err(err) => {
                return Err(err).with_context(|| format!("stat {}", next.display()));
            }
        }
    }
    Ok(cur)
}

/// Remove a file, symlink or directory tree without following symlinks.
fn remove_any(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.is_dir() {
                fs::remove_dir_all(path)
                    .with_context(|| format!("removing directory {}", path.display()))?;
            } else {
                fs::remove_file(path).with_context(|| format!("removing {}", path.display()))?;
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("stat {}", path.display())),
    }
    Ok(())
}

/// Delete every child of `dir`, leaving `dir` itself in place.
fn remove_dir_children(dir: &Path) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).with_context(|| format!("listing {}", dir.display())),
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        remove_any(&entry.path())?
    }
    Ok(())
}

fn layer_archive(blob: &Path, compression: Compression) -> Result<tar::Archive<Box<dyn Read>>> {
    let file = File::open(blob).with_context(|| format!("opening layer blob {}", blob.display()))?;
    let reader: Box<dyn Read> = match compression {
        Compression::None => Box::new(BufReader::new(file)),
        Compression::Gzip => Box::new(GzDecoder::new(BufReader::new(file))),
    };
    let mut archive = tar::Archive::new(reader);
    archive.set_preserve_permissions(true);
    archive.set_preserve_ownerships(true);
    archive.set_preserve_mtime(true);
    Ok(archive)
}

/// Apply one layer blob on top of the staging tree.
///
/// Two passes over the (already downloaded) blob:
/// 1. whiteouts and opaque markers, applied against what lower layers left;
/// 2. extraction of every non-whiteout entry.
pub fn apply_layer(staging: &Path, blob: &Path, compression: Compression) -> Result<WhiteoutStats> {
    let root_canon = fs::canonicalize(staging)
        .with_context(|| format!("resolving staging directory {}", staging.display()))?;
    let mut stats = WhiteoutStats::default();

    // Pass 1: whiteouts.
    {
        let mut archive = layer_archive(blob, compression)?;
        for entry in archive.entries().context("reading layer entries (whiteout pass)")? {
            let entry = entry.context("reading a layer entry (whiteout pass)")?;
            let entry_path = entry.path().context("decoding a layer entry path")?.into_owned();
            let components = safe_components(&entry_path)?;
            let Some(basename) = components.last() else {
                continue;
            };
            if basename == ".wh..wh..opq" {
                let dir = resolve_entry_target(&root_canon, &components[..components.len() - 1])?;
                remove_dir_children(&dir)?;
                stats.opaques += 1;
            } else if let Some(victim) = basename.strip_prefix(".wh.") {
                if victim.is_empty() {
                    bail!("malformed whiteout entry {:?}", entry_path.display());
                }
                let mut target_components = components[..components.len() - 1].to_vec();
                target_components.push(victim.to_string());
                let target = resolve_entry_target(&root_canon, &target_components)?;
                remove_any(&target)?;
                stats.whiteouts += 1;
            }
        }
    }

    // Pass 2: extraction.
    {
        let mut archive = layer_archive(blob, compression)?;
        for entry in archive.entries().context("reading layer entries")? {
            let mut entry = entry.context("reading a layer entry")?;
            let entry_path = entry.path().context("decoding a layer entry path")?.into_owned();
            let components = safe_components(&entry_path)?;
            let Some(basename) = components.last() else {
                continue;
            };
            if basename.starts_with(".wh.") {
                continue;
            }
            let is_dir = entry.header().entry_type().is_dir();
            let target = resolve_entry_target(&root_canon, &components)?;
            match fs::symlink_metadata(&target) {
                Ok(meta) if is_dir && meta.is_dir() => {
                    // Directory over directory: `unpack_in` only refreshes metadata.
                }
                Ok(_) => remove_any(&target)?,
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| format!("stat {}", target.display()));
                }
            }
            let unpacked = entry
                .unpack_in(staging)
                .with_context(|| format!("extracting {:?}", entry_path.display()))?;
            if !unpacked {
                bail!("tar refused to extract {:?} (path escape?)", entry_path.display());
            }
        }
    }

    Ok(stats)
}

/// Numeric uid/gid the app runs as.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ident {
    pub uid: u32,
    pub gid: u32,
}

fn passwd_entry(passwd: &str, predicate: impl Fn(&str, u32) -> bool) -> Option<(u32, u32)> {
    for line in passwd.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split(':');
        let name = match fields.next() {
            Some(name) => name,
            None => continue,
        };
        let _password = match fields.next() {
            Some(pw) => pw,
            None => continue,
        };
        let uid: u32 = match fields.next().and_then(|f| f.parse().ok()) {
            Some(uid) => uid,
            None => continue,
        };
        let gid: u32 = match fields.next().and_then(|f| f.parse().ok()) {
            Some(gid) => gid,
            None => continue,
        };
        if predicate(name, uid) {
            return Some((uid, gid));
        }
    }
    None
}

fn group_gid(group: &str, name: &str) -> Option<u32> {
    for line in group.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split(':');
        let group_name = fields.next()?;
        let _password = fields.next()?;
        let gid: u32 = fields.next()?.parse().ok()?;
        if group_name == name {
            return Some(gid);
        }
    }
    None
}

/// Resolve an image `User` value against the staging tree's passwd/group data.
///
/// Accepted forms: `""`, `name`, `uid`, `uid:gid`, `name:group`, `uid:group`.
/// The gid defaults to the user's primary group from `/etc/passwd`, else 0.
/// Unknown names are errors.
pub fn resolve_user(spec: &str, passwd: &str, group: &str) -> Result<Ident> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Ok(Ident::default());
    }
    let (user_part, group_part) = match spec.split_once(':') {
        Some((user, group)) => (user, Some(group)),
        None => (spec, None),
    };
    if user_part.is_empty() {
        bail!("image User {spec:?} has an empty user part; expected user[:group]");
    }

    let (uid, primary_gid) = match user_part.parse::<u32>() {
        Ok(uid) => (uid, passwd_entry(passwd, |_, entry_uid| entry_uid == uid).map(|(_, gid)| gid)),
        Err(_) => match passwd_entry(passwd, |name, _| name == user_part) {
            Some((uid, gid)) => (uid, Some(gid)),
            None => bail!("image User {spec:?}: user {user_part:?} not found in /etc/passwd"),
        },
    };

    let gid = match group_part {
        None | Some("") => primary_gid.unwrap_or(0),
        Some(group_name) => match group_name.parse::<u32>() {
            Ok(gid) => gid,
            Err(_) => match group_gid(group, group_name) {
                Some(gid) => gid,
                None => {
                    bail!("image User {spec:?}: group {group_name:?} not found in /etc/group")
                }
            },
        },
    };

    Ok(Ident { uid, gid })
}

/// Everything `/quoro/config` needs.
#[derive(Clone, Debug)]
pub struct ConfigSpec {
    pub image_ref: String,
    pub manifest_digest: String,
    pub args: Vec<String>,
    pub env: Vec<String>,
    pub uid: u32,
    pub gid: u32,
    pub workdir: String,
    pub hostname: Option<String>,
}

fn check_value(key: &str, value: &str) -> Result<()> {
    if value.contains('\n') || value.contains('\r') {
        bail!("refusing to write {key} value containing a newline: {value:?}");
    }
    Ok(())
}

/// Render `/quoro/config` (see `spikes/oci-microvm/config-format.md`).
pub fn render_config(spec: &ConfigSpec) -> Result<String> {
    check_value("image ref", &spec.image_ref)?;
    check_value("manifest digest", &spec.manifest_digest)?;
    if spec.args.is_empty() {
        bail!("refusing to write a config without argv");
    }
    if !spec.workdir.starts_with('/') {
        bail!("workdir {:?} is not absolute", spec.workdir);
    }

    let mut out = String::new();
    out.push_str(&format!(
        "# generated by oci2rootfs from {} manifest {}\n",
        spec.image_ref, spec.manifest_digest
    ));
    for arg in &spec.args {
        check_value("arg", arg)?;
        out.push_str("arg=");
        out.push_str(arg);
        out.push('\n');
    }
    for entry in &spec.env {
        check_value("env", entry)?;
        out.push_str("env=");
        out.push_str(entry);
        out.push('\n');
    }
    check_value("workdir", &spec.workdir)?;
    out.push_str(&format!("uid={}\n", spec.uid));
    out.push_str(&format!("gid={}\n", spec.gid));
    out.push_str("workdir=");
    out.push_str(&spec.workdir);
    out.push('\n');
    if let Some(hostname) = &spec.hostname {
        check_value("hostname", hostname)?;
        out.push_str("hostname=");
        out.push_str(hostname);
        out.push('\n');
    }
    Ok(out)
}

/// Parse an OCI image config JSON blob into the fields this spike cares about.
#[derive(Clone, Debug, Default)]
pub struct ImageConfig {
    pub entrypoint: Vec<String>,
    pub cmd: Vec<String>,
    pub env: Vec<String>,
    pub user: String,
    pub working_dir: String,
}

pub fn parse_image_config(json: &str) -> Result<ImageConfig> {
    let value: serde_json::Value = serde_json::from_str(json).context("parsing image config JSON")?;
    let config = value
        .get("config")
        .and_then(|c| c.as_object())
        .context("image config has no `config` object")?;

    fn strings(config: &serde_json::Map<String, serde_json::Value>, key: &str) -> Result<Vec<String>> {
        match config.get(key) {
            None | Some(serde_json::Value::Null) => Ok(Vec::new()),
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(str::to_owned)
                        .with_context(|| format!("image config `{key}` contains a non-string entry"))
                })
                .collect(),
            Some(_) => bail!("image config `{key}` is not an array"),
        }
    }

    fn string(config: &serde_json::Map<String, serde_json::Value>, key: &str) -> Result<String> {
        match config.get(key) {
            None | Some(serde_json::Value::Null) => Ok(String::new()),
            Some(serde_json::Value::String(value)) => Ok(value.clone()),
            Some(_) => bail!("image config `{key}` is not a string"),
        }
    }

    Ok(ImageConfig {
        entrypoint: strings(config, "Entrypoint")?,
        cmd: strings(config, "Cmd")?,
        env: strings(config, "Env")?,
        user: string(config, "User")?,
        working_dir: string(config, "WorkingDir")?,
    })
}

/// sha256 of a file, as `sha256:<hex>`.
pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(format!("sha256:{}", hex(&hasher.finalize())))
}

pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Apparent size of `path` (sum of `st_size`), without following symlinks.
pub fn apparent_size(path: &Path) -> Result<u64> {
    let meta = fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    if !meta.is_dir() {
        return Ok(meta.len());
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path).with_context(|| format!("listing {}", path.display()))? {
        let entry = entry?;
        total += apparent_size(&entry.path())?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::os::unix::fs::MetadataExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!(
                "oci2rootfs-test-{}-{}-{}",
                std::process::id(),
                tag,
                n
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("create test temp dir");
            TempDir { path }
        }

        fn join(&self, name: &str) -> PathBuf {
            self.path.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// uid/gid of the test process: layer headers must carry them because
    /// `preserve_ownerships` makes `chown` failures hard errors for non-root.
    fn me() -> (u64, u64) {
        let meta = fs::metadata("/proc/self").expect("stat /proc/self");
        (meta.uid() as u64, meta.gid() as u64)
    }

    enum Entry<'a> {
        File(&'a str, &'a [u8]),
        /// A file whose name is written straight into the header: `tar::Builder`
        /// refuses to emit `..` paths through its normal API, but a hostile
        /// layer can still contain them.
        RawFile(&'a str, &'a [u8]),
        Dir(&'a str),
        Symlink(&'a str, &'a str),
    }

    fn build_layer(path: &Path, entries: &[Entry<'_>]) {
        let (uid, gid) = me();
        let file = File::create(path).expect("create layer");
        let mut builder = tar::Builder::new(file);
        for entry in entries {
            let mut header = tar::Header::new_gnu();
            header.set_uid(uid);
            header.set_gid(gid);
            header.set_mtime(0);
            match entry {
                Entry::File(name, data) => {
                    header.set_size(data.len() as u64);
                    header.set_mode(0o644);
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_cksum();
                    builder
                        .append_data(&mut header, name, &data[..])
                        .expect("append file");
                }
                Entry::RawFile(name, data) => {
                    header.set_size(data.len() as u64);
                    header.set_mode(0o644);
                    header.set_entry_type(tar::EntryType::Regular);
                    header.as_mut_bytes()[..name.len()].copy_from_slice(name.as_bytes());
                    header.set_cksum();
                    builder.append(&header, &data[..]).expect("append raw file");
                }
                Entry::Dir(name) => {
                    header.set_size(0);
                    header.set_mode(0o755);
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_cksum();
                    builder
                        .append_data(&mut header, name, io::empty())
                        .expect("append dir");
                }
                Entry::Symlink(name, target) => {
                    header.set_size(0);
                    header.set_mode(0o777);
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_link_name(target).expect("link name");
                    header.set_cksum();
                    builder
                        .append_data(&mut header, name, io::empty())
                        .expect("append symlink");
                }
            }
        }
        builder.finish().expect("finish layer");
    }

    fn staging(dir: &TempDir) -> PathBuf {
        let path = dir.join("rootfs");
        fs::create_dir_all(&path).expect("create staging");
        path
    }

    #[test]
    fn file_whiteout_removes_only_that_entry() {
        let dir = TempDir::new("wh-file");
        let root = staging(&dir);
        let base = dir.join("l1.tar");
        let top = dir.join("l2.tar");
        build_layer(
            &base,
            &[
                Entry::Dir("app/"),
                Entry::File("app/keep", b"keep"),
                Entry::File("app/drop", b"drop"),
            ],
        );
        build_layer(&top, &[Entry::File("app/.wh.drop", b"")]);

        apply_layer(&root, &base, Compression::None).expect("apply base");
        let stats = apply_layer(&root, &top, Compression::None).expect("apply top");

        assert_eq!(stats, WhiteoutStats { whiteouts: 1, opaques: 0 });
        assert!(!root.join("app/drop").exists(), "whiteouted file must be gone");
        assert_eq!(
            fs::read(root.join("app/keep")).expect("keep readable"),
            b"keep"
        );
    }

    #[test]
    fn directory_whiteout_removes_the_whole_subtree() {
        let dir = TempDir::new("wh-dir");
        let root = staging(&dir);
        let base = dir.join("l1.tar");
        let top = dir.join("l2.tar");
        build_layer(
            &base,
            &[
                Entry::Dir("app/"),
                Entry::Dir("app/sub/"),
                Entry::File("app/sub/deep", b"deep"),
                Entry::File("app/other", b"other"),
            ],
        );
        build_layer(&top, &[Entry::File("app/.wh.sub", b"")]);

        apply_layer(&root, &base, Compression::None).expect("apply base");
        let stats = apply_layer(&root, &top, Compression::None).expect("apply top");

        assert_eq!(stats.whiteouts, 1);
        assert!(!root.join("app/sub").exists());
        assert!(root.join("app/other").exists());
    }

    #[test]
    fn opaque_directory_drops_lower_children_but_keeps_same_layer_ones() {
        let dir = TempDir::new("wh-opq");
        let root = staging(&dir);
        let base = dir.join("l1.tar");
        let top = dir.join("l2.tar");
        build_layer(
            &base,
            &[
                Entry::Dir("d/"),
                Entry::File("d/lower", b"lower"),
                Entry::Dir("d/lowerdir/"),
                Entry::File("d/lowerdir/x", b"x"),
            ],
        );
        build_layer(
            &top,
            &[
                Entry::File("d/.wh..wh..opq", b""),
                Entry::File("d/same", b"same"),
            ],
        );

        apply_layer(&root, &base, Compression::None).expect("apply base");
        let stats = apply_layer(&root, &top, Compression::None).expect("apply top");

        assert_eq!(stats, WhiteoutStats { whiteouts: 0, opaques: 1 });
        assert!(!root.join("d/lower").exists(), "lower file must be gone");
        assert!(!root.join("d/lowerdir").exists(), "lower dir must be gone");
        assert_eq!(
            fs::read(root.join("d/same")).expect("same readable"),
            b"same"
        );
    }

    #[test]
    fn file_replaced_by_symlink_does_not_keep_old_content() {
        let dir = TempDir::new("replace-symlink");
        let root = staging(&dir);
        let base = dir.join("l1.tar");
        let top = dir.join("l2.tar");
        build_layer(&base, &[Entry::File("link", b"i am a file")]);
        build_layer(&top, &[Entry::Symlink("link", "/bin/sh")]);

        apply_layer(&root, &base, Compression::None).expect("apply base");
        apply_layer(&root, &top, Compression::None).expect("apply top");

        let meta = fs::symlink_metadata(root.join("link")).expect("link exists");
        assert!(meta.file_type().is_symlink(), "entry must now be a symlink");
        assert_eq!(
            fs::read_link(root.join("link")).expect("read link"),
            PathBuf::from("/bin/sh")
        );
    }

    #[test]
    fn path_escape_is_rejected() {
        let dir = TempDir::new("escape");
        let root = staging(&dir);
        let evil = dir.join("evil.tar");
        build_layer(&evil, &[Entry::RawFile("../evil", b"pwned")]);

        let err = apply_layer(&root, &evil, Compression::None).expect_err("must reject");
        let message = format!("{err:#}");
        assert!(message.contains(".."), "unexpected error: {message}");
        assert!(!dir.join("evil").exists(), "nothing may be written outside");
    }

    #[test]
    fn symlinked_directory_escape_is_rejected() {
        let dir = TempDir::new("escape-link");
        let root = staging(&dir);
        let outside = dir.join("outside");
        fs::create_dir_all(&outside).expect("outside dir");
        fs::write(outside.join("victim"), b"keep me").expect("victim");

        let base = dir.join("l1.tar");
        build_layer(&base, &[Entry::Symlink("escape", "../outside")]);
        apply_layer(&root, &base, Compression::None).expect("apply base");

        let evil = dir.join("l2.tar");
        build_layer(&evil, &[Entry::File("escape/victim", b"pwned")]);
        let err = apply_layer(&root, &evil, Compression::None).expect_err("must reject");
        assert!(format!("{err:#}").contains("escapes"), "unexpected error: {err:#}");
        assert_eq!(
            fs::read(outside.join("victim")).expect("victim readable"),
            b"keep me"
        );
    }

    #[test]
    fn unsupported_media_type_names_the_type() {
        let err = compression_for("application/vnd.oci.image.layer.v1.tar+zstd")
            .expect_err("zstd is unsupported");
        assert!(format!("{err:#}").contains("tar+zstd"));
    }

    const PASSWD: &str = "\
root:x:0:0:root:/root:/bin/sh
app:x:1000:1000:App:/home/app:/bin/sh
nobody:x:65534:65534::/:/sbin/nologin
";
    const GROUP: &str = "\
root:x:0:
app:x:1000:
staff:x:50:
";

    #[test]
    fn user_forms_resolve() {
        let cases: &[(&str, Ident)] = &[
            ("", Ident { uid: 0, gid: 0 }),
            ("root", Ident { uid: 0, gid: 0 }),
            ("app", Ident { uid: 1000, gid: 1000 }),
            ("1000", Ident { uid: 1000, gid: 1000 }),
            ("1000:50", Ident { uid: 1000, gid: 50 }),
            ("app:staff", Ident { uid: 1000, gid: 50 }),
            ("1000:staff", Ident { uid: 1000, gid: 50 }),
            ("app:4242", Ident { uid: 1000, gid: 4242 }),
            ("1234", Ident { uid: 1234, gid: 0 }),
            ("nobody", Ident { uid: 65534, gid: 65534 }),
        ];
        for (spec, expected) in cases {
            let got = resolve_user(spec, PASSWD, GROUP)
                .unwrap_or_else(|err| panic!("resolve_user({spec:?}) failed: {err:#}"));
            assert_eq!(got, *expected, "resolve_user({spec:?})");
        }

        for spec in ["nosuchuser", "app:nosuchgroup", ":staff"] {
            assert!(
                resolve_user(spec, PASSWD, GROUP).is_err(),
                "resolve_user({spec:?}) should fail"
            );
        }
    }

    fn sample_spec() -> ConfigSpec {
        ConfigSpec {
            image_ref: "docker.io/library/alpine:3.22".into(),
            manifest_digest: "sha256:abcd".into(),
            args: vec!["/bin/sh".into(), "-c".into(), "echo hi".into()],
            env: vec!["PATH=/usr/bin:/bin".into()],
            uid: 1000,
            gid: 1000,
            workdir: "/app".into(),
            hostname: Some("spike".into()),
        }
    }

    #[test]
    fn config_renders_expected_keys() {
        let rendered = render_config(&sample_spec()).expect("render");
        assert!(rendered.starts_with("# generated by oci2rootfs from docker.io/library/alpine:3.22"));
        let lines: Vec<&str> = rendered.lines().filter(|l| !l.starts_with('#')).collect();
        assert_eq!(
            lines,
            vec![
                "arg=/bin/sh",
                "arg=-c",
                "arg=echo hi",
                "env=PATH=/usr/bin:/bin",
                "uid=1000",
                "gid=1000",
                "workdir=/app",
                "hostname=spike",
            ]
        );
    }

    #[test]
    fn config_writer_rejects_newlines() {
        let mut spec = sample_spec();
        spec.args.push("a\nb".into());
        let err = render_config(&spec).expect_err("newline in arg");
        assert!(format!("{err:#}").contains("newline"));

        let mut spec = sample_spec();
        spec.env.push("A=b\nc".into());
        assert!(render_config(&spec).is_err());

        let mut spec = sample_spec();
        spec.hostname = Some("bad\nhost".into());
        assert!(render_config(&spec).is_err());

        let mut spec = sample_spec();
        spec.workdir = "relative".into();
        assert!(render_config(&spec).is_err());
    }

    #[test]
    fn image_config_parses_entrypoint_and_cmd() {
        let json = r#"{
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "Entrypoint": ["/docker-entrypoint.sh"],
                "Cmd": ["nginx", "-g", "daemon off;"],
                "Env": ["PATH=/usr/bin", "NGINX_VERSION=1.29"],
                "User": "nginx",
                "WorkingDir": "/usr/share/nginx/html"
            }
        }"#;
        let parsed = parse_image_config(json).expect("parse");
        assert_eq!(parsed.entrypoint, vec!["/docker-entrypoint.sh"]);
        assert_eq!(parsed.cmd, vec!["nginx", "-g", "daemon off;"]);
        assert_eq!(parsed.env.len(), 2);
        assert_eq!(parsed.user, "nginx");
        assert_eq!(parsed.working_dir, "/usr/share/nginx/html");

        let empty = parse_image_config(r#"{"config":{}}"#).expect("parse empty");
        assert!(empty.cmd.is_empty() && empty.entrypoint.is_empty());
    }

    #[test]
    fn sha256_file_matches_known_digest() {
        let dir = TempDir::new("sha");
        let path = dir.join("blob");
        let mut file = File::create(&path).expect("create");
        file.write_all(b"abc").expect("write");
        drop(file);
        assert_eq!(
            sha256_file(&path).expect("hash"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
