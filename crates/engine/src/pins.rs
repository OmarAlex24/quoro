// SPDX-License-Identifier: AGPL-3.0-only
//! The one place where the Cloud Hypervisor release and the guest kernel are
//! pinned: `crates/engine/pins.txt`, compiled in with `include_str!` and read
//! by `crates/engine/scripts/fetch-pinned.sh` too.
//!
//! Neither upstream publishes signatures, SUMS files or build attestations
//! (F-0206), so the pin is GitHub's release-asset sha256 and the engine refuses
//! to run a file whose sha256 differs.
//!
//! INTERFACE FROZEN BY THE SUB-ORCHESTRATOR: keep the public names and
//! signatures.

use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::EngineError;

/// The pins file, verbatim.
pub const PINS_TXT: &str = include_str!("../pins.txt");

/// Name of the Cloud Hypervisor static binary pin in `pins.txt`.
pub const CLOUD_HYPERVISOR: &str = "cloud-hypervisor";
/// Name of the guest kernel pin in `pins.txt`.
pub const GUEST_KERNEL: &str = "guest-kernel";

/// Size of the chunk the digest is computed over.
const HASH_CHUNK: usize = 1 << 20;

/// One pinned artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pin {
    /// `cloud-hypervisor` or `guest-kernel`.
    pub name: String,
    /// Upstream release, for example `v54.0`.
    pub version: String,
    /// `x86_64` (the only architecture pinned so far).
    pub arch: String,
    /// File name the artifact is stored under in the artifacts directory.
    pub file: String,
    pub url: String,
    /// Lowercase hex sha256 of the file.
    pub sha256: String,
}

/// Every pin in [`PINS_TXT`].
///
/// # Panics
///
/// Never for the shipped file: a unit test parses it. A malformed edit fails
/// that test in CI before it can ship, and at run time the panic names the
/// offending line instead of silently running an unverified artifact.
#[must_use]
pub fn all() -> Vec<Pin> {
    match parse(PINS_TXT) {
        Ok(pins) => pins,
        Err(message) => panic!("{message}"),
    }
}

/// The pin called `name` for the build's architecture
/// (`std::env::consts::ARCH`).
///
/// # Errors
///
/// `Unsupported` when no pin exists for that name and architecture.
pub fn get(name: &str) -> Result<Pin, EngineError> {
    let arch = std::env::consts::ARCH;
    all()
        .into_iter()
        .find(|pin| pin.name == name && pin.arch == arch)
        .ok_or_else(|| EngineError::Unsupported(format!("no pinned {name} for {arch}")))
}

/// Checks that `path` is a regular file whose sha256 equals `pin.sha256`.
///
/// A symlink is rejected even when it points at a matching file: the engine
/// checks the file it will actually execute.
///
/// # Errors
///
/// `Io` when the file cannot be read, `Hypervisor` naming the file, the
/// expected and the actual digest when they differ (and when the file is a
/// symlink or otherwise not a regular file).
pub fn verify(path: &Path, pin: &Pin) -> Result<(), EngineError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| io_error(path, &err))?;
    if !metadata.is_file() {
        return Err(EngineError::Hypervisor(format!(
            "{}: not a regular file",
            path.display()
        )));
    }

    let mut file = File::open(path).map_err(|err| io_error(path, &err))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; HASH_CHUNK];
    loop {
        let read = file.read(&mut buffer).map_err(|err| io_error(path, &err))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = hex_digest(&hasher.finalize());

    if actual != pin.sha256 {
        return Err(EngineError::Hypervisor(format!(
            "{}: sha256 {actual} does not match the pinned {} for {} {}",
            path.display(),
            pin.sha256,
            pin.name,
            pin.version
        )));
    }
    Ok(())
}

/// Parses the pins file. Every non-comment line is
/// `name version arch file url sha256`.
///
/// # Errors
///
/// A message naming the line number when a line does not have exactly six
/// fields, a 64-digit lowercase hex sha256 or an `https://` url.
fn parse(text: &str) -> Result<Vec<Pin>, String> {
    let mut pins = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let number = index + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let fields: Vec<&str> = line.split_whitespace().collect();
        let [name, version, arch, file, url, sha256] = fields[..] else {
            return Err(format!(
                "pins.txt line {number}: expected 6 fields (name version arch file url sha256), found {}",
                fields.len()
            ));
        };
        if sha256.len() != 64
            || !sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "pins.txt line {number}: sha256 must be 64 lowercase hex digits"
            ));
        }
        if !url.starts_with("https://") {
            return Err(format!(
                "pins.txt line {number}: url must start with https://"
            ));
        }

        pins.push(Pin {
            name: name.to_owned(),
            version: version.to_owned(),
            arch: arch.to_owned(),
            file: file.to_owned(),
            url: url.to_owned(),
            sha256: sha256.to_owned(),
        });
    }
    Ok(pins)
}

/// Renders a digest as lowercase hex.
fn hex_digest(digest: &[u8]) -> String {
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// The one error mapping for this module: `Io` with the path in the message.
fn io_error(path: &Path, err: &std::io::Error) -> EngineError {
    EngineError::Io(format!("{}: {err}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{CLOUD_HYPERVISOR, GUEST_KERNEL, PINS_TXT, Pin, all, get, parse, verify};
    use crate::EngineError;

    fn temp_path(tag: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("quoro-pins-{}-{tag}-{seq}", std::process::id()))
    }

    fn pin_for(sha256: &str) -> Pin {
        Pin {
            name: CLOUD_HYPERVISOR.to_owned(),
            version: "v54.0".to_owned(),
            arch: "x86_64".to_owned(),
            file: "cloud-hypervisor-static".to_owned(),
            url: "https://example.invalid/cloud-hypervisor-static".to_owned(),
            sha256: sha256.to_owned(),
        }
    }

    #[test]
    fn shipped_file_parses_into_the_two_pins() {
        let pins = all();
        assert_eq!(pins.len(), 2, "one pin per artifact: {pins:?}");

        assert_eq!(pins[0].name, CLOUD_HYPERVISOR);
        assert_eq!(pins[0].version, "v54.0");
        assert_eq!(pins[0].arch, "x86_64");
        assert_eq!(pins[0].file, "cloud-hypervisor-static");
        assert_eq!(
            pins[0].sha256,
            "4f4f98a3d661a40f3e5272627cc895a1b2e525f2f6bedf5f895f9cd21ba5c65b"
        );

        assert_eq!(pins[1].name, GUEST_KERNEL);
        assert_eq!(pins[1].file, "vmlinux-x86_64");
        assert_eq!(
            pins[1].sha256,
            "edfeb8a8a6c04ff2979e9f1c700feaa29b8f564f952261834ef0b7d2e673a853"
        );

        assert!(pins.iter().all(|pin| pin.url.starts_with("https://")));
        assert_eq!(
            parse(PINS_TXT).expect("the shipped file parses"),
            pins,
            "all() and parse() agree"
        );
    }

    #[test]
    fn get_finds_both_pins_on_x86_64() {
        if std::env::consts::ARCH != "x86_64" {
            eprintln!("skipping: this build is not x86_64");
            return;
        }
        for name in [CLOUD_HYPERVISOR, GUEST_KERNEL] {
            let pin = get(name).expect("the pin exists for x86_64");
            assert_eq!(pin.name, name);
            assert_eq!(pin.arch, "x86_64");
        }
        assert!(matches!(
            get("no-such-artifact"),
            Err(EngineError::Unsupported(_))
        ));
    }

    #[test]
    fn verify_accepts_a_matching_file() {
        use sha2::{Digest, Sha256};

        let path = temp_path("match");
        let content = b"the pinned bytes\n";
        std::fs::write(&path, content).expect("write test file");
        let digest = Sha256::digest(content);
        let sha256 = super::hex_digest(&digest);
        let pin = pin_for(&sha256);

        let result = verify(&path, &pin);
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn verify_detects_a_mismatch_naming_both_digests() {
        let path = temp_path("mismatch");
        std::fs::write(&path, b"actual content").expect("write test file");
        let pin = pin_for("0000000000000000000000000000000000000000000000000000000000000000");
        let err = verify(&path, &pin).expect_err("the digest does not match");
        let _ = std::fs::remove_file(&path);

        let EngineError::Hypervisor(message) = err else {
            panic!("expected Hypervisor, got {err:?}");
        };
        assert!(message.contains(&path.display().to_string()), "{message}");
        assert!(
            message.contains("does not match the pinned 0000000000000000000000000000000000000000000000000000000000000000"),
            "{message}"
        );
        assert!(message.contains(" for cloud-hypervisor v54.0"), "{message}");
        let actual = message
            .split("sha256 ")
            .nth(1)
            .and_then(|rest| rest.split(' ').next())
            .expect("the message names the actual digest");
        assert_eq!(actual.len(), 64, "{message}");
        assert!(
            actual
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "{message}"
        );
    }

    #[test]
    fn verify_rejects_a_symlink_and_a_missing_file() {
        let target = temp_path("symlink-target");
        let link = temp_path("symlink-link");
        std::fs::write(&target, b"content").expect("write target");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink");

        let pin = pin_for("0000000000000000000000000000000000000000000000000000000000000000");
        let link_result = verify(&link, &pin);
        let missing_result = verify(&temp_path("absent"), &pin);

        let _ = std::fs::remove_file(&link);
        let _ = std::fs::remove_file(&target);

        assert!(
            matches!(link_result, Err(EngineError::Hypervisor(_))),
            "a symlink is rejected: {link_result:?}"
        );
        assert!(
            matches!(missing_result, Err(EngineError::Io(_))),
            "a missing file is Io: {missing_result:?}"
        );
    }

    #[test]
    #[should_panic(expected = "pins.txt line 1")]
    fn parse_panics_on_a_line_with_too_few_fields() {
        parse("cloud-hypervisor v54.0 x86_64 cloud-hypervisor-static\n").unwrap();
    }

    #[test]
    #[should_panic(expected = "pins.txt line 2")]
    fn parse_panics_on_a_short_sha256() {
        parse(
            "# comment\ncloud-hypervisor v54.0 x86_64 cloud-hypervisor-static https://example.invalid/x abcdef\n",
        ).unwrap();
    }

    #[test]
    #[should_panic(expected = "pins.txt line 1")]
    fn parse_panics_on_an_uppercase_sha256() {
        parse(&format!(
            "cloud-hypervisor v54.0 x86_64 cloud-hypervisor-static https://example.invalid/x {}\n",
            "A".repeat(64)
        ))
        .unwrap();
    }

    #[test]
    #[should_panic(expected = "pins.txt line 1")]
    fn parse_panics_on_a_non_https_url() {
        parse(&format!(
            "cloud-hypervisor v54.0 x86_64 cloud-hypervisor-static http://example.invalid/x {}\n",
            "0".repeat(64)
        ))
        .unwrap();
    }

    #[test]
    fn parse_skips_blank_and_comment_lines() {
        let pins = parse(&format!(
            "\n# name version arch file url sha256\n   \n  # indented comment\n\
             guest-kernel v1 x86_64 vmlinux https://example.invalid/vmlinux {}\n",
            "a".repeat(64)
        ))
        .expect("only one real line");
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].name, GUEST_KERNEL);
    }
}
