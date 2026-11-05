// SPDX-License-Identifier: AGPL-3.0-only
//! The `apk` package database.

use super::os_release::OsRelease;
use super::{Collector, ReadOutcome, Tree, purl, read_bounded};
use crate::InventoryPackage;

/// Largest `lib/apk/db/installed` read.
const MAX_APK_DB_BYTES: usize = 32 * 1024 * 1024;

/// The parts of one stanza the inventory uses.
#[derive(Default)]
struct Stanza {
    name: Option<String>,
    version: Option<String>,
    origin: Option<String>,
    arch: Option<String>,
}

/// What the purl and ecosystem need, computed once per image.
struct Context {
    ecosystem: String,
    namespace: String,
    distro: String,
}

impl Context {
    fn new(os: Option<&OsRelease>) -> Self {
        Self {
            ecosystem: ecosystem(os),
            namespace: namespace(os),
            distro: distro_qualifier(os),
        }
    }
}

/// Adds the packages of the image's `apk` database, if it has one.
pub fn collect<T: Tree + ?Sized>(tree: &T, os: Option<&OsRelease>, out: &mut Collector) {
    let context = Context::new(os);
    let data = match read_bounded(tree, "lib/apk/db/installed", MAX_APK_DB_BYTES) {
        ReadOutcome::Data(data) => data,
        ReadOutcome::TooLarge => {
            out.not_inventoried("apk_too_large");
            out.cover(&context.ecosystem);
            return;
        }
        ReadOutcome::Missing => return,
    };
    out.cover(&context.ecosystem);

    let text = String::from_utf8_lossy(&data);
    let mut stanza = Stanza::default();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !emit(&mut stanza, &context, out) {
                return;
            }
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match key {
            "P" => stanza.name = Some(value.to_owned()),
            "V" => stanza.version = Some(value.to_owned()),
            "o" => stanza.origin = Some(value.to_owned()),
            "A" => stanza.arch = Some(value.to_owned()),
            _ => {}
        }
    }
    let _ = emit(&mut stanza, &context, out);
}

/// Emits the stanza collected so far. Returns false once the inventory is
/// full and the caller should stop.
fn emit(stanza: &mut Stanza, context: &Context, out: &mut Collector) -> bool {
    let Stanza {
        name,
        version,
        origin,
        arch,
    } = std::mem::take(stanza);
    let (Some(name), Some(version)) = (name, version) else {
        return true;
    };
    if name.is_empty() || version.is_empty() {
        return true;
    }
    let source_name = origin.as_deref().filter(|origin| !origin.is_empty());
    let arch = arch.as_deref().unwrap_or("");
    let upstream = source_name.unwrap_or("");
    let package_purl = purl(
        "apk",
        &[&context.namespace],
        &name,
        &version,
        &[
            ("arch", arch),
            ("distro", &context.distro),
            ("upstream", upstream),
        ],
    );
    out.push(InventoryPackage {
        ecosystem: context.ecosystem.clone(),
        name,
        source_name: source_name.map(str::to_owned),
        version,
        purl: package_purl,
        path: None,
    })
}

/// The OSV ecosystem for an apk database.
fn ecosystem(os: Option<&OsRelease>) -> String {
    let Some(os) = os else {
        return "Alpine".to_owned();
    };
    match os.id.as_str() {
        "alpine" => match os.version_id.as_deref().and_then(release) {
            Some(release) => format!("Alpine:v{release}"),
            None => "Alpine".to_owned(),
        },
        "wolfi" => "Wolfi".to_owned(),
        "chainguard" => "Chainguard".to_owned(),
        _ => "Alpine".to_owned(),
    }
}

/// `3.22` from `3.22.1`; `None` when the version has no numeric
/// major and minor.
fn release(version_id: &str) -> Option<String> {
    let mut parts = version_id.split('.').filter(|part| !part.is_empty());
    let major = parts.next()?;
    let minor = parts.next()?;
    let numeric = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
    if numeric(major) && numeric(minor) {
        Some(format!("{major}.{minor}"))
    } else {
        None
    }
}

/// The purl namespace: the image's os id, `alpine` when it has none.
fn namespace(os: Option<&OsRelease>) -> String {
    match os {
        Some(os) if !os.id.is_empty() => os.id.clone(),
        _ => "alpine".to_owned(),
    }
}

/// `<id>-<version_id>` for the purl's `distro` qualifier.
fn distro_qualifier(os: Option<&OsRelease>) -> String {
    let Some(os) = os else {
        return String::new();
    };
    match os.version_id.as_deref().filter(|v| !v.is_empty()) {
        Some(version) => format!("{}-{version}", os.id),
        None => os.id.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ImageDigest;
    use crate::inventory::testing::MemTree;
    use crate::inventory::{build, kind};

    fn alpine(version_id: &str) -> OsRelease {
        OsRelease {
            id: "alpine".into(),
            version_id: Some(version_id.into()),
            id_like: Vec::new(),
            version_codename: None,
        }
    }

    const INSTALLED: &str = "C:Q1abcdef=\n\
                             P:musl\n\
                             V:1.2.5-r10\n\
                             A:x86_64\n\
                             o:musl\n\
                             T:the musl c library\n\
                             \n\
                             C:Q1other=\n\
                             P:libcrypto3\n\
                             V:3.5.1-r0\n\
                             A:x86_64\n\
                             o:openssl\n\
                             D:musl\n\
                             \n";

    fn collected(tree: &MemTree, os: &OsRelease) -> Collector {
        let mut out = Collector::new();
        collect(tree, Some(os), &mut out);
        out
    }

    #[test]
    fn reads_an_alpine_installed_database() {
        let tree = MemTree::new().file("lib/apk/db/installed", INSTALLED);
        let out = collected(&tree, &alpine("3.22.1"));

        let names: Vec<&str> = out.packages().iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["musl", "libcrypto3"]);
        assert_eq!(out.packages()[0].ecosystem, "Alpine:v3.22");
        assert_eq!(out.packages()[0].source_name.as_deref(), Some("musl"));
        assert_eq!(
            out.packages()[0].purl,
            "pkg:apk/alpine/musl@1.2.5-r10?arch=x86_64&distro=alpine-3.22.1&upstream=musl"
        );
        assert_eq!(
            out.packages()[1].purl,
            "pkg:apk/alpine/libcrypto3@3.5.1-r0?arch=x86_64&distro=alpine-3.22.1&upstream=openssl"
        );
    }

    #[test]
    fn covers_the_ecosystems_of_other_apk_distros() {
        let wolfi = OsRelease {
            id: "wolfi".into(),
            version_id: Some("20230201".into()),
            id_like: Vec::new(),
            version_codename: None,
        };
        let chainguard = OsRelease {
            id: "chainguard".into(),
            version_id: None,
            id_like: Vec::new(),
            version_codename: None,
        };
        let tree = MemTree::new().file(
            "lib/apk/db/installed",
            b"P:busybox\nV:1.36.1-r0\nA:x86_64\no:busybox\n\n",
        );
        let out = collected(&tree, &wolfi);
        assert_eq!(out.packages()[0].ecosystem, "Wolfi");
        assert_eq!(
            out.packages()[0].purl,
            "pkg:apk/wolfi/busybox@1.36.1-r0?arch=x86_64&distro=wolfi-20230201&upstream=busybox"
        );

        let out = collected(&tree, &chainguard);
        assert_eq!(out.packages()[0].ecosystem, "Chainguard");
        assert_eq!(
            out.packages()[0].purl,
            "pkg:apk/chainguard/busybox@1.36.1-r0?arch=x86_64&distro=chainguard&upstream=busybox"
        );
    }

    #[test]
    fn an_alpine_without_a_version_has_no_release() {
        let os = OsRelease {
            id: "alpine".into(),
            version_id: Some("edge".into()),
            id_like: Vec::new(),
            version_codename: None,
        };
        let tree =
            MemTree::new().file("lib/apk/db/installed", b"P:musl\nV:1.2.5-r10\nA:x86_64\n\n");
        let out = collected(&tree, &os);
        assert_eq!(out.packages()[0].ecosystem, "Alpine");
        assert_eq!(
            out.packages()[0].purl,
            "pkg:apk/alpine/musl@1.2.5-r10?arch=x86_64&distro=alpine-edge"
        );
    }

    #[test]
    fn an_empty_database_is_covered() {
        let tree = MemTree::new().file("lib/apk/db/installed", b"");
        let inventory = build(
            &tree,
            ImageDigest("sha256:00".into()),
            "2026-11-05T00:00:00Z".into(),
        );
        assert!(inventory.packages.is_empty());
        assert!(inventory.ecosystems.contains(&"Alpine".to_owned()));
    }

    #[test]
    fn malformed_stanzas_and_invalid_utf8_do_not_panic() {
        let tree = MemTree::new().file(
            "lib/apk/db/installed",
            b"P:\nV:1\n\n\
              P:no-version\nA:amd64\n\n\
              \xff\xfeP:garbage\n\n\
              P:ok\nV:2-r0\nA:amd64\n",
        );
        let out = collected(&tree, &alpine("3.22.1"));
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].name, "ok");
        assert_eq!(out.packages()[0].version, "2-r0");
    }

    #[test]
    fn binary_garbage_yields_no_packages() {
        let tree = MemTree::new()
            .file("etc/os-release", b"ID=alpine\nVERSION_ID=3.22.1\n")
            .file("lib/apk/db/installed", vec![0u8; 512]);
        let out = collected(&tree, &alpine("3.22.1"));
        assert!(out.packages().is_empty());
        let inventory = build(
            &tree,
            ImageDigest("sha256:00".into()),
            "2026-11-05T00:00:00Z".into(),
        );
        assert!(inventory.ecosystems.contains(&"Alpine:v3.22".to_owned()));
    }

    #[test]
    fn oversized_database_is_named_and_skipped() {
        let tree = MemTree::new().file("lib/apk/db/installed", vec![b'a'; MAX_APK_DB_BYTES + 1]);
        let out = collected(&tree, &alpine("3.22.1"));
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec!["apk_too_large".to_owned()]);
    }

    #[test]
    fn release_takes_the_numeric_major_and_minor() {
        assert_eq!(release("3.22.1").as_deref(), Some("3.22"));
        assert_eq!(release("3.22").as_deref(), Some("3.22"));
        assert_eq!(release("edge"), None);
        assert_eq!(release("3"), None);
        assert_eq!(release(""), None);
    }

    #[test]
    fn a_huge_number_of_stanzas_stops_at_the_bound() {
        let mut text = String::new();
        for i in 0..crate::inventory::MAX_PACKAGES + 50 {
            text.push_str(&format!("P:p{i}\nV:1\nA:x86_64\n\n"));
        }
        let tree = MemTree::new().file("lib/apk/db/installed", text.into_bytes());
        let out = collected(&tree, &alpine("3.22.1"));
        assert!(out.is_full());
        assert!(out.kinds().contains(&kind::TRUNCATED.to_owned()));
    }
}
