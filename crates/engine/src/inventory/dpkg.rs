// SPDX-License-Identifier: AGPL-3.0-only
//! The `dpkg` package database.

use super::os_release::{OsRelease, major};
use super::{Collector, EntryKind, ReadOutcome, Tree, join, purl, read_bounded};
use crate::InventoryPackage;

/// Largest `var/lib/dpkg/status` read.
const MAX_STATUS_BYTES: usize = 64 * 1024 * 1024;
/// Largest one `var/lib/dpkg/status.d/*` file read.
const MAX_STATUS_D_BYTES: usize = 1024 * 1024;
/// Most files read from `var/lib/dpkg/status.d`.
const MAX_STATUS_D_FILES: usize = 10_000;

/// The parts of one stanza the inventory uses.
#[derive(Default)]
struct Stanza {
    package: Option<String>,
    version: Option<String>,
    source: Option<String>,
    arch: Option<String>,
    status: Option<String>,
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

/// Adds the packages of the image's `dpkg` database, if it has one.
pub fn collect<T: Tree + ?Sized>(tree: &T, os: Option<&OsRelease>, out: &mut Collector) {
    let context = Context::new(os);
    let mut saw_database = false;

    match read_bounded(tree, "var/lib/dpkg/status", MAX_STATUS_BYTES) {
        ReadOutcome::Data(data) => {
            saw_database = true;
            collect_stanzas(&data, &context, out, true);
        }
        ReadOutcome::TooLarge => {
            saw_database = true;
            out.not_inventoried("dpkg_too_large");
        }
        ReadOutcome::Missing => {}
    }

    if let Some(entries) = tree.list("var/lib/dpkg/status.d") {
        saw_database = true;
        let mut files = 0usize;
        for entry in entries {
            if entry.kind != EntryKind::File || entry.name.ends_with(".md5sums") {
                continue;
            }
            files += 1;
            if files > MAX_STATUS_D_FILES {
                out.not_inventoried("dpkg_status_d_limit");
                break;
            }
            let path = join("var/lib/dpkg/status.d", &entry.name);
            match read_bounded(tree, &path, MAX_STATUS_D_BYTES) {
                ReadOutcome::Data(data) => collect_stanzas(&data, &context, out, false),
                ReadOutcome::TooLarge => out.not_inventoried("dpkg_status_d_too_large"),
                ReadOutcome::Missing => {}
            }
            if out.is_full() {
                break;
            }
        }
    }

    if saw_database {
        out.cover(&context.ecosystem);
    }
}

/// Parses stanzas separated by blank lines. `need_installed` keeps only the
/// stanzas whose `Status` third word is `installed` (a `status.d` file has no
/// `Status` and counts as installed).
fn collect_stanzas(data: &[u8], context: &Context, out: &mut Collector, need_installed: bool) {
    let text = String::from_utf8_lossy(data);
    let mut stanza = Stanza::default();
    for line in text.lines() {
        if line.trim().is_empty() {
            if !emit(&mut stanza, context, out, need_installed) {
                return;
            }
            continue;
        }
        // A continuation line belongs to the field above; none of the fields
        // the inventory reads carries one it needs.
        if line.starts_with(' ') || line.starts_with('\t') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match key {
            "Package" => stanza.package = Some(value.to_owned()),
            "Version" => stanza.version = Some(value.to_owned()),
            "Source" => stanza.source = Some(value.to_owned()),
            "Architecture" => stanza.arch = Some(value.to_owned()),
            "Status" => stanza.status = Some(value.to_owned()),
            _ => {}
        }
    }
    let _ = emit(&mut stanza, context, out, need_installed);
}

/// Emits the stanza collected so far. Returns false once the inventory is
/// full and the caller should stop.
fn emit(stanza: &mut Stanza, context: &Context, out: &mut Collector, need_installed: bool) -> bool {
    let Stanza {
        package,
        version,
        source,
        arch,
        status,
    } = std::mem::take(stanza);

    if need_installed {
        let installed = status
            .as_deref()
            .and_then(|s| s.split_whitespace().nth(2))
            .is_some_and(|word| word == "installed");
        if !installed {
            return true;
        }
    }

    let (Some(name), Some(version)) = (package, version) else {
        return true;
    };
    if name.is_empty() || version.is_empty() {
        return true;
    }
    let source_name = source.as_deref().and_then(source_name_of);
    let arch = arch.as_deref().unwrap_or("");
    let upstream = source_name.unwrap_or("");
    let package_purl = purl(
        "deb",
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

/// The name part of a `Source:` field: `openssl` from `openssl` or
/// `openssl (3.5.1-1)`.
fn source_name_of(source: &str) -> Option<&str> {
    let name = source
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()?
        .trim();
    (!name.is_empty()).then_some(name)
}

/// The OSV ecosystem for a dpkg database.
fn ecosystem(os: Option<&OsRelease>) -> String {
    let Some(os) = os else {
        return "Debian".to_owned();
    };
    match os.id.as_str() {
        "debian" => match os.version_id.as_deref().map(major) {
            Some(major) if !major.is_empty() => format!("Debian:{major}"),
            _ => "Debian".to_owned(),
        },
        "ubuntu" => match os.version_id.as_deref().filter(|v| !v.is_empty()) {
            Some(version) => format!("Ubuntu:{version}"),
            None => "Ubuntu".to_owned(),
        },
        id => capitalize(id),
    }
}

/// The purl namespace: `debian`/`ubuntu` for the distro and its derivatives.
fn namespace(os: Option<&OsRelease>) -> String {
    let Some(os) = os else {
        return "debian".to_owned();
    };
    if os.id == "debian" || os.id == "ubuntu" {
        return os.id.clone();
    }
    if os.id_like.iter().any(|like| like == "debian") {
        return "debian".to_owned();
    }
    if os.id_like.iter().any(|like| like == "ubuntu") {
        return "ubuntu".to_owned();
    }
    os.id.clone()
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

/// `raspbian` -> `Raspbian`; an empty id falls back to `Debian`.
fn capitalize(id: &str) -> String {
    let mut chars = id.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "Debian".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ImageDigest;
    use crate::inventory::testing::MemTree;
    use crate::inventory::{MAX_PACKAGES, build, kind};

    fn debian(version_id: &str) -> OsRelease {
        OsRelease {
            id: "debian".into(),
            version_id: Some(version_id.into()),
            id_like: Vec::new(),
            version_codename: None,
        }
    }

    fn ubuntu(version_id: &str) -> OsRelease {
        OsRelease {
            id: "ubuntu".into(),
            version_id: Some(version_id.into()),
            id_like: vec!["debian".into()],
            version_codename: None,
        }
    }

    const STATUS: &str = "Package: libssl3t64\n\
                          Status: install ok installed\n\
                          Architecture: amd64\n\
                          Source: openssl (3.5.1-1)\n\
                          Version: 3.5.1-1\n\
                          \n\
                          Package: openssl\n\
                          Status: install ok installed\n\
                          Architecture: amd64\n\
                          Version: 3.5.1-1\n\
                          \n\
                          Package: old-config\n\
                          Status: deinstall ok config-files\n\
                          Architecture: amd64\n\
                          Version: 1.0-1\n\
                          \n\
                          Package: base-files\n\
                          Status: install ok installed\n\
                          Architecture: all\n\
                          Version: 13.8\n";

    fn collected(tree: &MemTree, os: &OsRelease) -> Collector {
        let mut out = Collector::new();
        collect(tree, Some(os), &mut out);
        out
    }

    #[test]
    fn keeps_only_installed_stanzas_with_their_source() {
        let tree = MemTree::new().file("var/lib/dpkg/status", STATUS);
        let out = collected(&tree, &debian("13"));

        let names: Vec<&str> = out.packages().iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["libssl3t64", "openssl", "base-files"]);

        let ssl = &out.packages()[0];
        assert_eq!(ssl.ecosystem, "Debian:13");
        assert_eq!(ssl.source_name.as_deref(), Some("openssl"));
        assert_eq!(
            ssl.purl,
            "pkg:deb/debian/libssl3t64@3.5.1-1?arch=amd64&distro=debian-13&upstream=openssl"
        );
        assert_eq!(
            out.packages()[1].purl,
            "pkg:deb/debian/openssl@3.5.1-1?arch=amd64&distro=debian-13"
        );
        assert_eq!(
            out.packages()[2].purl,
            "pkg:deb/debian/base-files@13.8?arch=all&distro=debian-13"
        );
        assert_eq!(out.packages()[2].source_name, None);
    }

    #[test]
    fn ubuntu_uses_the_full_version_id() {
        let tree = MemTree::new().file("var/lib/dpkg/status", STATUS);
        let out = collected(&tree, &ubuntu("24.04"));
        assert_eq!(out.packages()[0].ecosystem, "Ubuntu:24.04");
        assert_eq!(
            out.packages()[0].purl,
            "pkg:deb/ubuntu/libssl3t64@3.5.1-1?arch=amd64&distro=ubuntu-24.04&upstream=openssl"
        );
    }

    #[test]
    fn a_derivative_keeps_its_name_but_uses_the_debian_namespace() {
        let os = OsRelease {
            id: "raspbian".into(),
            version_id: Some("12".into()),
            id_like: vec!["debian".into()],
            version_codename: None,
        };
        let tree = MemTree::new().file(
            "var/lib/dpkg/status",
            b"Package: bash\nStatus: install ok installed\nArchitecture: armhf\nVersion: 5.2.15-2\n\n",
        );
        let out = collected(&tree, &os);
        assert_eq!(out.packages()[0].ecosystem, "Raspbian");
        assert_eq!(
            out.packages()[0].purl,
            "pkg:deb/debian/bash@5.2.15-2?arch=armhf&distro=raspbian-12"
        );
    }

    #[test]
    fn reads_distroless_status_d_and_skips_md5sums() {
        let tree = MemTree::new()
            .file(
                "var/lib/dpkg/status.d/libssl3t64",
                b"Package: libssl3t64\nArchitecture: amd64\nVersion: 3.5.1-1\nSource: openssl\n",
            )
            .file(
                "var/lib/dpkg/status.d/base-files",
                b"Package: base-files\nArchitecture: amd64\nVersion: 13.8\n",
            )
            .file(
                "var/lib/dpkg/status.d/libssl3t64.md5sums",
                b"not a stanza\n",
            );
        let out = collected(&tree, &debian("13"));
        let names: Vec<&str> = out.packages().iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, vec!["base-files", "libssl3t64"]);
        let ssl = out
            .packages()
            .iter()
            .find(|p| p.name == "libssl3t64")
            .expect("libssl3t64");
        assert_eq!(
            ssl.purl,
            "pkg:deb/debian/libssl3t64@3.5.1-1?arch=amd64&distro=debian-13&upstream=openssl"
        );
    }

    #[test]
    fn an_empty_database_is_covered() {
        let tree = MemTree::new().file("var/lib/dpkg/status", b"");
        let inventory = build(
            &tree,
            ImageDigest("sha256:00".into()),
            "2026-11-05T00:00:00Z".into(),
        );
        assert!(inventory.packages.is_empty());
        assert!(inventory.ecosystems.contains(&"Debian".to_owned()));
    }

    #[test]
    fn malformed_and_truncated_input_yields_fewer_packages() {
        let tree = MemTree::new().file(
            "var/lib/dpkg/status",
            b"Package: broken\nStatus: install ok installed\nVersion\n\n\
              Package: truncated\nStatus: install ok installed\nVersion: 2",
        );
        let out = collected(&tree, &debian("13"));
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].name, "truncated");
        assert_eq!(out.packages()[0].version, "2");
    }

    #[test]
    fn oversized_status_is_named_and_skipped() {
        let tree = MemTree::new().file("var/lib/dpkg/status", vec![b'a'; MAX_STATUS_BYTES + 1]);
        let out = collected(&tree, &debian("13"));
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec!["dpkg_too_large".to_owned()]);
    }

    #[test]
    fn binary_garbage_and_continuation_lines_do_not_panic() {
        let tree = MemTree::new().file(
            "var/lib/dpkg/status",
            b"\x00\x01\x02garbage\nPackage: ok\n Description: continued\n with more\nStatus: install ok installed\nVersion: 2.0\n\n",
        );
        let out = collected(&tree, &debian("13"));
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].name, "ok");
        assert_eq!(out.packages()[0].version, "2.0");
    }

    #[test]
    fn a_huge_number_of_stanzas_stops_at_the_bound() {
        let mut text = String::new();
        for i in 0..MAX_PACKAGES + 50 {
            text.push_str(&format!(
                "Package: p{i}\nStatus: install ok installed\nArchitecture: all\nVersion: 1\n\n"
            ));
        }
        let tree = MemTree::new().file("var/lib/dpkg/status", text.into_bytes());
        let out = collected(&tree, &debian("13"));
        assert!(out.is_full());
        assert!(out.len() <= MAX_PACKAGES);
        assert!(out.kinds().contains(&kind::TRUNCATED.to_owned()));
    }
}
