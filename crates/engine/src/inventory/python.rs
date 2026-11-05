// SPDX-License-Identifier: AGPL-3.0-only
//! Installed Python distributions (`*.dist-info/METADATA`, `*.egg-info/PKG-INFO`).

use super::{Collector, ReadOutcome, Tree, join, purl, read_bounded};
use crate::InventoryPackage;

/// Largest metadata file read.
const MAX_METADATA_BYTES: usize = 4 * 1024 * 1024;

/// Whether a directory named `name` holds a distribution's metadata.
pub fn is_metadata_dir(name: &str) -> bool {
    name.ends_with(".dist-info") || name.ends_with(".egg-info")
}

/// Adds the distribution whose metadata directory is `dir`.
pub fn collect_dir<T: Tree + ?Sized>(tree: &T, dir: &str, out: &mut Collector) {
    let file = if dir.ends_with(".dist-info") {
        "METADATA"
    } else if dir.ends_with(".egg-info") {
        "PKG-INFO"
    } else {
        return;
    };
    let path = join(dir, file);
    let data = match read_bounded(tree, &path, MAX_METADATA_BYTES) {
        ReadOutcome::Data(data) => data,
        ReadOutcome::TooLarge => {
            out.not_inventoried("python_metadata_too_large");
            return;
        }
        ReadOutcome::Missing => return,
    };

    let text = String::from_utf8_lossy(&data);
    let mut name: Option<String> = None;
    let mut version: Option<String> = None;
    // The header block ends at the first empty line; the rest of the file is
    // the long description and may contain anything.
    for line in text.lines() {
        if line.trim().is_empty() {
            break;
        }
        // A continuation line belongs to the field above it.
        if line.starts_with(' ') || line.starts_with('\t') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        if key.trim().eq_ignore_ascii_case("Name") {
            if name.is_none() {
                name = Some(value.to_owned());
            }
        } else if key.trim().eq_ignore_ascii_case("Version") && version.is_none() {
            version = Some(value.to_owned());
        }
    }

    let (Some(name), Some(version)) = (name, version) else {
        return;
    };
    let package_purl = purl("pypi", &[], &normalize(&name), &version, &[]);
    out.push(InventoryPackage {
        ecosystem: "PyPI".to_owned(),
        name,
        source_name: None,
        version,
        purl: package_purl,
        path: Some(dir.to_owned()),
    });
}

/// The PEP 503 normalized name for the purl: lowercase, runs of `-`, `_` and
/// `.` folded into one `-`.
fn normalize(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut separator = false;
    for c in name.chars() {
        if matches!(c, '-' | '_' | '.') {
            if !separator {
                out.push('-');
                separator = true;
            }
        } else {
            for lower in c.to_lowercase() {
                out.push(lower);
            }
            separator = false;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::testing::MemTree;

    const DIST_INFO: &str = "usr/lib/python3.12/site-packages/Typing_Extensions-4.12.2.dist-info";
    const EGG_INFO: &str = "usr/lib/python3.11/site-packages/legacy.Package-0.1.egg-info";

    fn collected(tree: &MemTree, dir: &str) -> Collector {
        let mut out = Collector::new();
        collect_dir(tree, dir, &mut out);
        out
    }

    #[test]
    fn recognises_metadata_directories() {
        assert!(is_metadata_dir("flask-3.0.0.dist-info"));
        assert!(is_metadata_dir("legacy-0.1.egg-info"));
        assert!(!is_metadata_dir("flask"));
        assert!(!is_metadata_dir("dist-info"));
        assert!(!is_metadata_dir("flask-3.0.0.dist-info.bak"));
        assert!(is_metadata_dir(".dist-info"));
    }

    #[test]
    fn reads_a_dist_info_metadata_header() {
        let tree = MemTree::new().file(
            &join(DIST_INFO, "METADATA"),
            "Metadata-Version: 2.1\n\
             Name: Typing_Extensions\n\
             Version: 4.12.2\n\
             Summary: Backported and Experimental Type Hints\n\
             \n\
             The body starts here, Name: not a header\n"
                .as_bytes(),
        );
        let out = collected(&tree, DIST_INFO);
        assert_eq!(out.len(), 1);
        let package = &out.packages()[0];
        assert_eq!(package.ecosystem, "PyPI");
        assert_eq!(package.name, "Typing_Extensions");
        assert_eq!(package.version, "4.12.2");
        assert_eq!(package.purl, "pkg:pypi/typing-extensions@4.12.2");
        assert_eq!(package.path.as_deref(), Some(DIST_INFO));
    }

    #[test]
    fn reads_an_egg_info_pkg_info() {
        let tree = MemTree::new().file(
            &join(EGG_INFO, "PKG-INFO"),
            b"Metadata-Version: 1.0\nName: legacy.Package\nVersion: 0.1\n",
        );
        let out = collected(&tree, EGG_INFO);
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].name, "legacy.Package");
        assert_eq!(out.packages()[0].purl, "pkg:pypi/legacy-package@0.1");
    }

    #[test]
    fn the_header_stops_at_the_first_blank_line() {
        let tree = MemTree::new().file(
            &join(DIST_INFO, "METADATA"),
            b"Name: flask\nVersion: 3.0.0\n\nName: evil\nVersion: 9.9.9\n",
        );
        let out = collected(&tree, DIST_INFO);
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].name, "flask");
        assert_eq!(out.packages()[0].version, "3.0.0");
    }

    #[test]
    fn normalize_folds_runs_of_separators() {
        assert_eq!(normalize("Typing_Extensions"), "typing-extensions");
        assert_eq!(normalize("Foo..__--Bar"), "foo-bar");
        assert_eq!(normalize("zope.interface"), "zope-interface");
        assert_eq!(normalize("A"), "a");
        assert_eq!(normalize(""), "");
    }

    #[test]
    fn missing_or_header_less_metadata_yields_nothing() {
        let tree = MemTree::new();
        assert!(collected(&tree, DIST_INFO).is_empty());

        let tree = MemTree::new().file(&join(DIST_INFO, "METADATA"), b"Summary: no name\n");
        assert!(collected(&tree, DIST_INFO).is_empty());

        let tree = MemTree::new().file(&join(DIST_INFO, "METADATA"), b"Name: only\n");
        assert!(collected(&tree, DIST_INFO).is_empty());
    }

    #[test]
    fn malformed_and_invalid_utf8_metadata_does_not_panic() {
        let tree = MemTree::new().file(&join(DIST_INFO, "METADATA"), vec![0xffu8; 512]);
        assert!(collected(&tree, DIST_INFO).is_empty());

        let tree = MemTree::new().file(
            &join(DIST_INFO, "METADATA"),
            b"Name: caf\xe9\nVersion: 1.0\xff\n",
        );
        let out = collected(&tree, DIST_INFO);
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].version, "1.0\u{FFFD}");

        // Truncated with no trailing newline.
        let tree = MemTree::new().file(&join(DIST_INFO, "METADATA"), b"Name: flask\nVersion: 3.0");
        assert_eq!(collected(&tree, DIST_INFO).len(), 1);
    }

    #[test]
    fn an_egg_info_directory_without_pkg_info_yields_nothing() {
        let tree = MemTree::new().file(&join(EGG_INFO, "METADATA"), b"Name: x\nVersion: 1\n");
        assert!(collected(&tree, EGG_INFO).is_empty());
    }

    #[test]
    fn oversized_metadata_is_named_and_skipped() {
        let tree = MemTree::new().file(
            &join(DIST_INFO, "METADATA"),
            vec![b'a'; MAX_METADATA_BYTES + 1],
        );
        let out = collected(&tree, DIST_INFO);
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec!["python_metadata_too_large".to_owned()]);
    }
}
