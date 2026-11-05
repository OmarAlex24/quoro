// SPDX-License-Identifier: AGPL-3.0-only
//! The `rpm` package database: `rpmdb.sqlite` (the SQLite backend of rpm 4.16
//! and later), read through the bounded reader in [`super::sqlite`].
//!
//! The BDB (`Packages`) and NDB (`Packages.db`) backends are recognised and
//! named in `not_inventoried`, not read.

use super::os_release::OsRelease;
use super::sqlite::{Db, Value};
use super::{Collector, InventoryPackage, Tree, purl};

/// Where the SQLite database lives, first found wins.
const DB_PATHS: [&str; 2] = [
    "var/lib/rpm/rpmdb.sqlite",
    "usr/lib/sysimage/rpm/rpmdb.sqlite",
];
/// Largest database read; a bigger one is left alone.
const MAX_DB_BYTES: u64 = 1 << 30;
/// Header index entries and data bytes an `rpm` header may declare.
const MAX_INDEX_ENTRIES: u32 = 100_000;
const MAX_HEADER_DATA: u32 = 64 << 20;

/// `not_inventoried` kinds of this module.
const MALFORMED: &str = "rpm_sqlite_malformed";
const WAL: &str = "rpm_sqlite_wal";
const BDB: &str = "rpm_bdb";
const NDB: &str = "rpm_ndb";

/// rpm header tags the inventory reads.
const TAG_NAME: u32 = 1000;
const TAG_VERSION: u32 = 1001;
const TAG_RELEASE: u32 = 1002;
const TAG_EPOCH: u32 = 1003;
const TAG_ARCH: u32 = 1022;
const TAG_SOURCERPM: u32 = 1044;
/// rpm header types.
const TYPE_INT32: u32 = 4;
const TYPE_STRING: u32 = 6;
const TYPE_I18NSTRING: u32 = 9;

/// The fields of one rpm header the inventory needs.
#[derive(Debug, PartialEq, Eq)]
struct Meta {
    name: String,
    version: String,
    release: Option<String>,
    epoch: Option<i32>,
    arch: Option<String>,
    sourcerpm: Option<String>,
}

/// Adds the packages of the image's `rpm` database, if it has one.
pub fn collect<T: Tree + ?Sized>(tree: &T, os: Option<&OsRelease>, out: &mut Collector) {
    let mut found = None;
    for path in DB_PATHS {
        if let Some(file) = tree.open(path) {
            found = Some((path, file));
            break;
        }
    }
    let Some((path, file)) = found else {
        report_legacy(tree, out);
        return;
    };
    if file.len > MAX_DB_BYTES {
        out.not_inventoried("rpm_too_large");
        return;
    }
    // Images are checkpointed; a leftover WAL means rows may be missing.
    if let Some(wal) = tree.open(&format!("{path}-wal"))
        && wal.len > 0
    {
        out.not_inventoried(WAL);
    }
    let Some(mut db) = Db::open(file) else {
        out.not_inventoried(MALFORMED);
        return;
    };
    let Some(root) = db.table_root("Packages") else {
        out.not_inventoried(MALFORMED);
        return;
    };
    let ecosystem = ecosystem(os);
    out.cover(&ecosystem);
    let namespace = os.map(|os| os.id.as_str()).unwrap_or("rpm");
    let distro = match os {
        Some(os) => match &os.version_id {
            Some(version) => format!("{}-{version}", os.id),
            None => os.id.clone(),
        },
        None => String::new(),
    };
    db.for_each_row(root, |values| {
        // The hnum column is the INTEGER PRIMARY KEY and is stored as NULL,
        // but the blob is the only BLOB either way.
        let Some(blob) = values.iter().find_map(|value| match value {
            Value::Blob(blob) => Some(blob.as_slice()),
            _ => None,
        }) else {
            return true;
        };
        let Some(meta) = parse_header(blob) else {
            out.not_inventoried(MALFORMED);
            return true;
        };
        let Some(package) = package(&meta, &ecosystem, namespace, &distro) else {
            return true;
        };
        out.push(package)
    });
}

/// Names an rpm database from the pre-SQLite backends, if one is present.
fn report_legacy<T: Tree + ?Sized>(tree: &T, out: &mut Collector) {
    if tree.open("var/lib/rpm/Packages").is_some() {
        out.not_inventoried(BDB);
    }
    if tree.open("usr/lib/sysimage/rpm/Packages.db").is_some()
        || tree.open("var/lib/rpm/Packages.db").is_some()
    {
        out.not_inventoried(NDB);
    }
}

/// The OSV ecosystem for this image's packages.
fn ecosystem(os: Option<&OsRelease>) -> String {
    let Some(os) = os else {
        return "RPM".to_owned();
    };
    let version = os.version_id.as_deref().unwrap_or("");
    match os.id.as_str() {
        "rhel" | "centos" => "Red Hat".to_owned(),
        "rocky" => with_major("Rocky Linux", version),
        "almalinux" => with_major("AlmaLinux", version),
        "opensuse-leap" => with_version("openSUSE:Leap", version),
        "sles" => with_version("SUSE:Linux Enterprise Server", version),
        "" => "RPM".to_owned(),
        id => capitalize(id),
    }
}

/// `Name:<major>` from a version id, or just `Name` when there is none.
fn with_major(name: &str, version: &str) -> String {
    match version.split('.').next().filter(|major| !major.is_empty()) {
        Some(major) => format!("{name}:{major}"),
        None => name.to_owned(),
    }
}

/// `Name <version>` or just `Name`.
fn with_version(name: &str, version: &str) -> String {
    if version.is_empty() {
        name.to_owned()
    } else {
        format!("{name} {version}")
    }
}

/// The id with its first letter upper-cased, as the ecosystem names are.
fn capitalize(id: &str) -> String {
    let mut chars = id.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => "RPM".to_owned(),
    }
}

/// Turns one header into a package. `None` for a header without the fields a
/// package needs, and for the `gpg-pubkey` pseudo-packages.
fn package(
    meta: &Meta,
    ecosystem: &str,
    namespace: &str,
    distro: &str,
) -> Option<InventoryPackage> {
    if meta.name == "gpg-pubkey" || meta.name.is_empty() || meta.version.is_empty() {
        return None;
    }
    let mut version = String::new();
    match meta.epoch {
        Some(epoch) if epoch > 0 => {
            version.push_str(&epoch.to_string());
            version.push(':');
        }
        _ => {}
    }
    version.push_str(&meta.version);
    if let Some(release) = meta
        .release
        .as_deref()
        .filter(|release| !release.is_empty())
    {
        version.push('-');
        version.push_str(release);
    }
    let arch = meta.arch.clone().unwrap_or_default();
    let epoch = match meta.epoch {
        Some(epoch) if epoch > 0 => epoch.to_string(),
        _ => String::new(),
    };
    let upstream = meta.sourcerpm.clone().unwrap_or_default();
    let purl = purl(
        "rpm",
        &[namespace],
        &meta.name,
        &version,
        &[
            ("arch", &arch),
            ("distro", distro),
            ("epoch", &epoch),
            ("upstream", &upstream),
        ],
    );
    Some(InventoryPackage {
        ecosystem: ecosystem.to_owned(),
        name: meta.name.clone(),
        source_name: meta.sourcerpm.as_deref().and_then(source_name),
        version,
        purl,
        path: None,
    })
}

/// The source package name of a `SOURCERPM` value: its `.src.rpm` (or
/// `.nosrc.rpm`) suffix and the version and release parts are removed.
fn source_name(sourcerpm: &str) -> Option<String> {
    let base = sourcerpm
        .strip_suffix(".src.rpm")
        .or_else(|| sourcerpm.strip_suffix(".nosrc.rpm"))?;
    let mut parts = base.rsplitn(3, '-');
    let _release = parts.next()?;
    let _version = parts.next()?;
    let name = parts.next()?;
    (!name.is_empty()).then(|| name.to_owned())
}

/// Parses an rpmdb header blob: `il` index entries, then `dl` bytes of data.
fn parse_header(blob: &[u8]) -> Option<Meta> {
    let il = read_u32(blob, 0)?;
    let dl = read_u32(blob, 4)?;
    if il > MAX_INDEX_ENTRIES || dl > MAX_HEADER_DATA {
        return None;
    }
    let index_bytes = usize::try_from(il).ok()?.checked_mul(16)?;
    let data_start = 8usize.checked_add(index_bytes)?;
    let data_end = data_start.checked_add(usize::try_from(dl).ok()?)?;
    if data_end > blob.len() {
        return None;
    }
    let data = blob.get(data_start..data_end)?;
    let mut entries: Vec<Entry> = Vec::with_capacity(usize::try_from(il).ok()?.min(1024));
    for index in 0..il {
        let at = 8usize.checked_add(usize::try_from(index).ok()?.checked_mul(16)?)?;
        let tag = read_u32(blob, at)?;
        let kind = read_u32(blob, at.checked_add(4)?)?;
        // The offset is signed in the spec and must not be negative.
        let offset = read_u32(blob, at.checked_add(8)?)?;
        let count = read_u32(blob, at.checked_add(12)?)?;
        if offset > i32::MAX as u32 {
            continue;
        }
        entries.push(Entry {
            tag,
            kind,
            offset,
            count,
        });
    }
    let name = string_tag(&entries, data, TAG_NAME)?;
    let version = string_tag(&entries, data, TAG_VERSION)?;
    let release = string_tag(&entries, data, TAG_RELEASE);
    let epoch = int_tag(&entries, data, TAG_EPOCH);
    let arch = string_tag(&entries, data, TAG_ARCH);
    let sourcerpm = string_tag(&entries, data, TAG_SOURCERPM);
    Some(Meta {
        name,
        version,
        release,
        epoch,
        arch,
        sourcerpm,
    })
}

/// One header index entry.
struct Entry {
    tag: u32,
    kind: u32,
    offset: u32,
    count: u32,
}

/// A `STRING` (or the first string of an `I18NSTRING`) tag.
fn string_tag(entries: &[Entry], data: &[u8], tag: u32) -> Option<String> {
    let entry = entries.iter().find(|entry| {
        entry.tag == tag && (entry.kind == TYPE_STRING || entry.kind == TYPE_I18NSTRING)
    })?;
    let start = usize::try_from(entry.offset).ok()?;
    let rest = data.get(start..)?;
    // The string must end before the end of the data area.
    let nul = rest.iter().position(|&byte| byte == 0)?;
    String::from_utf8(rest.get(..nul)?.to_vec()).ok()
}

/// An `INT32` tag's first value.
fn int_tag(entries: &[Entry], data: &[u8], tag: u32) -> Option<i32> {
    let entry = entries
        .iter()
        .find(|entry| entry.tag == tag && entry.kind == TYPE_INT32 && entry.count >= 1)?;
    let start = usize::try_from(entry.offset).ok()?;
    let end = start.checked_add(4)?;
    let bytes: [u8; 4] = data.get(start..end)?.try_into().ok()?;
    Some(i32::from_be_bytes(bytes))
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    let end = at.checked_add(4)?;
    let slice = bytes.get(at..end)?;
    Some(u32::from_be_bytes(slice.try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::super::testing::MemTree;
    use super::*;

    const DB_1024: &[u8] = include_bytes!("testdata/rpmdb-1024.sqlite");
    const DB_4096: &[u8] = include_bytes!("testdata/rpmdb-4096.sqlite");

    fn os(id: &str, version: Option<&str>) -> OsRelease {
        OsRelease {
            id: id.to_owned(),
            version_id: version.map(str::to_owned),
            id_like: Vec::new(),
            version_codename: None,
        }
    }

    /// Builds an rpmdb header blob: `il`, `dl`, the index entries, the data.
    fn header(entries: &[(u32, u32, u32, u32)], data: &[u8]) -> Vec<u8> {
        let mut blob = Vec::new();
        blob.extend_from_slice(&u32::try_from(entries.len()).unwrap_or(0).to_be_bytes());
        blob.extend_from_slice(&u32::try_from(data.len()).unwrap_or(0).to_be_bytes());
        for entry in entries {
            for field in [entry.0, entry.1, entry.2, entry.3] {
                blob.extend_from_slice(&field.to_be_bytes());
            }
        }
        blob.extend_from_slice(data);
        blob
    }

    /// A header with the fields the inventory reads; `extra` is appended as a
    /// `STRING` under tag 1005 so a test can force an overflow chain.
    fn package_header(
        name: &str,
        version: &str,
        release: &str,
        epoch: Option<i32>,
        arch: &str,
        sourcerpm: Option<&str>,
        extra: Option<&str>,
    ) -> Vec<u8> {
        /// Appends one NUL-terminated string and its index entry.
        fn add_string(
            entries: &mut Vec<(u32, u32, u32, u32)>,
            data: &mut Vec<u8>,
            tag: u32,
            value: &str,
        ) {
            let offset = u32::try_from(data.len()).unwrap_or(0);
            data.extend_from_slice(value.as_bytes());
            data.push(0);
            entries.push((tag, TYPE_STRING, offset, 1));
        }

        let mut data = Vec::new();
        let mut entries: Vec<(u32, u32, u32, u32)> = Vec::new();
        add_string(&mut entries, &mut data, TAG_NAME, name);
        add_string(&mut entries, &mut data, TAG_VERSION, version);
        add_string(&mut entries, &mut data, TAG_RELEASE, release);
        if let Some(epoch) = epoch {
            let offset = u32::try_from(data.len()).unwrap_or(0);
            data.extend_from_slice(&epoch.to_be_bytes());
            entries.push((TAG_EPOCH, TYPE_INT32, offset, 1));
        }
        add_string(&mut entries, &mut data, TAG_ARCH, arch);
        if let Some(sourcerpm) = sourcerpm {
            add_string(&mut entries, &mut data, TAG_SOURCERPM, sourcerpm);
        }
        if let Some(extra) = extra {
            add_string(&mut entries, &mut data, 1005, extra);
        }
        header(&entries, &data)
    }

    fn collect_from(tree: &MemTree, os: Option<&OsRelease>) -> Collector {
        let mut out = Collector::new();
        collect(tree, os, &mut out);
        out
    }

    #[test]
    fn reads_the_sqlite_fixture() {
        let tree = MemTree::new().file("var/lib/rpm/rpmdb.sqlite", DB_1024.to_vec());
        let out = collect_from(&tree, Some(&os("rocky", Some("9.4"))));
        assert_eq!(out.len(), 301);
        assert_eq!(out.kinds(), Vec::<String>::new());
        let epochy = out
            .packages()
            .iter()
            .find(|pkg| pkg.name == "epochy")
            .expect("the fixture's epoch package");
        assert_eq!(epochy.version, "1:2.0-3");
        assert_eq!(epochy.source_name.as_deref(), Some("epochy"));
        assert_eq!(
            epochy.purl,
            "pkg:rpm/rocky/epochy@1%3A2.0-3?arch=x86_64&distro=rocky-9.4&epoch=1&upstream=epochy-2.0-3.el9.src.rpm"
        );
        assert!(
            out.packages()
                .iter()
                .all(|pkg| pkg.ecosystem == "Rocky Linux:9")
        );
    }

    #[test]
    fn reads_the_page_4096_fixture_and_skips_gpg_pubkey() {
        let tree = MemTree::new().file("usr/lib/sysimage/rpm/rpmdb.sqlite", DB_4096.to_vec());
        let out = collect_from(&tree, Some(&os("fedora", Some("42"))));
        assert_eq!(out.len(), 59, "60 rows less the gpg-pubkey row");
        assert!(out.packages().iter().all(|pkg| pkg.name != "gpg-pubkey"));
        assert!(out.packages().iter().all(|pkg| pkg.ecosystem == "Fedora"));
    }

    #[test]
    fn a_malformed_database_is_named_and_yields_nothing() {
        let tree = MemTree::new().file("var/lib/rpm/rpmdb.sqlite", vec![0u8; 4096]);
        let out = collect_from(&tree, None);
        assert!(out.is_empty());
        assert_eq!(out.kinds(), vec![MALFORMED.to_owned()]);
    }

    #[test]
    fn a_database_that_is_not_sqlite_is_malformed() {
        let tree = MemTree::new().file("var/lib/rpm/rpmdb.sqlite", b"not a database".to_vec());
        let out = collect_from(&tree, None);
        assert!(out.is_empty());
        assert_eq!(out.kinds(), vec![MALFORMED.to_owned()]);
    }

    #[test]
    fn a_nonempty_wal_is_named() {
        let tree = MemTree::new()
            .file("var/lib/rpm/rpmdb.sqlite", DB_4096.to_vec())
            .file("var/lib/rpm/rpmdb.sqlite-wal", vec![1u8, 2, 3]);
        let out = collect_from(&tree, None);
        assert_eq!(out.kinds(), vec![WAL.to_owned()]);
        assert_eq!(out.len(), 59);
    }

    #[test]
    fn legacy_backends_are_named_not_read() {
        let tree = MemTree::new().file("var/lib/rpm/Packages", vec![0u8; 8]);
        let out = collect_from(&tree, None);
        assert!(out.is_empty());
        assert_eq!(out.kinds(), vec![BDB.to_owned()]);

        let tree = MemTree::new().file("usr/lib/sysimage/rpm/Packages.db", vec![0u8; 8]);
        let out = collect_from(&tree, None);
        assert_eq!(out.kinds(), vec![NDB.to_owned()]);

        let tree = MemTree::new().file("var/lib/rpm/Packages.db", vec![0u8; 8]);
        let out = collect_from(&tree, None);
        assert_eq!(out.kinds(), vec![NDB.to_owned()]);
    }

    #[test]
    fn the_sqlite_database_wins_over_a_legacy_one() {
        let tree = MemTree::new()
            .file("var/lib/rpm/rpmdb.sqlite", DB_4096.to_vec())
            .file("var/lib/rpm/Packages", vec![0u8; 8]);
        let out = collect_from(&tree, None);
        assert_eq!(out.len(), 59);
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn epoch_zero_is_left_out_and_a_positive_epoch_is_kept() {
        let blob = package_header("a", "1.0", "2", Some(0), "noarch", None, None);
        let meta = parse_header(&blob).expect("header parses");
        let pkg = package(&meta, "RPM", "rpm", "").expect("package");
        assert_eq!(pkg.version, "1.0-2");
        assert!(!pkg.purl.contains("epoch="));

        let blob = package_header("a", "1.0", "2", Some(3), "noarch", None, None);
        let meta = parse_header(&blob).expect("header parses");
        let pkg = package(&meta, "RPM", "rpm", "").expect("package");
        assert_eq!(pkg.version, "3:1.0-2");
        assert!(pkg.purl.contains("epoch=3"));
    }

    #[test]
    fn gpg_pubkey_is_skipped() {
        let blob = package_header(
            "gpg-pubkey",
            "d4082792",
            "5b32db75",
            None,
            "noarch",
            None,
            None,
        );
        let meta = parse_header(&blob).expect("header parses");
        assert_eq!(package(&meta, "RPM", "rpm", ""), None);
    }

    #[test]
    fn sourcerpm_loses_its_suffix_version_and_release() {
        assert_eq!(
            source_name("openssl-3.2.2-1.el9.src.rpm").as_deref(),
            Some("openssl")
        );
        assert_eq!(
            source_name("glibc-2.34-100.el9.nosrc.rpm").as_deref(),
            Some("glibc")
        );
        assert_eq!(source_name("not-a-source-rpm"), None);
        assert_eq!(source_name("short.src.rpm").as_deref(), None);
    }

    #[test]
    fn a_header_whose_index_overflows_the_blob_is_rejected() {
        // il = 100_000 entries (the cap) but only eight bytes of file.
        let mut blob = Vec::new();
        blob.extend_from_slice(&MAX_INDEX_ENTRIES.to_be_bytes());
        blob.extend_from_slice(&0u32.to_be_bytes());
        assert_eq!(parse_header(&blob), None);

        // il over the cap.
        let mut blob = Vec::new();
        blob.extend_from_slice(&(MAX_INDEX_ENTRIES + 1).to_be_bytes());
        blob.extend_from_slice(&0u32.to_be_bytes());
        blob.extend_from_slice(&[0u8; 64]);
        assert_eq!(parse_header(&blob), None);

        // dl over the cap.
        let mut blob = Vec::new();
        blob.extend_from_slice(&0u32.to_be_bytes());
        blob.extend_from_slice(&(MAX_HEADER_DATA + 1).to_be_bytes());
        assert_eq!(parse_header(&blob), None);

        // A truncated blob.
        assert_eq!(parse_header(&[0, 0, 0, 1]), None);
    }

    #[test]
    fn a_negative_index_offset_is_ignored_and_a_string_without_nul_is_rejected() {
        let mut data = Vec::new();
        data.extend_from_slice(b"name-without-nul");
        let blob = header(&[(TAG_NAME, TYPE_STRING, 0, 1)], &data);
        assert_eq!(parse_header(&blob), None);

        // A negative offset (0x8000_0000 read as i32) cannot address data.
        let mut data = Vec::new();
        data.extend_from_slice(b"ok\0");
        let blob = header(
            &[
                (TAG_NAME, TYPE_STRING, 0x8000_0000, 1),
                (TAG_VERSION, TYPE_STRING, 0, 1),
            ],
            &data,
        );
        assert_eq!(parse_header(&blob), None);

        // A string that starts past the end of the data area.
        let entries = [Entry {
            tag: TAG_NAME,
            kind: TYPE_STRING,
            offset: 4,
            count: 1,
        }];
        assert_eq!(string_tag(&entries, b"a\0", TAG_NAME), None);
    }

    #[test]
    fn an_i18n_string_uses_its_first_string() {
        let mut data = Vec::new();
        data.extend_from_slice(b"first\0second\0");
        let version_offset = 6u32;
        let blob = header(
            &[
                (TAG_NAME, TYPE_I18NSTRING, 0, 2),
                (TAG_VERSION, TYPE_STRING, version_offset, 1),
            ],
            &data,
        );
        let meta = parse_header(&blob).expect("i18n name parses");
        assert_eq!(meta.name, "first");
        assert_eq!(meta.version, "second");
    }

    #[test]
    fn ecosystem_names_follow_the_os_release() {
        assert_eq!(ecosystem(None), "RPM");
        assert_eq!(ecosystem(Some(&os("rhel", Some("9.4")))), "Red Hat");
        assert_eq!(ecosystem(Some(&os("centos", None))), "Red Hat");
        assert_eq!(ecosystem(Some(&os("rocky", Some("8.10")))), "Rocky Linux:8");
        assert_eq!(
            ecosystem(Some(&os("almalinux", Some("9.3")))),
            "AlmaLinux:9"
        );
        assert_eq!(
            ecosystem(Some(&os("opensuse-leap", Some("15.5")))),
            "openSUSE:Leap 15.5"
        );
        assert_eq!(
            ecosystem(Some(&os("sles", Some("15.4")))),
            "SUSE:Linux Enterprise Server 15.4"
        );
        assert_eq!(ecosystem(Some(&os("fedora", Some("42")))), "Fedora");
        assert_eq!(ecosystem(Some(&os("", None))), "RPM");
    }

    #[test]
    fn the_purl_namespace_is_the_os_id() {
        let blob = package_header(
            "zlib",
            "1.2.13",
            "4.el9",
            None,
            "x86_64",
            Some("zlib-1.2.13-4.el9.src.rpm"),
            None,
        );
        let meta = parse_header(&blob).expect("header parses");
        let pkg = package(&meta, "Red Hat", "rhel", "rhel-9.4").expect("package");
        assert_eq!(pkg.version, "1.2.13-4.el9");
        assert_eq!(pkg.source_name.as_deref(), Some("zlib"));
        assert_eq!(
            pkg.purl,
            "pkg:rpm/rhel/zlib@1.2.13-4.el9?arch=x86_64&distro=rhel-9.4&upstream=zlib-1.2.13-4.el9.src.rpm"
        );
    }
}
