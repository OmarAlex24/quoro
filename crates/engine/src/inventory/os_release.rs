// SPDX-License-Identifier: AGPL-3.0-only
//! `os-release` (`etc/os-release`, then `usr/lib/os-release`).

use super::{ReadOutcome, Tree, read_bounded};

/// Largest file read: a real os-release is a few hundred bytes.
const MAX_OS_RELEASE_BYTES: usize = 64 * 1024;

/// What the inventory needs from os-release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OsRelease {
    /// `ID`, lowercase, for example `debian`, `alpine`, `rhel`.
    pub id: String,
    /// `VERSION_ID`, for example `13`, `3.22.1`, `9.4`.
    pub version_id: Option<String>,
    /// `ID_LIKE`, split on whitespace.
    pub id_like: Vec<String>,
    /// `VERSION_CODENAME`, for example `trixie`.
    pub version_codename: Option<String>,
}

/// Reads the image's os-release; `None` when it has none or it carries no `ID`.
pub fn read<T: Tree + ?Sized>(tree: &T) -> Option<OsRelease> {
    let data = match read_bounded(tree, "etc/os-release", MAX_OS_RELEASE_BYTES) {
        ReadOutcome::Data(data) => data,
        ReadOutcome::TooLarge | ReadOutcome::Missing => {
            match read_bounded(tree, "usr/lib/os-release", MAX_OS_RELEASE_BYTES) {
                ReadOutcome::Data(data) => data,
                ReadOutcome::TooLarge | ReadOutcome::Missing => return None,
            }
        }
    };
    parse(&data)
}

/// The leading run of digits of a version: `13` from `13`, `3` from
/// `3.22.1`, `""` from something that does not start with a digit.
pub fn major(version: &str) -> &str {
    let end = version
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(version.len());
    version.get(..end).unwrap_or("")
}

/// Parses an os-release file. A missing or malformed `ID` gives `None`.
fn parse(data: &[u8]) -> Option<OsRelease> {
    // An os-release is small and text; invalid UTF-8 becomes U+FFFD, which
    // the `ID` check below then treats as an ordinary character.
    let text = String::from_utf8_lossy(data);
    let mut id: Option<String> = None;
    let mut version_id: Option<String> = None;
    let mut id_like: Vec<String> = Vec::new();
    let mut version_codename: Option<String> = None;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let Some(value) = parse_value(raw) else {
            continue;
        };
        match key {
            "ID" => {
                let value = value.trim().to_lowercase();
                if !value.is_empty() {
                    id = Some(value);
                }
            }
            "VERSION_ID" => {
                let value = value.trim();
                if !value.is_empty() {
                    version_id = Some(value.to_owned());
                }
            }
            "ID_LIKE" => {
                id_like = value
                    .split_whitespace()
                    .map(str::to_lowercase)
                    .collect::<Vec<_>>();
            }
            "VERSION_CODENAME" => {
                let value = value.trim();
                if !value.is_empty() {
                    version_codename = Some(value.to_owned());
                }
            }
            _ => {}
        }
    }
    Some(OsRelease {
        id: id?,
        version_id,
        id_like,
        version_codename,
    })
}

/// The value of a `KEY=VALUE` line: quoted with `'` or `"`, or bare. Backslash
/// escapes are resolved only inside double quotes. `None` when a quote is
/// opened and never closed.
fn parse_value(raw: &str) -> Option<String> {
    let raw = raw.trim_end();
    if let Some(inner) = raw.strip_prefix('"') {
        // `strip_suffix` fails when the opening quote was never closed.
        let inner = inner.strip_suffix('"')?;
        return Some(unescape(inner));
    }
    if let Some(inner) = raw.strip_prefix('\'') {
        let inner = inner.strip_suffix('\'')?;
        return Some(inner.to_owned());
    }
    Some(raw.trim().to_owned())
}

/// Resolves the escapes an os-release double-quoted value may use; an
/// unknown escape keeps its backslash.
fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('\\') => out.push('\\'),
            Some('"') => out.push('"'),
            Some('\'') => out.push('\''),
            Some('$') => out.push('$'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::super::testing::MemTree;
    use super::*;

    fn tree_of(text: &str) -> MemTree {
        MemTree::new().file("etc/os-release", text.as_bytes())
    }

    #[test]
    fn reads_a_typical_os_release() {
        let tree = tree_of(
            "# This file is generated by the image build.\n\
             NAME=\"Debian GNU/Linux\"\n\
             VERSION=\"13 (trixie)\"\n\
             ID=debian\n\
             VERSION_ID=\"13\"\n\
             VERSION_CODENAME=trixie\n\
             ID_LIKE=\"debian\"\n\
             \n\
             this line has no equals sign\n\
             =no-key\n",
        );
        assert_eq!(
            read(&tree),
            Some(OsRelease {
                id: "debian".into(),
                version_id: Some("13".into()),
                id_like: vec!["debian".into()],
                version_codename: Some("trixie".into()),
            })
        );
    }

    #[test]
    fn reads_single_quotes_and_escapes() {
        let tree = tree_of(
            "ID='Ubuntu'\n\
             VERSION_ID=\"24.04\"\n\
             ID_LIKE=\"debian   ubuntu\"\n\
             VERSION_CODENAME=\"no\\\"ble\"\n",
        );
        let release = read(&tree).expect("os-release");
        assert_eq!(release.id, "ubuntu");
        assert_eq!(release.version_id.as_deref(), Some("24.04"));
        assert_eq!(release.id_like, vec!["debian", "ubuntu"]);
        assert_eq!(release.version_codename.as_deref(), Some("no\"ble"));
    }

    #[test]
    fn falls_back_to_usr_lib() {
        let tree = MemTree::new().file("usr/lib/os-release", b"ID=alpine\nVERSION_ID=3.22.1\n");
        let release = read(&tree).expect("os-release");
        assert_eq!(release.id, "alpine");
        assert_eq!(release.version_id.as_deref(), Some("3.22.1"));

        // etc/os-release that is over the bound must not stop the fallback.
        let tree = MemTree::new()
            .file("etc/os-release", vec![b'a'; MAX_OS_RELEASE_BYTES + 1])
            .file("usr/lib/os-release", b"ID=alpine\n");
        assert_eq!(read(&tree).map(|os| os.id), Some("alpine".into()));
    }

    #[test]
    fn missing_or_id_less_files_are_none() {
        assert_eq!(read(&MemTree::new()), None);
        assert_eq!(read(&tree_of("NAME=no-id-here\nVERSION_ID=1\n")), None);
        assert_eq!(read(&tree_of("ID=\"unterminated\n")), None);
        assert_eq!(read(&tree_of("ID=\n")), None);
    }

    #[test]
    fn oversized_and_invalid_utf8_do_not_panic() {
        let too_big = MemTree::new().file("etc/os-release", vec![b'x'; MAX_OS_RELEASE_BYTES + 1]);
        assert_eq!(read(&too_big), None);

        let garbage = MemTree::new().file("etc/os-release", vec![0xff, 0xfe, 0x00, 0x81]);
        assert_eq!(read(&garbage), None);

        let invalid = MemTree::new().file("etc/os-release", b"ID=deb\xffin\nVERSION_ID=13\n");
        let release = read(&invalid).expect("os-release");
        assert!(release.id.starts_with("deb"));
        assert_eq!(release.version_id.as_deref(), Some("13"));

        // Truncated: no trailing newline, still read.
        let truncated = MemTree::new().file("etc/os-release", b"ID=debian\nVERSION_ID=1");
        assert_eq!(
            read(&truncated).and_then(|os| os.version_id),
            Some("1".into())
        );
    }

    #[test]
    fn major_takes_the_leading_digits() {
        assert_eq!(major("13"), "13");
        assert_eq!(major("3.22.1"), "3");
        assert_eq!(major("24.04"), "24");
        assert_eq!(major(""), "");
        assert_eq!(major("v1"), "");
        assert_eq!(major("13.0"), "13");
    }
}
