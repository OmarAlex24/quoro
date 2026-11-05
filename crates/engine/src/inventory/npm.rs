// SPDX-License-Identifier: AGPL-3.0-only
//! npm lockfiles (`package-lock.json`, `node_modules/.package-lock.json`).
//!
//! The engine builds on every platform, so this module carries its own small
//! JSON reader instead of depending on `serde_json` (which is a Linux-only
//! dependency of this crate). The reader is bounded in depth and in the
//! number of values, and turns any error into "this file is malformed".

use super::{Collector, ReadOutcome, Tree, purl, read_bounded};
use crate::InventoryPackage;

/// Largest lockfile read.
const MAX_LOCKFILE_BYTES: usize = 32 * 1024 * 1024;
/// Deepest JSON nesting accepted; a deeper file is malformed.
const MAX_JSON_DEPTH: usize = 64;
/// Most JSON values parsed from one lockfile; a bigger file is malformed.
const MAX_JSON_NODES: usize = 1_000_000;

/// A JSON value, as much of one as the lockfile reader needs.
#[allow(dead_code, reason = "only strings, objects and booleans are read back")]
enum Value {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Value>),
    Obj(Vec<(String, Value)>),
}

/// Whether the walk should read `path` as an npm lockfile.
pub fn is_lockfile(path: &str) -> bool {
    let parts: Vec<&str> = path.split('/').collect();
    let Some((name, dirs)) = parts.split_last() else {
        return false;
    };
    match *name {
        "package-lock.json" => !dirs.contains(&"node_modules"),
        // The hidden lockfile npm writes inside a `node_modules` directory.
        ".package-lock.json" => dirs.last() == Some(&"node_modules"),
        _ => false,
    }
}

/// Adds the packages of the lockfile at `path`.
pub fn collect_file<T: Tree + ?Sized>(tree: &T, path: &str, out: &mut Collector) {
    let data = match read_bounded(tree, path, MAX_LOCKFILE_BYTES) {
        ReadOutcome::Data(data) => data,
        ReadOutcome::TooLarge => {
            out.not_inventoried("npm_lockfile_too_large");
            return;
        }
        ReadOutcome::Missing => return,
    };
    let Some(Value::Obj(root)) = Parser::new(&data).parse() else {
        out.not_inventoried("npm_lockfile_malformed");
        return;
    };
    if let Some(Value::Obj(packages)) = field(&root, "packages") {
        let _ = collect_v2(packages, path, out);
    } else if let Some(Value::Obj(dependencies)) = field(&root, "dependencies") {
        let _ = collect_v1(dependencies, path, out);
    }
}

/// Lockfile v2/v3: a flat `packages` map keyed by the install path.
fn collect_v2(packages: &[(String, Value)], path: &str, out: &mut Collector) -> bool {
    for (key, value) in packages {
        let Value::Obj(fields) = value else {
            continue;
        };
        // The root project (key "") is the image's own application, not a
        // dependency, and a link is a symlink to another entry already here.
        if key.is_empty() || is_link(fields) {
            continue;
        }
        let name = str_field(fields, "name")
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .or_else(|| name_from_key(key));
        let Some(name) = name else {
            continue;
        };
        let Some(version) = str_field(fields, "version") else {
            continue;
        };
        if !registry_version(version) {
            continue;
        }
        let package_purl = npm_purl(&name, version);
        if !out.push(InventoryPackage {
            ecosystem: "npm".to_owned(),
            name,
            source_name: None,
            version: version.to_owned(),
            purl: package_purl,
            path: Some(path.to_owned()),
        }) {
            return false;
        }
    }
    true
}

/// Lockfile v1: nested `dependencies` maps, each entry with its own version.
fn collect_v1(dependencies: &[(String, Value)], path: &str, out: &mut Collector) -> bool {
    for (name, value) in dependencies {
        let Value::Obj(fields) = value else {
            continue;
        };
        if let Some(version) = str_field(fields, "version").filter(|v| registry_version(v)) {
            let package_purl = npm_purl(name, version);
            if !out.push(InventoryPackage {
                ecosystem: "npm".to_owned(),
                name: name.clone(),
                source_name: None,
                version: version.to_owned(),
                purl: package_purl,
                path: Some(path.to_owned()),
            }) {
                return false;
            }
        }
        if let Some(Value::Obj(nested)) = field(fields, "dependencies")
            && !collect_v1(nested, path, out)
        {
            return false;
        }
    }
    true
}

/// The package name encoded in a `packages` key: the text after the last
/// `node_modules/`, when the key names an installed package at all.
fn name_from_key(key: &str) -> Option<String> {
    let (_, rest) = key.rsplit_once("node_modules/")?;
    let rest = rest.trim_end_matches('/');
    (!rest.is_empty()).then(|| rest.to_owned())
}

/// A version a registry could have published: not a URL, a git reference or
/// a path.
fn registry_version(version: &str) -> bool {
    !version.is_empty() && !version.contains(':') && !version.contains('/')
}

/// The purl of an npm package; a scoped name splits into namespace and name.
fn npm_purl(name: &str, version: &str) -> String {
    if let Some((scope, rest)) = name.split_once('/')
        && scope.starts_with('@')
        && !rest.is_empty()
    {
        purl("npm", &[scope], rest, version, &[])
    } else {
        purl("npm", &[], name, version, &[])
    }
}

fn field<'a>(obj: &'a [(String, Value)], key: &str) -> Option<&'a Value> {
    obj.iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
}

fn str_field<'a>(obj: &'a [(String, Value)], key: &str) -> Option<&'a str> {
    match field(obj, key) {
        Some(Value::Str(value)) => Some(value),
        _ => None,
    }
}

fn is_link(obj: &[(String, Value)]) -> bool {
    matches!(field(obj, "link"), Some(Value::Bool(true)))
}

/// A recursive-descent JSON reader over one buffer.
struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
    nodes: usize,
}

impl<'a> Parser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            nodes: 0,
        }
    }

    /// Parses exactly one value spanning the whole buffer.
    fn parse(mut self) -> Option<Value> {
        self.skip_whitespace();
        let value = self.value(0)?;
        self.skip_whitespace();
        (self.pos == self.data.len()).then_some(value)
    }

    fn peek(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.pos += 1;
        Some(byte)
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn value(&mut self, depth: usize) -> Option<Value> {
        if depth > MAX_JSON_DEPTH {
            return None;
        }
        self.nodes = self.nodes.checked_add(1)?;
        if self.nodes > MAX_JSON_NODES {
            return None;
        }
        self.skip_whitespace();
        match self.peek()? {
            b'{' => self.object(depth),
            b'[' => self.array(depth),
            b'"' => Some(Value::Str(self.string()?)),
            b't' => {
                self.literal(b"true")?;
                Some(Value::Bool(true))
            }
            b'f' => {
                self.literal(b"false")?;
                Some(Value::Bool(false))
            }
            b'n' => {
                self.literal(b"null")?;
                Some(Value::Null)
            }
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    fn object(&mut self, depth: usize) -> Option<Value> {
        self.bump()?; // '{'
        let mut entries = Vec::new();
        self.skip_whitespace();
        if self.peek()? == b'}' {
            self.pos += 1;
            return Some(Value::Obj(entries));
        }
        loop {
            self.skip_whitespace();
            let key = self.string()?;
            self.skip_whitespace();
            if self.bump()? != b':' {
                return None;
            }
            let value = self.value(depth + 1)?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.bump()? {
                b',' => continue,
                b'}' => return Some(Value::Obj(entries)),
                _ => return None,
            }
        }
    }

    fn array(&mut self, depth: usize) -> Option<Value> {
        self.bump()?; // '['
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek()? == b']' {
            self.pos += 1;
            return Some(Value::Arr(items));
        }
        loop {
            let item = self.value(depth + 1)?;
            items.push(item);
            self.skip_whitespace();
            match self.bump()? {
                b',' => continue,
                b']' => return Some(Value::Arr(items)),
                _ => return None,
            }
        }
    }

    /// A double-quoted string. Raw bytes are kept as they are (invalid UTF-8
    /// becomes U+FFFD) and escapes are decoded, including `\u` surrogate
    /// pairs.
    fn string(&mut self) -> Option<String> {
        if self.bump()? != b'"' {
            return None;
        }
        let mut out: Vec<u8> = Vec::new();
        loop {
            match self.bump()? {
                b'"' => return Some(String::from_utf8_lossy(&out).into_owned()),
                b'\\' => match self.bump()? {
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'/' => out.push(b'/'),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'u' => {
                        let ch = self.unicode_escape()?;
                        let mut buffer = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buffer).as_bytes());
                    }
                    _ => return None,
                },
                byte if byte < 0x20 => return None,
                byte => out.push(byte),
            }
        }
    }

    /// The character of a `\uXXXX` escape, pairing surrogates.
    fn unicode_escape(&mut self) -> Option<char> {
        let first = self.hex4()?;
        if (0xD800..=0xDBFF).contains(&first) {
            if self.bump()? != b'\\' || self.bump()? != b'u' {
                return None;
            }
            let second = self.hex4()?;
            if !(0xDC00..=0xDFFF).contains(&second) {
                return None;
            }
            let scalar = 0x1_0000u32
                .checked_add(u32::from(first).checked_sub(0xD800)?.checked_shl(10)?)?
                .checked_add(u32::from(second).checked_sub(0xDC00)?)?;
            char::from_u32(scalar)
        } else if (0xDC00..=0xDFFF).contains(&first) {
            // A lone low surrogate is not a character.
            None
        } else {
            char::from_u32(u32::from(first))
        }
    }

    fn hex4(&mut self) -> Option<u16> {
        let mut value: u16 = 0;
        for _ in 0..4 {
            let digit = match self.bump()? {
                byte @ b'0'..=b'9' => byte - b'0',
                byte @ b'a'..=b'f' => byte - b'a' + 10,
                byte @ b'A'..=b'F' => byte - b'A' + 10,
                _ => return None,
            };
            value = value.checked_mul(16)?.checked_add(u16::from(digit))?;
        }
        Some(value)
    }

    fn number(&mut self) -> Option<Value> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        let integer = self.pos;
        self.skip_digits();
        if self.pos == integer {
            return None;
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            let fraction = self.pos;
            self.skip_digits();
            if self.pos == fraction {
                return None;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            let exponent = self.pos;
            self.skip_digits();
            if self.pos == exponent {
                return None;
            }
        }
        let text = self.data.get(start..self.pos)?;
        Some(Value::Num(String::from_utf8_lossy(text).into_owned()))
    }

    fn skip_digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
    }

    fn literal(&mut self, word: &[u8]) -> Option<()> {
        let end = self.pos.checked_add(word.len())?;
        if self.data.get(self.pos..end)? != word {
            return None;
        }
        self.pos = end;
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::testing::MemTree;

    fn v3() -> &'static [u8] {
        br#"{
          "name": "app",
          "version": "1.0.0",
          "lockfileVersion": 3,
          "packages": {
            "": { "name": "app", "version": "1.0.0" },
            "node_modules/express": { "version": "4.21.2" },
            "node_modules/@types/node": { "version": "22.10.2" },
            "node_modules/a/node_modules/b": { "name": "b", "version": "2.0.0" },
            "node_modules/linked": { "link": true, "resolved": "../linked" },
            "node_modules/gitpkg": { "version": "git+https://github.com/x/y.git" },
            "node_modules/local": { "version": "file:../local" }
          }
        }"#
    }

    fn v1() -> &'static [u8] {
        br#"{
          "name": "app",
          "version": "1.0.0",
          "lockfileVersion": 1,
          "dependencies": {
            "express": {
              "version": "4.21.2",
              "dependencies": {
                "debug": { "version": "2.6.9" }
              }
            },
            "@scope/pkg": { "version": "1.2.3" },
            "filepkg": { "version": "file:../filepkg" }
          }
        }"#
    }

    fn collected(tree: &MemTree, path: &str) -> Collector {
        let mut out = Collector::new();
        collect_file(tree, path, &mut out);
        out
    }

    #[test]
    fn recognises_lockfile_paths() {
        assert!(is_lockfile("package-lock.json"));
        assert!(is_lockfile("app/package-lock.json"));
        assert!(is_lockfile("app/server/package-lock.json"));
        assert!(is_lockfile("node_modules/.package-lock.json"));
        assert!(is_lockfile("app/node_modules/.package-lock.json"));
        assert!(!is_lockfile("app/node_modules/package-lock.json"));
        assert!(!is_lockfile("app/node_modules/express/package-lock.json"));
        assert!(!is_lockfile("app/.package-lock.json"));
        assert!(!is_lockfile("pnpm-lock.yaml"));
        assert!(!is_lockfile("package-lock.json.bak"));
        assert!(!is_lockfile("node_modules/express.json"));
    }

    #[test]
    fn reads_a_v3_lockfile() {
        let tree = MemTree::new().file("app/package-lock.json", v3());
        let out = collected(&tree, "app/package-lock.json");

        let got: Vec<(&str, &str, &str)> = out
            .packages()
            .iter()
            .map(|p| (p.name.as_str(), p.version.as_str(), p.purl.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("express", "4.21.2", "pkg:npm/express@4.21.2"),
                ("@types/node", "22.10.2", "pkg:npm/%40types/node@22.10.2"),
                ("b", "2.0.0", "pkg:npm/b@2.0.0"),
            ]
        );
        assert!(out.packages().iter().all(|p| p.ecosystem == "npm"));
        assert!(
            out.packages()
                .iter()
                .all(|p| p.path.as_deref() == Some("app/package-lock.json"))
        );
    }

    #[test]
    fn reads_the_hidden_node_modules_lockfile() {
        let tree = MemTree::new().file(
            "app/node_modules/.package-lock.json",
            br#"{"lockfileVersion":3,"packages":{"node_modules/ms":{"version":"2.1.3"}}}"#,
        );
        let out = collected(&tree, "app/node_modules/.package-lock.json");
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].purl, "pkg:npm/ms@2.1.3");
        assert_eq!(
            out.packages()[0].path.as_deref(),
            Some("app/node_modules/.package-lock.json")
        );
    }

    #[test]
    fn reads_a_v1_lockfile_with_nested_dependencies() {
        let tree = MemTree::new().file("package-lock.json", v1());
        let out = collected(&tree, "package-lock.json");
        let got: Vec<(&str, &str, &str)> = out
            .packages()
            .iter()
            .map(|p| (p.name.as_str(), p.version.as_str(), p.purl.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("express", "4.21.2", "pkg:npm/express@4.21.2"),
                ("debug", "2.6.9", "pkg:npm/debug@2.6.9"),
                ("@scope/pkg", "1.2.3", "pkg:npm/%40scope/pkg@1.2.3"),
            ]
        );
    }

    #[test]
    fn decodes_unicode_escapes_including_surrogate_pairs() {
        let tree = MemTree::new().file(
            "package-lock.json",
            br#"{"packages":{
              "node_modules/caf\u00e9":{"version":"1.0.0"},
              "node_modules/emoji":{"name":"\ud83d\ude00","version":"2.0.0"}
            }}"#,
        );
        let out = collected(&tree, "package-lock.json");
        assert_eq!(out.packages()[0].name, "café");
        assert_eq!(out.packages()[0].purl, "pkg:npm/caf%C3%A9@1.0.0");
        assert_eq!(out.packages()[1].name, "😀");
        assert_eq!(out.packages()[1].purl, "pkg:npm/%F0%9F%98%80@2.0.0");
    }

    #[test]
    fn malformed_json_is_named_and_skipped() {
        for data in [
            br#"{"packages":{"node_modules/a":{"version":"1.0.0"}"#.as_slice(),
            b"{not json".as_slice(),
            b"".as_slice(),
            b"[]".as_slice(),
            b"{\"packages\":{\"node_modules/a\":{\"version\":\"1.0.0\",}}}".as_slice(),
            b"{\"packages\":{\"node_modules/a\":{\"version\":\"\\ud83d\"}}}".as_slice(),
        ] {
            let tree = MemTree::new().file("package-lock.json", data.to_vec());
            let out = collected(&tree, "package-lock.json");
            assert!(out.packages().is_empty());
            assert_eq!(
                out.kinds(),
                vec!["npm_lockfile_malformed".to_owned()],
                "for input {:?}",
                String::from_utf8_lossy(data)
            );
        }
    }

    #[test]
    fn binary_garbage_and_invalid_utf8_do_not_panic() {
        let tree = MemTree::new().file("package-lock.json", vec![0xffu8; 256]);
        let out = collected(&tree, "package-lock.json");
        assert!(out.packages().is_empty());

        let tree = MemTree::new().file(
            "package-lock.json",
            b"{\"packages\":{\"node_modules/caf\xff\":{\"version\":\"1.0.0\"}}}".to_vec(),
        );
        let out = collected(&tree, "package-lock.json");
        assert_eq!(out.len(), 1);
        assert!(out.packages()[0].name.contains('\u{FFFD}'));
    }

    #[test]
    fn deeply_nested_json_does_not_overflow_the_stack() {
        let deep = format!("{}{}", "[".repeat(10_000), "]".repeat(10_000));
        let tree = MemTree::new().file("package-lock.json", deep.into_bytes());
        let out = collected(&tree, "package-lock.json");
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec!["npm_lockfile_malformed".to_owned()]);

        // Nesting inside `packages` behaves the same way.
        let deep = format!(
            "{{\"packages\":{}{}}}",
            "[".repeat(10_000),
            "]".repeat(10_000)
        );
        let tree = MemTree::new().file("package-lock.json", deep.into_bytes());
        let out = collected(&tree, "package-lock.json");
        assert_eq!(out.kinds(), vec!["npm_lockfile_malformed".to_owned()]);
    }

    #[test]
    fn oversized_lockfile_is_named_and_skipped() {
        let tree = MemTree::new().file("package-lock.json", vec![b' '; MAX_LOCKFILE_BYTES + 1]);
        let out = collected(&tree, "package-lock.json");
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec!["npm_lockfile_too_large".to_owned()]);
    }

    #[test]
    fn entries_without_a_registry_version_are_skipped() {
        let tree = MemTree::new().file(
            "package-lock.json",
            br#"{"packages":{
              "node_modules/noversion":{"resolved":"https://example.invalid/x.tgz"},
              "node_modules/empty":{"version":""},
              "node_modules/ok":{"version":"1.0.0"}
            }}"#,
        );
        let out = collected(&tree, "package-lock.json");
        assert_eq!(out.len(), 1);
        assert_eq!(out.packages()[0].name, "ok");
    }
}
