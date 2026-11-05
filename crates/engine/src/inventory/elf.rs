// SPDX-License-Identifier: AGPL-3.0-only
//! ELF executables: Go build information and `cargo auditable` data.
//!
//! The binary comes from an image nobody here wrote, and the engine reads it
//! as root. Every offset, length and count below is checked against the file
//! before it is used, every table has a hard size bound, and nothing is
//! executed. A malformed binary yields fewer packages — sometimes a
//! `not_inventoried` kind — never an error and never a panic.

use std::io::Read;

use flate2::read::ZlibDecoder;

use super::{Collector, Tree, TreeFile, purl, read_range};
use crate::InventoryPackage;

/// `\x7fELF`.
const ELF_MAGIC: [u8; 4] = [0x7f, b'E', b'L', b'F'];
/// The 32-byte Go build information header's magic.
const GO_MAGIC: [u8; 14] = [
    0xff, b' ', b'G', b'o', b' ', b'b', b'u', b'i', b'l', b'd', b'i', b'n', b'f', b':',
];

/// Most program headers a binary may declare.
const MAX_PROGRAM_HEADERS: u64 = 4096;
/// Most section headers a binary may declare.
const MAX_SECTION_HEADERS: u64 = 65_536;
/// Most bytes one header table may take.
const MAX_TABLE_BYTES: u64 = 4 * 1024 * 1024;
/// Largest section-name string table read.
const MAX_SHSTRTAB_BYTES: u64 = 1024 * 1024;
/// Largest `.go.buildinfo` section read.
const MAX_GO_BUILDINFO_BYTES: u64 = 1024 * 1024;
/// Largest compressed `.dep-v0` section read.
const MAX_CARGO_SECTION_BYTES: u64 = 1024 * 1024;
/// Largest `.dep-v0` payload after decompression.
const MAX_CARGO_JSON_BYTES: usize = 8 * 1024 * 1024;
/// Deepest JSON value nested in `.dep-v0`.
const MAX_JSON_DEPTH: usize = 64;
/// Most JSON values read from `.dep-v0`, bounding the parse tree.
const MAX_JSON_VALUES: usize = 200_000;
/// Largest length a Go build information uvarint may encode.
const MAX_UVARINT: u64 = 1024 * 1024;

/// Kind recorded for a Go binary using the pre-1.18 pointer format.
const GO_UNSUPPORTED: &str = "go_binary_unsupported";
/// Kind recorded when `.dep-v0` decompresses past its bound.
const CARGO_TOO_LARGE: &str = "cargo_auditable_too_large";

fn max_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn read_u16(data: &[u8], offset: usize, little_endian: bool) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let bytes: [u8; 2] = data.get(offset..end)?.try_into().ok()?;
    Some(if little_endian {
        u16::from_le_bytes(bytes)
    } else {
        u16::from_be_bytes(bytes)
    })
}

fn read_u32(data: &[u8], offset: usize, little_endian: bool) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes: [u8; 4] = data.get(offset..end)?.try_into().ok()?;
    Some(if little_endian {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    })
}

fn read_u64(data: &[u8], offset: usize, little_endian: bool) -> Option<u64> {
    let end = offset.checked_add(8)?;
    let bytes: [u8; 8] = data.get(offset..end)?.try_into().ok()?;
    Some(if little_endian {
        u64::from_le_bytes(bytes)
    } else {
        u64::from_be_bytes(bytes)
    })
}

/// The parts of the ELF header the parser needs, already validated.
#[derive(Clone, Copy)]
struct Header {
    is_64: bool,
    little_endian: bool,
    phoff: u64,
    phentsize: u64,
    phnum: u64,
    shoff: u64,
    shentsize: u64,
    shnum: u64,
    shstrndx: u64,
}

/// Parses the ELF header from the first bytes of the file. `None` for a file
/// that is not ELF or whose header is short or inconsistent.
fn parse_header(data: &[u8]) -> Option<Header> {
    if data.get(0..4)? != &ELF_MAGIC[..] {
        return None;
    }
    let is_64 = match *data.get(4)? {
        1 => false,
        2 => true,
        _ => return None,
    };
    let little_endian = match *data.get(5)? {
        1 => true,
        2 => false,
        _ => return None,
    };
    let header_len = if is_64 { 64 } else { 52 };
    if data.len() < header_len {
        return None;
    }
    let (phoff, shoff, phentsize, phnum, shentsize, shnum, shstrndx) = if is_64 {
        (
            read_u64(data, 32, little_endian)?,
            read_u64(data, 40, little_endian)?,
            u64::from(read_u16(data, 54, little_endian)?),
            u64::from(read_u16(data, 56, little_endian)?),
            u64::from(read_u16(data, 58, little_endian)?),
            u64::from(read_u16(data, 60, little_endian)?),
            u64::from(read_u16(data, 62, little_endian)?),
        )
    } else {
        (
            u64::from(read_u32(data, 28, little_endian)?),
            u64::from(read_u32(data, 32, little_endian)?),
            u64::from(read_u16(data, 42, little_endian)?),
            u64::from(read_u16(data, 44, little_endian)?),
            u64::from(read_u16(data, 46, little_endian)?),
            u64::from(read_u16(data, 48, little_endian)?),
            u64::from(read_u16(data, 50, little_endian)?),
        )
    };
    let expected_ph = if is_64 { 56 } else { 32 };
    let expected_sh = if is_64 { 64 } else { 40 };
    if phnum > MAX_PROGRAM_HEADERS {
        return None;
    }
    if phnum > 0 && phentsize != expected_ph {
        return None;
    }
    if phnum.checked_mul(phentsize)? > MAX_TABLE_BYTES {
        return None;
    }
    if shoff != 0 && shentsize != expected_sh {
        return None;
    }
    if shnum > MAX_SECTION_HEADERS {
        return None;
    }
    if shnum.checked_mul(shentsize)? > MAX_TABLE_BYTES {
        return None;
    }
    Some(Header {
        is_64,
        little_endian,
        phoff,
        phentsize,
        phnum,
        shoff,
        shentsize,
        shnum,
        shstrndx,
    })
}

/// Whether the binary declares `PT_INTERP` and `PT_DYNAMIC`.
fn program_headers(file: &mut TreeFile<'_>, header: &Header) -> Option<(bool, bool)> {
    let mut interp = false;
    let mut dynamic = false;
    if header.phnum == 0 {
        return Some((interp, dynamic));
    }
    let table_len = header.phnum.checked_mul(header.phentsize)?;
    let table = read_range(file, header.phoff, table_len, max_usize(MAX_TABLE_BYTES))?;
    let entry_size = usize::try_from(header.phentsize).ok()?;
    for entry in table.chunks_exact(entry_size) {
        match read_u32(entry, 0, header.little_endian)? {
            3 => interp = true,
            2 => dynamic = true,
            _ => {}
        }
    }
    Some((interp, dynamic))
}

/// One section header, cut to what the inventory uses.
#[derive(Clone, Copy)]
struct Section {
    offset: u64,
    size: u64,
}

#[derive(Clone, Copy, Default)]
struct Sections {
    go_buildinfo: Option<Section>,
    dep_v0: Option<Section>,
}

/// One section header's fields, read without trusting the entry size.
struct Shdr {
    name: u32,
    offset: u64,
    size: u64,
    link: u32,
}

fn parse_shdr(entry: &[u8], is_64: bool, little_endian: bool) -> Option<Shdr> {
    if is_64 {
        Some(Shdr {
            name: read_u32(entry, 0, little_endian)?,
            offset: read_u64(entry, 24, little_endian)?,
            size: read_u64(entry, 32, little_endian)?,
            link: read_u32(entry, 40, little_endian)?,
        })
    } else {
        Some(Shdr {
            name: read_u32(entry, 0, little_endian)?,
            offset: u64::from(read_u32(entry, 16, little_endian)?),
            size: u64::from(read_u32(entry, 20, little_endian)?),
            link: read_u32(entry, 24, little_endian)?,
        })
    }
}

/// The name at `name` in the section-name string table.
fn section_name(strtab: &[u8], name: u32) -> Option<&str> {
    let start = usize::try_from(name).ok()?;
    let rest = strtab.get(start..)?;
    let end = rest.iter().position(|byte| *byte == 0)?;
    std::str::from_utf8(rest.get(..end)?).ok()
}

/// Finds the sections the inventory reads, following the extended numbering
/// forms (`e_shnum == 0`, `e_shstrndx == SHN_XINDEX`).
fn find_sections(file: &mut TreeFile<'_>, header: &Header) -> Sections {
    let mut found = Sections::default();
    if header.shoff == 0 || header.shentsize == 0 {
        return found;
    }
    let entry_size = header.shentsize;
    let mut count = header.shnum;
    let mut strndx = header.shstrndx;
    if count == 0 || strndx == 0xffff {
        let Some(first) = read_range(file, header.shoff, entry_size, 64) else {
            return found;
        };
        let Some(section_zero) = parse_shdr(&first, header.is_64, header.little_endian) else {
            return found;
        };
        if count == 0 {
            count = section_zero.size;
        }
        if strndx == 0xffff {
            strndx = u64::from(section_zero.link);
        }
    }
    if count == 0 || count > MAX_SECTION_HEADERS {
        return found;
    }
    let Some(table_len) = count.checked_mul(entry_size) else {
        return found;
    };
    if table_len > MAX_TABLE_BYTES {
        return found;
    }
    let Some(table) = read_range(file, header.shoff, table_len, max_usize(MAX_TABLE_BYTES)) else {
        return found;
    };
    let Ok(entry_size) = usize::try_from(entry_size) else {
        return found;
    };
    if entry_size == 0 {
        return found;
    }
    if strndx == 0 || strndx >= count {
        return found;
    }
    let Some(strtab_base) = strndx
        .checked_mul(header.shentsize)
        .and_then(|value| usize::try_from(value).ok())
    else {
        return found;
    };
    let Some(strtab_end) = strtab_base.checked_add(entry_size) else {
        return found;
    };
    let Some(strtab_entry) = table.get(strtab_base..strtab_end) else {
        return found;
    };
    let Some(strtab_header) = parse_shdr(strtab_entry, header.is_64, header.little_endian) else {
        return found;
    };
    if strtab_header.size == 0 || strtab_header.size > MAX_SHSTRTAB_BYTES {
        return found;
    }
    let Some(strtab) = read_range(
        file,
        strtab_header.offset,
        strtab_header.size,
        max_usize(MAX_SHSTRTAB_BYTES),
    ) else {
        return found;
    };

    for (index, entry) in table.chunks_exact(entry_size).enumerate() {
        if index == 0 {
            continue;
        }
        let Some(shdr) = parse_shdr(entry, header.is_64, header.little_endian) else {
            continue;
        };
        let Some(name) = section_name(&strtab, shdr.name) else {
            continue;
        };
        let section = Section {
            offset: shdr.offset,
            size: shdr.size,
        };
        match name {
            ".go.buildinfo" if found.go_buildinfo.is_none() => {
                found.go_buildinfo = Some(section);
            }
            ".dep-v0" if found.dep_v0.is_none() => {
                found.dep_v0 = Some(section);
            }
            _ => {}
        }
    }
    found
}

/// Reads one LEB128 uvarint, at most 10 bytes and at most [`MAX_UVARINT`].
fn read_uvarint(data: &[u8], position: &mut usize) -> Option<usize> {
    let mut value: u64 = 0;
    let mut index: u32 = 0;
    loop {
        let byte = *data.get(*position)?;
        *position = (*position).checked_add(1)?;
        let shift = index.checked_mul(7)?;
        if shift >= 64 {
            return None;
        }
        let low = u64::from(byte & 0x7f);
        if shift > 0 && (low >> (64 - shift)) != 0 {
            return None;
        }
        value = value.checked_add(low.checked_shl(shift)?)?;
        index = index.checked_add(1)?;
        if byte & 0x80 == 0 {
            break;
        }
        if index >= 10 {
            return None;
        }
    }
    if value > MAX_UVARINT {
        return None;
    }
    usize::try_from(value).ok()
}

/// Takes `len` bytes at `position`.
fn take_bytes<'a>(data: &'a [u8], position: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = (*position).checked_add(len)?;
    let slice = data.get(*position..end)?;
    *position = end;
    Some(slice)
}

/// The module information text, without the 16-byte sentinels Go wraps it in.
fn module_text(bytes: &[u8]) -> Option<&str> {
    if bytes.len() >= 33
        && let Some(inner) = bytes.get(16..bytes.len() - 16)
        && let Ok(text) = std::str::from_utf8(inner)
    {
        return Some(text);
    }
    std::str::from_utf8(bytes).ok()
}

/// The Go toolchain version without the `go` prefix or a build suffix.
fn go_version(version: &str) -> Option<&str> {
    let trimmed = version.split(' ').next()?;
    let trimmed = trimmed.split("-X:").next()?;
    let trimmed = trimmed.split('+').next()?;
    let trimmed = trimmed.strip_prefix("go").unwrap_or(trimmed);
    if trimmed.is_empty() || trimmed.starts_with("devel") {
        None
    } else {
        Some(trimmed)
    }
}

/// One Go module, after replace directives have been applied.
struct GoModule {
    path: String,
    version: String,
}

/// The replacement a `=>` directive names.
fn replacement<'a>(fields: &[&'a str]) -> Option<(&'a str, Option<&'a str>)> {
    let index = fields.iter().position(|field| *field == "=>")?;
    let path = fields.get(index.checked_add(1)?).copied()?;
    let version = fields.get(index.checked_add(2)?).copied();
    Some((path, version))
}

/// Parses the module information lines, applying replacements in order.
fn go_modules(text: &str) -> Vec<GoModule> {
    let mut modules: Vec<GoModule> = Vec::new();
    let mut last: Option<usize> = None;
    for line in text.split('\n') {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        match fields.first().copied() {
            Some("mod") | Some("dep") => {
                let kind = fields.first().copied().unwrap_or_default();
                let mut path = fields.get(1).copied().unwrap_or_default();
                let mut version = fields.get(2).copied().unwrap_or_default();
                let mut drop = false;
                if let Some((path_after, version_after)) = replacement(&fields) {
                    match version_after {
                        Some(version_after) if !version_after.is_empty() => {
                            path = path_after;
                            version = version_after;
                        }
                        _ => drop = true,
                    }
                }
                if drop || (kind == "mod" && (version.is_empty() || version == "(devel)")) {
                    last = None;
                    continue;
                }
                modules.push(GoModule {
                    path: path.to_owned(),
                    version: version.to_owned(),
                });
                last = modules.len().checked_sub(1);
            }
            Some("=>") => {
                let Some(index) = last else {
                    continue;
                };
                let Some((path, version)) = replacement(&fields) else {
                    continue;
                };
                match version {
                    Some(version) if !version.is_empty() => {
                        if let Some(module) = modules.get_mut(index) {
                            module.path = path.to_owned();
                            module.version = version.to_owned();
                        }
                    }
                    _ => {
                        if index < modules.len() {
                            modules.remove(index);
                        }
                        last = None;
                    }
                }
            }
            _ => {}
        }
    }
    modules
}

/// Emits one Go module. Returns false once the collector is full.
fn push_go_module(module: &GoModule, path: &str, out: &mut Collector) -> bool {
    let mut segments: Vec<&str> = module.path.split('/').collect();
    let Some(name) = segments.pop() else {
        return true;
    };
    // OSV keys Go records by the full module path.
    out.push(InventoryPackage {
        ecosystem: "Go".into(),
        name: module.path.clone(),
        source_name: None,
        version: module.version.clone(),
        purl: purl("golang", &segments, name, &module.version, &[]),
        path: Some(path.to_owned()),
    })
}

/// The outcome of reading `.go.buildinfo`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GoStatus {
    /// No usable build information.
    Absent,
    /// The pre-1.18 pointer format, which is not read.
    Unsupported,
    /// The build information was read.
    Inventoried,
}

/// Parses the contents of `.go.buildinfo`.
fn go_buildinfo(data: &[u8], path: &str, out: &mut Collector) -> GoStatus {
    if data.len() < 32 || data.get(0..GO_MAGIC.len()) != Some(&GO_MAGIC[..]) {
        return GoStatus::Absent;
    }
    let Some(flags) = data.get(15) else {
        return GoStatus::Absent;
    };
    if flags & 0x2 == 0 {
        return GoStatus::Unsupported;
    }
    let mut position = 32usize;
    let Some(version_len) = read_uvarint(data, &mut position) else {
        return GoStatus::Absent;
    };
    let Some(version_bytes) = take_bytes(data, &mut position, version_len) else {
        return GoStatus::Absent;
    };
    let Ok(version) = std::str::from_utf8(version_bytes) else {
        return GoStatus::Absent;
    };
    let Some(module_len) = read_uvarint(data, &mut position) else {
        return GoStatus::Absent;
    };
    let Some(module_bytes) = take_bytes(data, &mut position, module_len) else {
        return GoStatus::Absent;
    };

    if let Some(version) = go_version(version)
        && !out.push(InventoryPackage {
            ecosystem: "Go".into(),
            name: "stdlib".into(),
            source_name: None,
            version: version.to_owned(),
            purl: purl("golang", &[], "stdlib", version, &[]),
            path: Some(path.to_owned()),
        })
    {
        return GoStatus::Inventoried;
    }
    let Some(text) = module_text(module_bytes) else {
        return GoStatus::Inventoried;
    };
    for module in go_modules(text) {
        if !push_go_module(&module, path, out) {
            break;
        }
    }
    GoStatus::Inventoried
}

/// Reads and parses `.go.buildinfo`; the section's own size is bounded.
fn go_section(
    file: &mut TreeFile<'_>,
    section: &Section,
    path: &str,
    out: &mut Collector,
) -> GoStatus {
    if section.size == 0 {
        return GoStatus::Absent;
    }
    let len = section.size.min(MAX_GO_BUILDINFO_BYTES);
    let Some(data) = read_range(file, section.offset, len, max_usize(MAX_GO_BUILDINFO_BYTES))
    else {
        return GoStatus::Absent;
    };
    go_buildinfo(&data, path, out)
}

/// A JSON value from `.dep-v0`; only the shapes the inventory reads are kept.
enum Json {
    Null,
    Bool,
    Num,
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    fn field(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(entries) => entries
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(text) => Some(text),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(items) => Some(items),
            _ => None,
        }
    }
}

/// A bounded recursive-descent JSON reader.
struct JsonParser<'a> {
    data: &'a [u8],
    position: usize,
    values: usize,
}

impl<'a> JsonParser<'a> {
    fn parse(data: &'a [u8]) -> Option<Json> {
        let mut parser = JsonParser {
            data,
            position: 0,
            values: 0,
        };
        let value = parser.value(0)?;
        parser.skip_whitespace();
        if parser.position != parser.data.len() {
            return None;
        }
        Some(value)
    }

    fn peek(&self) -> Option<u8> {
        self.data.get(self.position).copied()
    }

    fn bump(&mut self) -> Option<()> {
        self.position = self.position.checked_add(1)?;
        Some(())
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            if self.bump().is_none() {
                break;
            }
        }
    }

    fn value(&mut self, depth: usize) -> Option<Json> {
        if depth > MAX_JSON_DEPTH {
            return None;
        }
        self.values = self.values.checked_add(1)?;
        if self.values > MAX_JSON_VALUES {
            return None;
        }
        self.skip_whitespace();
        match self.peek()? {
            b'{' => self.object(depth),
            b'[' => self.array(depth),
            b'"' => Some(Json::Str(self.string()?)),
            b't' => {
                self.literal(b"true")?;
                Some(Json::Bool)
            }
            b'f' => {
                self.literal(b"false")?;
                Some(Json::Bool)
            }
            b'n' => {
                self.literal(b"null")?;
                Some(Json::Null)
            }
            b'-' | b'0'..=b'9' => {
                self.number()?;
                Some(Json::Num)
            }
            _ => None,
        }
    }

    fn literal(&mut self, word: &[u8]) -> Option<()> {
        let end = self.position.checked_add(word.len())?;
        if self.data.get(self.position..end)? != word {
            return None;
        }
        self.position = end;
        Some(())
    }

    fn array(&mut self, depth: usize) -> Option<Json> {
        self.bump()?;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek()? == b']' {
            self.bump()?;
            return Some(Json::Arr(items));
        }
        loop {
            items.push(self.value(depth + 1)?);
            self.skip_whitespace();
            match self.peek()? {
                b',' => self.bump()?,
                b']' => {
                    self.bump()?;
                    return Some(Json::Arr(items));
                }
                _ => return None,
            }
        }
    }

    fn object(&mut self, depth: usize) -> Option<Json> {
        self.bump()?;
        let mut entries = Vec::new();
        self.skip_whitespace();
        if self.peek()? == b'}' {
            self.bump()?;
            return Some(Json::Obj(entries));
        }
        loop {
            self.skip_whitespace();
            if self.peek()? != b'"' {
                return None;
            }
            let key = self.string()?;
            self.skip_whitespace();
            if self.peek()? != b':' {
                return None;
            }
            self.bump()?;
            let value = self.value(depth + 1)?;
            entries.push((key, value));
            self.skip_whitespace();
            match self.peek()? {
                b',' => self.bump()?,
                b'}' => {
                    self.bump()?;
                    return Some(Json::Obj(entries));
                }
                _ => return None,
            }
        }
    }

    fn string(&mut self) -> Option<String> {
        if self.peek()? != b'"' {
            return None;
        }
        self.bump()?;
        let mut out = String::new();
        loop {
            match self.peek()? {
                b'"' => {
                    self.bump()?;
                    return Some(out);
                }
                b'\\' => {
                    self.bump()?;
                    let escaped = self.peek()?;
                    self.bump()?;
                    match escaped {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let first = self.hex4()?;
                            let code = if (0xD800..=0xDBFF).contains(&first) {
                                if self.peek()? != b'\\' {
                                    return None;
                                }
                                self.bump()?;
                                if self.peek()? != b'u' {
                                    return None;
                                }
                                self.bump()?;
                                let second = self.hex4()?;
                                if !(0xDC00..=0xDFFF).contains(&second) {
                                    return None;
                                }
                                let high = u32::from(first).checked_sub(0xD800)?;
                                let low = u32::from(second).checked_sub(0xDC00)?;
                                0x10000u32
                                    .checked_add(high.checked_mul(0x400)?)?
                                    .checked_add(low)?
                            } else {
                                u32::from(first)
                            };
                            out.push(char::from_u32(code)?);
                        }
                        _ => return None,
                    }
                }
                0x00..=0x1f => return None,
                _ => {
                    let (character, end) = next_char(self.data, self.position)?;
                    out.push(character);
                    self.position = end;
                }
            }
        }
    }

    fn hex4(&mut self) -> Option<u16> {
        let mut value: u16 = 0;
        for _ in 0..4 {
            let byte = self.peek()?;
            let digit = match byte {
                b'0'..=b'9' => byte.checked_sub(b'0')?,
                b'a'..=b'f' => byte.checked_sub(b'a')?.checked_add(10)?,
                b'A'..=b'F' => byte.checked_sub(b'A')?.checked_add(10)?,
                _ => return None,
            };
            value = value.checked_mul(16)?.checked_add(u16::from(digit))?;
            self.bump()?;
        }
        Some(value)
    }

    fn number(&mut self) -> Option<()> {
        if self.peek()? == b'-' {
            self.bump()?;
        }
        match self.peek()? {
            b'0' => self.bump()?,
            b'1'..=b'9' => self.digits()?,
            _ => return None,
        }
        if self.peek() == Some(b'.') {
            self.bump()?;
            if !self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                return None;
            }
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.bump()?;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.bump()?;
            }
            if !self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                return None;
            }
            self.digits()?;
        }
        Some(())
    }

    fn digits(&mut self) -> Option<()> {
        let mut any = false;
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            any = true;
            self.bump()?;
        }
        if any { Some(()) } else { None }
    }
}

/// Decodes one UTF-8 character at `position`.
fn next_char(data: &[u8], position: usize) -> Option<(char, usize)> {
    let first = *data.get(position)?;
    let width = match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return None,
    };
    let end = position.checked_add(width)?;
    let character = std::str::from_utf8(data.get(position..end)?)
        .ok()?
        .chars()
        .next()?;
    Some((character, end))
}

/// Reads and parses `.dep-v0`, emitting every `crates.io` package.
/// Returns true when the section held well-formed data.
fn cargo_auditable(
    file: &mut TreeFile<'_>,
    section: &Section,
    path: &str,
    out: &mut Collector,
) -> bool {
    if section.size == 0 {
        return false;
    }
    if section.size > MAX_CARGO_SECTION_BYTES {
        out.not_inventoried(CARGO_TOO_LARGE);
        return false;
    }
    let Some(compressed) = read_range(
        file,
        section.offset,
        section.size,
        max_usize(MAX_CARGO_SECTION_BYTES),
    ) else {
        return false;
    };
    let mut json = Vec::new();
    let limit = u64::try_from(MAX_CARGO_JSON_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut decoder = ZlibDecoder::new(compressed.as_slice()).take(limit);
    if decoder.read_to_end(&mut json).is_err() {
        if json.len() > MAX_CARGO_JSON_BYTES {
            out.not_inventoried(CARGO_TOO_LARGE);
        }
        return false;
    }
    if json.len() > MAX_CARGO_JSON_BYTES {
        out.not_inventoried(CARGO_TOO_LARGE);
        return false;
    }
    let Some(root) = JsonParser::parse(&json) else {
        return false;
    };
    let Some(packages) = root.field("packages").and_then(Json::as_array) else {
        return false;
    };
    for package in packages {
        let Some(name) = package.field("name").and_then(Json::as_str) else {
            continue;
        };
        let Some(version) = package.field("version").and_then(Json::as_str) else {
            continue;
        };
        if package.field("source").and_then(Json::as_str) != Some("crates.io") {
            continue;
        }
        let pushed = out.push(InventoryPackage {
            ecosystem: "crates.io".into(),
            name: name.to_owned(),
            source_name: None,
            version: version.to_owned(),
            purl: purl("cargo", &[], name, version, &[]),
            path: Some(path.to_owned()),
        });
        if !pushed {
            break;
        }
    }
    true
}

/// Inspects the executable at `path`: adds the Go modules or Rust crates it
/// records, or `static_binary` for a static executable that records neither.
pub fn inspect<T: Tree + ?Sized>(tree: &T, path: &str, out: &mut Collector) {
    let Some(mut file) = tree.open(path) else {
        return;
    };
    let header_len = file.len.min(64);
    let Some(head) = read_range(&mut file, 0, header_len, 64) else {
        return;
    };
    let Some(header) = parse_header(&head) else {
        return;
    };
    let (has_interp, has_dynamic) = program_headers(&mut file, &header).unwrap_or((false, false));
    let sections = find_sections(&mut file, &header);

    let mut inventoried = false;
    if let Some(section) = sections.go_buildinfo {
        match go_section(&mut file, &section, path, out) {
            GoStatus::Inventoried => inventoried = true,
            GoStatus::Unsupported => {
                out.not_inventoried(GO_UNSUPPORTED);
                return;
            }
            GoStatus::Absent => {}
        }
    }
    if let Some(section) = sections.dep_v0
        && cargo_auditable(&mut file, &section, path, out)
    {
        inventoried = true;
    }
    if !inventoried && !has_interp && !has_dynamic {
        out.not_inventoried(super::kind::STATIC_BINARY);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::ZlibEncoder;

    use super::*;
    use crate::inventory::kind;
    use crate::inventory::testing::MemTree;

    /// A built test binary and where its section headers live.
    struct Fixture {
        bytes: Vec<u8>,
        shoff: usize,
        shentsize: usize,
        shnum: usize,
    }

    fn put16(bytes: &mut [u8], offset: usize, value: u16, little_endian: bool) {
        let raw = if little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        bytes[offset..offset + 2].copy_from_slice(&raw);
    }

    fn put32(bytes: &mut [u8], offset: usize, value: u32, little_endian: bool) {
        let raw = if little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        bytes[offset..offset + 4].copy_from_slice(&raw);
    }

    fn put64(bytes: &mut [u8], offset: usize, value: u64, little_endian: bool) {
        let raw = if little_endian {
            value.to_le_bytes()
        } else {
            value.to_be_bytes()
        };
        bytes[offset..offset + 8].copy_from_slice(&raw);
    }

    /// Builds an ELF image: a header, `program_types` program headers, a null
    /// section, the named sections and a `.shstrtab`.
    fn build_elf(
        is_64: bool,
        little_endian: bool,
        program_types: &[u32],
        sections: &[(&str, Vec<u8>)],
    ) -> Fixture {
        let header_len = if is_64 { 64 } else { 52 };
        let phentsize = if is_64 { 56 } else { 32 };
        let shentsize = if is_64 { 64 } else { 40 };
        let phnum = program_types.len();
        let phoff = header_len;
        let shoff = phoff + phnum * phentsize;
        let shnum = sections.len() + 2;

        let mut shstrtab = vec![0u8];
        let mut name_offsets = Vec::new();
        for (name, _) in sections {
            name_offsets.push(shstrtab.len());
            shstrtab.extend_from_slice(name.as_bytes());
            shstrtab.push(0);
        }
        let shstrtab_name = shstrtab.len();
        shstrtab.extend_from_slice(b".shstrtab\0");

        let mut cursor = shoff + shnum * shentsize;
        let mut offsets = Vec::new();
        for (_, content) in sections {
            offsets.push(cursor);
            cursor += content.len();
        }
        let shstrtab_offset = cursor;
        cursor += shstrtab.len();

        let mut bytes = vec![0u8; cursor];
        bytes[0..4].copy_from_slice(&[0x7f, b'E', b'L', b'F']);
        bytes[4] = if is_64 { 2 } else { 1 };
        bytes[5] = if little_endian { 1 } else { 2 };
        bytes[6] = 1;
        put16(&mut bytes, 16, 2, little_endian);
        put16(&mut bytes, 18, if is_64 { 62 } else { 20 }, little_endian);
        put32(&mut bytes, 20, 1, little_endian);
        if is_64 {
            put64(&mut bytes, 24, 0, little_endian);
            put64(&mut bytes, 32, phoff as u64, little_endian);
            put64(&mut bytes, 40, shoff as u64, little_endian);
            put32(&mut bytes, 48, 0, little_endian);
            put16(&mut bytes, 52, header_len as u16, little_endian);
            put16(&mut bytes, 54, phentsize as u16, little_endian);
            put16(&mut bytes, 56, phnum as u16, little_endian);
            put16(&mut bytes, 58, shentsize as u16, little_endian);
            put16(&mut bytes, 60, shnum as u16, little_endian);
            put16(&mut bytes, 62, (shnum - 1) as u16, little_endian);
        } else {
            put32(&mut bytes, 24, 0, little_endian);
            put32(&mut bytes, 28, phoff as u32, little_endian);
            put32(&mut bytes, 32, shoff as u32, little_endian);
            put32(&mut bytes, 36, 0, little_endian);
            put16(&mut bytes, 40, header_len as u16, little_endian);
            put16(&mut bytes, 42, phentsize as u16, little_endian);
            put16(&mut bytes, 44, phnum as u16, little_endian);
            put16(&mut bytes, 46, shentsize as u16, little_endian);
            put16(&mut bytes, 48, shnum as u16, little_endian);
            put16(&mut bytes, 50, (shnum - 1) as u16, little_endian);
        }
        for (index, program_type) in program_types.iter().enumerate() {
            put32(
                &mut bytes,
                phoff + index * phentsize,
                *program_type,
                little_endian,
            );
        }
        for (index, ((_, content), name_offset)) in
            sections.iter().zip(name_offsets.iter()).enumerate()
        {
            let base = shoff + (index + 1) * shentsize;
            put32(&mut bytes, base, *name_offset as u32, little_endian);
            put32(&mut bytes, base + 4, 1, little_endian);
            if is_64 {
                put64(&mut bytes, base + 24, offsets[index] as u64, little_endian);
                put64(&mut bytes, base + 32, content.len() as u64, little_endian);
                put64(&mut bytes, base + 48, 1, little_endian);
            } else {
                put32(&mut bytes, base + 16, offsets[index] as u32, little_endian);
                put32(&mut bytes, base + 20, content.len() as u32, little_endian);
                put32(&mut bytes, base + 32, 1, little_endian);
            }
            bytes[offsets[index]..offsets[index] + content.len()].copy_from_slice(content);
        }
        let base = shoff + (shnum - 1) * shentsize;
        put32(&mut bytes, base, shstrtab_name as u32, little_endian);
        put32(&mut bytes, base + 4, 3, little_endian);
        if is_64 {
            put64(&mut bytes, base + 24, shstrtab_offset as u64, little_endian);
            put64(&mut bytes, base + 32, shstrtab.len() as u64, little_endian);
            put64(&mut bytes, base + 48, 1, little_endian);
        } else {
            put32(&mut bytes, base + 16, shstrtab_offset as u32, little_endian);
            put32(&mut bytes, base + 20, shstrtab.len() as u32, little_endian);
            put32(&mut bytes, base + 32, 1, little_endian);
        }
        bytes[shstrtab_offset..shstrtab_offset + shstrtab.len()].copy_from_slice(&shstrtab);

        Fixture {
            bytes,
            shoff,
            shentsize,
            shnum,
        }
    }

    fn push_uvarint(out: &mut Vec<u8>, mut value: usize) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    /// A `.go.buildinfo` section in the inline (Go >= 1.18) format.
    fn inline_buildinfo(version: &str, modules: &str) -> Vec<u8> {
        let mut data = vec![0u8; 32];
        data[0..GO_MAGIC.len()].copy_from_slice(&GO_MAGIC);
        data[14] = 8;
        data[15] = 0x2;
        push_uvarint(&mut data, version.len());
        data.extend_from_slice(version.as_bytes());
        let framed = frame_module_info(modules);
        push_uvarint(&mut data, framed.len());
        data.extend_from_slice(&framed);
        data
    }

    /// Wraps module information in Go's 16-byte sentinels.
    fn frame_module_info(text: &str) -> Vec<u8> {
        let mut framed = vec![0xff; 16];
        framed.extend_from_slice(text.as_bytes());
        framed.extend_from_slice(b"buildinfo.buildi");
        framed
    }

    fn zlib(data: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn go_package(module: &str, version: &str) -> InventoryPackage {
        let mut segments: Vec<&str> = module.split('/').collect();
        let name = segments.pop().unwrap();
        InventoryPackage {
            ecosystem: "Go".into(),
            name: module.into(),
            source_name: None,
            version: version.into(),
            purl: purl("golang", &segments, name, version, &[]),
            path: Some("app/bin".into()),
        }
    }

    fn stdlib(version: &str) -> InventoryPackage {
        InventoryPackage {
            ecosystem: "Go".into(),
            name: "stdlib".into(),
            source_name: None,
            version: version.into(),
            purl: purl("golang", &[], "stdlib", version, &[]),
            path: Some("app/bin".into()),
        }
    }

    fn crate_package(name: &str, version: &str) -> InventoryPackage {
        InventoryPackage {
            ecosystem: "crates.io".into(),
            name: name.into(),
            source_name: None,
            version: version.into(),
            purl: purl("cargo", &[], name, version, &[]),
            path: Some("app/bin".into()),
        }
    }

    fn inspect_bytes(bytes: Vec<u8>) -> Collector {
        let tree = MemTree::new().exe("app/bin", bytes);
        let mut out = Collector::new();
        inspect(&tree, "app/bin", &mut out);
        out
    }

    #[test]
    fn go_inline_buildinfo_yields_modules_and_toolchain() {
        let modules = "path\texample.com/app\n\
                       mod\texample.com/app\t(devel)\n\
                       dep\tgithub.com/a/b\tv1.2.3\th1:abc=\n\
                       dep\tgithub.com/c/d\tv0.1.0\th1:def=\n\
                       =>\tgithub.com/c/e\tv0.2.0\th1:ghi=\n\
                       dep\tgithub.com/f/g\tv1.0.0\th1:jkl=\n\
                       =>\t../local/g\n";
        let buildinfo = inline_buildinfo("go1.22.3", modules);
        let fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let out = inspect_bytes(fixture.bytes);

        assert_eq!(
            out.packages(),
            &[
                stdlib("1.22.3"),
                go_package("github.com/a/b", "v1.2.3"),
                go_package("github.com/c/e", "v0.2.0"),
            ]
        );
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn go_inline_buildinfo_keeps_prerelease_versions() {
        let buildinfo = inline_buildinfo("go1.21rc2", "path\texample.com/app\n");
        let fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let out = inspect_bytes(fixture.bytes);
        assert_eq!(out.packages(), &[stdlib("1.21rc2")]);
    }

    #[test]
    fn go_old_format_is_not_inventoried() {
        let mut buildinfo = vec![0u8; 32];
        buildinfo[0..GO_MAGIC.len()].copy_from_slice(&GO_MAGIC);
        buildinfo[14] = 8;
        buildinfo[15] = 0;
        let fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec![GO_UNSUPPORTED.to_owned()]);
    }

    #[test]
    fn cargo_auditable_yields_crates_io_packages() {
        let json = br#"{"packages":[
            {"name":"serde","version":"1.0.200","source":"crates.io","kind":"normal"},
            {"name":"mylocal","version":"0.1.0","source":"local"},
            {"name":"buildy","version":"0.2.0","source":"crates.io","kind":"build"}
        ]}"#;
        let fixture = build_elf(true, true, &[2], &[(".dep-v0", zlib(json))]);
        let out = inspect_bytes(fixture.bytes);
        assert_eq!(
            out.packages(),
            &[
                crate_package("serde", "1.0.200"),
                crate_package("buildy", "0.2.0"),
            ]
        );
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn cargo_auditable_in_elf32_big_endian() {
        let json = br#"{"packages":[{"name":"anyhow","version":"1.0.86","source":"crates.io"}]}"#;
        let fixture = build_elf(false, false, &[2], &[(".dep-v0", zlib(json))]);
        let out = inspect_bytes(fixture.bytes);
        assert_eq!(out.packages(), &[crate_package("anyhow", "1.0.86")]);
    }

    #[test]
    fn cargo_auditable_decodes_json_escapes() {
        let json = br#"{"packages":[{"name":"s\u0065rde\ud83d\ude00","version":"1.0.0","source":"crates.io"}]}"#;
        let fixture = build_elf(true, true, &[2], &[(".dep-v0", zlib(json))]);
        let out = inspect_bytes(fixture.bytes);
        assert_eq!(out.packages().len(), 1);
        assert_eq!(out.packages()[0].name, "serde\u{1f600}");
    }

    #[test]
    fn static_binary_without_metadata_is_noted() {
        let fixture = build_elf(true, true, &[], &[]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec![kind::STATIC_BINARY.to_owned()]);
    }

    #[test]
    fn dynamic_binary_without_metadata_is_silent() {
        let fixture = build_elf(true, true, &[2], &[]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn interpreted_binary_without_metadata_is_silent() {
        let fixture = build_elf(true, true, &[3], &[]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn extended_section_numbering_is_followed() {
        let buildinfo = inline_buildinfo("go1.22.3", "mod\texample.com/app\tv1.0.0\n");
        let mut fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let shnum = fixture.shnum as u64;
        let strndx = (fixture.shnum - 1) as u64;
        // e_shnum = 0, e_shstrndx = SHN_XINDEX.
        put16(&mut fixture.bytes, 60, 0, true);
        put16(&mut fixture.bytes, 62, 0xffff, true);
        // Section 0 carries the real count in sh_size and the index in sh_link.
        put64(&mut fixture.bytes, fixture.shoff + 32, shnum, true);
        put32(&mut fixture.bytes, fixture.shoff + 40, strndx as u32, true);
        let out = inspect_bytes(fixture.bytes);
        assert_eq!(
            out.packages(),
            &[stdlib("1.22.3"), go_package("example.com/app", "v1.0.0")]
        );
    }

    #[test]
    fn truncated_header_is_ignored() {
        let out = inspect_bytes(b"\x7fELF\x02\x01".to_vec());
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn empty_and_non_elf_files_are_ignored() {
        for bytes in [Vec::new(), b"#!/bin/sh\necho hi\n".to_vec()] {
            let out = inspect_bytes(bytes);
            assert!(out.packages().is_empty());
            assert!(out.kinds().is_empty());
        }
    }

    #[test]
    fn section_table_beyond_eof_is_ignored() {
        let mut fixture = build_elf(true, true, &[2], &[]);
        put64(&mut fixture.bytes, 40, u64::MAX, true);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn huge_section_count_is_ignored() {
        let mut fixture = build_elf(true, true, &[2], &[]);
        put16(&mut fixture.bytes, 60, 65535, true);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn overflowing_section_range_is_ignored() {
        let buildinfo = inline_buildinfo("go1.22.3", "mod\texample.com/app\tv1.0.0\n");
        let mut fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let base = fixture.shoff + fixture.shentsize;
        put64(&mut fixture.bytes, base + 24, u64::MAX, true);
        put64(&mut fixture.bytes, base + 32, u64::MAX, true);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
    }

    #[test]
    fn go_varint_of_eleven_bytes_is_ignored() {
        let mut buildinfo = vec![0u8; 32];
        buildinfo[0..GO_MAGIC.len()].copy_from_slice(&GO_MAGIC);
        buildinfo[14] = 8;
        buildinfo[15] = 0x2;
        buildinfo.extend_from_slice(&[0x80; 11]);
        let fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
    }

    #[test]
    fn go_varint_past_the_section_is_ignored() {
        let mut buildinfo = vec![0u8; 32];
        buildinfo[0..GO_MAGIC.len()].copy_from_slice(&GO_MAGIC);
        buildinfo[14] = 8;
        buildinfo[15] = 0x2;
        push_uvarint(&mut buildinfo, 5000);
        buildinfo.extend_from_slice(b"go1.22");
        let fixture = build_elf(true, true, &[2], &[(".go.buildinfo", buildinfo)]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
    }

    #[test]
    fn cargo_auditable_bomb_is_noted() {
        let bomb = zlib(&vec![0u8; 16 * 1024 * 1024]);
        assert!(bomb.len() <= MAX_CARGO_SECTION_BYTES as usize);
        let fixture = build_elf(true, true, &[2], &[(".dep-v0", bomb)]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert_eq!(out.kinds(), vec![CARGO_TOO_LARGE.to_owned()]);
    }

    #[test]
    fn deeply_nested_json_is_rejected() {
        let mut json = vec![b'['; 100_000];
        json.extend(std::iter::repeat_n(b']', 100_000));
        let fixture = build_elf(true, true, &[2], &[(".dep-v0", zlib(&json))]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }

    #[test]
    fn corrupt_zlib_is_ignored() {
        let fixture = build_elf(true, true, &[2], &[(".dep-v0", vec![0x78, 0x9c, 0xff])]);
        let out = inspect_bytes(fixture.bytes);
        assert!(out.packages().is_empty());
        assert!(out.kinds().is_empty());
    }
}
