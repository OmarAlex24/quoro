// SPDX-License-Identifier: AGPL-3.0-only
//! The exec channel between the engine and the init (`engine-trait.md`
//! section 6, ADR 0113): one vsock connection is one PTY session.
//!
//! Every frame is `[u8 type][u32 length, big-endian][payload]`, with a payload
//! of at most [`MAX_PAYLOAD`] bytes. A larger or unknown frame ends the
//! session. Host to guest: [`OPEN`] (JSON, exactly once, first), [`INPUT`],
//! [`RESIZE`] (`u16 rows`, `u16 cols`, big-endian), [`CLOSE`]. Guest to host:
//! [`OUTPUT`], [`EXIT`] (`i32`, big-endian, `128 + signal` when a signal ended
//! the program), [`ERROR`] (UTF-8 message; the guest then closes).
//!
//! Both sides use this module, so there is one copy of the format. It has no
//! dependencies: the open request's JSON is written and read by hand, and the
//! reader accepts exactly the shape the writer produces plus insignificant
//! whitespace, so a malformed open is refused rather than guessed at.

use std::fmt;

/// Host to guest: the open request, JSON, exactly once and first.
pub const OPEN: u8 = 1;
/// Host to guest: bytes for the program's input.
pub const INPUT: u8 = 2;
/// Host to guest: a new window size, `u16 rows` then `u16 cols`, big-endian.
pub const RESIZE: u8 = 3;
/// Host to guest: end the session (`SIGHUP` to its process group).
pub const CLOSE: u8 = 4;
/// Guest to host: bytes the program wrote to its terminal.
pub const OUTPUT: u8 = 129;
/// Guest to host: the program's exit code, `i32` big-endian.
pub const EXIT: u8 = 130;
/// Guest to host: a UTF-8 message; the guest closes after it.
pub const ERROR: u8 = 131;

/// Bytes of a frame header: the type and the big-endian length.
pub const HEADER_LEN: usize = 5;
/// The largest payload of one frame: 64 KiB.
pub const MAX_PAYLOAD: usize = 64 * 1024;

/// The most `argv` items an open may carry (`engine-trait.md` section 3.18).
pub const MAX_ARGS: usize = 64;
/// The longest `argv` item, in bytes.
pub const MAX_ARG_BYTES: usize = 4096;
/// The longest `user` value, in bytes.
pub const MAX_USER_BYTES: usize = 64;
/// The largest `rows` or `cols` value.
pub const MAX_DIMENSION: u16 = 1000;

/// A frame or an open request that breaks the format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameError(pub String);

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FrameError {}

/// What the host asks for when it opens a session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRequest {
    /// Never empty; no item contains NUL; at most [`MAX_ARGS`] items of at most
    /// [`MAX_ARG_BYTES`] bytes.
    pub argv: Vec<String>,
    /// A user name or uid, optionally `:` and a group; `None` is the app's user.
    pub user: Option<String>,
    /// 1 to [`MAX_DIMENSION`].
    pub rows: u16,
    /// 1 to [`MAX_DIMENSION`].
    pub cols: u16,
}

/// A frame the host sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostFrame {
    Open(OpenRequest),
    Input(Vec<u8>),
    Resize { rows: u16, cols: u16 },
    Close,
}

/// A frame the guest sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GuestFrame {
    Output(Vec<u8>),
    Exit(i32),
    Error(String),
}

/// Encodes one frame.
///
/// # Errors
///
/// A payload larger than [`MAX_PAYLOAD`].
pub fn encode_frame(kind: u8, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    if payload.len() > MAX_PAYLOAD {
        return Err(FrameError(format!(
            "frame payload is {} bytes, more than {MAX_PAYLOAD}",
            payload.len()
        )));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(kind);
    // The bound above keeps the length well inside u32.
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(payload);
    Ok(out)
}

/// Reads a frame header and checks its length against [`MAX_PAYLOAD`].
///
/// # Errors
///
/// A length larger than [`MAX_PAYLOAD`].
pub fn parse_header(header: [u8; HEADER_LEN]) -> Result<(u8, usize), FrameError> {
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    let len = usize::try_from(len).unwrap_or(usize::MAX);
    if len > MAX_PAYLOAD {
        return Err(FrameError(format!(
            "frame of type {} announces {len} bytes, more than {MAX_PAYLOAD}",
            header[0]
        )));
    }
    Ok((header[0], len))
}

/// A buffer that turns a byte stream into whole frames. It never holds more
/// than one frame's header and payload beyond what the caller fed it, and it
/// refuses a header that announces an oversized payload as soon as it sees it.
#[derive(Debug, Default)]
pub struct FrameReader {
    buf: Vec<u8>,
}

impl FrameReader {
    /// An empty reader.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends bytes read from the connection.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Bytes buffered and not yet returned as a frame.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    /// The next whole frame as `(type, payload)`, or `None` when more bytes
    /// are needed.
    ///
    /// # Errors
    ///
    /// A header that announces more than [`MAX_PAYLOAD`] bytes. The reader is
    /// then unusable: the caller ends the session.
    pub fn next_frame(&mut self) -> Result<Option<(u8, Vec<u8>)>, FrameError> {
        if self.buf.len() < HEADER_LEN {
            return Ok(None);
        }
        let mut header = [0_u8; HEADER_LEN];
        header.copy_from_slice(&self.buf[..HEADER_LEN]);
        let (kind, len) = parse_header(header)?;
        if self.buf.len() < HEADER_LEN + len {
            return Ok(None);
        }
        let payload = self.buf[HEADER_LEN..HEADER_LEN + len].to_vec();
        self.buf.drain(..HEADER_LEN + len);
        Ok(Some((kind, payload)))
    }
}

impl HostFrame {
    /// Encodes the frame.
    ///
    /// # Errors
    ///
    /// An open request that breaks [`OpenRequest::validate`], or a payload
    /// larger than [`MAX_PAYLOAD`].
    pub fn encode(&self) -> Result<Vec<u8>, FrameError> {
        match self {
            Self::Open(request) => encode_frame(OPEN, request.to_json()?.as_bytes()),
            Self::Input(bytes) => encode_frame(INPUT, bytes),
            Self::Resize { rows, cols } => {
                check_dimension("rows", *rows)?;
                check_dimension("cols", *cols)?;
                let mut payload = [0_u8; 4];
                payload[..2].copy_from_slice(&rows.to_be_bytes());
                payload[2..].copy_from_slice(&cols.to_be_bytes());
                encode_frame(RESIZE, &payload)
            }
            Self::Close => encode_frame(CLOSE, &[]),
        }
    }

    /// Decodes a frame the host sent.
    ///
    /// # Errors
    ///
    /// An unknown type, a malformed payload, or an open that breaks
    /// [`OpenRequest::validate`].
    pub fn decode(kind: u8, payload: &[u8]) -> Result<Self, FrameError> {
        match kind {
            OPEN => {
                let text = std::str::from_utf8(payload)
                    .map_err(|_| FrameError("the open request is not UTF-8".to_string()))?;
                Ok(Self::Open(OpenRequest::from_json(text)?))
            }
            INPUT => Ok(Self::Input(payload.to_vec())),
            RESIZE => {
                let [r0, r1, c0, c1] = <[u8; 4]>::try_from(payload).map_err(|_| {
                    FrameError(format!("a resize frame is 4 bytes, got {}", payload.len()))
                })?;
                let rows = u16::from_be_bytes([r0, r1]);
                let cols = u16::from_be_bytes([c0, c1]);
                check_dimension("rows", rows)?;
                check_dimension("cols", cols)?;
                Ok(Self::Resize { rows, cols })
            }
            CLOSE => {
                if payload.is_empty() {
                    Ok(Self::Close)
                } else {
                    Err(FrameError("a close frame carries no payload".to_string()))
                }
            }
            other => Err(FrameError(format!("unknown host frame type {other}"))),
        }
    }
}

impl GuestFrame {
    /// Encodes the frame. An error message longer than [`MAX_PAYLOAD`] is cut
    /// at a character boundary.
    ///
    /// # Errors
    ///
    /// Output larger than [`MAX_PAYLOAD`].
    pub fn encode(&self) -> Result<Vec<u8>, FrameError> {
        match self {
            Self::Output(bytes) => encode_frame(OUTPUT, bytes),
            Self::Exit(code) => encode_frame(EXIT, &code.to_be_bytes()),
            Self::Error(message) => {
                let mut end = message.len().min(MAX_PAYLOAD);
                while !message.is_char_boundary(end) {
                    end -= 1;
                }
                encode_frame(ERROR, &message.as_bytes()[..end])
            }
        }
    }

    /// Decodes a frame the guest sent. The guest is untrusted: an error
    /// message that is not UTF-8 is decoded lossily, never refused.
    ///
    /// # Errors
    ///
    /// An unknown type or an exit frame that is not 4 bytes.
    pub fn decode(kind: u8, payload: &[u8]) -> Result<Self, FrameError> {
        match kind {
            OUTPUT => Ok(Self::Output(payload.to_vec())),
            EXIT => {
                let bytes = <[u8; 4]>::try_from(payload).map_err(|_| {
                    FrameError(format!("an exit frame is 4 bytes, got {}", payload.len()))
                })?;
                Ok(Self::Exit(i32::from_be_bytes(bytes)))
            }
            ERROR => Ok(Self::Error(String::from_utf8_lossy(payload).into_owned())),
            other => Err(FrameError(format!("unknown guest frame type {other}"))),
        }
    }
}

fn check_dimension(name: &str, value: u16) -> Result<(), FrameError> {
    if value == 0 || value > MAX_DIMENSION {
        return Err(FrameError(format!(
            "{name} is {value}, outside 1 to {MAX_DIMENSION}"
        )));
    }
    Ok(())
}

impl OpenRequest {
    /// Checks the bounds of `engine-trait.md` section 3.18.
    ///
    /// # Errors
    ///
    /// An empty `argv`, too many or too long items, a NUL, an empty or too long
    /// `user`, or `rows`/`cols` outside 1 to [`MAX_DIMENSION`].
    pub fn validate(&self) -> Result<(), FrameError> {
        if self.argv.is_empty() {
            return Err(FrameError("argv is empty".to_string()));
        }
        if self.argv.len() > MAX_ARGS {
            return Err(FrameError(format!(
                "argv has {} items, more than {MAX_ARGS}",
                self.argv.len()
            )));
        }
        for (index, arg) in self.argv.iter().enumerate() {
            if arg.len() > MAX_ARG_BYTES {
                return Err(FrameError(format!(
                    "argv[{index}] is {} bytes, more than {MAX_ARG_BYTES}",
                    arg.len()
                )));
            }
            if arg.contains('\0') {
                return Err(FrameError(format!("argv[{index}] contains a NUL")));
            }
        }
        if self.argv[0].is_empty() {
            return Err(FrameError("argv[0] is empty".to_string()));
        }
        if let Some(user) = &self.user
            && (user.is_empty() || user.len() > MAX_USER_BYTES || user.contains('\0'))
        {
            return Err(FrameError(format!(
                "user must be 1 to {MAX_USER_BYTES} bytes without NUL"
            )));
        }
        check_dimension("rows", self.rows)?;
        check_dimension("cols", self.cols)
    }

    /// The JSON payload of the open frame:
    /// `{"argv":[...],"user":null|"...","rows":R,"cols":C}`.
    ///
    /// # Errors
    ///
    /// A request that breaks [`OpenRequest::validate`].
    pub fn to_json(&self) -> Result<String, FrameError> {
        self.validate()?;
        let mut out = String::from("{\"argv\":[");
        for (index, arg) in self.argv.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            push_json_string(&mut out, arg);
        }
        out.push_str("],\"user\":");
        match &self.user {
            Some(user) => push_json_string(&mut out, user),
            None => out.push_str("null"),
        }
        out.push_str(&format!(",\"rows\":{},\"cols\":{}}}", self.rows, self.cols));
        Ok(out)
    }

    /// Parses the JSON payload of an open frame. The four members may come in
    /// any order; `user` may be absent (same as `null`); any other member, a
    /// duplicate, or trailing data is refused.
    ///
    /// # Errors
    ///
    /// Malformed JSON, a missing or extra member, or a request that breaks
    /// [`OpenRequest::validate`].
    pub fn from_json(text: &str) -> Result<Self, FrameError> {
        let mut parser = Parser {
            bytes: text.as_bytes(),
            pos: 0,
        };
        let mut argv: Option<Vec<String>> = None;
        let mut user: Option<Option<String>> = None;
        let mut rows: Option<u16> = None;
        let mut cols: Option<u16> = None;

        parser.expect(b'{')?;
        if !parser.eat(b'}') {
            loop {
                let key = parser.string()?;
                parser.expect(b':')?;
                match key.as_str() {
                    "argv" if argv.is_none() => argv = Some(parser.string_array()?),
                    "user" if user.is_none() => {
                        user = Some(if parser.eat_literal(b"null") {
                            None
                        } else {
                            Some(parser.string()?)
                        });
                    }
                    "rows" if rows.is_none() => rows = Some(parser.dimension()?),
                    "cols" if cols.is_none() => cols = Some(parser.dimension()?),
                    other => {
                        return Err(FrameError(format!(
                            "unexpected or repeated member {other:?} in the open request"
                        )));
                    }
                }
                if parser.eat(b',') {
                    continue;
                }
                parser.expect(b'}')?;
                break;
            }
        }
        parser.skip_ws();
        if parser.pos != parser.bytes.len() {
            return Err(FrameError(
                "trailing data after the open request".to_string(),
            ));
        }
        let request = Self {
            argv: argv.ok_or_else(|| FrameError("the open request has no argv".to_string()))?,
            user: user.unwrap_or(None),
            rows: rows.ok_or_else(|| FrameError("the open request has no rows".to_string()))?,
            cols: cols.ok_or_else(|| FrameError("the open request has no cols".to_string()))?,
        };
        request.validate()?;
        Ok(request)
    }
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// A strict reader for the small JSON subset of the open request.
struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn skip_ws(&mut self) {
        while let Some(b' ' | b'\t' | b'\n' | b'\r') = self.bytes.get(self.pos) {
            self.pos += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> bool {
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&byte) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_literal(&mut self, literal: &[u8]) -> bool {
        self.skip_ws();
        if self.bytes[self.pos..].starts_with(literal) {
            self.pos += literal.len();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), FrameError> {
        if self.eat(byte) {
            Ok(())
        } else {
            Err(FrameError(format!(
                "expected {:?} at byte {} of the open request",
                char::from(byte),
                self.pos
            )))
        }
    }

    fn string_array(&mut self) -> Result<Vec<String>, FrameError> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        if self.eat(b']') {
            return Ok(items);
        }
        loop {
            if items.len() == MAX_ARGS {
                return Err(FrameError(format!("argv has more than {MAX_ARGS} items")));
            }
            items.push(self.string()?);
            if self.eat(b',') {
                continue;
            }
            self.expect(b']')?;
            return Ok(items);
        }
    }

    fn dimension(&mut self) -> Result<u16, FrameError> {
        self.skip_ws();
        let start = self.pos;
        while let Some(b'0'..=b'9') = self.bytes.get(self.pos) {
            self.pos += 1;
        }
        let digits = &self.bytes[start..self.pos];
        if digits.is_empty() || digits.len() > 5 || (digits.len() > 1 && digits[0] == b'0') {
            return Err(FrameError(format!(
                "expected a small integer at byte {start} of the open request"
            )));
        }
        let text = std::str::from_utf8(digits).unwrap_or("0");
        text.parse::<u16>()
            .map_err(|_| FrameError(format!("{text} is out of range")))
    }

    fn string(&mut self) -> Result<String, FrameError> {
        self.expect(b'"')?;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let Some(&byte) = self.bytes.get(self.pos) else {
                return Err(FrameError(
                    "unterminated string in the open request".to_string(),
                ));
            };
            self.pos += 1;
            match byte {
                b'"' => break,
                b'\\' => {
                    let Some(&escape) = self.bytes.get(self.pos) else {
                        return Err(FrameError("unterminated escape".to_string()));
                    };
                    self.pos += 1;
                    match escape {
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
                            let mut buf = [0_u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                        other => {
                            return Err(FrameError(format!(
                                "bad escape \\{} in the open request",
                                char::from(other)
                            )));
                        }
                    }
                }
                0x00..=0x1f => {
                    return Err(FrameError(
                        "control character inside a string in the open request".to_string(),
                    ));
                }
                other => out.push(other),
            }
            if out.len() > MAX_ARG_BYTES {
                return Err(FrameError(format!(
                    "a string in the open request is longer than {MAX_ARG_BYTES} bytes"
                )));
            }
        }
        String::from_utf8(out).map_err(|_| FrameError("a string is not UTF-8".to_string()))
    }

    fn hex4(&mut self) -> Result<u32, FrameError> {
        let Some(digits) = self.bytes.get(self.pos..self.pos + 4) else {
            return Err(FrameError("short \\u escape".to_string()));
        };
        let text = std::str::from_utf8(digits).map_err(|_| FrameError("bad \\u".to_string()))?;
        let value =
            u32::from_str_radix(text, 16).map_err(|_| FrameError("bad \\u escape".to_string()))?;
        self.pos += 4;
        Ok(value)
    }

    fn unicode_escape(&mut self) -> Result<char, FrameError> {
        let first = self.hex4()?;
        let code = if (0xd800..0xdc00).contains(&first) {
            if !self.bytes[self.pos..].starts_with(b"\\u") {
                return Err(FrameError("unpaired surrogate".to_string()));
            }
            self.pos += 2;
            let second = self.hex4()?;
            if !(0xdc00..0xe000).contains(&second) {
                return Err(FrameError("unpaired surrogate".to_string()));
            }
            0x10000 + ((first - 0xd800) << 10) + (second - 0xdc00)
        } else {
            first
        };
        char::from_u32(code).ok_or_else(|| FrameError("bad \\u escape".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> OpenRequest {
        OpenRequest {
            argv: vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo \"hi\"\n\u{1}é".to_string(),
            ],
            user: Some("1000:1000".to_string()),
            rows: 24,
            cols: 80,
        }
    }

    #[test]
    fn open_request_round_trips() {
        let request = request();
        let json = request.to_json().expect("encodes");
        assert_eq!(OpenRequest::from_json(&json).expect("decodes"), request);

        let anonymous = OpenRequest {
            user: None,
            ..request
        };
        let json = anonymous.to_json().expect("encodes");
        assert!(json.contains("\"user\":null"));
        assert_eq!(OpenRequest::from_json(&json).expect("decodes"), anonymous);
    }

    #[test]
    fn open_request_accepts_whitespace_order_and_a_missing_user() {
        let text = " { \"cols\" : 132 , \"argv\" : [ \"/bin/sh\" ] , \"rows\" : 50 } ";
        let parsed = OpenRequest::from_json(text).expect("decodes");
        assert_eq!(parsed.argv, vec!["/bin/sh".to_string()]);
        assert_eq!(parsed.user, None);
        assert_eq!((parsed.rows, parsed.cols), (50, 132));
        let escaped = r#"{"argv":["é😀\/"],"rows":1,"cols":1000}"#;
        assert_eq!(
            OpenRequest::from_json(escaped).expect("decodes").argv[0],
            "é😀/"
        );
    }

    #[test]
    fn open_request_refuses_bad_input() {
        for text in [
            "",
            "{}",
            r#"{"argv":[],"rows":1,"cols":1}"#,
            r#"{"argv":[""],"rows":1,"cols":1}"#,
            r#"{"argv":["sh"],"rows":0,"cols":1}"#,
            r#"{"argv":["sh"],"rows":1,"cols":1001}"#,
            r#"{"argv":["sh"],"rows":1,"cols":99999999}"#,
            r#"{"argv":["sh"],"rows":-1,"cols":1}"#,
            r#"{"argv":["sh"],"rows":1.5,"cols":1}"#,
            r#"{"argv":["sh"],"rows":01,"cols":1}"#,
            r#"{"argv":["sh"],"rows":1,"cols":1,"extra":1}"#,
            r#"{"argv":["sh"],"argv":["sh"],"rows":1,"cols":1}"#,
            r#"{"argv":["a\u0000"],"rows":1,"cols":1}"#,
            r#"{"argv":["sh"],"user":"","rows":1,"cols":1}"#,
            r#"{"argv":["sh"],"rows":1,"cols":1} x"#,
            r#"{"argv":["sh\x"],"rows":1,"cols":1}"#,
            r#"{"argv":["\ud800"],"rows":1,"cols":1}"#,
            "{\"argv\":[\"a\nb\"],\"rows\":1,\"cols\":1}",
        ] {
            assert!(
                OpenRequest::from_json(text).is_err(),
                "{text:?} should be refused"
            );
        }
        let too_many = format!(
            "{{\"argv\":[{}],\"rows\":1,\"cols\":1}}",
            vec!["\"a\""; MAX_ARGS + 1].join(",")
        );
        assert!(OpenRequest::from_json(&too_many).is_err());
        let too_long = format!(
            "{{\"argv\":[\"{}\"],\"rows\":1,\"cols\":1}}",
            "a".repeat(MAX_ARG_BYTES + 1)
        );
        assert!(OpenRequest::from_json(&too_long).is_err());
        let longest = format!(
            "{{\"argv\":[\"{}\"],\"rows\":1,\"cols\":1}}",
            "a".repeat(MAX_ARG_BYTES)
        );
        assert!(OpenRequest::from_json(&longest).is_ok());
    }

    #[test]
    fn frames_round_trip_through_the_reader() {
        let frames = [
            HostFrame::Open(request()).encode().expect("open"),
            HostFrame::Input(b"uname -a\n".to_vec())
                .encode()
                .expect("input"),
            HostFrame::Resize {
                rows: 50,
                cols: 132,
            }
            .encode()
            .expect("resize"),
            HostFrame::Close.encode().expect("close"),
        ];
        let stream: Vec<u8> = frames.concat();
        let mut reader = FrameReader::new();
        let mut decoded = Vec::new();
        // Feed one byte at a time: frames must reassemble across reads.
        for byte in stream {
            reader.push(&[byte]);
            while let Some((kind, payload)) = reader.next_frame().expect("frame") {
                decoded.push(HostFrame::decode(kind, &payload).expect("host frame"));
            }
        }
        assert_eq!(
            decoded,
            vec![
                HostFrame::Open(request()),
                HostFrame::Input(b"uname -a\n".to_vec()),
                HostFrame::Resize {
                    rows: 50,
                    cols: 132
                },
                HostFrame::Close,
            ]
        );
        assert_eq!(reader.buffered(), 0);

        for frame in [
            GuestFrame::Output(vec![0, 1, 255]),
            GuestFrame::Exit(-1),
            GuestFrame::Exit(130),
            GuestFrame::Error("no such user".to_string()),
        ] {
            let bytes = frame.encode().expect("encodes");
            let mut reader = FrameReader::new();
            reader.push(&bytes);
            let (kind, payload) = reader.next_frame().expect("ok").expect("whole");
            assert_eq!(GuestFrame::decode(kind, &payload).expect("decodes"), frame);
        }
    }

    #[test]
    fn oversized_and_unknown_frames_are_refused() {
        assert!(encode_frame(OUTPUT, &vec![0; MAX_PAYLOAD]).is_ok());
        assert!(encode_frame(OUTPUT, &vec![0; MAX_PAYLOAD + 1]).is_err());

        let mut reader = FrameReader::new();
        let len = u32::try_from(MAX_PAYLOAD + 1).expect("fits");
        let mut header = vec![OUTPUT];
        header.extend_from_slice(&len.to_be_bytes());
        reader.push(&header);
        assert!(
            reader.next_frame().is_err(),
            "refused from the header alone"
        );

        assert!(HostFrame::decode(9, &[]).is_err());
        assert!(
            HostFrame::decode(OUTPUT, &[]).is_err(),
            "guest types are not host frames"
        );
        assert!(GuestFrame::decode(INPUT, &[]).is_err());
        assert!(HostFrame::decode(RESIZE, &[0, 1, 0]).is_err());
        assert!(
            HostFrame::decode(RESIZE, &[0, 0, 0, 1]).is_err(),
            "zero rows"
        );
        assert!(HostFrame::decode(CLOSE, &[1]).is_err());
        assert!(GuestFrame::decode(EXIT, &[0, 0, 1]).is_err());
        assert_eq!(
            GuestFrame::decode(ERROR, &[0xff, b'x']).expect("lossy"),
            GuestFrame::Error("\u{fffd}x".to_string())
        );
        let long = GuestFrame::Error("é".repeat(MAX_PAYLOAD))
            .encode()
            .expect("cut");
        assert!(long.len() <= HEADER_LEN + MAX_PAYLOAD);
    }
}
