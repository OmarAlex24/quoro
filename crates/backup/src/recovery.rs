// SPDX-License-Identifier: AGPL-3.0-only
//! The recovery key and its text form (`backup-format.md` section 3).
//!
//! The recovery key is 32 random bytes the primary generates when it creates
//! the repository and each time it is replaced. It is shown to the owner once
//! and never stored in plaintext. Its text form is `qrk1-` followed by the
//! 256-bit value as a big-endian number in Crockford base32: 52 symbols with
//! the four leading padding bits zero, in 13 groups of 4 separated by `-`.
//!
//! Parsing ignores case, drops every `-`, reads `O` as `0` and `I` or `L` as
//! `1`, and refuses `U` and any other symbol, a wrong length and nonzero
//! padding bits.

use blake3::derive_key;
use zeroize::Zeroizing;

use crate::{Error, Key, Result, util};

/// Context of the recovery key wrap key, fixed for format version 1.
const WRAP_KEY_CONTEXT: &str = "quoro backup recovery wrap v1";
/// Context of the recovery key fingerprint, fixed for format version 1.
const FINGERPRINT_CONTEXT: &str = "quoro backup recovery fingerprint v1";

/// The text prefix of a recovery key (section 3).
const PREFIX: &str = "qrk1-";
/// The Crockford base32 alphabet, without `I`, `L`, `O` and `U`.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
/// Symbols in the text form: 52 symbols of 5 bits for 256 bits plus 4 padding.
const SYMBOLS: usize = 52;
/// Symbols in one `-` separated group.
const GROUP: usize = 4;
/// Leading zero bits of the 260-bit text form.
const PADDING_BITS: usize = 4;

/// A 256-bit recovery key only the owner holds (section 3).
///
/// It is not stored anywhere in plaintext; the repository config holds
/// `repo_key` wrapped under the key derived from it.
#[derive(Clone)]
pub struct RecoveryKey(Key);

impl RecoveryKey {
    /// 32 fresh random bytes from the operating-system generator.
    ///
    /// # Panics
    ///
    /// Panics if the operating system's random generator fails.
    #[must_use]
    pub fn generate() -> Self {
        Self(Key::generate())
    }

    /// The key from its 32 bytes, as read back from a key file.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Key::from_bytes(bytes))
    }

    /// The text form shown to the owner once: `qrk1-` and 13 groups of 4
    /// symbols, upper case.
    #[must_use]
    pub fn to_text(&self) -> String {
        let symbols = encode(self.0.as_bytes());
        let mut text = String::with_capacity(PREFIX.len() + SYMBOLS + SYMBOLS / GROUP);
        text.push_str(PREFIX);
        for (index, group) in symbols.as_bytes().chunks(GROUP).enumerate() {
            if index > 0 {
                text.push('-');
            }
            text.push_str(std::str::from_utf8(group).expect("the alphabet is ASCII"));
        }
        text
    }

    /// Parses the text form, as an owner types it back in.
    ///
    /// Surrounding whitespace is ignored, the `qrk1-` prefix is
    /// case-insensitive, every `-` after it is dropped, and `O`, `I` and `L`
    /// read as `0`, `1` and `1`.
    ///
    /// # Errors
    ///
    /// [`Error::RecoveryKey`] for a missing or wrong prefix, a wrong number of
    /// symbols, an unknown symbol such as `U`, or nonzero padding bits.
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim();
        let bytes = text.as_bytes();
        if bytes.len() < PREFIX.len() || !bytes[..PREFIX.len()].eq_ignore_ascii_case(b"qrk1-") {
            return Err(Error::RecoveryKey(
                "the text does not start with qrk1-".into(),
            ));
        }
        // The prefix matched case-insensitively as ASCII, so this boundary is
        // a character boundary.
        let rest = &text[PREFIX.len()..];
        // Every buffer that holds the symbols or the key is wiped when it
        // drops, on the error paths too. `symbols` is a fixed array, so no
        // growing buffer leaves an unwiped copy behind.
        let mut symbols = Zeroizing::new([0u8; SYMBOLS]);
        let mut count = 0;
        for byte in rest.bytes() {
            if byte == b'-' {
                continue;
            }
            if count == SYMBOLS {
                return Err(Error::RecoveryKey(
                    "the text has more than 52 symbols".into(),
                ));
            }
            symbols[count] = byte;
            count += 1;
        }
        if count != SYMBOLS {
            return Err(Error::RecoveryKey(
                "the text does not have 52 symbols".into(),
            ));
        }
        let bytes = decode(&symbols)?;
        Ok(Self(Key::from_bytes(*bytes)))
    }

    /// The first 16 hex characters of `derive_key("quoro backup recovery
    /// fingerprint v1", recovery_key)`, as the config and the API show it.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        let digest = derive_key(FINGERPRINT_CONTEXT, self.0.as_bytes());
        util::to_hex(&digest[..8])
    }

    /// The key that wraps `repo_key` in the config object (section 3). It is
    /// derived, never stored.
    pub(crate) fn wrap_key(&self) -> Key {
        Key::from_bytes(derive_key(WRAP_KEY_CONTEXT, self.0.as_bytes()))
    }
}

impl std::fmt::Debug for RecoveryKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RecoveryKey")
            .field(&self.fingerprint())
            .finish()
    }
}

/// The 260-bit text form: 4 padding bits then the 256 bits, 5 bits per symbol.
fn encode(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(SYMBOLS);
    for symbol in 0..SYMBOLS {
        let mut value = 0u8;
        for bit in 0..5 {
            let position = symbol * 5 + bit;
            let bit = if position < PADDING_BITS {
                0
            } else {
                let index = position - PADDING_BITS;
                (bytes[index / 8] >> (7 - index % 8)) & 1
            };
            value = (value << 1) | bit;
        }
        out.push(char::from(ALPHABET[usize::from(value)]));
    }
    out
}

/// The 32 bytes of 52 symbols, refusing unknown symbols and nonzero padding.
/// The bytes are wiped when they drop, on an error too.
fn decode(symbols: &[u8; SYMBOLS]) -> Result<Zeroizing<[u8; 32]>> {
    let mut bytes = Zeroizing::new([0u8; 32]);
    for (symbol, text) in symbols.iter().enumerate() {
        let value = value_of(*text)?;
        for bit in 0..5 {
            let position = symbol * 5 + bit;
            let bit_value = (value >> (4 - bit)) & 1;
            if position < PADDING_BITS {
                if bit_value != 0 {
                    return Err(Error::RecoveryKey(
                        "the leading padding bits are not zero".into(),
                    ));
                }
            } else {
                let index = position - PADDING_BITS;
                bytes[index / 8] |= bit_value << (7 - index % 8);
            }
        }
    }
    Ok(bytes)
}

/// One Crockford symbol, with the aliases section 3 fixes.
fn value_of(byte: u8) -> Result<u8> {
    let upper = byte.to_ascii_uppercase();
    match upper {
        b'O' => Ok(0),
        b'I' | b'L' => Ok(1),
        _ => ALPHABET
            .iter()
            .position(|candidate| *candidate == upper)
            .map(|index| index as u8)
            .ok_or_else(|| Error::RecoveryKey(format!("unknown symbol {:?}", char::from(byte)))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: [u8; 32]) -> RecoveryKey {
        RecoveryKey::from_bytes(bytes)
    }

    #[test]
    fn text_round_trips_and_is_upper_case_in_groups() {
        let key = key([0x11; 32]);
        let text = key.to_text();
        assert!(text.starts_with("qrk1-"));
        assert_eq!(text.len(), 5 + SYMBOLS + 12);
        assert!(
            text[PREFIX.len()..]
                .chars()
                .all(|c| !c.is_ascii_lowercase())
        );
        for group in text[5..].split('-') {
            assert_eq!(group.len(), GROUP);
        }
        let parsed = RecoveryKey::parse(&text).unwrap();
        assert_eq!(parsed.fingerprint(), key.fingerprint());
        assert_eq!(
            RecoveryKey::parse(&format!("  {text}\n"))
                .unwrap()
                .to_text(),
            text
        );
    }

    #[test]
    fn the_first_symbol_carries_only_the_padding_bits() {
        for byte in [0x00u8, 0x01, 0x7f, 0x80, 0xff] {
            let text = key([byte; 32]).to_text();
            let first = text.as_bytes()[PREFIX.len()];
            assert!(first == b'0' || first == b'1', "{text}");
        }
    }

    #[test]
    fn nonzero_padding_bits_are_refused() {
        // Set the top bit of the value: 52 symbols then start with `8`.
        let text = key([0xff; 32]).to_text();
        let mut symbols: Vec<char> = text[PREFIX.len()..].chars().filter(|c| *c != '-').collect();
        symbols[0] = '3';
        let mangled: String = symbols.iter().collect();
        let error = RecoveryKey::parse(&format!("qrk1-{mangled}")).unwrap_err();
        assert!(matches!(error, Error::RecoveryKey(_)));
    }

    #[test]
    fn aliases_case_and_wrong_lengths() {
        let text = key([0xab; 32]).to_text();
        let lower = text.to_ascii_lowercase();
        assert_eq!(
            RecoveryKey::parse(&lower).unwrap().fingerprint(),
            key([0xab; 32]).fingerprint()
        );

        // `O`, `I` and `L` read as `0`, `1` and `1`. Only the symbol part is
        // rewritten: the prefix itself contains `1`.
        let alias = |text: &str, from: char, to: &str| {
            let (prefix, symbols) = text.split_at(PREFIX.len());
            format!("{prefix}{}", symbols.replacen(from, to, 1))
        };
        let base = key([0x00; 32]).to_text();
        assert_eq!(
            RecoveryKey::parse(&alias(&base, '0', "O"))
                .unwrap()
                .fingerprint(),
            key([0x00; 32]).fingerprint()
        );
        let one = key([0xff; 32]).to_text();
        assert_eq!(
            RecoveryKey::parse(&alias(&one, '1', "I"))
                .unwrap()
                .fingerprint(),
            key([0xff; 32]).fingerprint()
        );
        assert_eq!(
            RecoveryKey::parse(&alias(&one, '1', "l"))
                .unwrap()
                .fingerprint(),
            key([0xff; 32]).fingerprint()
        );

        let symbols: String = text[PREFIX.len()..].chars().filter(|c| *c != '-').collect();
        assert!(RecoveryKey::parse(&format!("qrk1-{}", &symbols[..51])).is_err());
        assert!(RecoveryKey::parse(&format!("qrk1-{symbols}0")).is_err());
        assert!(RecoveryKey::parse(&symbols).is_err());
        assert!(RecoveryKey::parse("").is_err());
    }

    #[test]
    fn u_and_other_symbols_are_refused() {
        let text = key([0x42; 32]).to_text();
        let symbols: String = text[PREFIX.len()..].chars().filter(|c| *c != '-').collect();
        let mut replaced: Vec<char> = symbols.chars().collect();
        replaced[10] = 'U';
        let mangled: String = replaced.iter().collect();
        assert!(RecoveryKey::parse(&format!("qrk1-{mangled}")).is_err());
        let mut replaced: Vec<char> = symbols.chars().collect();
        replaced[10] = '$';
        let mangled: String = replaced.iter().collect();
        assert!(RecoveryKey::parse(&format!("qrk1-{mangled}")).is_err());
    }

    #[test]
    fn fingerprint_is_16_lowercase_hex() {
        let fingerprint = key([0x7f; 32]).fingerprint();
        assert_eq!(fingerprint.len(), 16);
        assert!(
            fingerprint
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_ne!(key([1; 32]).fingerprint(), key([2; 32]).fingerprint());
    }

    #[test]
    fn debug_prints_the_fingerprint_not_the_key() {
        let key = key([0x42; 32]);
        let rendered = format!("{key:?}");
        assert!(rendered.contains(&key.fingerprint()));
        assert!(!rendered.contains("4242"));
    }

    #[test]
    fn text_form_and_fingerprint_match_pinned_vectors() {
        // Computed independently (big-endian number, Crockford base32, the
        // fingerprint by BLAKE3 derive_key) so a change of encoding fails.
        let key = RecoveryKey::from_bytes([0x42; 32]);
        assert_eq!(
            key.to_text(),
            "qrk1-0GJ2-8914-4GJ2-8914-4GJ2-8914-4GJ2-8914-4GJ2-8914-4GJ2-8914-4GJ2"
        );
        assert_eq!(key.fingerprint(), "47b359f0b1576a7d");
        let max = RecoveryKey::from_bytes([0xff; 32]);
        assert_eq!(
            max.to_text(),
            "qrk1-1ZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZ"
        );
        assert_eq!(max.fingerprint(), "dc50c6b359d41e6f");
        let parsed = RecoveryKey::parse(
            " QRK1-0gj2-8914-4gj2-8914-4gj2-8914-4gj2-8914-4gj2-8914-4gj2-8914-4gj2 ",
        )
        .expect("lower case parses");
        assert_eq!(parsed.fingerprint(), key.fingerprint());
    }
}
