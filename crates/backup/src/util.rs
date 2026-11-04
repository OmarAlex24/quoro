// SPDX-License-Identifier: AGPL-3.0-only
//! Lowercase hex and the operating-system random generator.
//!
//! Chunk ids and digests are 64 lowercase hex characters (section 5). No `hex`
//! crate is worth its bytes for two functions.

/// The lowercase hex form of `bytes`.
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

/// Parses exactly 64 lowercase hex characters into 32 bytes.
pub(crate) fn from_hex_32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    let raw = text.as_bytes();
    for (index, pair) in raw.as_chunks::<2>().0.iter().enumerate() {
        let high = digit(pair[0])?;
        let low = digit(pair[1])?;
        bytes[index] = (high << 4) | low;
    }
    Some(bytes)
}

/// One lowercase hex digit, `None` for anything else (upper case included).
fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// `N` bytes from the operating-system generator.
///
/// # Panics
///
/// Panics if the operating system's random generator fails; every caller in
/// this crate needs randomness to produce a valid object.
pub(crate) fn random_bytes<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).expect("OS random generator");
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_refuses_anything_else() {
        let bytes = [0x00, 0x0f, 0xa5, 0xff];
        assert_eq!(to_hex(&bytes), "000fa5ff");
        let id = [0xabu8; 32];
        assert_eq!(from_hex_32(&to_hex(&id)), Some(id));
        assert_eq!(from_hex_32(&to_hex(&id).to_uppercase()), None);
        assert_eq!(from_hex_32("00"), None);
        assert_eq!(from_hex_32(&"0".repeat(64)), Some([0; 32]));
        assert_eq!(from_hex_32(&"g".repeat(64)), None);
    }
}
