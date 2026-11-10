// SPDX-License-Identifier: AGPL-3.0-only
//! Key custody seals (`node-agent-protocol.md` section 7.2, ADR 0180,
//! ADR 0231).
//!
//! Every machine with the `node` role holds an X25519 custody key, a
//! [`CustodyKey`], and reports its public half, a [`CustodyPublicKey`], in
//! its enroll request and in every `hello`. The primary seals each cluster key
//! to that public key with [`seal`]; the machine opens a seal with [`open`].
//!
//! The construction is HPKE (RFC 9180) in base mode with one fixed suite:
//! DHKEM(X25519, HKDF-SHA256), HKDF-SHA256 and ChaCha20-Poly1305 (KEM
//! `0x0020`, KDF `0x0001`, AEAD `0x0003`). `info` binds a seal to its cluster,
//! machine, key kind and key ([`SealContext::info`]), and the AAD is empty. A
//! sealed value is the 32-byte encapsulated key followed by the ciphertext and
//! its 16-byte tag.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use hpke::rand_core::{CryptoRng, UnwrapErr};
use hpke::{Deserializable as _, Kem as _, OpModeR, OpModeS, Serializable as _};
use serde::{Deserialize, Serialize};

/// The wrapper that wipes a serialized private key and an opened seal on
/// drop, so callers can name the types this module returns.
pub use zeroize::Zeroizing;

type Kem = hpke::kem::X25519HkdfSha256;
type Kdf = hpke::kdf::HkdfSha256;
type Aead = hpke::aead::ChaCha20Poly1305;
type PublicKey = <Kem as hpke::Kem>::PublicKey;
type PrivateKey = <Kem as hpke::Kem>::PrivateKey;
type EncappedKey = <Kem as hpke::Kem>::EncappedKey;

/// Length of a custody private or public key, in bytes.
pub const KEY_LEN: usize = 32;
/// Length of the encapsulated key that starts every sealed value.
pub const ENC_LEN: usize = 32;
/// Length of the ChaCha20-Poly1305 tag that ends every sealed value.
pub const TAG_LEN: usize = 16;
/// The text `info` starts with (section 7.2).
pub const INFO_LABEL: &str = "quoro custody v1";
/// The longest `info` a seal takes. HPKE refuses an `info` of 2^16 bytes or
/// more; the five members of a real one are a few dozen bytes each.
pub const MAX_INFO_LEN: usize = 4096;

/// Why a custody key could not be read, or a seal made or opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CustodyError {
    /// A key is not 32 bytes, or a public key is not standard base64 of 32
    /// bytes.
    BadKey,
    /// A member of the seal's context holds a zero byte, which would make
    /// `info` ambiguous, or `info` is longer than [`MAX_INFO_LEN`].
    BadContext,
    /// The recipient's public key gives no shared secret (a low-order point),
    /// so nothing can be sealed to it.
    Seal,
    /// The sealed value is shorter than the encapsulated key and the tag.
    TooShort,
    /// The seal does not open: another key, another context, or a changed
    /// byte.
    Open,
}

impl std::fmt::Display for CustodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadKey => f.write_str("the custody key is not a 32-byte X25519 key"),
            Self::BadContext => f.write_str("the seal context cannot be encoded as info"),
            Self::Seal => f.write_str("nothing can be sealed to this custody public key"),
            Self::TooShort => f.write_str("the sealed value is too short"),
            Self::Open => f.write_str("the seal does not open"),
        }
    }
}

impl std::error::Error for CustodyError {}

/// The kind of cluster key a seal holds (`key_kind`, section 7.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyKind {
    /// The 32-byte Ed25519 seed of a cluster signing key.
    Signing,
    /// The 32-byte cluster data-encryption key.
    Dek,
    /// The primary's TLS certificate chain and private key.
    Tls,
}

impl KeyKind {
    /// The text `key_kind` carries, in `info` and in `node_key_custody`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Signing => "signing",
            Self::Dek => "dek",
            Self::Tls => "tls",
        }
    }
}

/// What a seal is bound to: its cluster, machine, key kind and key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealContext<'a> {
    /// `cls_` id of the cluster.
    pub cluster_id: &'a str,
    /// `nod_` id of the machine the seal is made for.
    pub node_id: &'a str,
    /// The kind of key sealed.
    pub key_kind: KeyKind,
    /// The sealed key's `key_id` (section 7.2).
    pub key_id: &'a str,
}

impl SealContext<'_> {
    /// The HPKE `info`: the ASCII text [`INFO_LABEL`], `cluster_id`,
    /// `node_id`, `key_kind` and `key_id`, each followed by one zero byte
    /// (section 7.2, ADR 0231).
    ///
    /// # Errors
    ///
    /// [`CustodyError::BadContext`] when a member holds a zero byte, which
    /// would let two contexts share one `info`, or when `info` would be
    /// longer than [`MAX_INFO_LEN`].
    pub fn info(&self) -> Result<Vec<u8>, CustodyError> {
        let parts = [
            INFO_LABEL,
            self.cluster_id,
            self.node_id,
            self.key_kind.as_str(),
            self.key_id,
        ];
        let mut info = Vec::with_capacity(parts.iter().map(|part| part.len() + 1).sum());
        for part in parts {
            if part.as_bytes().contains(&0) {
                return Err(CustodyError::BadContext);
            }
            info.extend_from_slice(part.as_bytes());
            info.push(0);
        }
        if info.len() > MAX_INFO_LEN {
            return Err(CustodyError::BadContext);
        }
        Ok(info)
    }
}

/// The operating system's generator, as the RNG [`CustodyKey::generate`] and
/// [`seal`] take.
///
/// # Panics
///
/// The generator panics when the operating system provides no random bytes,
/// as the node agent's own random helpers do: there is no safe way to go on
/// without a key or an ephemeral secret.
#[must_use]
pub fn os_rng() -> impl CryptoRng {
    UnwrapErr(getrandom::SysRng)
}

/// The public half of a custody key, as `custody_public_key` carries it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustodyPublicKey(PublicKey);

impl CustodyPublicKey {
    /// Parses 32 raw bytes.
    ///
    /// # Errors
    ///
    /// [`CustodyError::BadKey`] for any other length.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CustodyError> {
        PublicKey::from_bytes(bytes)
            .map(Self)
            .map_err(|_| CustodyError::BadKey)
    }

    /// Parses the standard base64 form of 32 bytes (section 5.1).
    ///
    /// # Errors
    ///
    /// [`CustodyError::BadKey`] for anything else.
    pub fn from_base64(text: &str) -> Result<Self, CustodyError> {
        let bytes = STANDARD
            .decode(text.trim())
            .map_err(|_| CustodyError::BadKey)?;
        Self::from_bytes(&bytes)
    }

    /// The 32 raw bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; KEY_LEN] {
        let mut bytes = [0u8; KEY_LEN];
        self.0.write_exact(&mut bytes);
        bytes
    }

    /// The standard base64 form (44 characters) `hello` and the enroll
    /// request carry.
    #[must_use]
    pub fn to_base64(&self) -> String {
        STANDARD.encode(self.to_bytes())
    }
}

/// A custody key: an X25519 private key and its public half.
///
/// The private key is wiped from memory when the value is dropped, and its
/// [`std::fmt::Debug`] form never shows it.
#[derive(Clone)]
pub struct CustodyKey {
    private: PrivateKey,
    public: CustodyPublicKey,
}

impl CustodyKey {
    /// Generates a key pair from a CSPRNG, such as [`os_rng`].
    #[must_use]
    pub fn generate(rng: &mut impl CryptoRng) -> Self {
        let (private, public) = Kem::gen_keypair_with_rng(rng);
        Self {
            private,
            public: CustodyPublicKey(public),
        }
    }

    /// Parses a 32-byte private key, as [`CustodyKey::to_bytes`] writes it.
    ///
    /// # Errors
    ///
    /// [`CustodyError::BadKey`] for any other length.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CustodyError> {
        let private = PrivateKey::from_bytes(bytes).map_err(|_| CustodyError::BadKey)?;
        let public = CustodyPublicKey(Kem::sk_to_pk(&private));
        Ok(Self { private, public })
    }

    /// The 32-byte private key, wiped when the returned value is dropped.
    #[must_use]
    pub fn to_bytes(&self) -> Zeroizing<[u8; KEY_LEN]> {
        let mut bytes = Zeroizing::new([0u8; KEY_LEN]);
        self.private.write_exact(&mut bytes[..]);
        bytes
    }

    /// The public half.
    #[must_use]
    pub fn public_key(&self) -> &CustodyPublicKey {
        &self.public
    }
}

impl std::fmt::Debug for CustodyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustodyKey")
            .field("public", &self.public.to_base64())
            .finish_non_exhaustive()
    }
}

/// Seals `plaintext` to `recipient` under `context` (section 7.2): the
/// 32-byte encapsulated key, then the ciphertext and its 16-byte tag, so
/// 48 bytes more than the plaintext.
///
/// # Errors
///
/// [`CustodyError::BadContext`] from [`SealContext::info`], and
/// [`CustodyError::Seal`] when `recipient` gives no shared secret.
pub fn seal(
    recipient: &CustodyPublicKey,
    context: &SealContext<'_>,
    plaintext: &[u8],
    rng: &mut impl CryptoRng,
) -> Result<Vec<u8>, CustodyError> {
    seal_with_aad(recipient, &context.info()?, &[], plaintext, rng)
}

/// Opens a value [`seal`] made for `key` under `context`.
///
/// # Errors
///
/// [`CustodyError::BadContext`] from [`SealContext::info`],
/// [`CustodyError::TooShort`] for fewer than 48 bytes, and
/// [`CustodyError::Open`] for anything that does not open.
pub fn open(
    key: &CustodyKey,
    context: &SealContext<'_>,
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, CustodyError> {
    open_with_aad(key, &context.info()?, &[], sealed)
}

/// [`seal`] with any `info` and AAD; custody always uses an empty AAD. The
/// RFC 9180 vectors use a non-empty one, so the tests reach the suite here.
fn seal_with_aad(
    recipient: &CustodyPublicKey,
    info: &[u8],
    aad: &[u8],
    plaintext: &[u8],
    rng: &mut impl CryptoRng,
) -> Result<Vec<u8>, CustodyError> {
    let (encapped, ciphertext) = hpke::single_shot_seal_with_rng::<Aead, Kdf, Kem>(
        &OpModeS::Base,
        &recipient.0,
        info,
        plaintext,
        aad,
        rng,
    )
    .map_err(|_| CustodyError::Seal)?;
    let mut sealed = Vec::with_capacity(ENC_LEN + ciphertext.len());
    sealed.extend_from_slice(&encapped.to_bytes());
    sealed.extend_from_slice(&ciphertext);
    Ok(sealed)
}

/// [`open`] with any `info` and AAD.
fn open_with_aad(
    key: &CustodyKey,
    info: &[u8],
    aad: &[u8],
    sealed: &[u8],
) -> Result<Zeroizing<Vec<u8>>, CustodyError> {
    if sealed.len() < ENC_LEN + TAG_LEN {
        return Err(CustodyError::TooShort);
    }
    let (encapped, ciphertext) = sealed.split_at(ENC_LEN);
    let encapped = EncappedKey::from_bytes(encapped).map_err(|_| CustodyError::Open)?;
    hpke::single_shot_open::<Aead, Kdf, Kem>(
        &OpModeR::Base,
        &key.private,
        &encapped,
        info,
        ciphertext,
        aad,
    )
    .map(Zeroizing::new)
    .map_err(|_| CustodyError::Open)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hpke::rand_core::{Infallible, TryCryptoRng, TryRng};

    // RFC 9180 Appendix A.2.1, "DHKEM(X25519, HKDF-SHA256), HKDF-SHA256,
    // ChaCha20Poly1305", Base Setup Information (mode 0, kem_id 32, kdf_id 1,
    // aead_id 3), and its first encryption, sequence number 0 (A.2.1.1).
    // Copied from https://www.rfc-editor.org/rfc/rfc9180.txt.
    const RFC_INFO: &str = "4f6465206f6e2061204772656369616e2055726e";
    const RFC_IKM_E: &str = "909a9b35d3dc4713a5e72a4da274b55d3d3821a37e5d099e74a647db583a904b";
    const RFC_IKM_R: &str = "1ac01f181fdf9f352797655161c58b75c656a6cc2716dcb66372da835542e1df";
    const RFC_PK_RM: &str = "4310ee97d88cc1f088a5576c77ab0cf5c3ac797f3d95139c6c84b5429c59662a";
    const RFC_SK_RM: &str = "8057991eef8f1f1af18f4a9491d16a1ce333f695d4db8e38da75975c4478e0fb";
    const RFC_ENC: &str = "1afa08d3dec047a643885163f1180476fa7ddb54c6a8029ea33f95796bf2ac4a";
    const RFC_PT: &str = "4265617574792069732074727574682c20747275746820626561757479";
    const RFC_AAD: &str = "436f756e742d30";
    const RFC_CT: &str = "1c5250d8034ec2b784ba2cfd69dbdb8af406cfe3ff938e131f0def8c8b60b4db\
                          21993c62ce81883d2dd1b51a28";

    fn hex(text: &str) -> Vec<u8> {
        let text: String = text.chars().filter(|c| !c.is_whitespace()).collect();
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    }

    /// An RNG that hands out `bytes` once, so a seal's ephemeral key is the
    /// one the vector derives from `ikmE`. A second draw panics: the seal
    /// must take exactly one 32-byte draw.
    struct OnceRng(Option<Vec<u8>>);

    impl TryRng for OnceRng {
        type Error = Infallible;

        fn try_next_u32(&mut self) -> Result<u32, Infallible> {
            unreachable!("the seal draws bytes, not words")
        }

        fn try_next_u64(&mut self) -> Result<u64, Infallible> {
            unreachable!("the seal draws bytes, not words")
        }

        fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Infallible> {
            let bytes = self.0.take().expect("one draw only");
            dst.copy_from_slice(&bytes);
            Ok(())
        }
    }

    impl TryCryptoRng for OnceRng {}

    fn context() -> SealContext<'static> {
        SealContext {
            cluster_id: "cls_01JC9Y0000000000000000000A",
            node_id: "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZP",
            key_kind: KeyKind::Signing,
            key_id: "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a35",
        }
    }

    /// RFC 9180 A.2.1: `DeriveKeyPair(ikmR)` gives `pkRm`, and the vector's
    /// own `skRm` parses to a key whose public half is `pkRm`.
    #[test]
    fn the_rfc_9180_recipient_key_derives_from_ikm_r() {
        let (_, public) = Kem::derive_keypair(&hex(RFC_IKM_R));
        assert_eq!(CustodyPublicKey(public).to_bytes().to_vec(), hex(RFC_PK_RM));
        let key = CustodyKey::from_bytes(&hex(RFC_SK_RM)).unwrap();
        assert_eq!(key.public_key().to_bytes().to_vec(), hex(RFC_PK_RM));
    }

    /// RFC 9180 A.2.1.1, sequence number 0: `skRm` opens `enc` and `ct` under
    /// the vector's `info` and `aad` to its `pt`.
    #[test]
    fn the_rfc_9180_first_encryption_opens() {
        let key = CustodyKey::from_bytes(&hex(RFC_SK_RM)).unwrap();
        let mut sealed = hex(RFC_ENC);
        sealed.extend(hex(RFC_CT));
        let plaintext = open_with_aad(&key, &hex(RFC_INFO), &hex(RFC_AAD), &sealed).unwrap();
        assert_eq!(plaintext.to_vec(), hex(RFC_PT));
    }

    /// RFC 9180 A.2.1.1: sealing `pt` to `pkRm` with the ephemeral key that
    /// `ikmE` derives gives the vector's `enc` followed by its `ct`.
    #[test]
    fn sealing_with_the_rfc_9180_ephemeral_key_gives_its_enc_and_ct() {
        let recipient = CustodyPublicKey::from_bytes(&hex(RFC_PK_RM)).unwrap();
        let mut rng = OnceRng(Some(hex(RFC_IKM_E)));
        let sealed = seal_with_aad(
            &recipient,
            &hex(RFC_INFO),
            &hex(RFC_AAD),
            &hex(RFC_PT),
            &mut rng,
        )
        .unwrap();
        let mut expected = hex(RFC_ENC);
        expected.extend(hex(RFC_CT));
        assert_eq!(sealed, expected);
    }

    /// Section 7.2: the public [`seal`] and [`open`] pass the context's
    /// `info` as HPKE `info` and an empty AAD. A seal made by [`seal`] with
    /// the RFC 9180 A.2.1 ephemeral key (from `ikmE`) to `pkRm` starts with
    /// the vector's `enc` and opens through [`open_with_aad`] under
    /// `ctx.info()` and an empty AAD, and under nothing else; a seal made by
    /// [`seal_with_aad`] that way opens through [`open`]. Swapping `info`
    /// and the AAD, or passing any AAD, in either public function fails it.
    #[test]
    fn the_public_seal_and_open_use_the_context_as_info_and_an_empty_aad() {
        let key = CustodyKey::from_bytes(&hex(RFC_SK_RM)).unwrap();
        let ctx = context();
        let info = ctx.info().unwrap();
        let plaintext = hex(RFC_PT);
        let rfc_aad = hex(RFC_AAD);

        let mut rng = OnceRng(Some(hex(RFC_IKM_E)));
        let sealed = seal(key.public_key(), &ctx, &plaintext, &mut rng).unwrap();
        assert_eq!(sealed[..ENC_LEN].to_vec(), hex(RFC_ENC));
        assert_eq!(sealed.len(), ENC_LEN + plaintext.len() + TAG_LEN);
        assert_eq!(
            open_with_aad(&key, &info, &[], &sealed).unwrap().to_vec(),
            plaintext,
            "a public seal opens under info and an empty AAD"
        );
        for (what, other_info, aad) in [
            ("info and the AAD swapped", &[][..], &info[..]),
            ("the context as AAD too", &info[..], &info[..]),
            ("the vector's AAD", &info[..], &rfc_aad[..]),
            ("no info", &[][..], &[][..]),
        ] {
            assert_eq!(
                open_with_aad(&key, other_info, aad, &sealed),
                Err(CustodyError::Open),
                "a public seal must not open with {what}"
            );
        }

        let mut rng = OnceRng(Some(hex(RFC_IKM_E)));
        let made = seal_with_aad(key.public_key(), &info, &[], &plaintext, &mut rng).unwrap();
        assert_eq!(made, sealed, "the public seal is seal_with_aad(info, [])");
        assert_eq!(
            open(&key, &ctx, &made).unwrap().to_vec(),
            plaintext,
            "the public open takes info and an empty AAD"
        );
        let mut rng = OnceRng(Some(hex(RFC_IKM_E)));
        let swapped = seal_with_aad(key.public_key(), &[], &info, &plaintext, &mut rng).unwrap();
        assert_eq!(
            open(&key, &ctx, &swapped),
            Err(CustodyError::Open),
            "the public open must not take info as the AAD"
        );
    }

    /// Section 7.2: `info` is the label, `cluster_id`, `node_id`, `key_kind`
    /// and `key_id`, each followed by one zero byte.
    #[test]
    fn info_is_the_label_and_the_four_members_each_ended_by_a_zero_byte() {
        let context = SealContext {
            cluster_id: "cls_1",
            node_id: "nod_2",
            key_kind: KeyKind::Dek,
            key_id: "0a1b",
        };
        assert_eq!(
            context.info().unwrap(),
            b"quoro custody v1\x00cls_1\x00nod_2\x00dek\x000a1b\x00".to_vec()
        );
        for (kind, text) in [
            (KeyKind::Signing, "signing"),
            (KeyKind::Dek, "dek"),
            (KeyKind::Tls, "tls"),
        ] {
            assert_eq!(kind.as_str(), text);
            assert_eq!(serde_json::to_value(kind).unwrap(), text);
        }
    }

    /// A zero byte inside a member would let two contexts share one `info`,
    /// and an `info` past [`MAX_INFO_LEN`] would make HPKE panic: both are
    /// refused before anything is sealed or opened.
    #[test]
    fn a_context_that_cannot_be_encoded_is_refused() {
        let key = CustodyKey::generate(&mut os_rng());
        let long = "a".repeat(MAX_INFO_LEN);
        for context in [
            SealContext {
                node_id: "nod_1\0dek",
                ..context()
            },
            SealContext {
                key_id: &long,
                ..context()
            },
        ] {
            assert_eq!(context.info(), Err(CustodyError::BadContext));
            assert_eq!(
                seal(key.public_key(), &context, b"x", &mut os_rng()),
                Err(CustodyError::BadContext)
            );
        }
    }

    /// A seal opens with its key and context to its plaintext, and is 32 + n
    /// + 16 bytes long; two seals of one plaintext differ.
    #[test]
    fn a_seal_opens_to_its_plaintext_and_is_48_bytes_longer() {
        let key = CustodyKey::generate(&mut os_rng());
        for plaintext in [&b""[..], &[7u8; 32][..], &[1u8; 1000][..]] {
            let sealed = seal(key.public_key(), &context(), plaintext, &mut os_rng()).unwrap();
            assert_eq!(sealed.len(), ENC_LEN + plaintext.len() + TAG_LEN);
            assert_eq!(
                open(&key, &context(), &sealed).unwrap().as_slice(),
                plaintext
            );
            let again = seal(key.public_key(), &context(), plaintext, &mut os_rng()).unwrap();
            assert_ne!(sealed, again, "every seal has its own ephemeral key");
        }
    }

    /// Section 7.2: `info` binds a seal to its cluster, machine, kind and key,
    /// so a seal opens under no other context; nor with another key, nor
    /// with one byte changed in its encapsulated key or its ciphertext.
    #[test]
    fn a_seal_opens_under_no_other_context_key_or_byte() {
        let key = CustodyKey::generate(&mut os_rng());
        let sealed = seal(key.public_key(), &context(), &[9u8; 32], &mut os_rng()).unwrap();
        assert!(open(&key, &context(), &sealed).is_ok());

        let others = [
            (
                "cluster_id",
                SealContext {
                    cluster_id: "cls_01JC9Y0000000000000000000B",
                    ..context()
                },
            ),
            (
                "node_id",
                SealContext {
                    node_id: "nod_01JC9Y2M4Q7V8B1N6T3R5K9XZQ",
                    ..context()
                },
            ),
            (
                "key_kind",
                SealContext {
                    key_kind: KeyKind::Dek,
                    ..context()
                },
            ),
            (
                "key_id",
                SealContext {
                    key_id: "3f9a1c0e7b52d4a86e1f09c3b7d25e4a90c18f6b2d73e5a41c0b98f7e26d1a36",
                    ..context()
                },
            ),
        ];
        for (member, other) in others {
            assert_eq!(
                open(&key, &other, &sealed),
                Err(CustodyError::Open),
                "a seal must not open under another {member}"
            );
        }

        let other_key = CustodyKey::generate(&mut os_rng());
        assert_eq!(
            open(&other_key, &context(), &sealed),
            Err(CustodyError::Open),
            "a seal must not open with another key"
        );

        for (what, index) in [
            ("encapsulated key", 0),
            ("encapsulated key", ENC_LEN - 1),
            ("ciphertext", ENC_LEN),
            ("tag", sealed.len() - 1),
        ] {
            let mut changed = sealed.clone();
            changed[index] ^= 0x01;
            assert_eq!(
                open(&key, &context(), &changed),
                Err(CustodyError::Open),
                "a seal with a changed byte in its {what} must not open"
            );
        }
    }

    /// A value shorter than the encapsulated key and the tag is refused
    /// before HPKE runs; 48 bytes that are no seal do not open.
    #[test]
    fn a_short_or_forged_value_does_not_open() {
        let key = CustodyKey::generate(&mut os_rng());
        for length in [0, 1, ENC_LEN, ENC_LEN + TAG_LEN - 1] {
            assert_eq!(
                open(&key, &context(), &vec![0x55; length]),
                Err(CustodyError::TooShort),
                "{length} bytes"
            );
        }
        assert_eq!(
            open(&key, &context(), &[0x55; ENC_LEN + TAG_LEN]),
            Err(CustodyError::Open)
        );
    }

    /// The all-zero public key is a low-order point: it gives no shared
    /// secret, so a seal to it fails instead of producing a value anyone
    /// could open.
    #[test]
    fn nothing_is_sealed_to_a_low_order_public_key() {
        let recipient = CustodyPublicKey::from_bytes(&[0u8; KEY_LEN]).unwrap();
        assert_eq!(
            seal(&recipient, &context(), b"secret", &mut os_rng()),
            Err(CustodyError::Seal)
        );
    }

    /// The private key round-trips through its 32 bytes, the public key
    /// through standard base64 of 32 bytes, and anything else is refused.
    #[test]
    fn keys_round_trip_and_bad_ones_are_refused() {
        let key = CustodyKey::generate(&mut os_rng());
        let bytes = key.to_bytes();
        let again = CustodyKey::from_bytes(&bytes[..]).unwrap();
        assert_eq!(again.to_bytes(), bytes);
        assert_eq!(again.public_key(), key.public_key());

        let text = key.public_key().to_base64();
        assert_eq!(text.len(), 44);
        assert_eq!(STANDARD.decode(&text).unwrap().len(), KEY_LEN);
        assert_eq!(
            CustodyPublicKey::from_base64(&text).unwrap(),
            *key.public_key()
        );
        assert_ne!(
            CustodyKey::generate(&mut os_rng()).public_key(),
            key.public_key(),
            "every generated key is new"
        );

        for bad in [&[0u8; 31][..], &[0u8; 33][..], &[][..]] {
            assert_eq!(
                CustodyKey::from_bytes(bad).unwrap_err(),
                CustodyError::BadKey
            );
            assert_eq!(
                CustodyPublicKey::from_bytes(bad).unwrap_err(),
                CustodyError::BadKey
            );
        }
        let short = STANDARD.encode([1u8; 31]);
        for bad in ["", "not base64!", short.as_str()] {
            assert_eq!(
                CustodyPublicKey::from_base64(bad).unwrap_err(),
                CustodyError::BadKey
            );
        }
    }

    /// The debug form names the public key and never the private one.
    #[test]
    fn a_custody_key_never_shows_its_private_half() {
        let key = CustodyKey::from_bytes(&[0x42; KEY_LEN]).unwrap();
        let printed = format!("{key:?}");
        assert!(printed.contains(&key.public_key().to_base64()), "{printed}");
        let private = STANDARD.encode(&key.to_bytes()[..]);
        assert!(!printed.contains(&private), "{printed}");
        assert!(!printed.contains("66, 66"), "{printed}");
    }
}
