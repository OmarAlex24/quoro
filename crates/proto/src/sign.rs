// SPDX-License-Identifier: AGPL-3.0-only
//! The cluster signature on `desired_state` and `signing_key_rotate`
//! (`node-agent-protocol.md` sections 5.11 and 5.21, ADR 0050).
//!
//! The signature covers the RFC 8785 serialization of the whole `body` object
//! with the `signature` member removed, and is carried base64 in `signature`.
//! The primary signs with [`sign_body`]; the node verifies the raw body with
//! [`verify_body`] before it parses anything else.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::{Signature, Signer as _, Verifier as _};
use serde_json::Value;

pub use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::jcs;

/// Why a signature could not be produced or did not verify.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignError {
    /// The body is not a JSON object.
    NotAnObject,
    /// The body has no canonical form (an unsafe integer, a float with an
    /// exponent form).
    NotCanonical(String),
    /// `signature` is missing, not a string or not base64 of 64 bytes.
    BadSignatureField,
    /// The public key is not base64 of a valid 32-byte Ed25519 key.
    BadPublicKey,
    /// The signature does not verify against the key.
    Mismatch,
}

impl std::fmt::Display for SignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnObject => f.write_str("the signed body is not a JSON object"),
            Self::NotCanonical(reason) => write!(f, "the body has no canonical form: {reason}"),
            Self::BadSignatureField => f.write_str("signature is missing or malformed"),
            Self::BadPublicKey => f.write_str("the public key is not a valid Ed25519 key"),
            Self::Mismatch => f.write_str("the signature does not verify"),
        }
    }
}

impl std::error::Error for SignError {}

/// The exact text the signature covers: JCS of `body` without `signature`.
///
/// # Errors
///
/// [`SignError::NotAnObject`] or [`SignError::NotCanonical`].
pub fn signing_input(body: &Value) -> Result<String, SignError> {
    let Value::Object(object) = body else {
        return Err(SignError::NotAnObject);
    };
    let mut object = object.clone();
    object.remove("signature");
    jcs::canonicalize(&Value::Object(object)).map_err(|e| SignError::NotCanonical(e.0))
}

/// Signs `body` in place: computes the signature over [`signing_input`] and
/// sets the `signature` member, replacing any earlier one.
///
/// # Errors
///
/// [`SignError::NotAnObject`] or [`SignError::NotCanonical`].
pub fn sign_body(key: &SigningKey, body: &mut Value) -> Result<(), SignError> {
    let input = signing_input(body)?;
    let signature = key.sign(input.as_bytes());
    let Value::Object(object) = body else {
        return Err(SignError::NotAnObject);
    };
    object.insert(
        "signature".to_owned(),
        Value::String(STANDARD.encode(signature.to_bytes())),
    );
    Ok(())
}

/// Verifies the `signature` member of `body` against `key`.
///
/// # Errors
///
/// Any [`SignError`]; [`SignError::Mismatch`] when the signature is well
/// formed but wrong (another key, or a changed body).
pub fn verify_body(key: &VerifyingKey, body: &Value) -> Result<(), SignError> {
    let encoded = body
        .get("signature")
        .and_then(Value::as_str)
        .ok_or(SignError::BadSignatureField)?;
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| SignError::BadSignatureField)?;
    let bytes: [u8; 64] = bytes.try_into().map_err(|_| SignError::BadSignatureField)?;
    let input = signing_input(body)?;
    key.verify(input.as_bytes(), &Signature::from_bytes(&bytes))
        .map_err(|_| SignError::Mismatch)
}

/// The standard base64 form of a public key, as `cluster_public_key` and the
/// enroll request's `public_key` carry it.
#[must_use]
pub fn public_key_to_base64(key: &VerifyingKey) -> String {
    STANDARD.encode(key.as_bytes())
}

/// Parses a base64 Ed25519 public key.
///
/// # Errors
///
/// [`SignError::BadPublicKey`] for anything that is not 32 bytes of a valid
/// point.
pub fn public_key_from_base64(text: &str) -> Result<VerifyingKey, SignError> {
    let bytes = STANDARD
        .decode(text.trim())
        .map_err(|_| SignError::BadPublicKey)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| SignError::BadPublicKey)?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| SignError::BadPublicKey)
}

/// A signing key from its 32-byte seed, the form the key files hold.
#[must_use]
pub fn signing_key_from_seed(seed: &[u8; 32]) -> SigningKey {
    SigningKey::from_bytes(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(byte: u8) -> SigningKey {
        signing_key_from_seed(&[byte; 32])
    }

    #[test]
    fn a_signed_body_verifies_and_a_changed_one_does_not() {
        let key = key(7);
        let mut body = json!({ "rev": 3, "spec": { "vms": [] }, "term": 1 });
        sign_body(&key, &mut body).unwrap();
        assert!(body["signature"].is_string());
        verify_body(&key.verifying_key(), &body).unwrap();

        let mut changed = body.clone();
        changed["rev"] = json!(4);
        assert_eq!(
            verify_body(&key.verifying_key(), &changed),
            Err(SignError::Mismatch)
        );
        assert_eq!(
            verify_body(&super::tests::key(8).verifying_key(), &body),
            Err(SignError::Mismatch)
        );
    }

    #[test]
    fn member_order_does_not_matter() {
        let key = key(1);
        let mut body = json!({ "b": 1, "a": 2 });
        sign_body(&key, &mut body).unwrap();
        let reordered: Value = serde_json::from_str(&format!(
            r#"{{"signature":{},"a":2,"b":1}}"#,
            body["signature"]
        ))
        .unwrap();
        verify_body(&key.verifying_key(), &reordered).unwrap();
    }

    #[test]
    fn malformed_inputs_are_refused() {
        let key = key(2).verifying_key();
        assert_eq!(
            verify_body(&key, &json!({ "rev": 1 })),
            Err(SignError::BadSignatureField)
        );
        assert_eq!(
            verify_body(&key, &json!({ "rev": 1, "signature": "!!" })),
            Err(SignError::BadSignatureField)
        );
        assert_eq!(signing_input(&json!([1, 2])), Err(SignError::NotAnObject));
        assert!(matches!(
            signing_input(&json!({ "x": 1e21 })),
            Err(SignError::NotCanonical(_))
        ));
    }

    #[test]
    fn public_keys_round_trip_through_base64() {
        let public = key(3).verifying_key();
        let text = public_key_to_base64(&public);
        assert_eq!(public_key_from_base64(&text).unwrap(), public);
        assert_eq!(public_key_from_base64("AAAA"), Err(SignError::BadPublicKey));
    }
}
