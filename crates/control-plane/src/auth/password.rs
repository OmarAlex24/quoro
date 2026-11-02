//! Argon2id password hashing (`users.password_hash`, permission-model.md).

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};

/// Argon2id PHC string of `password` with a fresh random salt and the crate's
/// default parameters (Argon2id v19, m = 19 MiB, t = 2, p = 1, the OWASP
/// minimum).
///
/// # Errors
///
/// The hasher's error, or a failing OS random generator.
pub fn hash(password: &str) -> Result<String, String> {
    let mut salt = [0u8; 16];
    getrandom::fill(&mut salt).map_err(|e| e.to_string())?;
    let salt = SaltString::encode_b64(&salt).map_err(|e| e.to_string())?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| e.to_string())
}

/// Whether `password` matches the stored PHC string. A malformed stored hash
/// never matches.
#[must_use]
pub fn verify(password: &str, stored: &str) -> bool {
    PasswordHash::new(stored).is_ok_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

/// A valid hash of a random password, verified against when the email is
/// unknown so that a login takes the same time whether or not the user exists.
#[must_use]
pub fn dummy_hash() -> &'static str {
    static DUMMY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    DUMMY.get_or_init(|| hash(&crate::store::random_secret()).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_verify_and_differ() {
        let a = hash("correct horse battery").unwrap();
        let b = hash("correct horse battery").unwrap();
        assert!(a.starts_with("$argon2id$"));
        assert_ne!(a, b);
        assert!(verify("correct horse battery", &a));
        assert!(!verify("wrong horse battery", &a));
        assert!(!verify("x", "not a phc string"));
    }
}
