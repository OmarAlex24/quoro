// SPDX-License-Identifier: AGPL-3.0-only
//! `app_env.value_enc`: an app's environment values under the cluster
//! data-encryption key (ADR 0050, `db-schema.sql`).
//!
//! One place fixes the AEAD binding so the writer (`replaceAppEnv`) and the
//! reader (the desired-state builder, which decrypts the values for the node
//! that runs the app) can never disagree. Each value is bound to its column
//! and to `<app_id>/<name>`, so a ciphertext copied to another app or another
//! variable does not decrypt.
//!
//! Plaintext leaves this module only as [`Secret`] or inside the desired-state
//! `env` map that the session signs and sends to the node over the pinned TLS
//! session (`node-agent-protocol.md` section 5.11). Never log either.

use crate::crypto::{CryptoError, DataKey, Secret};
use sqlx::SqliteConnection;
use std::collections::BTreeMap;

/// The AEAD context of `app_env.value_enc`.
pub const VALUE_CONTEXT: &str = "app_env.value_enc";

/// The row identity a value is bound to.
fn row_id(app_id: &str, name: &str) -> String {
    format!("{app_id}/{name}")
}

/// Encrypts one value for `app_id`'s variable `name`.
#[must_use]
pub fn encrypt_value(key: &DataKey, app_id: &str, name: &str, value: &str) -> Vec<u8> {
    key.encrypt(VALUE_CONTEXT, &row_id(app_id, name), value.as_bytes())
}

/// Decrypts one value written by [`encrypt_value`] for the same app and name.
///
/// # Errors
///
/// [`CryptoError`] when the blob was written under another key, for another
/// row, or was tampered with.
pub fn decrypt_value(
    key: &DataKey,
    app_id: &str,
    name: &str,
    blob: &[u8],
) -> Result<Secret, CryptoError> {
    key.decrypt(VALUE_CONTEXT, &row_id(app_id, name), blob)
}

/// Why an app's environment could not be read.
#[derive(Debug)]
pub enum EnvError {
    Db(sqlx::Error),
    /// A stored value did not decrypt; carries the variable name only.
    Crypto {
        name: String,
        error: CryptoError,
    },
}

impl std::fmt::Display for EnvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Db(e) => write!(f, "cannot read app_env: {e}"),
            Self::Crypto { name, error } => write!(f, "cannot decrypt app_env {name}: {error}"),
        }
    }
}

impl std::error::Error for EnvError {}

/// Every variable of `app_id`, decrypted, keyed by name.
///
/// # Errors
///
/// [`EnvError`] on a database error or a value that does not decrypt.
pub async fn load_plain(
    conn: &mut SqliteConnection,
    key: &DataKey,
    app_id: &str,
) -> Result<BTreeMap<String, Secret>, EnvError> {
    let rows: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT name, value_enc FROM app_env WHERE app_id = ? ORDER BY name")
            .bind(app_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(EnvError::Db)?;
    let mut env = BTreeMap::new();
    for (name, blob) in rows {
        let value = decrypt_value(key, app_id, &name, &blob).map_err(|error| EnvError::Crypto {
            name: name.clone(),
            error,
        })?;
        env.insert(name, value);
    }
    Ok(env)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_is_bound_to_its_app_and_name() {
        let key = DataKey::ephemeral();
        let blob = encrypt_value(&key, "app_a", "TOKEN", "s3cret");
        assert_eq!(
            decrypt_value(&key, "app_a", "TOKEN", &blob)
                .unwrap()
                .expose(),
            "s3cret"
        );
        assert!(decrypt_value(&key, "app_b", "TOKEN", &blob).is_err());
        assert!(decrypt_value(&key, "app_a", "OTHER", &blob).is_err());
    }
}
