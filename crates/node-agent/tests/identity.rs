// SPDX-License-Identifier: AGPL-3.0-only
//! Identity files (`node-agent-protocol.md` section 2.5): the directory is
//! `0700`, the files `0600`, and a missing or malformed file is a clear error.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;

use quoro_node_agent::identity;
use quoro_proto::sign;

fn temp_dir() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "quoro-identity-test-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn identity_files_are_private_and_load_back() {
    let dir = temp_dir();
    let cluster = sign::signing_key_from_seed(&[3; 32]);
    let public_key = sign::public_key_to_base64(&cluster.verifying_key());

    identity::ensure_data_dir(&dir).unwrap();
    identity::write_seed(&dir, &[7; 32]).unwrap();
    identity::write_credential(&dir, "secret-credential").unwrap();
    let id = quoro_node_agent::Identity {
        node_id: "nod_1".to_owned(),
        cluster_id: "cls_1".to_owned(),
        primary_url: "https://primary.test".to_owned(),
        fingerprint: format!("sha256:{}", "ab".repeat(32)),
        cluster_public_key: public_key.clone(),
        cluster_key_id: quoro_node_agent::Identity::key_id_for(&public_key).unwrap(),
    };
    identity::save_identity(&dir, &id).unwrap();

    assert!(identity::is_enrolled(&dir));
    assert_eq!(identity::load_identity(&dir).unwrap(), id);
    assert_eq!(identity::read_seed(&dir).unwrap(), [7; 32]);
    assert_eq!(
        identity::read_credential(&dir).unwrap(),
        "secret-credential"
    );

    let directory_mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(directory_mode, 0o700, "the data directory is root-only");
    for name in [
        identity::SEED_FILE,
        identity::CREDENTIAL_FILE,
        identity::IDENTITY_FILE,
    ] {
        let mode = std::fs::metadata(dir.join(name))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "{name} is root-only");
    }

    // A malformed file is refused with the file named.
    std::fs::write(dir.join(identity::IDENTITY_FILE), b"{ not json").unwrap();
    let error = identity::load_identity(&dir).unwrap_err().to_string();
    assert!(error.contains(identity::IDENTITY_FILE), "{error}");

    // A missing file means "not enrolled", not a panic.
    std::fs::remove_file(dir.join(identity::SEED_FILE)).unwrap();
    assert!(!identity::is_enrolled(&dir));
    let error = identity::read_seed(&dir).unwrap_err().to_string();
    assert!(error.contains("not enrolled"), "{error}");

    let _ = std::fs::remove_dir_all(&dir);
}
