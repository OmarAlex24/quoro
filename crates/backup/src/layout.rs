// SPDX-License-Identifier: AGPL-3.0-only
//! Object keys of a repository (`backup-format.md` section 2).
//!
//! Every key is relative to `<prefix>/<cluster_id>/`. The prefix is the
//! repository's `prefix` in its `object_store` provider; nothing here knows the
//! bucket name or the endpoint.

use crate::ChunkId;

/// The repository root, `<prefix>/<cluster_id>/`.
///
/// A trailing `/` on either part is not doubled, and an empty prefix yields
/// just `<cluster_id>/`.
#[must_use]
pub fn root(prefix: &str, cluster_id: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    let cluster_id = cluster_id.trim_matches('/');
    if prefix.is_empty() {
        format!("{cluster_id}/")
    } else {
        format!("{prefix}/{cluster_id}/")
    }
}

/// `config-<n>.json`, a repository config (section 4).
#[must_use]
pub fn config(n: u64) -> String {
    format!("config-{n}.json")
}

/// `chunks/<first 2 hex of id>/<id>`, one encrypted chunk (section 5).
#[must_use]
pub fn chunk(id: &ChunkId) -> String {
    let id = id.to_hex();
    format!("chunks/{}/{}", &id[..2], id)
}

/// `manifests/<source_id>/<backup_id>`, one encrypted manifest (section 6.1).
#[must_use]
pub fn manifest(source_id: &str, backup_id: &str) -> String {
    format!("manifests/{source_id}/{backup_id}")
}

/// `seals/<source_id>/<backup_id>.json`, the signed seal (section 6.2).
#[must_use]
pub fn seal(source_id: &str, backup_id: &str) -> String {
    format!("seals/{source_id}/{backup_id}.json")
}

/// The counter of a `config-<n>.json` key (section 2).
///
/// `None` for any other key, for padding such as `config-01.json`, and for
/// `config-0.json`, because the counter starts at 1. A reader uses the config
/// with the highest `n` that the given recovery key opens.
#[must_use]
pub fn parse_config_n(key: &str) -> Option<u64> {
    let counter = key.strip_prefix("config-")?.strip_suffix(".json")?;
    if counter.is_empty() || !counter.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if counter.len() > 1 && counter.starts_with('0') {
        return None;
    }
    let n: u64 = counter.parse().ok()?;
    if n == 0 { None } else { Some(n) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_follow_section_2() {
        let id = ChunkId([0x9c; 32]);
        assert_eq!(root("backups", "cls_one"), "backups/cls_one/");
        assert_eq!(root("backups/", "cls_one"), "backups/cls_one/");
        assert_eq!(root("", "cls_one"), "cls_one/");
        assert_eq!(config(1), "config-1.json");
        assert_eq!(config(12), "config-12.json");
        assert_eq!(chunk(&id), format!("chunks/9c/{}", "9c".repeat(32)));
        assert_eq!(manifest("app_one", "bkp_one"), "manifests/app_one/bkp_one");
        assert_eq!(seal("app_one", "bkp_one"), "seals/app_one/bkp_one.json");
    }

    #[test]
    fn counters_parse_without_padding() {
        assert_eq!(parse_config_n("config-1.json"), Some(1));
        assert_eq!(parse_config_n("config-12.json"), Some(12));
        assert_eq!(parse_config_n("config-0.json"), None);
        assert_eq!(parse_config_n("config-01.json"), None);
        assert_eq!(parse_config_n("config-.json"), None);
        assert_eq!(parse_config_n("config-1.jsonx"), None);
        assert_eq!(parse_config_n("configs/config-1.json"), None);
        assert_eq!(parse_config_n("config-one.json"), None);
        assert_eq!(parse_config_n("config-18446744073709551616.json"), None);
    }
}
