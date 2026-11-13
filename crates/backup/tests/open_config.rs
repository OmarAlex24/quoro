// SPDX-License-Identifier: AGPL-3.0-only
//! `open_newest_config`, the config reader that `quoro restore` and a primary
//! restoring the repository key share (`backup-format.md` sections 4 and 8.2
//! step 1).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use quoro_backup::{
    FORMAT, OpenConfigError, RecoveryKey, RepoConfig, RepoKey, config_keys_newest_first,
    open_newest_config, wrap_repo_key,
};

const ROOT: &str = "quoro-backups/cls_01JC9Z1A2B3C4D5E6F7G8H9J0K/";
const CLUSTER: &str = "cls_01JC9Z1A2B3C4D5E6F7G8H9J0K";
const REPOSITORY: &str = "brp_01JCA0A2B3C4D5E6F7G8H9J0KM";

/// An in-memory bucket keyed by full object key, counting the reads.
#[derive(Default)]
struct Objects {
    map: HashMap<String, Vec<u8>>,
    reads: AtomicUsize,
}

impl Objects {
    fn put(&mut self, name: &str, bytes: Vec<u8>) {
        self.map.insert(format!("{ROOT}{name}"), bytes);
    }

    fn put_config(&mut self, name: &str, config: &RepoConfig) {
        self.put(name, serde_json::to_vec(config).unwrap());
    }

    fn keys(&self) -> Vec<String> {
        self.map.keys().cloned().collect()
    }

    async fn open(
        &self,
        repository_id: Option<&str>,
        recovery: &RecoveryKey,
    ) -> Result<(RepoConfig, RepoKey), OpenConfigError> {
        open_newest_config(
            ROOT,
            self.keys(),
            CLUSTER,
            repository_id,
            recovery,
            |key| async move {
                self.reads.fetch_add(1, Ordering::SeqCst);
                self.map.get(&key).cloned().ok_or("not found")
            },
        )
        .await
    }
}

fn config(
    repo: &RepoKey,
    recovery: &RecoveryKey,
    cluster_id: &str,
    repository_id: &str,
) -> RepoConfig {
    RepoConfig {
        format: FORMAT,
        repository_id: repository_id.to_string(),
        cluster_id: cluster_id.to_string(),
        cluster_public_key: "u3VbJk9rQ0Lw8m1Wq2zT5yHc6nE4aP7sDf0gXiK2oVQ=".to_string(),
        recovery_key_fingerprint: recovery.fingerprint(),
        wrapped_repo_key: wrap_repo_key(repo, recovery, repository_id),
        created_at: "2026-11-05T02:00:00Z".to_string(),
    }
}

#[test]
fn config_keys_sort_by_counter_and_drop_other_keys() {
    let listed = [
        "config-2.json",
        "config-10.json",
        "config-1.json",
        "config-01.json",
        "config-0.json",
        "config-3.json.tmp",
        "seals/x.json",
    ]
    .into_iter()
    .map(|name| format!("{ROOT}{name}"));
    assert_eq!(
        config_keys_newest_first(ROOT, listed),
        vec![
            format!("{ROOT}config-10.json"),
            format!("{ROOT}config-2.json"),
            format!("{ROOT}config-1.json"),
        ]
    );
}

#[tokio::test]
async fn the_newest_config_the_key_opens_wins() {
    let recovery = RecoveryKey::generate();
    let newest = RepoKey::generate();
    let older = RepoKey::generate();
    let mut objects = Objects::default();
    objects.put_config(
        "config-2.json",
        &config(&older, &recovery, CLUSTER, REPOSITORY),
    );
    // `10` sorts below `2` as text; the counter is a number.
    objects.put_config(
        "config-10.json",
        &config(&newest, &recovery, CLUSTER, REPOSITORY),
    );

    let (opened, key) = objects.open(None, &recovery).await.unwrap();
    assert_eq!(key.as_bytes(), newest.as_bytes());
    assert_eq!(opened.repository_id, REPOSITORY);
    assert_eq!(objects.reads.load(Ordering::SeqCst), 1);

    let (_, key) = objects.open(Some(REPOSITORY), &recovery).await.unwrap();
    assert_eq!(key.as_bytes(), newest.as_bytes());
}

/// After a rotation the old key no longer opens the newest config; the reader
/// goes down to an older one it opens, as section 4 says.
#[tokio::test]
async fn a_key_that_opens_only_an_older_config_gets_that_one() {
    let repo = RepoKey::generate();
    let old = RecoveryKey::generate();
    let new = RecoveryKey::generate();
    let mut objects = Objects::default();
    objects.put_config("config-1.json", &config(&repo, &old, CLUSTER, REPOSITORY));
    objects.put_config("config-2.json", &config(&repo, &new, CLUSTER, REPOSITORY));

    let (opened, key) = objects.open(None, &old).await.unwrap();
    assert_eq!(key.as_bytes(), repo.as_bytes());
    assert_eq!(opened.recovery_key_fingerprint, old.fingerprint());
    let (opened, _) = objects.open(None, &new).await.unwrap();
    assert_eq!(opened.recovery_key_fingerprint, new.fingerprint());
}

/// An unreadable object, an object that is not a config, a config of another
/// cluster or, when one is named, of another repository is skipped.
#[tokio::test]
async fn configs_that_do_not_fit_are_skipped() {
    let recovery = RecoveryKey::generate();
    let repo = RepoKey::generate();
    let foreign = RepoKey::generate();
    let other_repository = "brp_01JCA0A2B3C4D5E6F7G8H9J0KN";
    let mut objects = Objects::default();
    objects.put_config(
        "config-1.json",
        &config(&repo, &recovery, CLUSTER, REPOSITORY),
    );
    objects.put("config-2.json", b"not json".to_vec());
    objects.put_config(
        "config-3.json",
        &config(
            &foreign,
            &recovery,
            "cls_01JC9Z1A2B3C4D5E6F7G8H9J0M",
            REPOSITORY,
        ),
    );
    objects.put_config(
        "config-4.json",
        &config(&foreign, &recovery, CLUSTER, other_repository),
    );
    // Listed but gone by the time it is read.
    let mut listed = objects.keys();
    listed.push(format!("{ROOT}config-5.json"));

    let (opened, key) = open_newest_config(
        ROOT,
        listed.clone(),
        CLUSTER,
        Some(REPOSITORY),
        &recovery,
        |key| {
            let object = objects.map.get(&key).cloned();
            async move { object.ok_or("not found") }
        },
    )
    .await
    .unwrap();
    assert_eq!(key.as_bytes(), repo.as_bytes());
    assert_eq!(opened.repository_id, REPOSITORY);

    // Without a repository to match, `quoro restore` takes the newest config
    // of its cluster that the key opens, whatever repository it names.
    let (opened, key) = open_newest_config(ROOT, listed, CLUSTER, None, &recovery, |key| {
        let object = objects.map.get(&key).cloned();
        async move { object.ok_or("not found") }
    })
    .await
    .unwrap();
    assert_eq!(key.as_bytes(), foreign.as_bytes());
    assert_eq!(opened.repository_id, other_repository);
}

#[tokio::test]
async fn a_key_that_opens_nothing_is_refused_without_key_material() {
    let mut objects = Objects::default();
    assert_eq!(
        objects
            .open(None, &RecoveryKey::generate())
            .await
            .unwrap_err(),
        OpenConfigError::NoneOpens
    );
    objects.put_config(
        "config-1.json",
        &config(
            &RepoKey::generate(),
            &RecoveryKey::generate(),
            CLUSTER,
            REPOSITORY,
        ),
    );
    let other = RecoveryKey::generate();
    let error = objects.open(Some(REPOSITORY), &other).await.unwrap_err();
    assert_eq!(error, OpenConfigError::NoneOpens);
    // `quoro restore` prints this text, unchanged since the reader moved here.
    assert_eq!(
        error.to_string(),
        "the recovery key opens no config object of this repository"
    );
    assert!(!format!("{error:?}").contains(&other.to_text()));
}

/// The newest config the key opens decides: an unknown format stops the
/// search instead of falling back to an older config.
#[tokio::test]
async fn an_unknown_format_ends_the_search() {
    let recovery = RecoveryKey::generate();
    let repo = RepoKey::generate();
    let mut objects = Objects::default();
    objects.put_config(
        "config-1.json",
        &config(&repo, &recovery, CLUSTER, REPOSITORY),
    );
    let mut future = config(&repo, &recovery, CLUSTER, REPOSITORY);
    future.format = FORMAT + 1;
    objects.put_config("config-2.json", &future);

    let error = objects.open(None, &recovery).await.unwrap_err();
    assert_eq!(error, OpenConfigError::UnknownFormat(FORMAT + 1));
    assert_eq!(
        error.to_string(),
        format!("the config object names unknown format {}", FORMAT + 1)
    );
}

/// When a listed config object cannot be read and the key opens none of
/// those read, the error says so, with the last read error, rather than that
/// the key opens none: the unread one may be the config the key opens. Every
/// config read and none opened, or no config object at all, is still
/// `NoneOpens`.
#[tokio::test]
async fn config_objects_that_cannot_be_read_are_unreadable() {
    let recovery = RecoveryKey::generate();
    let repo = RepoKey::generate();
    let mut objects = Objects::default();
    objects.put_config(
        "config-1.json",
        &config(&repo, &recovery, CLUSTER, REPOSITORY),
    );
    objects.put_config(
        "config-2.json",
        &config(&repo, &recovery, CLUSTER, REPOSITORY),
    );

    let error = open_newest_config(
        ROOT,
        objects.keys(),
        CLUSTER,
        Some(REPOSITORY),
        &recovery,
        |_key| async { Err::<Vec<u8>, _>("denied: reads are refused") },
    )
    .await
    .unwrap_err();
    assert_eq!(
        error,
        OpenConfigError::Unreadable("denied: reads are refused".to_string())
    );
    assert_eq!(
        error.to_string(),
        "a config object could not be read: denied: reads are refused"
    );
    assert!(!format!("{error:?}").contains(&recovery.to_text()));

    // config-2 cannot be read, config-1 is read and does not open: config-2
    // may be the one the key opens, so the failed read is reported.
    let other = RecoveryKey::generate();
    let error = open_newest_config(ROOT, objects.keys(), CLUSTER, None, &other, |key| {
        let object = objects
            .map
            .get(&key)
            .filter(|_| key.ends_with("config-1.json"))
            .cloned();
        async move { object.ok_or("denied") }
    })
    .await
    .unwrap_err();
    assert_eq!(error, OpenConfigError::Unreadable("denied".to_string()));

    // Nothing listed that is a config object.
    let error = open_newest_config(
        ROOT,
        vec![format!("{ROOT}seals/x.json")],
        CLUSTER,
        None,
        &recovery,
        |_key| async { Err::<Vec<u8>, _>("denied") },
    )
    .await
    .unwrap_err();
    assert_eq!(error, OpenConfigError::NoneOpens);
}
