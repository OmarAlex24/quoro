//! Integration tests for [`quoro_providers::s3`] against a real S3-compatible
//! server. They do nothing unless `QUORO_S3_TEST_ENDPOINT`,
//! `QUORO_S3_TEST_ACCESS_KEY` and `QUORO_S3_TEST_SECRET_KEY` are set: the
//! throwaway RustFS instance of the storage lab is the intended target.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use quoro_providers::object_store::{
    LockProbe, LockStrength, ObjectStore, ObjectStoreError, probe_lock,
};
use quoro_providers::s3::{S3Client, S3Config};
use zeroize::Zeroizing;

struct Target {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

/// `None` means "not configured here": the tests skip instead of failing on a
/// laptop that has no S3 server.
fn target() -> Option<Target> {
    let endpoint = std::env::var("QUORO_S3_TEST_ENDPOINT").ok()?;
    let access_key = std::env::var("QUORO_S3_TEST_ACCESS_KEY")
        .expect("QUORO_S3_TEST_ACCESS_KEY is set when the endpoint is");
    let secret_key = std::env::var("QUORO_S3_TEST_SECRET_KEY")
        .expect("QUORO_S3_TEST_SECRET_KEY is set when the endpoint is");
    Some(Target {
        endpoint,
        access_key,
        secret_key,
    })
}

/// A fresh bucket name, lowercase as S3 requires.
fn bucket_name(kind: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("quoro-test-{kind}-{}-{nanos:x}", std::process::id())
}

fn client(target: &Target, bucket: &str) -> S3Client {
    S3Client::new(S3Config {
        endpoint: target.endpoint.clone(),
        region: "auto".to_string(),
        bucket: bucket.to_string(),
        access_key_id: target.access_key.clone(),
        secret_access_key: Zeroizing::new(target.secret_key.clone()),
    })
    .expect("a client")
}

/// Creates one bucket through the signed `PUT /<bucket>` the API defines. It
/// lives only in this test file: bucket creation is operator work, not part of
/// the client Quoro ships.
async fn create_bucket(target: &Target, bucket: &str, object_lock: bool) {
    let url = format!("{}/{bucket}", target.endpoint.trim_end_matches('/'));
    let identity: Identity = Credentials::new(
        target.access_key.clone(),
        target.secret_key.clone(),
        None,
        None,
        "quoro-test",
    )
    .into();

    let mut settings = SigningSettings::default();
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.signature_location = SignatureLocation::Headers;

    let extra: Vec<(&str, &str)> = if object_lock {
        vec![("x-amz-bucket-object-lock-enabled", "true")]
    } else {
        Vec::new()
    };
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("auto")
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("signing parameters")
        .into();
    let signable = SignableRequest::new(
        "PUT",
        url.as_str(),
        extra.iter().copied(),
        SignableBody::empty(),
    )
    .expect("a signable request");
    let (instructions, _signature) = sign(signable, &params)
        .expect("the request signs")
        .into_parts();

    // reqwest is built without a default crypto provider; install ring.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut request = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("a client")
        .put(&url);
    for (name, value) in instructions.headers() {
        request = request.header(name, value);
    }
    for (name, value) in &extra {
        request = request.header(*name, *value);
    }
    let response = request.send().await.expect("the create-bucket call");
    let status = response.status();
    assert!(
        status.is_success(),
        "creating {bucket} failed with {status}: {}",
        response.text().await.unwrap_or_default()
    );
}

#[tokio::test]
async fn objects_are_written_read_listed_and_deleted() {
    let Some(target) = target() else {
        eprintln!("skipping: QUORO_S3_TEST_ENDPOINT is not set");
        return;
    };
    let bucket = bucket_name("objects");
    create_bucket(&target, &bucket, false).await;
    let store = client(&target, &bucket);

    // Section 5 of backup-format.md: `If-None-Match: *`, 412 means present.
    assert_eq!(
        store
            .put_new("chunks/00/00a1", b"first chunk".to_vec(), None)
            .await
            .expect("the first write"),
        None,
        "an unversioned bucket has no version id"
    );
    assert_eq!(
        store
            .put_new("chunks/00/00a1", b"second write".to_vec(), None)
            .await,
        Err(ObjectStoreError::AlreadyExists)
    );
    assert_eq!(store.get("chunks/00/00a1").await.unwrap(), b"first chunk");
    assert_eq!(store.head("chunks/00/00a1").await.unwrap().size, 11);

    // Pagination: ListObjectsV2 returns at most 1,000 keys per page.
    for index in 0..1500 {
        let key = format!("list/{index:04}-{:032x}", index);
        store
            .put_new(&key, format!("object {index}").into_bytes(), None)
            .await
            .unwrap_or_else(|err| panic!("writing {key}: {err}"));
    }
    let listed = store.list("list/").await.expect("the list");
    assert_eq!(listed.len(), 1500);
    assert!(
        listed.windows(2).all(|pair| pair[0].key <= pair[1].key),
        "the list is in key order"
    );
    assert_eq!(listed[0].size, "object 0".len() as u64);

    store.delete("chunks/00/00a1").await.expect("the delete");
    assert_eq!(
        store.get("chunks/00/00a1").await,
        Err(ObjectStoreError::NotFound)
    );
    assert_eq!(
        store.head("chunks/00/00a1").await,
        Err(ObjectStoreError::NotFound)
    );
    // Deleting what is already gone is a success.
    store
        .delete("chunks/00/00a1")
        .await
        .expect("a repeat delete");
}

fn assert_probe(probe: &LockProbe, expected: LockStrength) {
    assert_eq!(probe.strength, expected, "detail: {}", probe.detail);
    assert!(!probe.detail.is_empty());
}

#[tokio::test]
async fn the_probe_reports_none_on_a_plain_bucket_and_compliance_on_a_locked_one() {
    let Some(target) = target() else {
        eprintln!("skipping: QUORO_S3_TEST_ENDPOINT is not set");
        return;
    };

    let plain = bucket_name("plain");
    create_bucket(&target, &plain, false).await;
    let plain_store = client(&target, &plain);
    let probe = probe_lock(
        &plain_store,
        "audit/probe/",
        "2026-11-05T00:00:00Z",
        "2030-01-01T00:00:00Z",
    )
    .await;
    assert_probe(&probe, LockStrength::None);

    // section 8.2 of audit-log-format.md: object lock enabled on the bucket.
    let locked = bucket_name("locked");
    create_bucket(&target, &locked, true).await;
    let locked_store = client(&target, &locked);
    let probe = probe_lock(
        &locked_store,
        "audit/probe/",
        "2026-11-05T00:00:00Z",
        "2030-01-01T00:00:00Z",
    )
    .await;
    assert_probe(&probe, LockStrength::Compliance);

    // The probe left an overwritten version and a delete marker behind, and
    // the original version is still readable by id.
    let versions = locked_store
        .list_versions("audit/probe/")
        .await
        .expect("the version list");
    assert!(versions.len() >= 2, "{versions:?}");
    let mut found_original = false;
    for version in versions.iter().filter(|version| !version.is_delete_marker) {
        let body = locked_store
            .get_version(&version.key, &version.version_id)
            .await
            .expect("the version body");
        found_original |= body == b"quoro lock probe";
    }
    assert!(found_original, "the locked original is still readable");
}
