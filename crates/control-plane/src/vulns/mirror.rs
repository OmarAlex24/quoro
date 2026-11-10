// SPDX-License-Identifier: AGPL-3.0-only
//! The primary's copy of the OSV data and the loop that keeps it fresh.
//!
//! One pass fetches, one request at a time, the ecosystems something needs:
//! `Linux` (kernels), every ecosystem an inventory named, every bucket
//! `osv_mirror` already knows and every bucket [`Vulns::want`] was told about.
//! The first time a bucket is needed its archive is downloaded; afterwards it
//! is refreshed daily from `modified_id.csv`, and a change list longer than
//! [`Limits::delta_max_ids`] becomes a download again.
//!
//! The cache file holds the records; `osv_mirror` holds only the data age.
//! Every attempt stamps `checked_at`; a success sets `refreshed_at` and the
//! counts, a failure keeps them and records why in `last_error`.

use crate::store::Store;
use crate::vulns::cache::Cache;
use crate::vulns::changes;
use crate::vulns::ecosystem;
use crate::vulns::fetch::{self, Endpoints, Fetch, Limits};
use crate::vulns::kev::{self, KEV};
use crate::vulns::osv::{self, ZipReader};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The refresh period: a bucket brought up to date this long ago is due.
const REFRESH_PERIOD_NANOS: i128 = 24 * 60 * 60 * 1_000_000_000;

/// How often the loop runs when nothing wakes it.
const IDLE: Duration = Duration::from_secs(60 * 60);

/// The longest error written to `osv_mirror.last_error`, in characters.
const MAX_ERROR_CHARS: usize = 500;

/// The primary's vulnerability data: the cache, the loop that refreshes it,
/// and the `osv_mirror` rows that say how old it is.
#[derive(Clone)]
pub struct Vulns {
    cache: Cache,
    store: Store,
    data_dir: PathBuf,
    endpoints: Endpoints,
    client: reqwest::Client,
    limits: Arc<Mutex<Limits>>,
    wanted: Arc<Mutex<HashSet<String>>>,
    wake: Arc<tokio::sync::Notify>,
    spawned: Arc<AtomicBool>,
}

impl Vulns {
    /// Opens the cache in `data_dir` and prepares the loop.
    ///
    /// A cache file that had to be created means the primary holds no data,
    /// so `osv_mirror` is emptied: its rows would describe data that is gone.
    ///
    /// # Errors
    ///
    /// The cache or the database could not be opened or written.
    pub async fn open(data_dir: &Path, store: Store) -> Result<Self, String> {
        let (cache, created) = Cache::open(&data_dir.join("vulns-cache.db"))
            .await
            .map_err(|error| format!("cannot open the vulnerability cache: {error}"))?;
        if created {
            let mut write = store
                .begin_write()
                .await
                .map_err(|error| format!("cannot clear osv_mirror: {error}"))?;
            sqlx::query("DELETE FROM osv_mirror")
                .execute(&mut *write.tx)
                .await
                .map_err(|error| format!("cannot clear osv_mirror: {error}"))?;
            write
                .commit()
                .await
                .map_err(|error| format!("cannot clear osv_mirror: {error}"))?;
        }
        fetch::clear_leftovers(data_dir);
        Ok(Self {
            cache,
            store,
            data_dir: data_dir.to_path_buf(),
            endpoints: Endpoints::default(),
            client: fetch::client()?,
            limits: Arc::new(Mutex::new(Limits::default())),
            wanted: Arc::new(Mutex::new(HashSet::new())),
            wake: Arc::new(tokio::sync::Notify::new()),
            spawned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Points the mirror at another origin, for a test or a mirror host.
    #[must_use]
    pub fn with_endpoints(mut self, endpoints: Endpoints) -> Self {
        self.endpoints = endpoints;
        self
    }

    /// Lowers the size and count limits, so a test can exercise the same code
    /// with tiny bodies.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = Arc::new(Mutex::new(limits));
        self
    }

    /// The cache the matcher reads.
    #[must_use]
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Notes that an inventory needs `ecosystem`'s data.
    ///
    /// An ecosystem with no OSV bucket is ignored: no URL is built and no
    /// `osv_mirror` row is created for it.
    pub fn want(&self, ecosystem: &str) {
        let Some(bucket) = ecosystem::bucket_for(ecosystem) else {
            tracing::debug!(ecosystem, "no OSV data source for this ecosystem");
            return;
        };
        let fresh = lock(&self.wanted).insert(bucket.to_string());
        if fresh {
            self.wake.notify_one();
        }
    }

    /// Starts the background loop: one pass at start, then on a wake-up or
    /// every hour, whichever comes first.
    ///
    /// Only the first call starts a loop; a second call is a no-op, so a
    /// caller can be careless about it.
    pub fn spawn(&self) {
        if self.spawned.swap(true, Ordering::SeqCst) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                // A demoted primary downloads and writes nothing more
                // (`node-agent-protocol.md` section 7.4).
                if this
                    .store
                    .stops_for_demotion("the vulnerability data download")
                {
                    break;
                }
                this.run_once(OffsetDateTime::now_utc()).await;
                tokio::select! {
                    () = this.wake.notified() => {}
                    () = tokio::time::sleep(IDLE) => {}
                }
            }
        });
    }

    /// One pass over every wanted bucket and KEV, at most one fetch at a time.
    ///
    /// `now` is the time the pass is taken to run at: it decides what is due
    /// and what is stamped as `checked_at`/`refreshed_at`.
    pub async fn run_once(&self, now: OffsetDateTime) -> Vec<(String, Result<(), String>)> {
        let mut results = Vec::new();
        for bucket in self.wanted_buckets().await {
            let result = self.refresh_bucket(&bucket, now).await;
            if let Err(error) = &result {
                tracing::warn!(bucket, error, "vulnerability data refresh failed");
            }
            results.push((bucket, result));
        }
        let result = self.refresh_kev(now).await;
        if let Err(error) = &result {
            tracing::warn!(error, "KEV refresh failed");
        }
        results.push((KEV.to_string(), result));
        results
    }

    /// Every bucket this pass should bring up to date.
    async fn wanted_buckets(&self) -> Vec<String> {
        let mut wanted: HashSet<String> = ecosystem::ALWAYS
            .iter()
            .map(|bucket| (*bucket).to_string())
            .collect();
        wanted.extend(lock(&self.wanted).iter().cloned());
        match sqlx::query_as::<_, (String,)>("SELECT ecosystem FROM osv_mirror")
            .fetch_all(self.store.pool())
            .await
        {
            Ok(rows) => {
                for (name,) in rows {
                    if ecosystem::is_bucket(&name) {
                        wanted.insert(name);
                    }
                }
            }
            Err(error) => tracing::warn!(error = %error, "cannot read osv_mirror"),
        }
        match self.cache.inventory_digests().await {
            Ok(digests) => {
                for digest in digests {
                    let Ok(Some((_, json))) = self.cache.inventory(&digest).await else {
                        continue;
                    };
                    self.want_from_inventory(&json, &mut wanted);
                }
            }
            Err(error) => tracing::warn!(error = %error, "cannot read the inventories"),
        }
        let mut wanted: Vec<String> = wanted.into_iter().collect();
        wanted.sort();
        wanted
    }

    /// Adds the buckets an inventory's `packages[].ecosystem` names.
    ///
    /// Only that field is read: an inventory can be 4 MiB, and nothing else in
    /// it decides which data the primary needs.
    fn want_from_inventory(&self, json: &str, wanted: &mut HashSet<String>) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
            return;
        };
        let Some(packages) = value.get("packages").and_then(serde_json::Value::as_array) else {
            return;
        };
        for package in packages {
            if let Some(name) = package.get("ecosystem").and_then(serde_json::Value::as_str)
                && let Some(bucket) = ecosystem::bucket_for(name)
            {
                wanted.insert(bucket.to_string());
            }
        }
    }

    async fn refresh_bucket(&self, bucket: &str, now: OffsetDateTime) -> Result<(), String> {
        let state = self
            .cache
            .bucket(bucket)
            .await
            .map_err(|error| format!("cannot read the cache: {error}"))?;
        let outcome = match &state {
            // Never fetched: the whole archive.
            None => self.download_bucket(bucket).await,
            Some(state) => {
                if !is_due(&state.refreshed_at, now) {
                    return Ok(());
                }
                self.delta_bucket(bucket, state).await
            }
        };
        match outcome {
            Ok(()) => {
                let state = self
                    .cache
                    .bucket(bucket)
                    .await
                    .map_err(|error| format!("cannot read the cache: {error}"))?;
                let (count, modified) = state
                    .map(|state| (state.record_count, state.modified))
                    .unwrap_or((0, None));
                self.mark_success(bucket, "download", count, modified, now)
                    .await
            }
            Err(error) => {
                if let Err(write) = self.mark_failure(bucket, &error, now).await {
                    tracing::warn!(error = %write, "cannot record a refresh failure");
                }
                Err(error)
            }
        }
    }

    /// Downloads and replaces one bucket's archive.
    async fn download_bucket(&self, bucket: &str) -> Result<(), String> {
        let path = self.fetch().download_zip(bucket).await?;
        let outcome = self.import_zip(bucket, &path).await;
        // The archive is worthless once imported, whatever the outcome.
        let _ = std::fs::remove_file(&path);
        outcome
    }

    /// Replaces one bucket from the archive at `path`.
    async fn import_zip(&self, bucket: &str, path: &Path) -> Result<(), String> {
        let mut reader = ZipReader::open(path)?;
        self.cache
            .replace_bucket(bucket, osv::records(&mut reader))
            .await
            .map_err(|error| format!("cannot write the cache: {error}"))?;
        let stats = reader.finish()?;
        if stats.skipped > 0 {
            tracing::debug!(
                bucket,
                skipped = stats.skipped,
                "records skipped in archive"
            );
        }
        Ok(())
    }

    /// Brings one bucket up to date from its change list.
    async fn delta_bucket(
        &self,
        bucket: &str,
        state: &crate::vulns::cache::BucketState,
    ) -> Result<(), String> {
        let Some(cursor) = state.modified_nanos else {
            // Nothing to start from: ask for the whole archive.
            return self.download_bucket(bucket).await;
        };
        let url = ecosystem::modified_csv_url(&self.endpoints.osv_base, bucket)
            .ok_or_else(|| format!("{bucket:?} is not an OSV bucket"))?;
        let limits = self.limits();
        let csv = self.fetch().get_text(&url, limits.max_csv_bytes).await?;
        let delta = changes::scan(&csv, cursor, limits.delta_max_ids);
        if delta.malformed > 0 {
            tracing::debug!(
                bucket,
                lines = delta.malformed,
                "malformed change-list lines"
            );
        }
        if delta.too_many {
            tracing::debug!(bucket, "the change list is too long for a delta");
            return self.download_bucket(bucket).await;
        }

        let mut records = Vec::new();
        let mut removed: Vec<String> = Vec::new();
        let mut skipped = 0u64;
        // Oldest first: the bucket's cursor is the newest record it holds, so a
        // pass that stops part-way must not have stored a newer record before
        // the older ones it still owes.
        for id in delta.ids.iter().rev() {
            let Some(url) = ecosystem::record_url(&self.endpoints.osv_base, bucket, id) else {
                skipped += 1;
                continue;
            };
            match self
                .fetch()
                .get_optional(&url, limits.max_record_bytes)
                .await?
            {
                // A 404 during a delta means the record was removed.
                None => removed.push(id.clone()),
                Some(body) => match osv::parse_record(&body) {
                    Some(record) => records.push(record),
                    None => skipped += 1,
                },
            }
            if records.len() >= 500 {
                self.cache
                    .upsert_records(bucket, records.drain(..))
                    .await
                    .map_err(|error| format!("cannot write the cache: {error}"))?;
            }
            if removed.len() >= 500 {
                self.cache
                    .delete_records(bucket, &removed)
                    .await
                    .map_err(|error| format!("cannot write the cache: {error}"))?;
                removed.clear();
            }
        }
        // An empty write still refreshes the bucket's timestamp, so the next
        // pass is not due for another day.
        self.cache
            .upsert_records(bucket, records)
            .await
            .map_err(|error| format!("cannot write the cache: {error}"))?;
        if !removed.is_empty() {
            self.cache
                .delete_records(bucket, &removed)
                .await
                .map_err(|error| format!("cannot write the cache: {error}"))?;
        }
        if skipped > 0 {
            tracing::debug!(bucket, skipped, "records skipped in the change list");
        }
        Ok(())
    }

    /// Fetches KEV when its row is a day old or missing.
    async fn refresh_kev(&self, now: OffsetDateTime) -> Result<(), String> {
        let refreshed = sqlx::query_as::<_, (Option<String>,)>(
            "SELECT refreshed_at FROM osv_mirror WHERE ecosystem = ?",
        )
        .bind(KEV)
        .fetch_optional(self.store.pool())
        .await
        .map_err(|error| format!("cannot read osv_mirror: {error}"))?;
        if let Some((Some(refreshed_at),)) = refreshed
            && !is_due(&refreshed_at, now)
        {
            return Ok(());
        }
        match self.download_kev().await {
            Ok((count, date)) => self.mark_success(KEV, "download", count, date, now).await,
            Err(error) => {
                if let Err(write) = self.mark_failure(KEV, &error, now).await {
                    tracing::warn!(error = %write, "cannot record a KEV refresh failure");
                }
                Err(error)
            }
        }
    }

    async fn download_kev(&self) -> Result<(u64, Option<String>), String> {
        let body = self
            .fetch()
            .get_bytes(&self.endpoints.kev_url, self.limits().max_kev_bytes)
            .await?;
        let (entries, date) = kev::parse(&body)?;
        let count = u64::try_from(entries.len()).unwrap_or(u64::MAX);
        self.cache
            .replace_kev(entries)
            .await
            .map_err(|error| format!("cannot write the cache: {error}"))?;
        Ok((count, date))
    }

    fn fetch(&self) -> Fetch<'_> {
        Fetch::new(&self.client, &self.endpoints, &self.data_dir, self.limits())
    }

    fn limits(&self) -> Limits {
        lock(&self.limits).clone()
    }

    /// Writes the `osv_mirror` row of a successful pass.
    async fn mark_success(
        &self,
        name: &str,
        source: &str,
        count: u64,
        modified: Option<String>,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        let stamp = stamp(now);
        let mut write = self
            .store
            .begin_write()
            .await
            .map_err(|error| format!("cannot write osv_mirror: {error}"))?;
        sqlx::query(
            "INSERT INTO osv_mirror (ecosystem, source, record_count, data_modified_at, \
             refreshed_at, checked_at, last_error) VALUES (?, ?, ?, ?, ?, ?, NULL) \
             ON CONFLICT(ecosystem) DO UPDATE SET source = excluded.source, \
             record_count = excluded.record_count, data_modified_at = excluded.data_modified_at, \
             refreshed_at = excluded.refreshed_at, checked_at = excluded.checked_at, \
             last_error = NULL",
        )
        .bind(name)
        .bind(source)
        .bind(i64::try_from(count).unwrap_or(i64::MAX))
        .bind(modified)
        .bind(&stamp)
        .bind(&stamp)
        .execute(&mut *write.tx)
        .await
        .map_err(|error| format!("cannot write osv_mirror: {error}"))?;
        write
            .commit()
            .await
            .map_err(|error| format!("cannot write osv_mirror: {error}"))
    }

    /// Stamps a failed attempt: `checked_at` and `last_error` only, so the age
    /// the API shows keeps describing the data the primary still holds.
    async fn mark_failure(
        &self,
        name: &str,
        error: &str,
        now: OffsetDateTime,
    ) -> Result<(), String> {
        let message: String = error.chars().take(MAX_ERROR_CHARS).collect();
        let stamp = stamp(now);
        let mut write = self
            .store
            .begin_write()
            .await
            .map_err(|error| format!("cannot write osv_mirror: {error}"))?;
        sqlx::query(
            "INSERT INTO osv_mirror (ecosystem, source, record_count, data_modified_at, \
             refreshed_at, checked_at, last_error) VALUES (?, 'download', 0, NULL, NULL, ?, ?) \
             ON CONFLICT(ecosystem) DO UPDATE SET checked_at = excluded.checked_at, \
             last_error = excluded.last_error",
        )
        .bind(name)
        .bind(&stamp)
        .bind(&message)
        .execute(&mut *write.tx)
        .await
        .map_err(|error| format!("cannot write osv_mirror: {error}"))?;
        write
            .commit()
            .await
            .map_err(|error| format!("cannot write osv_mirror: {error}"))
    }
}

/// Locks without panicking on a poisoned mutex: the data is a set of names or
/// numbers, and a panic elsewhere is no reason to stop refreshing.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Whether `refreshed_at` is a day or more before `now`.
fn is_due(refreshed_at: &str, now: OffsetDateTime) -> bool {
    match OffsetDateTime::parse(refreshed_at, &Rfc3339) {
        Ok(then) => {
            now.unix_timestamp_nanos() - then.unix_timestamp_nanos() >= REFRESH_PERIOD_NANOS
        }
        // A stamp this code cannot read is no proof of freshness.
        Err(_) => true,
    }
}

/// `now` in the fixed-width form the control plane stores, with milliseconds.
fn stamp(now: OffsetDateTime) -> String {
    let now = now.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
        now.millisecond()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use std::collections::HashMap;
    use std::io::Write as _;
    use std::sync::Mutex as StdMutex;
    use zip::write::SimpleFileOptions;

    /// What the mock server answers, keyed by path.
    type Responses = Arc<StdMutex<HashMap<String, (u16, Vec<u8>)>>>;

    #[derive(Clone)]
    struct Mock {
        responses: Responses,
        paths: Arc<StdMutex<Vec<String>>>,
    }

    async fn handler(State(mock): State<Mock>, uri: Uri) -> Response {
        let path = uri.path().to_string();
        mock.paths.lock().unwrap().push(path.clone());
        let found = mock.responses.lock().unwrap().get(&path).cloned();
        match found {
            Some((status, body)) => Response::builder()
                .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
                .body(Body::from(body))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response()),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    /// Serves `responses` on an ephemeral port and returns the base URL and the
    /// list of paths requested so far.
    async fn serve(
        responses: Vec<(String, u16, Vec<u8>)>,
    ) -> (String, Responses, Arc<StdMutex<Vec<String>>>) {
        let responses: Responses = Arc::new(StdMutex::new(
            responses
                .into_iter()
                .map(|(path, status, body)| (path, (status, body)))
                .collect(),
        ));
        let paths = Arc::new(StdMutex::new(Vec::new()));
        let mock = Mock {
            responses: Arc::clone(&responses),
            paths: Arc::clone(&paths),
        };
        let app = Router::new().fallback(handler).with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{address}"), responses, paths)
    }

    fn zip_bytes(entries: &[(&str, String)]) -> Vec<u8> {
        let mut buffer = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut buffer);
            for (name, body) in entries {
                writer
                    .start_file(*name, SimpleFileOptions::default())
                    .unwrap();
                writer.write_all(body.as_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }
        buffer.into_inner()
    }

    fn record(id: &str, modified: &str) -> String {
        format!(
            r#"{{"id":"{id}","modified":"{modified}","affected":[{{"package":{{"ecosystem":"Debian:13","name":"{id}"}},"ranges":[{{"type":"ECOSYSTEM","events":[{{"introduced":"0"}}]}}]}}]}}"#
        )
    }

    fn kev_body() -> Vec<u8> {
        br#"{"dateReleased":"2026-11-04T20:00:05.774Z","vulnerabilities":[{"cveID":"CVE-2024-27834","dateAdded":"2026-11-04"}]}"#.to_vec()
    }

    fn temp_dir() -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn open(dir: &Path, base: &str) -> Vulns {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = Store::open_in_memory().await.unwrap();
        Vulns::open(dir, store)
            .await
            .unwrap()
            .with_endpoints(Endpoints {
                osv_base: base.to_string(),
                kev_url: format!("{base}/kev.json"),
            })
    }

    async fn mirror_rows(
        vulns: &Vulns,
    ) -> Vec<(
        String,
        String,
        i64,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    )> {
        sqlx::query_as(
            "SELECT ecosystem, source, record_count, data_modified_at, refreshed_at, checked_at, last_error \
             FROM osv_mirror ORDER BY ecosystem",
        )
        .fetch_all(vulns.store.pool())
        .await
        .unwrap()
    }

    fn requested(paths: &Arc<StdMutex<Vec<String>>>) -> Vec<String> {
        paths.lock().unwrap().clone()
    }

    fn assert_no_top_level_archive(paths: &[String]) {
        assert!(
            !paths.iter().any(|path| path == "/all.zip"),
            "the top-level archive was requested: {paths:?}"
        );
    }

    #[tokio::test]
    async fn a_first_pass_downloads_the_wanted_buckets_and_kev() {
        let dir = temp_dir();
        let (base, _responses, paths) = serve(vec![
            (
                "/Debian/all.zip".to_string(),
                200,
                zip_bytes(&[
                    ("CVE-1.json", record("CVE-1", "2026-01-01T00:00:00Z")),
                    ("CVE-9.json", record("CVE-9", "2026-02-01T00:00:00Z")),
                ]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
        ])
        .await;
        let vulns = open(&dir, &base).await;
        vulns.want("Debian:13");
        let now = OffsetDateTime::now_utc();
        let results = vulns.run_once(now).await;
        assert!(
            results.iter().all(|(_, result)| result.is_ok()),
            "{results:?}"
        );

        let paths = requested(&paths);
        assert!(paths.contains(&"/Debian/all.zip".to_string()), "{paths:?}");
        assert!(paths.contains(&"/Linux/all.zip".to_string()), "{paths:?}");
        assert!(paths.contains(&"/kev.json".to_string()), "{paths:?}");
        assert_no_top_level_archive(&paths);

        let rows = mirror_rows(&vulns).await;
        let debian = rows.iter().find(|row| row.0 == "Debian").unwrap();
        assert_eq!(debian.1, "download");
        assert_eq!(debian.2, 2);
        assert_eq!(debian.3.as_deref(), Some("2026-02-01T00:00:00Z"));
        assert_eq!(debian.4.as_deref(), Some(stamp(now).as_str()));
        assert!(debian.6.is_none());
        let kev = rows.iter().find(|row| row.0 == "KEV").unwrap();
        assert_eq!(kev.2, 1);
        assert_eq!(kev.3.as_deref(), Some("2026-11-04T20:00:05.774Z"));
        assert!(vulns.cache().is_kev(&["CVE-2024-27834"]).await.unwrap());
        assert_eq!(
            vulns
                .cache()
                .bucket("Debian")
                .await
                .unwrap()
                .unwrap()
                .record_count,
            2
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_second_pass_within_a_day_fetches_nothing() {
        let dir = temp_dir();
        let (base, _responses, paths) = serve(vec![
            (
                "/Debian/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-1.json", record("CVE-1", "2026-01-01T00:00:00Z"))]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
        ])
        .await;
        let vulns = open(&dir, &base).await;
        vulns.want("Debian:13");
        let now = OffsetDateTime::now_utc();
        vulns.run_once(now).await;
        let after_first = requested(&paths).len();

        vulns.run_once(now + time::Duration::hours(1)).await;
        assert_eq!(requested(&paths).len(), after_first, "nothing is due yet");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_pass_a_day_later_reads_the_change_list_and_deletes_what_404s() {
        let dir = temp_dir();
        let (base, _responses, paths) = serve(vec![
            (
                "/Debian/all.zip".to_string(),
                200,
                zip_bytes(&[
                    ("CVE-1.json", record("CVE-1", "2026-01-01T00:00:00Z")),
                    ("CVE-9.json", record("CVE-9", "2026-02-01T00:00:00Z")),
                ]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
            (
                "/Debian/modified_id.csv".to_string(),
                200,
                b"2026-03-01T00:00:00Z,CVE-3\n2026-02-01T00:00:00Z,CVE-9\n2026-01-01T00:00:00Z,CVE-1\n"
                    .to_vec(),
            ),
            (
                "/Debian/CVE-3.json".to_string(),
                200,
                record("CVE-3", "2026-03-01T00:00:00Z").into_bytes(),
            ),
            ("/Linux/modified_id.csv".to_string(), 200, Vec::new()),
        ])
        .await;
        let vulns = open(&dir, &base).await;
        vulns.want("Debian:13");
        let now = OffsetDateTime::now_utc();
        vulns.run_once(now).await;
        let generation = vulns.cache().generation().await.unwrap();

        let results = vulns.run_once(now + time::Duration::hours(25)).await;
        assert!(
            results.iter().all(|(_, result)| result.is_ok()),
            "{results:?}"
        );

        let paths = requested(&paths);
        assert!(
            paths.contains(&"/Debian/CVE-3.json".to_string()),
            "{paths:?}"
        );
        assert!(
            paths.contains(&"/Debian/CVE-9.json".to_string()),
            "the line at the cursor is fetched again: {paths:?}"
        );
        assert!(
            !paths.contains(&"/Debian/CVE-1.json".to_string()),
            "an older line is not fetched: {paths:?}"
        );
        assert_eq!(
            paths
                .iter()
                .filter(|path| path.as_str() == "/Debian/all.zip")
                .count(),
            1,
            "the delta did not download the archive again: {paths:?}"
        );
        assert_no_top_level_archive(&paths);

        assert!(vulns.cache().generation().await.unwrap() > generation);
        assert!(
            vulns
                .cache()
                .affected_for("Debian:13", "CVE-9")
                .await
                .unwrap()
                .is_empty(),
            "the record that 404'd is gone"
        );
        assert_eq!(
            vulns
                .cache()
                .affected_for("Debian:13", "CVE-3")
                .await
                .unwrap()
                .len(),
            1
        );
        let debian = vulns.cache().bucket("Debian").await.unwrap().unwrap();
        assert_eq!(debian.record_count, 2, "CVE-1 and CVE-3 remain");
        assert_eq!(debian.modified.as_deref(), Some("2026-03-01T00:00:00Z"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_long_change_list_downloads_the_whole_archive_again() {
        let dir = temp_dir();
        let (base, _responses, paths) = serve(vec![
            (
                "/Debian/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-1.json", record("CVE-1", "2026-01-01T00:00:00Z"))]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
            (
                "/Debian/modified_id.csv".to_string(),
                200,
                b"2026-03-01T00:00:00Z,CVE-3\n2026-02-02T00:00:00Z,CVE-4\n2026-02-01T00:00:00Z,CVE-9\n"
                    .to_vec(),
            ),
            ("/Linux/modified_id.csv".to_string(), 200, Vec::new()),
        ])
        .await;
        let mut vulns = open(&dir, &base).await;
        vulns = vulns.with_limits(Limits {
            delta_max_ids: 2,
            ..Limits::default()
        });
        vulns.want("Debian:13");
        let now = OffsetDateTime::now_utc();
        vulns.run_once(now).await;
        vulns.run_once(now + time::Duration::hours(25)).await;

        let paths = requested(&paths);
        let archives = paths
            .iter()
            .filter(|path| path.as_str() == "/Debian/all.zip")
            .count();
        assert_eq!(
            archives, 2,
            "the whole archive was downloaded again: {paths:?}"
        );
        assert!(
            !paths.iter().any(|path| path.starts_with("/Debian/CVE-")),
            "no record was fetched one by one: {paths:?}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_age_and_records_the_error() {
        let dir = temp_dir();
        let (base, responses, _paths) = serve(vec![
            (
                "/Debian/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-1.json", record("CVE-1", "2026-01-01T00:00:00Z"))]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
        ])
        .await;
        let vulns = open(&dir, &base).await;
        vulns.want("Debian:13");
        let now = OffsetDateTime::now_utc();
        vulns.run_once(now).await;
        let before = mirror_rows(&vulns)
            .await
            .into_iter()
            .find(|row| row.0 == "Debian")
            .unwrap();

        // The change list now answers 500.
        responses.lock().unwrap().insert(
            "/Debian/modified_id.csv".to_string(),
            (500, b"broken".to_vec()),
        );
        let later = now + time::Duration::hours(25);
        let results = vulns.run_once(later).await;
        let (_, debian_result) = results.iter().find(|(name, _)| name == "Debian").unwrap();
        assert!(debian_result.is_err());

        let after = mirror_rows(&vulns)
            .await
            .into_iter()
            .find(|row| row.0 == "Debian")
            .unwrap();
        assert_eq!(after.2, before.2, "record_count is kept");
        assert_eq!(after.3, before.3, "data_modified_at is kept");
        assert_eq!(after.4, before.4, "refreshed_at is kept");
        assert_eq!(after.5, stamp(later), "checked_at is the attempt");
        assert!(
            after
                .6
                .as_deref()
                .is_some_and(|error| error.contains("500")),
            "{:?}",
            after.6
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_body_over_the_cap_fails_and_leaves_no_part_file() {
        let dir = temp_dir();
        let (base, _responses, _paths) = serve(vec![
            (
                "/Debian/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-1.json", record("CVE-1", "2026-01-01T00:00:00Z"))]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
        ])
        .await;
        let vulns = open(&dir, &base).await.with_limits(Limits {
            max_zip_bytes: 16,
            ..Limits::default()
        });
        vulns.want("Debian:13");
        let results = vulns.run_once(OffsetDateTime::now_utc()).await;
        let (_, debian) = results.iter().find(|(name, _)| name == "Debian").unwrap();
        assert!(debian.is_err(), "the archive is over the cap");

        let download = dir.join("vulns-download");
        let leftovers: Vec<_> = std::fs::read_dir(&download)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.ends_with(".part"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");

        let row = mirror_rows(&vulns)
            .await
            .into_iter()
            .find(|row| row.0 == "Debian")
            .unwrap();
        assert_eq!(row.2, 0);
        assert_eq!(row.1, "download");
        assert!(row.6.is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_fresh_cache_file_empties_osv_mirror() {
        let dir = temp_dir();
        let _ = rustls::crypto::ring::default_provider().install_default();
        let store = Store::open_in_memory().await.unwrap();
        let mut write = store.begin_write().await.unwrap();
        sqlx::query(
            "INSERT INTO osv_mirror (ecosystem, source, record_count, checked_at) \
             VALUES ('Debian', 'download', 7, '2026-01-01T00:00:00.000Z')",
        )
        .execute(&mut *write.tx)
        .await
        .unwrap();
        write.commit().await.unwrap();

        let vulns = Vulns::open(&dir, store).await.unwrap();
        assert!(
            mirror_rows(&vulns).await.is_empty(),
            "a cache file that had to be created means the rows describe nothing"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn want_adds_a_supported_ecosystem_and_ignores_an_unsupported_one() {
        let dir = temp_dir();
        let (base, _responses, paths) = serve(vec![
            (
                "/Alpine/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-7.json", record("CVE-7", "2026-01-01T00:00:00Z"))]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
        ])
        .await;
        let vulns = open(&dir, &base).await;
        vulns.want("Alpine:v3.22");
        vulns.want("Fedora");
        vulns.run_once(OffsetDateTime::now_utc()).await;

        let rows = mirror_rows(&vulns).await;
        assert!(rows.iter().any(|row| row.0 == "Alpine"));
        assert!(!rows.iter().any(|row| row.0 == "Fedora"));
        let paths = requested(&paths);
        assert!(paths.contains(&"/Alpine/all.zip".to_string()));
        assert!(
            !paths.iter().any(|path| path.contains("Fedora")),
            "{paths:?}"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn an_inventory_brings_in_its_ecosystems() {
        let dir = temp_dir();
        let (base, _responses, paths) = serve(vec![
            (
                "/Alpine/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-7.json", record("CVE-7", "2026-01-01T00:00:00Z"))]),
            ),
            (
                "/Linux/all.zip".to_string(),
                200,
                zip_bytes(&[("CVE-5.json", record("CVE-5", "2026-03-01T00:00:00Z"))]),
            ),
            ("/kev.json".to_string(), 200, kev_body()),
        ])
        .await;
        let vulns = open(&dir, &base).await;
        vulns
            .cache()
            .put_inventory(
                "sha256:abc",
                "abc",
                r#"{"packages":[{"ecosystem":"Alpine:v3.22","name":"musl"}]}"#,
            )
            .await
            .unwrap();
        vulns.run_once(OffsetDateTime::now_utc()).await;
        assert!(requested(&paths).contains(&"/Alpine/all.zip".to_string()));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
