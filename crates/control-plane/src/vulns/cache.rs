// SPDX-License-Identifier: AGPL-3.0-only
//! The cache file next to the database: OSV records, KEV entries and the image
//! inventories the primary received.
//!
//! `<data_dir>/vulns-cache.db` is a second SQLite database. It is large, it
//! can always be fetched again, and it is not replicated and not part of a
//! control-plane backup, so it lives outside `quoro.db`. Only the data's age
//! (`osv_mirror`) and the findings stay in the control-plane database.
//!
//! A `meta.generation` counter is bumped by every write that changed
//! something, so a reader (the matcher) can tell whether its work is stale
//! without listing the tables. It lives in the file, not in this process, so
//! the server also sees what `quoro vuln import` wrote.
//!
//! A record is keyed by its bucket and id: one GitHub advisory (`GHSA-…`) is
//! published in every ecosystem it affects, so the same id arrives in several
//! archives, and each bucket keeps and replaces its own copy.
//!
//! Writes go through one gate, as in the control-plane store: a writer waits
//! on it before it takes a pool connection, so writers queued behind a long
//! one (a bucket replace parses its archive inside its transaction) never
//! hold every connection, and reads keep one. A node session reads this
//! file on its loop.

use crate::store::now_rfc3339;
use crate::vulns::osv::{self, Affected, Range, Record, Severity};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Row as _, Sqlite, SqlitePool, Transaction};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, OwnedMutexGuard};

/// The cache schema version. A file that holds another version is recreated.
const SCHEMA_VERSION: &str = "1";

/// The schema every cache file has.
const SCHEMA: &str = r"
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE buckets (
    bucket         TEXT PRIMARY KEY,
    record_count   INTEGER NOT NULL,
    modified_nanos INTEGER,
    modified       TEXT,
    refreshed_at   TEXT NOT NULL
);
CREATE TABLE records (
    id             TEXT NOT NULL,
    bucket         TEXT NOT NULL,
    modified_nanos INTEGER NOT NULL,
    modified       TEXT NOT NULL,
    published      TEXT,
    withdrawn      TEXT,
    summary        TEXT,
    aliases        TEXT NOT NULL,
    upstream       TEXT NOT NULL,
    severity       TEXT NOT NULL,
    db_severity    TEXT,
    PRIMARY KEY (bucket, id)
);
CREATE INDEX records_bucket ON records (bucket, modified_nanos);
CREATE TABLE affected (
    bucket    TEXT NOT NULL,
    record_id TEXT NOT NULL,
    ecosystem TEXT NOT NULL,
    package   TEXT NOT NULL,
    ranges    TEXT NOT NULL,
    versions  TEXT NOT NULL
);
CREATE INDEX affected_pkg ON affected (ecosystem, package);
CREATE INDEX affected_record ON affected (bucket, record_id);
CREATE TABLE kev (cve_id TEXT PRIMARY KEY, date_added TEXT);
CREATE TABLE inventories (
    digest      TEXT PRIMARY KEY,
    sha256      TEXT NOT NULL,
    received_at TEXT NOT NULL,
    json        TEXT NOT NULL
);
";

/// The tables the schema owns, dropped when the version differs.
const TABLES: [&str; 6] = [
    "meta",
    "buckets",
    "records",
    "affected",
    "kev",
    "inventories",
];

/// How many rows one insert batch holds.
const BATCH: usize = 500;

/// What one bucket of the cache holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketState {
    pub bucket: String,
    pub record_count: u64,
    /// The newest `modified` held, in unix nanoseconds.
    pub modified_nanos: Option<i128>,
    /// The newest `modified` held, as the record wrote it.
    pub modified: Option<String>,
    /// When the bucket was last brought up to date.
    pub refreshed_at: String,
}

/// One affected row joined with its record, for the matcher.
#[derive(Clone, Debug)]
pub struct AffectedRow {
    pub id: String,
    pub aliases: Vec<String>,
    pub upstream: Vec<String>,
    pub severity: Vec<Severity>,
    pub db_severity: Option<String>,
    pub summary: Option<String>,
    pub withdrawn: Option<String>,
    pub modified: String,
    pub ranges: Vec<Range>,
    pub versions: Vec<String>,
}

/// The counts of one bucket after a write.
struct BucketData {
    count: u64,
    nanos: Option<i128>,
    modified: Option<String>,
}

/// The cache database.
#[derive(Clone)]
pub struct Cache {
    pool: SqlitePool,
    /// The single-writer gate of this process.
    writer: Arc<Mutex<()>>,
}

/// A write transaction holding the writer gate. Dropping it without
/// [`CacheWrite::commit`] or [`CacheWrite::rollback`] rolls back.
struct CacheWrite {
    tx: Transaction<'static, Sqlite>,
    _gate: OwnedMutexGuard<()>,
}

impl CacheWrite {
    /// Commits and releases the gate.
    async fn commit(self) -> Result<(), sqlx::Error> {
        self.tx.commit().await
    }

    /// Rolls back and releases the gate.
    async fn rollback(self) -> Result<(), sqlx::Error> {
        self.tx.rollback().await
    }
}

impl Cache {
    /// Opens the cache at `path`, creating it when it is missing.
    ///
    /// The second value is `true` when the file was created or its schema was
    /// from another version and had to be thrown away; the caller then knows
    /// that `osv_mirror` describes data it no longer holds.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn open(path: &Path) -> Result<(Self, bool), sqlx::Error> {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;

        let has_meta: Option<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'meta'")
                .fetch_optional(&pool)
                .await?;
        let version = if has_meta.is_some() {
            sqlx::query_as::<_, (String,)>("SELECT value FROM meta WHERE key = 'schema'")
                .fetch_optional(&pool)
                .await?
                .map(|(value,)| value)
        } else {
            None
        };
        let mut created = false;
        if version.as_deref() != Some(SCHEMA_VERSION) {
            let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
            for table in TABLES {
                sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
                    .execute(&mut *tx)
                    .await?;
            }
            sqlx::raw_sql(SCHEMA).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO meta (key, value) VALUES ('schema', ?)")
                .bind(SCHEMA_VERSION)
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO meta (key, value) VALUES ('generation', '0')")
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            created = true;
        }
        Ok((
            Self {
                pool,
                writer: Arc::new(Mutex::new(())),
            },
            created,
        ))
    }

    /// Closes the pool and waits until its connections are closed, so a test
    /// can remove the directory the file is in.
    #[cfg(test)]
    pub(crate) async fn close(&self) {
        self.pool.close().await;
    }

    /// Starts a write transaction: the gate first, then a connection.
    ///
    /// It starts with `BEGIN IMMEDIATE`, so it takes SQLite's write lock at
    /// once and waits on `busy_timeout` for a writer in another process
    /// (`quoro vuln import`). A deferred `BEGIN` that reads first could not
    /// upgrade its snapshot once that process committed in between.
    async fn begin_write(&self) -> Result<CacheWrite, sqlx::Error> {
        let gate = Arc::clone(&self.writer).lock_owned().await;
        let tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        Ok(CacheWrite { tx, _gate: gate })
    }

    /// The counter bumped by every write that changed something, as the file
    /// holds it (another process may have written it).
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn generation(&self) -> Result<i64, sqlx::Error> {
        let value =
            sqlx::query_as::<_, (String,)>("SELECT value FROM meta WHERE key = 'generation'")
                .fetch_optional(&self.pool)
                .await?;
        Ok(value.and_then(|(value,)| value.parse().ok()).unwrap_or(0))
    }

    /// One bucket's state, or `None` while it was never fetched.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn bucket(&self, bucket: &str) -> Result<Option<BucketState>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT bucket, record_count, modified_nanos, modified, refreshed_at \
             FROM buckets WHERE bucket = ?",
        )
        .bind(bucket)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| BucketState {
            bucket: row.get("bucket"),
            record_count: u64::try_from(row.get::<i64, _>("record_count")).unwrap_or(0),
            modified_nanos: row.get::<Option<i64>, _>("modified_nanos").map(i128::from),
            modified: row.get("modified"),
            refreshed_at: row.get("refreshed_at"),
        }))
    }

    /// Every bucket the cache holds, by name.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn buckets(&self) -> Result<Vec<BucketState>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT bucket, record_count, modified_nanos, modified, refreshed_at \
             FROM buckets ORDER BY bucket",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| BucketState {
                bucket: row.get("bucket"),
                record_count: u64::try_from(row.get::<i64, _>("record_count")).unwrap_or(0),
                modified_nanos: row.get::<Option<i64>, _>("modified_nanos").map(i128::from),
                modified: row.get("modified"),
                refreshed_at: row.get("refreshed_at"),
            })
            .collect())
    }

    /// Replaces every record of `bucket` with `records`, in one transaction.
    ///
    /// Returns the number of records the bucket now holds. The whole bucket is
    /// written in batches, so an archive is never held in memory. When the
    /// data is the same as before nothing is kept and `generation` stays.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn replace_bucket<I>(&self, bucket: &str, records: I) -> Result<u64, sqlx::Error>
    where
        I: IntoIterator<Item = Record>,
    {
        let mut w = self.begin_write().await?;
        reset_temp(&mut w.tx).await?;
        sqlx::query(
            "CREATE TEMP TABLE old_records (id TEXT PRIMARY KEY, modified_nanos INTEGER NOT NULL)",
        )
        .execute(&mut *w.tx)
        .await?;
        sqlx::query(
            "INSERT INTO old_records (id, modified_nanos) \
             SELECT id, modified_nanos FROM records WHERE bucket = ?",
        )
        .bind(bucket)
        .execute(&mut *w.tx)
        .await?;
        sqlx::query("DELETE FROM affected WHERE bucket = ?")
            .bind(bucket)
            .execute(&mut *w.tx)
            .await?;

        let mut batch = Vec::with_capacity(BATCH);
        for record in records {
            batch.push(record);
            if batch.len() == BATCH {
                flush(&mut w.tx, bucket, &mut batch, true).await?;
            }
        }
        flush(&mut w.tx, bucket, &mut batch, true).await?;

        let (old, new) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT (SELECT count(*) FROM old_records), (SELECT count(*) FROM incoming_records)",
        )
        .fetch_one(&mut *w.tx)
        .await?;
        let same = sqlx::query_as::<_, (i64,)>(
            "SELECT count(*) FROM incoming_records i \
             JOIN old_records o ON o.id = i.id AND o.modified_nanos = i.modified_nanos",
        )
        .fetch_one(&mut *w.tx)
        .await?
        .0;
        let changed = old != new || same != new;

        if !changed {
            w.rollback().await?;
            self.touch_bucket(bucket).await?;
            self.checkpoint().await;
            return Ok(u64::try_from(new).unwrap_or(0));
        }
        sqlx::query(
            "DELETE FROM records WHERE bucket = ? AND id NOT IN (SELECT id FROM incoming_records)",
        )
        .bind(bucket)
        .execute(&mut *w.tx)
        .await?;
        let data = recompute(&mut w.tx, bucket).await?;
        write_bucket(&mut w.tx, bucket, &data).await?;
        bump(&mut w.tx).await?;
        w.commit().await?;
        self.checkpoint().await;
        Ok(data.count)
    }

    /// Folds the write-ahead log back into the file and truncates it. A
    /// bucket replace writes the whole bucket in one transaction, so without
    /// this the log stays as large as the largest archive (npm's is over a
    /// gigabyte unpacked) on a disk the primary shares. Best effort: a reader
    /// holding an old snapshot only delays it to the next replace.
    ///
    /// A truncating checkpoint blocks writers while it waits for readers, so
    /// it holds the gate like a write.
    async fn checkpoint(&self) {
        let _gate = self.writer.lock().await;
        if let Err(error) = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await
        {
            tracing::debug!(error = %error, "vulnerability cache checkpoint failed");
        }
    }

    /// Inserts or replaces `records` in `bucket` and refreshes its timestamp.
    ///
    /// Used by a delta refresh: only the ids the change list named are
    /// touched. A record whose `modified` is unchanged is left alone.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn upsert_records<I>(&self, bucket: &str, records: I) -> Result<u64, sqlx::Error>
    where
        I: IntoIterator<Item = Record>,
    {
        let mut w = self.begin_write().await?;
        let mut changed = 0u64;
        let mut batch = Vec::with_capacity(BATCH);
        for record in records {
            let existing = sqlx::query_as::<_, (i64,)>(
                "SELECT modified_nanos FROM records WHERE bucket = ? AND id = ?",
            )
            .bind(bucket)
            .bind(&record.id)
            .fetch_optional(&mut *w.tx)
            .await?
            .map(|(nanos,)| nanos);
            if existing == Some(nanos_i64(modified_nanos(&record))) {
                continue;
            }
            changed += 1;
            batch.push(record);
            if batch.len() == BATCH {
                flush(&mut w.tx, bucket, &mut batch, false).await?;
            }
        }
        flush(&mut w.tx, bucket, &mut batch, false).await?;
        let data = recompute(&mut w.tx, bucket).await?;
        write_bucket(&mut w.tx, bucket, &data).await?;
        if changed > 0 {
            bump(&mut w.tx).await?;
        }
        w.commit().await?;
        Ok(changed)
    }

    /// Deletes the records `ids` names from `bucket` and refreshes its
    /// timestamp. A 404 on a record during a delta means it was removed.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn delete_records(&self, bucket: &str, ids: &[String]) -> Result<u64, sqlx::Error> {
        let mut w = self.begin_write().await?;
        let mut deleted = 0u64;
        for id in ids {
            sqlx::query("DELETE FROM affected WHERE bucket = ? AND record_id = ?")
                .bind(bucket)
                .bind(id)
                .execute(&mut *w.tx)
                .await?;
            let result = sqlx::query("DELETE FROM records WHERE id = ? AND bucket = ?")
                .bind(id)
                .bind(bucket)
                .execute(&mut *w.tx)
                .await?;
            deleted += result.rows_affected();
        }
        let data = recompute(&mut w.tx, bucket).await?;
        write_bucket(&mut w.tx, bucket, &data).await?;
        if deleted > 0 {
            bump(&mut w.tx).await?;
        }
        w.commit().await?;
        Ok(deleted)
    }

    /// Records that `bucket` was brought up to date without changing anything.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn touch_bucket(&self, bucket: &str) -> Result<(), sqlx::Error> {
        let mut w = self.begin_write().await?;
        let data = recompute(&mut w.tx, bucket).await?;
        write_bucket(&mut w.tx, bucket, &data).await?;
        w.commit().await
    }

    /// Replaces the KEV catalog. The whole catalog is small, so it is written
    /// as one swap.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn replace_kev<I>(&self, entries: I) -> Result<(), sqlx::Error>
    where
        I: IntoIterator<Item = (String, Option<String>)>,
    {
        let incoming: HashMap<String, Option<String>> = entries.into_iter().collect();
        let rows = sqlx::query("SELECT cve_id, date_added FROM kev")
            .fetch_all(&self.pool)
            .await?;
        let existing: HashMap<String, Option<String>> = rows
            .into_iter()
            .map(|row| (row.get("cve_id"), row.get("date_added")))
            .collect();
        if existing == incoming {
            return Ok(());
        }
        let mut w = self.begin_write().await?;
        sqlx::query("DELETE FROM kev").execute(&mut *w.tx).await?;
        let mut batch: Vec<(String, Option<String>)> = Vec::with_capacity(BATCH);
        for entry in incoming {
            batch.push(entry);
            if batch.len() == BATCH {
                flush_kev(&mut w.tx, &mut batch).await?;
            }
        }
        flush_kev(&mut w.tx, &mut batch).await?;
        bump(&mut w.tx).await?;
        w.commit().await?;
        Ok(())
    }

    /// Whether any of `ids` is in the KEV catalog.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn is_kev(&self, ids: &[&str]) -> Result<bool, sqlx::Error> {
        for id in ids {
            let found = sqlx::query_as::<_, (i64,)>("SELECT 1 FROM kev WHERE cve_id = ?")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
            if found.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The affected rows of `package` in `ecosystem`, joined with their
    /// records.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn affected_for(
        &self,
        ecosystem: &str,
        package: &str,
    ) -> Result<Vec<AffectedRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT a.record_id, a.ranges, a.versions, r.aliases, r.upstream, r.severity, \
             r.db_severity, r.summary, r.withdrawn, r.modified \
             FROM affected a JOIN records r ON r.bucket = a.bucket AND r.id = a.record_id \
             WHERE a.ecosystem = ? AND a.package = ? \
             ORDER BY r.modified_nanos DESC",
        )
        .bind(ecosystem)
        .bind(package)
        .fetch_all(&self.pool)
        .await?;
        // A record held by several buckets is the same record: list it once.
        let mut seen = std::collections::HashSet::new();
        Ok(rows
            .into_iter()
            .filter(|row| {
                seen.insert((
                    row.get::<String, _>("record_id"),
                    row.get::<String, _>("ranges"),
                    row.get::<String, _>("versions"),
                ))
            })
            .map(|row| AffectedRow {
                id: row.get("record_id"),
                aliases: from_json(row.get("aliases")),
                upstream: from_json(row.get("upstream")),
                severity: from_json(row.get("severity")),
                db_severity: row.get("db_severity"),
                summary: row.get("summary"),
                withdrawn: row.get("withdrawn"),
                modified: row.get("modified"),
                ranges: from_json(row.get("ranges")),
                versions: from_json(row.get("versions")),
            })
            .collect())
    }

    /// Keeps one image inventory, keyed by its digest.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn put_inventory(
        &self,
        digest: &str,
        sha256: &str,
        json: &str,
    ) -> Result<(), sqlx::Error> {
        let existing = sqlx::query_as::<_, (String, String)>(
            "SELECT sha256, json FROM inventories WHERE digest = ?",
        )
        .bind(digest)
        .fetch_optional(&self.pool)
        .await?;
        if existing.as_ref() == Some(&(sha256.to_string(), json.to_string())) {
            return Ok(());
        }
        let mut w = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO inventories (digest, sha256, received_at, json) VALUES (?, ?, ?, ?) \
             ON CONFLICT(digest) DO UPDATE SET sha256 = excluded.sha256, \
             received_at = excluded.received_at, json = excluded.json",
        )
        .bind(digest)
        .bind(sha256)
        .bind(now_rfc3339())
        .bind(json)
        .execute(&mut *w.tx)
        .await?;
        bump(&mut w.tx).await?;
        w.commit().await?;
        Ok(())
    }

    /// One inventory's `sha256` and JSON.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn inventory(&self, digest: &str) -> Result<Option<(String, String)>, sqlx::Error> {
        let row = sqlx::query_as::<_, (String, String)>(
            "SELECT sha256, json FROM inventories WHERE digest = ?",
        )
        .bind(digest)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// The hash of the inventory held for `digest`, without reading its JSON.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn inventory_sha(&self, digest: &str) -> Result<Option<String>, sqlx::Error> {
        let row = sqlx::query_as::<_, (String,)>("SELECT sha256 FROM inventories WHERE digest = ?")
            .bind(digest)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(sha256,)| sha256))
    }

    /// Every inventory digest held.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn inventory_digests(&self) -> Result<Vec<String>, sqlx::Error> {
        let rows = sqlx::query_as::<_, (String,)>("SELECT digest FROM inventories ORDER BY digest")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(|(digest,)| digest).collect())
    }

    /// Forgets one inventory.
    ///
    /// # Errors
    ///
    /// Any database error.
    pub async fn delete_inventory(&self, digest: &str) -> Result<bool, sqlx::Error> {
        let mut w = self.begin_write().await?;
        let result = sqlx::query("DELETE FROM inventories WHERE digest = ?")
            .bind(digest)
            .execute(&mut *w.tx)
            .await?;
        let deleted = result.rows_affected() > 0;
        if deleted {
            bump(&mut w.tx).await?;
        }
        w.commit().await?;
        Ok(deleted)
    }
}

/// The unix nanoseconds of an i128 instant, as SQLite stores integers.
fn nanos_i64(nanos: i128) -> i64 {
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

/// Empties the per-transaction temp tables.
async fn reset_temp(tx: &mut Transaction<'_, Sqlite>) -> Result<(), sqlx::Error> {
    sqlx::query("DROP TABLE IF EXISTS temp.incoming_records")
        .execute(&mut **tx)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS temp.old_records")
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "CREATE TEMP TABLE incoming_records (id TEXT PRIMARY KEY, modified_nanos INTEGER NOT NULL)",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Writes a batch of records of `bucket`, and records its ids in the temp
/// table when `track` says the caller compares the whole bucket afterwards.
async fn flush(
    tx: &mut Transaction<'_, Sqlite>,
    bucket: &str,
    batch: &mut Vec<Record>,
    track: bool,
) -> Result<(), sqlx::Error> {
    for record in batch.drain(..) {
        insert_record(tx, bucket, &record).await?;
        if track {
            sqlx::query(
                "INSERT OR REPLACE INTO incoming_records (id, modified_nanos) VALUES (?, ?)",
            )
            .bind(&record.id)
            .bind(nanos_i64(modified_nanos(&record)))
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

/// Writes a batch of KEV entries.
async fn flush_kev(
    tx: &mut Transaction<'_, Sqlite>,
    batch: &mut Vec<(String, Option<String>)>,
) -> Result<(), sqlx::Error> {
    for (cve_id, date_added) in batch.drain(..) {
        sqlx::query("INSERT INTO kev (cve_id, date_added) VALUES (?, ?)")
            .bind(cve_id)
            .bind(date_added)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

/// Inserts or replaces one record and its affected rows.
async fn insert_record(
    tx: &mut Transaction<'_, Sqlite>,
    bucket: &str,
    record: &Record,
) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM affected WHERE bucket = ? AND record_id = ?")
        .bind(bucket)
        .bind(&record.id)
        .execute(&mut **tx)
        .await?;
    let aliases = to_json(&record.aliases);
    let upstream = to_json(&record.upstream);
    let severity = to_json(&record.severity);
    sqlx::query(
        "INSERT INTO records (id, bucket, modified_nanos, modified, published, withdrawn, \
         summary, aliases, upstream, severity, db_severity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(bucket, id) DO UPDATE SET \
         modified_nanos = excluded.modified_nanos, modified = excluded.modified, \
         published = excluded.published, withdrawn = excluded.withdrawn, summary = excluded.summary, \
         aliases = excluded.aliases, upstream = excluded.upstream, severity = excluded.severity, \
         db_severity = excluded.db_severity",
    )
    .bind(&record.id)
    .bind(bucket)
    .bind(nanos_i64(modified_nanos(record)))
    .bind(&record.modified)
    .bind(&record.published)
    .bind(&record.withdrawn)
    .bind(&record.summary)
    .bind(aliases)
    .bind(upstream)
    .bind(severity)
    .bind(&record.db_severity)
    .execute(&mut **tx)
    .await?;
    for affected in &record.affected {
        insert_affected(tx, bucket, &record.id, affected).await?;
    }
    Ok(())
}

async fn insert_affected(
    tx: &mut Transaction<'_, Sqlite>,
    bucket: &str,
    record_id: &str,
    affected: &Affected,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO affected (bucket, record_id, ecosystem, package, ranges, versions) \
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(bucket)
    .bind(record_id)
    .bind(&affected.ecosystem)
    .bind(&affected.package)
    .bind(to_json(&affected.ranges))
    .bind(to_json(&affected.versions))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The counts of one bucket as the records table holds them.
async fn recompute(
    tx: &mut Transaction<'_, Sqlite>,
    bucket: &str,
) -> Result<BucketData, sqlx::Error> {
    let (count, nanos) = sqlx::query_as::<_, (i64, Option<i64>)>(
        "SELECT count(*), max(modified_nanos) FROM records WHERE bucket = ?",
    )
    .bind(bucket)
    .fetch_one(&mut **tx)
    .await?;
    let modified = sqlx::query_as::<_, (String,)>(
        "SELECT modified FROM records WHERE bucket = ? ORDER BY modified_nanos DESC LIMIT 1",
    )
    .bind(bucket)
    .fetch_optional(&mut **tx)
    .await?
    .map(|(modified,)| modified);
    Ok(BucketData {
        count: u64::try_from(count).unwrap_or(0),
        nanos: nanos.map(i128::from),
        modified,
    })
}

async fn write_bucket(
    tx: &mut Transaction<'_, Sqlite>,
    bucket: &str,
    data: &BucketData,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO buckets (bucket, record_count, modified_nanos, modified, refreshed_at) \
         VALUES (?, ?, ?, ?, ?) ON CONFLICT(bucket) DO UPDATE SET \
         record_count = excluded.record_count, modified_nanos = excluded.modified_nanos, \
         modified = excluded.modified, refreshed_at = excluded.refreshed_at",
    )
    .bind(bucket)
    .bind(i64::try_from(data.count).unwrap_or(i64::MAX))
    .bind(data.nanos.map(nanos_i64))
    .bind(&data.modified)
    .bind(now_rfc3339())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Bumps the generation counter inside `tx`.
async fn bump(tx: &mut Transaction<'_, Sqlite>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'generation'",
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn to_json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "[]".to_string())
}

fn from_json<T: serde::de::DeserializeOwned + Default>(text: String) -> T {
    serde_json::from_str(&text).unwrap_or_default()
}

/// A record's `modified` in unix nanoseconds.
fn modified_nanos(record: &Record) -> i128 {
    osv::modified_nanos(&record.modified).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path() -> (std::path::PathBuf, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("vulns-cache.db");
        (dir, path)
    }

    fn record(id: &str, modified: &str, package: &str, fixed: &str) -> Record {
        Record {
            id: id.to_string(),
            modified: modified.to_string(),
            summary: Some(format!("{id} summary")),
            affected: vec![Affected {
                ecosystem: "Debian:13".to_string(),
                package: package.to_string(),
                ranges: vec![Range {
                    kind: "ECOSYSTEM".to_string(),
                    events: vec![
                        osv::Event {
                            introduced: Some("0".to_string()),
                            ..Default::default()
                        },
                        osv::Event {
                            fixed: Some(fixed.to_string()),
                            ..Default::default()
                        },
                    ],
                }],
                versions: vec![],
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn one_advisory_in_two_buckets_is_kept_by_each() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        let shared = record("GHSA-xxxx", "2026-01-01T00:00:00Z", "a", "1");
        cache
            .replace_bucket("npm", vec![shared.clone()])
            .await
            .unwrap();
        cache.replace_bucket("PyPI", vec![shared]).await.unwrap();
        // Replacing one bucket without it leaves the other's copy alone.
        cache.replace_bucket("npm", Vec::new()).await.unwrap();
        assert_eq!(cache.bucket("npm").await.unwrap().unwrap().record_count, 0);
        assert_eq!(cache.bucket("PyPI").await.unwrap().unwrap().record_count, 1);
        let rows = cache.affected_for("Debian:13", "a").await.unwrap();
        assert_eq!(rows.len(), 1);
        // Held by both again, it is still listed once.
        cache
            .replace_bucket(
                "npm",
                vec![record("GHSA-xxxx", "2026-01-01T00:00:00Z", "a", "1")],
            )
            .await
            .unwrap();
        assert_eq!(cache.affected_for("Debian:13", "a").await.unwrap().len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn another_process_bumping_the_generation_is_seen() {
        let (dir, path) = temp_path();
        let (server, _) = Cache::open(&path).await.unwrap();
        let (cli, created) = Cache::open(&path).await.unwrap();
        assert!(!created);
        let before = server.generation().await.unwrap();
        cli.replace_bucket(
            "Debian",
            vec![record("CVE-1", "2026-01-01T00:00:00Z", "a", "1")],
        )
        .await
        .unwrap();
        assert!(server.generation().await.unwrap() > before);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn replace_bucket_swaps_one_bucket_and_leaves_others_alone() {
        let (dir, path) = temp_path();
        let (cache, created) = Cache::open(&path).await.unwrap();
        assert!(created);
        assert_eq!(cache.generation().await.unwrap(), 0);
        let first = cache
            .replace_bucket(
                "Debian",
                vec![record("CVE-1", "2026-01-01T00:00:00Z", "a", "1")],
            )
            .await
            .unwrap();
        assert_eq!(first, 1);
        cache
            .replace_bucket(
                "Linux",
                vec![record("CVE-9", "2026-01-01T00:00:00Z", "k", "1")],
            )
            .await
            .unwrap();
        let generation = cache.generation().await.unwrap();
        assert!(generation > 0);

        let count = cache
            .replace_bucket(
                "Debian",
                vec![
                    record("CVE-2", "2026-02-01T00:00:00Z", "a", "2"),
                    record("CVE-3", "2026-01-15T00:00:00Z", "b", "3"),
                ],
            )
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert!(cache.generation().await.unwrap() > generation);
        let debian = cache.bucket("Debian").await.unwrap().unwrap();
        assert_eq!(debian.record_count, 2);
        assert_eq!(debian.modified.as_deref(), Some("2026-02-01T00:00:00Z"));
        assert_eq!(
            debian.modified_nanos,
            Some(osv::modified_nanos("2026-02-01T00:00:00Z").unwrap())
        );
        let linux = cache.bucket("Linux").await.unwrap().unwrap();
        assert_eq!(linux.record_count, 1, "the other bucket is untouched");
        assert_eq!(cache.buckets().await.unwrap().len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn the_generation_stays_when_a_replace_changes_nothing() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        let data = vec![record("CVE-1", "2026-01-01T00:00:00Z", "a", "1")];
        cache.replace_bucket("Debian", data.clone()).await.unwrap();
        let generation = cache.generation().await.unwrap();
        cache.replace_bucket("Debian", data).await.unwrap();
        assert_eq!(cache.generation().await.unwrap(), generation);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn upsert_replaces_affected_rows_and_delete_removes_them() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![record("CVE-1", "2026-01-01T00:00:00Z", "openssl", "1.0")],
            )
            .await
            .unwrap();
        let rows = cache.affected_for("Debian:13", "openssl").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ranges[0].events[1].fixed.as_deref(), Some("1.0"));

        let generation = cache.generation().await.unwrap();
        cache
            .upsert_records(
                "Debian",
                vec![record("CVE-1", "2026-03-01T00:00:00Z", "openssl", "2.0")],
            )
            .await
            .unwrap();
        assert!(cache.generation().await.unwrap() > generation);
        let rows = cache.affected_for("Debian:13", "openssl").await.unwrap();
        assert_eq!(rows.len(), 1, "the old affected row was replaced");
        assert_eq!(rows[0].ranges[0].events[1].fixed.as_deref(), Some("2.0"));
        assert_eq!(rows[0].modified, "2026-03-01T00:00:00Z");

        assert!(
            cache
                .delete_records("Debian", &["CVE-1".to_string()])
                .await
                .unwrap()
                == 1
        );
        assert!(
            cache
                .affected_for("Debian:13", "openssl")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            cache.bucket("Debian").await.unwrap().unwrap().record_count,
            0
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn affected_for_finds_by_ecosystem_and_package() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_bucket(
                "Debian",
                vec![
                    record("CVE-1", "2026-01-01T00:00:00Z", "openssl", "1.0"),
                    record("CVE-2", "2026-01-01T00:00:00Z", "bash", "5.0"),
                ],
            )
            .await
            .unwrap();
        let rows = cache.affected_for("Debian:13", "bash").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "CVE-2");
        assert!(
            cache
                .affected_for("Debian:13", "zsh")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            cache
                .affected_for("Alpine", "bash")
                .await
                .unwrap()
                .is_empty()
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn kev_checks_any_of_several_ids() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .replace_kev(vec![
                ("CVE-2024-27834".to_string(), Some("2026-11-04".to_string())),
                ("CVE-2023-1".to_string(), None),
            ])
            .await
            .unwrap();
        assert!(
            cache
                .is_kev(&["CVE-0000-0", "CVE-2024-27834"])
                .await
                .unwrap()
        );
        assert!(!cache.is_kev(&["CVE-0000-0"]).await.unwrap());
        assert!(!cache.is_kev(&[]).await.unwrap());
        let generation = cache.generation().await.unwrap();
        cache
            .replace_kev(vec![
                ("CVE-2024-27834".to_string(), Some("2026-11-04".to_string())),
                ("CVE-2023-1".to_string(), None),
            ])
            .await
            .unwrap();
        assert_eq!(cache.generation().await.unwrap(), generation);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn inventories_are_keyed_by_digest() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        cache
            .put_inventory("sha256:aaa", "abc", "{\"packages\":[]}")
            .await
            .unwrap();
        assert_eq!(
            cache.inventory("sha256:aaa").await.unwrap(),
            Some(("abc".to_string(), "{\"packages\":[]}".to_string()))
        );
        assert_eq!(cache.inventory_digests().await.unwrap(), vec!["sha256:aaa"]);
        let generation = cache.generation().await.unwrap();
        cache
            .put_inventory("sha256:aaa", "abc", "{\"packages\":[]}")
            .await
            .unwrap();
        assert_eq!(cache.generation().await.unwrap(), generation);
        assert!(cache.delete_inventory("sha256:aaa").await.unwrap());
        assert!(!cache.delete_inventory("sha256:aaa").await.unwrap());
        assert!(cache.inventory("sha256:aaa").await.unwrap().is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A long cache write (a bucket replace parsing an archive) while several
    /// image inventories arrive: the reads a node session waits on must not
    /// queue behind the writers for a pool connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_long_write_leaves_reads_a_connection() {
        let (dir, path) = temp_path();
        let (cache, _) = Cache::open(&path).await.unwrap();
        let hold = Duration::from_millis(2_000);
        let slow = cache.clone();
        let replace = tokio::spawn(async move {
            // The archive iterator parses inside the write transaction.
            let records = (0..20).map(|index| {
                std::thread::sleep(hold / 20);
                record(&format!("CVE-{index}"), "2026-01-01T00:00:00Z", "a", "1")
            });
            slow.replace_bucket("Debian", records).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut writes = Vec::new();
        for index in 0..4 {
            let cache = cache.clone();
            writes.push(tokio::spawn(async move {
                cache
                    .put_inventory(&format!("sha256:{index}"), "abc", "{\"packages\":[]}")
                    .await
                    .unwrap();
            }));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = std::time::Instant::now();
        cache.inventory_sha("sha256:none").await.unwrap();
        let waited = started.elapsed();
        eprintln!("inventory_sha waited {waited:?} behind a {hold:?} write");
        replace.await.unwrap();
        for write in writes {
            write.await.unwrap();
        }
        assert_eq!(cache.inventory_digests().await.unwrap().len(), 4);
        assert!(
            waited < Duration::from_millis(500),
            "a read waited {waited:?} for a connection"
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_file_with_another_schema_is_recreated() {
        let (dir, path) = temp_path();
        let (cache, created) = Cache::open(&path).await.unwrap();
        assert!(created);
        cache
            .replace_bucket(
                "Debian",
                vec![record("CVE-1", "2026-01-01T00:00:00Z", "a", "1")],
            )
            .await
            .unwrap();
        {
            let options = SqliteConnectOptions::new().filename(&path);
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .unwrap();
            sqlx::query("UPDATE meta SET value = '999' WHERE key = 'schema'")
                .execute(&pool)
                .await
                .unwrap();
            pool.close().await;
        }
        let (cache, created) = Cache::open(&path).await.unwrap();
        assert!(created, "an old schema reports the cache as recreated");
        assert!(cache.bucket("Debian").await.unwrap().is_none());
        assert_eq!(cache.generation().await.unwrap(), 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
