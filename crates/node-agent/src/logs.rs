// SPDX-License-Identifier: AGPL-3.0-only
//! Local app-output and deployment-progress storage (`node-agent-protocol.md`
//! sections 5.7, 5.9, 5.15, 5.17 and 12).
//!
//! App output is captured from the engine's tagged console stream, one reader
//! per VM, and appended to SQLite in batches. Every stored line carries a
//! node-wide strictly increasing instant in nanoseconds; the control plane
//! turns that instant back into the SSE event id and the `since` cursor, so it
//! is the ordering key and nothing else may reorder it.
//!
//! The live fan-out is a bounded broadcast channel. A stream that falls behind
//! skips lines rather than blocking capture, as section 5.7 allows: the stored
//! rows remain the source of truth.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use quoro_engine::{Engine, EngineError, VmId};
use quoro_proto::wire::LogStream;
use sqlx::Row as _;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::AgentError;
use crate::store::NodeStore;

/// Lines kept per VM; older ones are pruned after each batch.
pub const RETENTION_PER_VM: i64 = 10_000;
/// Lines kept across the node; older ones are pruned after each batch.
pub const RETENTION_TOTAL: i64 = 200_000;

/// How many lines are written in one transaction.
pub const BATCH_LINES: usize = 200;
/// How often a partially filled batch is flushed.
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(200);
/// How long a collector waits before re-attaching to a VM's stream.
pub const REATTACH_DELAY: Duration = Duration::from_secs(1);

/// One captured line, as it goes to storage and to the live fan-out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturedLine {
    /// The VM that produced the line.
    pub vm_id: String,
    /// The app the VM belonged to, when the spec named one.
    pub app_id: Option<String>,
    /// The node-wide strictly increasing instant.
    pub time_ns: i64,
    /// `stdout` or `stderr`.
    pub stream: LogStream,
    /// The line without its trailing newline.
    pub line: String,
}

/// One line to append; the store assigns its instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLogLine {
    /// The VM that produced the line.
    pub vm_id: String,
    /// The app the VM belonged to, when known.
    pub app_id: Option<String>,
    /// `stdout` or `stderr`.
    pub stream: LogStream,
    /// The line without its trailing newline.
    pub line: String,
}

/// One stored log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredLogLine {
    /// The VM that produced the line.
    pub vm_id: String,
    /// The app the VM belonged to, when known.
    pub app_id: Option<String>,
    /// The node-wide strictly increasing instant.
    pub time_ns: i64,
    /// `stdout` or `stderr`.
    pub stream: LogStream,
    /// The line without its trailing newline.
    pub line: String,
}

/// Which lines a query or a stream selects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogFilter {
    /// Every VM of this app, including VMs created later.
    App(String),
    /// One VM.
    Vm(String),
}

impl LogFilter {
    /// Whether a captured line matches.
    #[must_use]
    pub fn matches(&self, line: &CapturedLine) -> bool {
        match self {
            Self::App(app_id) => line.app_id.as_deref() == Some(app_id.as_str()),
            Self::Vm(vm_id) => line.vm_id == *vm_id,
        }
    }
}

/// One stored deployment-progress line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredDeploymentLine {
    /// The deployment the line belongs to.
    pub deployment_id: String,
    /// `pull`, `rootfs`, `boot` or `health`.
    pub phase: String,
    /// The node-wide strictly increasing instant.
    pub time_ns: i64,
    /// The message.
    pub message: String,
    /// The VM the line is about, when it is known.
    pub vm_id: Option<String>,
}

impl NodeStore {
    /// Appends a batch, assigning each line its strictly increasing instant,
    /// pruning retention, and broadcasting the lines.
    ///
    /// The write is one transaction, so a crash never leaves half a batch;
    /// the broadcast happens after the commit, so a reader never sees a line
    /// that is not stored.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn append_log_lines(&self, lines: &[NewLogLine]) -> Result<(), AgentError> {
        if lines.is_empty() {
            return Ok(());
        }
        let _gate = self.log_gate.lock().await;
        let captured: Vec<CapturedLine> = lines
            .iter()
            .map(|line| CapturedLine {
                vm_id: line.vm_id.clone(),
                app_id: line.app_id.clone(),
                time_ns: self.next_log_time(),
                stream: line.stream,
                line: line.line.clone(),
            })
            .collect();

        let mut tx = self.pool.begin().await.map_err(log_error)?;
        for line in &captured {
            sqlx::query(
                "INSERT INTO log_lines (vm_id, app_id, time_ns, stream, line) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(&line.vm_id)
            .bind(line.app_id.as_deref())
            .bind(line.time_ns)
            .bind(stream_name(&line.stream))
            .bind(&line.line)
            .execute(&mut *tx)
            .await
            .map_err(log_error)?;
        }
        prune_vm(&mut tx, &captured).await?;
        let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM log_lines")
            .fetch_one(&mut *tx)
            .await
            .map_err(log_error)?
            .try_get("n")
            .map_err(log_error)?;
        if count > RETENTION_TOTAL {
            sqlx::query(
                "DELETE FROM log_lines WHERE id IN \
                 (SELECT id FROM log_lines ORDER BY id ASC LIMIT ?)",
            )
            .bind(count - RETENTION_TOTAL)
            .execute(&mut *tx)
            .await
            .map_err(log_error)?;
        }
        tx.commit().await.map_err(log_error)?;

        for line in captured {
            // No receiver is not an error: the line is stored.
            let _ = self.logs_tx.send(line);
        }
        Ok(())
    }

    /// The lines matching `filter` between two instants, oldest first.
    ///
    /// `since` and `until` are inclusive; `tail` keeps only the newest N
    /// matching lines.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure or an unparseable stream.
    pub async fn query_log_lines(
        &self,
        filter: &LogFilter,
        since_ns: Option<i64>,
        until_ns: Option<i64>,
        tail: Option<usize>,
    ) -> Result<Vec<StoredLogLine>, AgentError> {
        let mut sql = String::from(
            "SELECT id, vm_id, app_id, time_ns, stream, line FROM log_lines WHERE 1 = 1",
        );
        match filter {
            LogFilter::App(_) => sql.push_str(" AND app_id = ?"),
            LogFilter::Vm(_) => sql.push_str(" AND vm_id = ?"),
        }
        if since_ns.is_some() {
            sql.push_str(" AND time_ns >= ?");
        }
        if until_ns.is_some() {
            sql.push_str(" AND time_ns <= ?");
        }
        let newest_first = tail.is_some();
        let sql = if tail.is_some() {
            format!(
                "SELECT * FROM ({sql} ORDER BY time_ns ASC, id ASC) \
                 ORDER BY time_ns DESC, id DESC LIMIT ?"
            )
        } else {
            format!("{sql} ORDER BY time_ns ASC, id ASC")
        };
        let mut query = match filter {
            LogFilter::App(app_id) => sqlx::query(&sql).bind(app_id.clone()),
            LogFilter::Vm(vm_id) => sqlx::query(&sql).bind(vm_id.clone()),
        };
        if let Some(since) = since_ns {
            query = query.bind(since);
        }
        if let Some(until) = until_ns {
            query = query.bind(until);
        }
        if newest_first {
            let limit = i64::try_from(tail.unwrap_or(0)).unwrap_or(i64::MAX);
            query = query.bind(limit);
        }
        let rows = query.fetch_all(&self.pool).await.map_err(log_error)?;
        let mut lines = Vec::with_capacity(rows.len());
        for row in rows {
            let stream: String = row.try_get("stream").map_err(log_error)?;
            lines.push(StoredLogLine {
                vm_id: row.try_get("vm_id").map_err(log_error)?,
                app_id: row.try_get("app_id").map_err(log_error)?,
                time_ns: row.try_get("time_ns").map_err(log_error)?,
                stream: parse_stream(&stream)?,
                line: row.try_get("line").map_err(log_error)?,
            });
        }
        if newest_first {
            lines.reverse();
        }
        Ok(lines)
    }

    /// Appends one deployment-progress line.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn append_deployment_line(
        &self,
        deployment_id: &str,
        phase: &str,
        message: &str,
        vm_id: Option<&str>,
    ) -> Result<(), AgentError> {
        sqlx::query(
            "INSERT INTO deployment_lines (deployment_id, phase, time_ns, message, vm_id) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(deployment_id)
        .bind(phase)
        .bind(self.next_log_time())
        .bind(message)
        .bind(vm_id)
        .execute(&self.pool)
        .await
        .map_err(log_error)
        .map(|_| ())
    }

    /// Every stored progress line of one deployment, oldest first.
    ///
    /// # Errors
    ///
    /// [`AgentError::Store`] on a SQLite failure.
    pub async fn query_deployment_lines(
        &self,
        deployment_id: &str,
        phase: Option<&str>,
    ) -> Result<Vec<StoredDeploymentLine>, AgentError> {
        let rows = match phase {
            Some(phase) => {
                sqlx::query(
                    "SELECT deployment_id, phase, time_ns, message, vm_id \
                     FROM deployment_lines WHERE deployment_id = ? AND phase = ? \
                     ORDER BY time_ns ASC, id ASC",
                )
                .bind(deployment_id)
                .bind(phase)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT deployment_id, phase, time_ns, message, vm_id \
                     FROM deployment_lines WHERE deployment_id = ? \
                     ORDER BY time_ns ASC, id ASC",
                )
                .bind(deployment_id)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(log_error)?;
        let mut lines = Vec::with_capacity(rows.len());
        for row in rows {
            lines.push(StoredDeploymentLine {
                deployment_id: row.try_get("deployment_id").map_err(log_error)?,
                phase: row.try_get("phase").map_err(log_error)?,
                time_ns: row.try_get("time_ns").map_err(log_error)?,
                message: row.try_get("message").map_err(log_error)?,
                vm_id: row.try_get("vm_id").map_err(log_error)?,
            });
        }
        Ok(lines)
    }
}

/// Prunes each VM that appeared in the batch to [`RETENTION_PER_VM`] rows.
async fn prune_vm(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    lines: &[CapturedLine],
) -> Result<(), AgentError> {
    let mut seen = HashSet::new();
    for line in lines {
        if !seen.insert(line.vm_id.as_str()) {
            continue;
        }
        let count: i64 = sqlx::query("SELECT COUNT(*) AS n FROM log_lines WHERE vm_id = ?")
            .bind(&line.vm_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(log_error)?
            .try_get("n")
            .map_err(log_error)?;
        if count > RETENTION_PER_VM {
            sqlx::query(
                "DELETE FROM log_lines WHERE id IN \
                 (SELECT id FROM log_lines WHERE vm_id = ? ORDER BY id ASC LIMIT ?)",
            )
            .bind(&line.vm_id)
            .bind(count - RETENTION_PER_VM)
            .execute(&mut **tx)
            .await
            .map_err(log_error)?;
        }
    }
    Ok(())
}

fn log_error(error: sqlx::Error) -> AgentError {
    AgentError::Store(error.to_string())
}

/// The stored name of a stream.
fn stream_name(stream: &LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
    }
}

fn parse_stream(text: &str) -> Result<LogStream, AgentError> {
    match text {
        "stdout" => Ok(LogStream::Stdout),
        "stderr" => Ok(LogStream::Stderr),
        other => Err(AgentError::Store(format!(
            "a stored log line has an unknown stream {other:?}"
        ))),
    }
}

/// Keeps exactly one output reader alive per running VM.
///
/// The engine allows one reader per VM, so a second `output` call is a
/// conflict; the task re-attaches after a stream ends (a VMM restart or a VM
/// restart) with a small backoff, and returns for a VM with no console. VMs
/// created before consoles existed return `NotFound` and are remembered so
/// they are not retried every pass.
pub struct LogCollector<E: Engine> {
    engine: Arc<E>,
    store: NodeStore,
    tasks: Mutex<HashMap<String, JoinHandle<()>>>,
    no_console: Arc<Mutex<HashSet<String>>>,
}

impl<E: Engine + 'static> LogCollector<E> {
    /// Builds a collector for `engine` over `store`.
    #[must_use]
    pub fn new(engine: Arc<E>, store: NodeStore) -> Self {
        Self {
            engine,
            store,
            tasks: Mutex::new(HashMap::new()),
            no_console: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Starts the reader for `vm_id` unless one is already running.
    pub fn attach(&self, vm_id: &str, app_id: Option<String>) {
        let mut tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if tasks.get(vm_id).is_some_and(|handle| !handle.is_finished()) {
            return;
        }
        if self
            .no_console
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(vm_id)
        {
            return;
        }
        let id = VmId(vm_id.to_owned());
        let engine = Arc::clone(&self.engine);
        let store = self.store.clone();
        let no_console = Arc::clone(&self.no_console);
        let vm = vm_id.to_owned();
        let handle = tokio::spawn(async move {
            read_output(engine, store, id, vm, app_id, no_console).await;
        });
        tasks.insert(vm_id.to_owned(), handle);
    }

    /// Stops the reader for `vm_id`, after the VM was removed.
    pub fn detach(&self, vm_id: &str) {
        if let Some(handle) = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(vm_id)
        {
            handle.abort();
        }
        self.no_console
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(vm_id);
    }
}

impl<E: Engine> Drop for LogCollector<E> {
    fn drop(&mut self) {
        for (_, handle) in self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain()
        {
            handle.abort();
        }
    }
}

/// Reads one VM's output until its stream ends, then re-attaches.
async fn read_output<E: Engine>(
    engine: Arc<E>,
    store: NodeStore,
    id: VmId,
    vm_id: String,
    app_id: Option<String>,
    no_console: Arc<Mutex<HashSet<String>>>,
) {
    loop {
        let mut stream = match engine.output(&id) {
            Ok(stream) => stream,
            Err(EngineError::NotFound) => {
                debug!(vm_id, "the VM has no console output; not reading it");
                no_console
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(vm_id);
                return;
            }
            Err(EngineError::Conflict(_)) => {
                debug!(vm_id, "another reader already holds this VM's output");
                return;
            }
            Err(error) => {
                debug!(vm_id, error = ?error, "could not open the VM's output; retrying");
                tokio::time::sleep(REATTACH_DELAY).await;
                continue;
            }
        };

        let mut batch: Vec<NewLogLine> = Vec::with_capacity(BATCH_LINES);
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                next = stream.next() => {
                    match next {
                        Some(line) => {
                            batch.push(NewLogLine {
                                vm_id: vm_id.clone(),
                                app_id: app_id.clone(),
                                stream: match line.stream {
                                    quoro_engine::OutputStream::Stdout => LogStream::Stdout,
                                    quoro_engine::OutputStream::Stderr => LogStream::Stderr,
                                },
                                line: line.line,
                            });
                            if batch.len() >= BATCH_LINES {
                                flush(&store, &mut batch).await;
                            }
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    flush(&store, &mut batch).await;
                }
            }
        }
        flush(&store, &mut batch).await;
        tokio::time::sleep(REATTACH_DELAY).await;
    }
}

async fn flush(store: &NodeStore, batch: &mut Vec<NewLogLine>) {
    if batch.is_empty() {
        return;
    }
    if let Err(error) = store.append_log_lines(batch).await {
        warn!(error = %error, "could not store captured log lines");
    }
    batch.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-logs-{}",
                util::hex(&util::random_bytes::<8>())
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn line(vm: &str, app: &str, text: &str) -> NewLogLine {
        NewLogLine {
            vm_id: vm.to_owned(),
            app_id: Some(app.to_owned()),
            stream: LogStream::Stdout,
            line: text.to_owned(),
        }
    }

    #[tokio::test]
    async fn instants_are_strictly_increasing_across_vms_and_deployments() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        store
            .append_log_lines(&[
                line("vm_a", "app_1", "one"),
                line("vm_b", "app_2", "two"),
                line("vm_a", "app_1", "three"),
            ])
            .await
            .unwrap();
        store
            .append_deployment_line("dep_1", "pull", "pulling", Some("vm_a"))
            .await
            .unwrap();
        store
            .append_deployment_line("dep_1", "boot", "VM created", Some("vm_a"))
            .await
            .unwrap();

        let lines = store
            .query_log_lines(&LogFilter::Vm("vm_a".to_owned()), None, None, None)
            .await
            .unwrap();
        let deployments = store.query_deployment_lines("dep_1", None).await.unwrap();
        let mut instants: Vec<i64> = lines.iter().map(|line| line.time_ns).collect();
        instants.extend(deployments.iter().map(|line| line.time_ns));
        for pair in instants.windows(2) {
            assert!(pair[0] < pair[1], "{instants:?}");
        }

        // A restart resumes after the newest stored instant.
        let reopened = NodeStore::open(&dir.0).await.unwrap();
        assert!(reopened.next_log_time() > *instants.last().unwrap());
    }

    #[tokio::test]
    async fn filters_since_until_and_tail_select_the_right_lines() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let mut batch = Vec::new();
        for index in 0..10 {
            batch.push(line(
                if index % 2 == 0 { "vm_a" } else { "vm_b" },
                "app_1",
                &format!("l{index}"),
            ));
        }
        store.append_log_lines(&batch).await.unwrap();
        let all = store
            .query_log_lines(&LogFilter::App("app_1".to_owned()), None, None, None)
            .await
            .unwrap();
        assert_eq!(all.len(), 10);
        assert!(
            all.iter()
                .all(|line| line.app_id.as_deref() == Some("app_1"))
        );

        let tail = store
            .query_log_lines(&LogFilter::App("app_1".to_owned()), None, None, Some(3))
            .await
            .unwrap();
        assert_eq!(
            tail.iter()
                .map(|line| line.line.as_str())
                .collect::<Vec<_>>(),
            vec!["l7", "l8", "l9"]
        );

        let since = all[4].time_ns;
        let filtered = store
            .query_log_lines(&LogFilter::App("app_1".to_owned()), Some(since), None, None)
            .await
            .unwrap();
        assert_eq!(filtered.len(), 6, "since is inclusive");
        assert_eq!(filtered[0].line, "l4");

        let until = all[2].time_ns;
        let filtered = store
            .query_log_lines(&LogFilter::App("app_1".to_owned()), None, Some(until), None)
            .await
            .unwrap();
        assert_eq!(filtered.len(), 3, "until is inclusive");
    }

    #[tokio::test]
    async fn retention_prunes_the_oldest_lines_per_vm() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let batch: Vec<NewLogLine> = (0..RETENTION_PER_VM as usize + 12)
            .map(|index| line("vm_a", "app_1", &format!("line {index}")))
            .collect();
        store.append_log_lines(&batch).await.unwrap();
        let lines = store
            .query_log_lines(&LogFilter::Vm("vm_a".to_owned()), None, None, None)
            .await
            .unwrap();
        assert_eq!(lines.len(), RETENTION_PER_VM as usize);
        assert_eq!(lines[0].line, "line 12", "the oldest were pruned");
    }

    #[tokio::test]
    async fn live_lines_reach_a_subscriber() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let mut rx = store.subscribe_logs();
        store
            .append_log_lines(&[line("vm_a", "app_1", "hello")])
            .await
            .unwrap();
        let captured = rx.recv().await.unwrap();
        assert_eq!(captured.line, "hello");
        assert_eq!(captured.vm_id, "vm_a");
        assert_eq!(captured.app_id.as_deref(), Some("app_1"));
    }
}
