// SPDX-License-Identifier: AGPL-3.0-only
//! The promotion freeze (`node-agent-protocol.md` section 7.3 "The freeze"
//! and "`--abort`", section 12 `node_meta`, ADR 0261, ADR 0280).
//!
//! `quoro cluster promote` freezes the agent on its own host before its
//! first real vote request, "through a local call". The agent has no control
//! socket, so the call is a write to the agent's own store: [`freeze`] opens
//! `<data_dir>/node.db` from the command's process and sets
//! [`PROMOTION_FREEZE`] in `node_meta`. The store is SQLite in WAL mode, with
//! SQLite's default locking mode and a busy timeout on every connection
//! ([`crate::store::connect_options`]), so that second process opens it next
//! to the running agent; its write waits for the agent's writer, and the
//! agent's next transaction sees it.
//!
//! While the flag is set, the agent:
//!
//! - **stores no `log_entries`.** The transaction that would store a batch
//!   reads the flag first, holding SQLite's write lock
//!   ([`NodeStore::store_log_batch`]), so a batch is either in the log the
//!   command reads after its freeze or never stored, and a batch never stored
//!   is never acknowledged;
//! - **keeps its log as it stands.** After the freeze commits, the agent
//!   stores no entry and changes neither its log nor its refollowing state:
//!   every write to them reads the flag in its own transaction, under the
//!   write lock ([`NodeStore::store_log_batch`],
//!   [`NodeStore::set_refollowing`], [`NodeStore::mark_quarantine`],
//!   [`NodeStore::quarantine_log`]). The only later writes are two writes
//!   of the mark, and neither turns a refusal of the promotion into a pass:
//!   the mark lowered to the head a refollowing agent recorded before the
//!   freeze, which the promotion refuses anyway (a lower mark only adds the
//!   below-the-mark refusal, and comes down from [`follow::MAX_SEQ`] only
//!   over an empty log); and the mark raised to a `seq` the frozen log
//!   holds, for a batch that committed before the freeze or by `hello`;
//! - **ends its session.** The session checks the flag every
//!   [`SESSION_FREEZE_CHECK`], even when no `log_entries` arrives, and sends
//!   no `hello` while it is set;
//! - **does not dial the primary** (section 4.4). The dial loop checks the
//!   flag before every dial and, while it is set, checks again every
//!   [`FREEZE_POLL`]; once it clears, the agent dials at once. A dial
//!   attempt under way when the freeze commits ends within [`FREEZE_POLL`].
//!
//! The flag is in `node_meta`, so a frozen agent stays frozen across a
//! restart. Running VMs, the engine, the mesh and the voter's vote state are
//! untouched: the agent's own vote does not depend on the freeze.
//!
//! A promotion takes [`PromotionLock`] once its vote pre-check passed, just
//! before it freezes the agent, and holds it to its end; its earlier
//! refusals and its pre-check change nothing. [`abort`] ends a freeze that a
//! killed or interrupted command left, after a reboot too, and is refused
//! while a promotion holds the lock.

use std::fs::OpenOptions;
use std::path::Path;
use std::time::Duration;

use sqlx::Connection as _;
use sqlx::SqliteConnection;

use crate::AgentError;
use crate::follow;
use crate::store::{NodeStore, PROMOTION_FREEZE, Refollowing, connect_options};
use crate::util;

/// File name of the promotion lock in the agent's data directory, mode
/// `0600`: a promotion holds an exclusive `flock` on it from just before its
/// freeze to its end.
pub const PROMOTE_LOCK_FILE: &str = "promote.lock";

/// How often the dial loop of a frozen agent checks whether the freeze
/// ended (section 4.4).
pub const FREEZE_POLL: Duration = Duration::from_secs(1);

/// How often an open session checks the flag, so it ends within about a
/// second of a freeze even when no `log_entries` arrives. A batch that does
/// arrive is refused in its own transaction, whatever this interval.
pub const SESSION_FREEZE_CHECK: Duration = Duration::from_millis(500);

/// One connection to the agent's existing store, with the agent's own
/// settings. It never creates the file: a host whose agent never opened its
/// store has nothing to freeze.
async fn connect(data_dir: &Path) -> Result<SqliteConnection, AgentError> {
    SqliteConnection::connect_with(&connect_options(data_dir))
        .await
        .map_err(|e| AgentError::Store(format!("could not open the agent's store: {e}")))
}

/// Freezes the agent whose data directory is `data_dir`, from any process
/// (section 7.3 "The freeze"): sets [`PROMOTION_FREEZE`] to the current time,
/// RFC 3339, in one statement, which is its own transaction. Once this
/// returns the freeze is durable, and no `log_entries` batch the agent
/// stores after it is committed (see the module documentation).
///
/// It is idempotent: a second call keeps the time of the first.
///
/// # Errors
///
/// [`AgentError::Store`] when the store does not exist, cannot be opened, or
/// the write fails; the agent is not frozen then.
pub async fn freeze(data_dir: &Path) -> Result<(), AgentError> {
    let mut conn = connect(data_dir).await?;
    let written =
        sqlx::query("INSERT INTO node_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING")
            .bind(PROMOTION_FREEZE)
            .bind(util::now_rfc3339())
            .execute(&mut conn)
            .await;
    let _ = conn.close().await;
    written
        .map(|_| ())
        .map_err(|e| AgentError::Store(format!("could not freeze the agent: {e}")))
}

/// Ends a freeze from any process: clears [`PROMOTION_FREEZE`]. The agent's
/// dial loop sees it within [`FREEZE_POLL`] and dials at once. Clearing a
/// flag that is not set changes nothing.
///
/// A promotion calls it while it holds [`PromotionLock`]; an operator goes
/// through [`abort`], which takes the lock first.
///
/// # Errors
///
/// [`AgentError::Store`] when the store cannot be opened or the write fails.
pub async fn unfreeze(data_dir: &Path) -> Result<(), AgentError> {
    let mut conn = connect(data_dir).await?;
    let deleted = sqlx::query("DELETE FROM node_meta WHERE key = ?")
        .bind(PROMOTION_FREEZE)
        .execute(&mut conn)
        .await;
    let _ = conn.close().await;
    deleted
        .map(|_| ())
        .map_err(|e| AgentError::Store(format!("could not end the freeze: {e}")))
}

/// Whether a promotion on this host froze the agent.
///
/// # Errors
///
/// [`AgentError::Store`] on a SQLite failure.
pub async fn is_frozen(store: &NodeStore) -> Result<bool, AgentError> {
    Ok(store.meta(PROMOTION_FREEZE).await?.is_some())
}

/// The exclusive advisory lock a promotion holds from just before its freeze
/// to its end (section 7.3 "`--abort`", ADR 0280): an `flock` on
/// [`PROMOTE_LOCK_FILE`] in the agent's data directory. The kernel drops it
/// when the holder exits, a killed one or a reboot included, so a lock is
/// never left behind. Dropping the value releases it.
#[derive(Debug)]
pub struct PromotionLock {
    _file: std::fs::File,
}

impl PromotionLock {
    /// Takes the lock without waiting, creating the file `0600` when it is
    /// missing.
    ///
    /// # Errors
    ///
    /// [`AgentError::PromotionRunning`] at once when another holder has it,
    /// another process or another open of the file in this one;
    /// [`AgentError::Io`] when the file cannot be opened or locked.
    pub fn acquire(data_dir: &Path) -> Result<Self, AgentError> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let file = options
            .open(data_dir.join(PROMOTE_LOCK_FILE))
            .map_err(|e| AgentError::io("open the promotion lock", e))?;
        match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(Self { _file: file }),
            Err(rustix::io::Errno::WOULDBLOCK) => Err(AgentError::PromotionRunning),
            Err(e) => Err(AgentError::io("lock the promotion lock", e.into())),
        }
    }
}

/// `quoro cluster promote --abort` (section 7.3, ADR 0280): ends a freeze a
/// killed or interrupted promotion left, after a reboot too, when nothing
/// holds the lock. It takes [`PromotionLock`] without waiting and holds it
/// while it clears the flag, so no promotion starts in between.
///
/// # Errors
///
/// [`AgentError::PromotionRunning`] while a promotion runs on this host, and
/// the flag stays set; otherwise the errors of [`PromotionLock::acquire`] and
/// [`unfreeze`].
pub async fn abort(data_dir: &Path) -> Result<(), AgentError> {
    let _lock = PromotionLock::acquire(data_dir)?;
    unfreeze(data_dir).await
}

/// What `quoro cluster promote` reads from the agent's store for its
/// refusals and its step 1 (section 7.3), before the freeze and again after
/// it. Each value is read once, on its own, and [`Self::below_mark`] is
/// worked out from the `log_seq` and `mark` read here, so the two always
/// agree. After the freeze commits, the agent
/// stores no entry and changes neither its log nor its refollowing state;
/// the only later writes are the two mark writes of the module
/// documentation, and neither turns a refusal into a pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotionReads {
    /// The last verified `seq` of the followed log, 0 when it is empty.
    pub log_seq: u64,
    /// The hash of that entry, 64 zeros when the log is empty.
    pub log_hash: String,
    /// The `head_seq` the primary last reported and when the node received
    /// it, RFC 3339 ([`NodeStore::log_head`]).
    pub head: Option<(u64, String)>,
    /// The change-log mark ([`follow::read_mark`]), `None` without a mark
    /// file.
    pub mark: Option<u64>,
    /// Whether `log_seq` is below `mark` (section 7.1 "The node's mark",
    /// [`follow::below_mark`]); no log is below a missing mark.
    pub below_mark: bool,
    /// The refollowing state ([`NodeStore::refollowing`]).
    pub refollowing: Option<Refollowing>,
    /// Whether the log is quarantined: the node holds a copy the primary
    /// declared diverged, or has not yet learned the head it refollows to
    /// (section 7.1). That is the mark at [`follow::MAX_SEQ`], which a
    /// quarantine writes first and which stays until the head is known
    /// (ADR 0285), or the refollowing state "head unknown". A node that
    /// learned its head is refollowing instead, which `--accept-loss` lifts;
    /// a quarantined log it does not.
    pub quarantined: bool,
    /// The newest `welcome.schema_version` stored
    /// ([`NodeStore::welcome_schema_version`]).
    pub welcome_schema_version: Option<u32>,
    /// When a promotion froze the agent, RFC 3339; `None` when it is not
    /// frozen.
    pub frozen_at: Option<String>,
}

impl PromotionReads {
    /// Whether the followed log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.log_seq == 0
    }
}

/// Reads [`PromotionReads`] from the agent's store.
///
/// # Errors
///
/// The errors of the store's reads and of [`follow::read_mark`].
pub async fn promotion_reads(store: &NodeStore) -> Result<PromotionReads, AgentError> {
    let (log_seq, log_hash) = store.log_position().await?;
    let head = store.log_head().await?;
    let mark = follow::read_mark(store.data_dir())?;
    let refollowing = store.refollowing().await?;
    let quarantined =
        mark == Some(follow::MAX_SEQ) || refollowing == Some(Refollowing::HeadUnknown);
    Ok(PromotionReads {
        log_seq,
        log_hash,
        head,
        mark,
        below_mark: follow::below_mark(log_seq, mark),
        refollowing,
        quarantined,
        welcome_schema_version: store.welcome_schema_version().await?,
        frozen_at: store.meta(PROMOTION_FREEZE).await?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::follow::{ZERO_HASH, verify_batch};
    use crate::identity::LOG_MARK_FILE;
    use crate::store::{BatchStored, FreezeChecked, LogQuarantine};

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "quoro-freeze-{}",
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

    fn verified(
        from_seq: u64,
        prev_hash: &str,
        count: u64,
        salt: &str,
    ) -> Vec<crate::follow::LogEntry> {
        let entries = follow::testing::chain(from_seq, prev_hash, count, salt);
        verify_batch(from_seq - 1, prev_hash, &follow::testing::message(entries)).unwrap()
    }

    /// Section 7.3 "The freeze": a freeze set over another connection, as
    /// another process sets it, is seen by the agent's open store. It is
    /// idempotent, keeps the time it was first set, and ends with
    /// `unfreeze`; a host with no store has nothing to freeze, and none is
    /// created.
    #[tokio::test]
    async fn a_freeze_from_another_connection_is_seen_and_ends() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        assert!(!is_frozen(&store).await.unwrap());

        freeze(&dir.0).await.unwrap();
        assert!(is_frozen(&store).await.unwrap());
        let first = store.meta(PROMOTION_FREEZE).await.unwrap().unwrap();
        assert!(util::parse_rfc3339_nanos(&first).is_some(), "{first}");
        tokio::time::sleep(Duration::from_millis(5)).await;
        freeze(&dir.0).await.unwrap();
        assert_eq!(
            store.meta(PROMOTION_FREEZE).await.unwrap().as_deref(),
            Some(first.as_str()),
            "a second freeze keeps the first time"
        );

        unfreeze(&dir.0).await.unwrap();
        assert!(!is_frozen(&store).await.unwrap());
        unfreeze(&dir.0).await.unwrap();

        let empty = TempDir::new();
        assert!(freeze(&empty.0).await.is_err());
        assert!(!empty.0.join("node.db").exists());
    }

    /// Section 7.3 "The freeze", ADR 0261: once a freeze committed, a batch
    /// is not stored, and the log, its `head_seq` and the refollowing state
    /// stay as they were; after the freeze ends the same batch is stored.
    #[tokio::test]
    async fn a_batch_after_a_committed_freeze_is_not_stored() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let first = verified(1, ZERO_HASH, 3, "a");
        assert_eq!(
            store.store_log_batch(&first, Some(5)).await.unwrap(),
            BatchStored::Stored
        );
        assert_eq!(
            store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
            FreezeChecked::Written
        );

        freeze(&dir.0).await.unwrap();
        let second = verified(4, &first[2].hash, 2, "a");
        assert_eq!(
            store.store_log_batch(&second, Some(9)).await.unwrap(),
            BatchStored::Frozen
        );
        assert_eq!(
            store.log_position().await.unwrap(),
            (3, first[2].hash.clone())
        );
        assert_eq!(store.log_head().await.unwrap().unwrap().0, 5);
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(5))
        );

        // A quarantine keeps the log as it stands too, and leaves no file.
        assert_eq!(
            store.quarantine_log("test").await.unwrap(),
            LogQuarantine::Frozen
        );
        assert_eq!(store.log_entries().await.unwrap(), first);
        let quarantine = dir.0.join("quarantine");
        assert!(
            !quarantine.exists() || std::fs::read_dir(&quarantine).unwrap().count() == 0,
            "no quarantine file"
        );

        unfreeze(&dir.0).await.unwrap();
        assert_eq!(
            store.store_log_batch(&second, Some(9)).await.unwrap(),
            BatchStored::RefollowingEnded
        );
        assert_eq!(store.log_position().await.unwrap().0, 5);
    }

    /// Section 7.3 "`--abort`", ADR 0280: while a promotion holds the lock,
    /// another lock and `abort` are refused at once and the freeze stays;
    /// once the lock is dropped, `abort` ends the freeze. The lock file is
    /// `0600`.
    #[tokio::test]
    async fn abort_is_refused_while_a_promotion_holds_the_lock() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        freeze(&dir.0).await.unwrap();

        let lock = PromotionLock::acquire(&dir.0).unwrap();
        assert!(matches!(
            PromotionLock::acquire(&dir.0),
            Err(AgentError::PromotionRunning)
        ));
        let refused = abort(&dir.0).await.unwrap_err();
        assert!(matches!(refused, AgentError::PromotionRunning), "{refused}");
        assert_eq!(refused.to_string(), "a promotion runs on this host");
        assert!(is_frozen(&store).await.unwrap(), "the freeze stays");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(dir.0.join(PROMOTE_LOCK_FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        drop(lock);
        abort(&dir.0).await.unwrap();
        assert!(!is_frozen(&store).await.unwrap());
        // `abort` let go of the lock.
        drop(PromotionLock::acquire(&dir.0).unwrap());
    }

    /// Section 7.3: the reads reflect the log, its head, the mark, the
    /// refollowing state, the schema version and the freeze. A quarantine's
    /// mark, with or without a refollowing state, and "head unknown" read as
    /// quarantined; a known head reads as refollowing only.
    #[tokio::test]
    async fn the_promotion_reads_reflect_the_store() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let fresh = promotion_reads(&store).await.unwrap();
        assert_eq!(
            fresh,
            PromotionReads {
                log_seq: 0,
                log_hash: ZERO_HASH.to_owned(),
                head: None,
                mark: None,
                below_mark: false,
                refollowing: None,
                quarantined: false,
                welcome_schema_version: None,
                frozen_at: None,
            }
        );
        assert!(fresh.is_empty());

        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(7)).await.unwrap();
        store.raise_welcome_schema_version(14).await.unwrap();
        std::fs::write(dir.0.join(LOG_MARK_FILE), "5").unwrap();
        assert_eq!(
            store.set_refollowing(Refollowing::Head(7)).await.unwrap(),
            FreezeChecked::Written
        );
        freeze(&dir.0).await.unwrap();

        let reads = promotion_reads(&store).await.unwrap();
        assert_eq!(reads.log_seq, 3);
        assert_eq!(reads.log_hash, entries[2].hash);
        let (head, at) = reads.head.clone().unwrap();
        assert_eq!(head, 7);
        assert!(util::parse_rfc3339_nanos(&at).is_some(), "{at}");
        assert_eq!(reads.mark, Some(5));
        assert!(reads.below_mark);
        assert!(!reads.is_empty());
        assert_eq!(reads.refollowing, Some(Refollowing::Head(7)));
        assert!(!reads.quarantined, "a known head is refollowing only");
        assert_eq!(reads.welcome_schema_version, Some(14));
        assert_eq!(reads.frozen_at, store.meta(PROMOTION_FREEZE).await.unwrap());
        assert!(reads.frozen_at.is_some());

        // ADR 0285: the quarantine's mark with no state recorded, over a log
        // not yet moved aside.
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        store.store_log_batch(&entries, None).await.unwrap();
        std::fs::write(dir.0.join(LOG_MARK_FILE), follow::MAX_SEQ.to_string()).unwrap();
        let reads = promotion_reads(&store).await.unwrap();
        assert_eq!(reads.log_seq, 3);
        assert_eq!(reads.refollowing, None);
        assert!(reads.quarantined);

        // The same mark over a recorded head, and "head unknown" alone.
        assert_eq!(
            store.set_refollowing(Refollowing::Head(7)).await.unwrap(),
            FreezeChecked::Written
        );
        assert!(promotion_reads(&store).await.unwrap().quarantined);
        std::fs::write(dir.0.join(LOG_MARK_FILE), "7").unwrap();
        assert!(!promotion_reads(&store).await.unwrap().quarantined);
        assert_eq!(
            store
                .set_refollowing(Refollowing::HeadUnknown)
                .await
                .unwrap(),
            FreezeChecked::Written
        );
        assert!(promotion_reads(&store).await.unwrap().quarantined);
    }

    /// The mark file's text, `None` when there is none.
    fn mark_text(dir: &TempDir) -> Option<String> {
        std::fs::read_to_string(dir.0.join(LOG_MARK_FILE)).ok()
    }

    /// How many copies the store recorded as quarantined.
    async fn quarantined_copies(store: &NodeStore) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM quarantine")
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    /// Section 7.3 "The freeze", section 7.1 "Refollowing": once a freeze
    /// set over another connection committed, recording the refollowing
    /// state returns frozen and writes nothing, over no state and over a
    /// recorded one. Without the freeze it writes.
    #[tokio::test]
    async fn the_refollowing_state_is_not_written_once_a_freeze_committed() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        freeze(&dir.0).await.unwrap();
        for state in [Refollowing::HeadUnknown, Refollowing::Head(5)] {
            assert_eq!(
                store.set_refollowing(state).await.unwrap(),
                FreezeChecked::Frozen
            );
            assert_eq!(store.refollowing().await.unwrap(), None, "{state:?}");
        }

        unfreeze(&dir.0).await.unwrap();
        assert_eq!(
            store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
            FreezeChecked::Written
        );
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(5))
        );

        freeze(&dir.0).await.unwrap();
        assert_eq!(
            store
                .set_refollowing(Refollowing::HeadUnknown)
                .await
                .unwrap(),
            FreezeChecked::Frozen
        );
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(5))
        );
    }

    /// Section 7.1 "Refollowing", section 7.3 "The freeze" (ADR 0261): once
    /// a freeze set over another connection committed, the quarantine's
    /// first writes return frozen and leave the mark file, there or missing,
    /// and the refollowing state as they were. Without the freeze they write
    /// the mark as 2^53 - 1 and record "head unknown", and move nothing.
    #[tokio::test]
    async fn the_quarantine_s_mark_and_state_are_not_written_once_a_freeze_committed() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(3)).await.unwrap();
        freeze(&dir.0).await.unwrap();

        std::fs::write(dir.0.join(LOG_MARK_FILE), "3").unwrap();
        assert_eq!(
            store.mark_quarantine().await.unwrap(),
            FreezeChecked::Frozen
        );
        assert_eq!(mark_text(&dir).as_deref(), Some("3"));
        assert_eq!(store.refollowing().await.unwrap(), None);

        std::fs::remove_file(dir.0.join(LOG_MARK_FILE)).unwrap();
        assert_eq!(
            store.mark_quarantine().await.unwrap(),
            FreezeChecked::Frozen
        );
        assert_eq!(mark_text(&dir), None);
        assert_eq!(store.refollowing().await.unwrap(), None);

        unfreeze(&dir.0).await.unwrap();
        std::fs::write(dir.0.join(LOG_MARK_FILE), "3").unwrap();
        assert_eq!(
            store.mark_quarantine().await.unwrap(),
            FreezeChecked::Written
        );
        assert_eq!(mark_text(&dir), Some(follow::MAX_SEQ.to_string()));
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::HeadUnknown)
        );
        assert_eq!(store.log_entries().await.unwrap(), entries);
    }

    /// Section 7.1 "Refollowing", section 7.3 "The freeze" (ADR 0261, ADR
    /// 0285): a freeze that commits between a quarantine's first writes and
    /// the move leaves the log in place, read as quarantined. The move and
    /// `recover` return frozen, not an error, and the move is finished by
    /// `recover` once the freeze ends, not before. A mark at
    /// 2^53 - 1 with no state recorded is not finished while frozen either:
    /// `recover` returns frozen and records nothing.
    #[tokio::test]
    async fn a_quarantine_a_freeze_stopped_reads_as_quarantined_and_finishes_after_it() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(3)).await.unwrap();
        assert_eq!(
            store.mark_quarantine().await.unwrap(),
            FreezeChecked::Written
        );
        freeze(&dir.0).await.unwrap();

        assert_eq!(
            store.quarantine_log("log_diverged: test").await.unwrap(),
            LogQuarantine::Frozen
        );
        assert_eq!(
            follow::recover(&store).await.unwrap(),
            FreezeChecked::Frozen
        );
        assert_eq!(store.log_entries().await.unwrap(), entries);
        assert_eq!(quarantined_copies(&store).await, 0);
        let reads = promotion_reads(&store).await.unwrap();
        assert_eq!(reads.log_seq, 3);
        assert!(reads.quarantined);
        assert_eq!(reads.refollowing, Some(Refollowing::HeadUnknown));
        assert_eq!(reads.mark, Some(follow::MAX_SEQ));

        unfreeze(&dir.0).await.unwrap();
        assert_eq!(
            follow::recover(&store).await.unwrap(),
            FreezeChecked::Written
        );
        assert!(store.log_entries().await.unwrap().is_empty());
        assert_eq!(quarantined_copies(&store).await, 1);
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::HeadUnknown)
        );

        // The mark of a quarantine with no state recorded (ADR 0285).
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        store.store_log_batch(&entries, Some(3)).await.unwrap();
        std::fs::write(dir.0.join(LOG_MARK_FILE), follow::MAX_SEQ.to_string()).unwrap();
        freeze(&dir.0).await.unwrap();
        assert_eq!(
            follow::recover(&store).await.unwrap(),
            FreezeChecked::Frozen
        );
        assert_eq!(store.refollowing().await.unwrap(), None);
        assert_eq!(store.log_entries().await.unwrap(), entries);
        assert_eq!(quarantined_copies(&store).await, 0);
        assert!(promotion_reads(&store).await.unwrap().quarantined);

        unfreeze(&dir.0).await.unwrap();
        assert_eq!(
            follow::recover(&store).await.unwrap(),
            FreezeChecked::Written
        );
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::HeadUnknown)
        );
        assert!(store.log_entries().await.unwrap().is_empty());
        assert_eq!(quarantined_copies(&store).await, 1);
    }

    /// Section 7.3 "The freeze", section 7.1: a move of the copy that a
    /// freeze stopped is reported as frozen, not as an error, so the session
    /// and `recover` end as frozen. The copy, the quarantine table and the
    /// quarantine directory stay as they were; once the freeze ends, the
    /// copy moves.
    #[tokio::test]
    async fn a_move_a_freeze_stopped_is_reported_as_frozen_not_as_an_error() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(3)).await.unwrap();
        freeze(&dir.0).await.unwrap();

        assert_eq!(
            store.quarantine_log("log_diverged: test").await.unwrap(),
            LogQuarantine::Frozen
        );
        assert_eq!(store.log_entries().await.unwrap(), entries);
        assert_eq!(quarantined_copies(&store).await, 0);
        let files = std::fs::read_dir(dir.0.join("quarantine")).map_or(0, Iterator::count);
        assert_eq!(files, 0, "no quarantine file is left");

        unfreeze(&dir.0).await.unwrap();
        let LogQuarantine::Moved(path) = store.quarantine_log("log_diverged: test").await.unwrap()
        else {
            panic!("the copy moves once the freeze ended");
        };
        assert!(path.exists());
        assert_eq!(quarantined_copies(&store).await, 1);
        assert!(store.log_entries().await.unwrap().is_empty());
    }

    /// Runs `call` while a second connection to `node.db`, as another
    /// process, holds SQLite's write lock with the freeze written and not
    /// yet committed (section 7.3 "The freeze"). The call must still be
    /// waiting after 300 ms: in WAL mode only a write waits on that lock, so
    /// the call waits in its own transaction. The freeze then commits, well
    /// inside the store's 5 s busy timeout, and the call's result is
    /// returned.
    async fn race_a_freeze<F>(dir: &TempDir, call: F) -> FreezeChecked
    where
        F: Future<Output = Result<FreezeChecked, AgentError>> + Send + 'static,
    {
        let mut conn = connect(&dir.0).await.unwrap();
        let mut freezing = conn.begin_with("BEGIN IMMEDIATE").await.unwrap();
        sqlx::query("INSERT INTO node_meta (key, value) VALUES (?, ?)")
            .bind(PROMOTION_FREEZE)
            .bind(util::now_rfc3339())
            .execute(&mut *freezing)
            .await
            .unwrap();
        let call = tokio::spawn(call);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !call.is_finished(),
            "the call waits on the write lock the freeze holds"
        );
        freezing.commit().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("the call ends once the freeze committed")
            .unwrap()
            .unwrap();
        let _ = conn.close().await;
        result
    }

    /// Section 7.1 "Refollowing", section 7.3 "The freeze" (ADR 0261): the
    /// quarantine's first writes read the freeze inside their transaction,
    /// not before it. A freeze that another process commits while the call
    /// waits on its write lock is read: nothing is written, and the mark
    /// file, the refollowing state and the log stay as they were.
    #[tokio::test]
    async fn the_quarantine_s_mark_reads_a_freeze_that_commits_while_it_waits() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(3)).await.unwrap();
        std::fs::write(dir.0.join(LOG_MARK_FILE), "3").unwrap();

        let quarantining = store.clone();
        let result = race_a_freeze(&dir, async move { quarantining.mark_quarantine().await }).await;
        assert_eq!(result, FreezeChecked::Frozen);
        assert!(is_frozen(&store).await.unwrap());
        assert_eq!(mark_text(&dir).as_deref(), Some("3"));
        assert_eq!(store.refollowing().await.unwrap(), None);
        assert_eq!(store.log_entries().await.unwrap(), entries);
    }

    /// Section 7.1 "Refollowing", section 7.3 "The freeze" (ADR 0261): the
    /// refollowing state reads the freeze inside its transaction, not before
    /// it. A freeze that another process commits while the call waits on
    /// its write lock is read: nothing is written, and the recorded head,
    /// the mark file and the log stay as they were.
    #[tokio::test]
    async fn the_refollowing_state_reads_a_freeze_that_commits_while_it_waits() {
        let dir = TempDir::new();
        let store = NodeStore::open(&dir.0).await.unwrap();
        let entries = verified(1, ZERO_HASH, 3, "a");
        store.store_log_batch(&entries, Some(5)).await.unwrap();
        assert_eq!(
            store.set_refollowing(Refollowing::Head(5)).await.unwrap(),
            FreezeChecked::Written
        );
        std::fs::write(dir.0.join(LOG_MARK_FILE), "5").unwrap();

        let refollowing = store.clone();
        let result = race_a_freeze(&dir, async move {
            refollowing.set_refollowing(Refollowing::HeadUnknown).await
        })
        .await;
        assert_eq!(result, FreezeChecked::Frozen);
        assert!(is_frozen(&store).await.unwrap());
        assert_eq!(
            store.refollowing().await.unwrap(),
            Some(Refollowing::Head(5))
        );
        assert_eq!(mark_text(&dir).as_deref(), Some("5"));
        assert_eq!(store.log_entries().await.unwrap(), entries);
    }
}
