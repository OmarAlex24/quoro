//! The host's [`Steps`]: the agent's store opened read-only, the vote
//! client, the freeze, the database build and write of the control plane,
//! the key installation of [`super::install_keys`] and the host preparation
//! of [`super::prepare`].
//!
//! Everything runs in this process, as root, and nothing here changes its
//! uid: the vote client refuses to send unless the effective uid is 0, and
//! the only step that hands anything to the control plane's user,
//! `prepare::prepare_host`, runs after every vote request (issue #19).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use quoro_control_plane::promotion::{self, Promotion, Written};
use quoro_control_plane::rebuild::database::{self, Built, FollowedEntry};
use quoro_control_plane::store::Store;
use quoro_node_agent::AgentError;
use quoro_node_agent::follow::LogEntry;
use quoro_node_agent::freeze::{self, PromotionLock, PromotionReads};
use quoro_node_agent::identity::Identity;
use quoro_node_agent::store::NodeStore;
use quoro_node_agent::vote;
use quoro_node_agent::vote_client::{
    Candidate, CheckOutcome, Round, VoteClient, VoteClientOptions,
};
use quoro_proto::sign;
use quoro_proto::wire::{DesiredState, SpecVotes};

use super::command::Paths;
use super::install_keys::{self, PrimaryKeys, RealKeyFs};
use super::keys;
use super::prepare;
use super::refusals::Refusal;
use super::steps::Steps;
use crate::host::{DATABASE, Out};

/// This host, for the agent whose identity is `identity`.
pub(crate) struct HostSteps {
    paths: Paths,
    identity: Identity,
}

impl HostSteps {
    pub(crate) fn new(paths: Paths, identity: Identity) -> Self {
        Self { paths, identity }
    }

    fn node_dir(&self) -> &Path {
        &self.paths.node_data_dir
    }

    /// The control plane's database: where step 4 builds it.
    fn database(&self) -> PathBuf {
        self.paths.data_dir.join(DATABASE)
    }

    /// The agent's store, opened read-only: it creates nothing.
    async fn store(&self) -> Result<NodeStore, String> {
        NodeStore::open_read_only(self.node_dir())
            .await
            .map_err(|e| format!("cannot open the agent's store: {e}"))
    }

    /// The vote client of this machine over `votes`.
    fn client(&self, votes: &SpecVotes) -> Result<VoteClient, String> {
        VoteClient::new(
            votes.clone(),
            Candidate {
                cluster_id: self.identity.cluster_id.clone(),
                node_id: self.identity.node_id.clone(),
            },
            VoteClientOptions::default(),
        )
        .map_err(|e| e.to_string())
    }
}

impl Steps for HostSteps {
    type Keys = PrimaryKeys;
    type Lock = PromotionLock;

    /// `prepare::prepare_host` runs the control plane from
    /// `/etc/quoro/quoro.toml`, the file this command reads, and refuses a
    /// `data_dir` other than [`prepare::DATA_DIR`], the one directory its
    /// unit may write. The second is checked here, before any change.
    fn check_host(&self) -> Result<(), String> {
        let data_dir = Path::new(prepare::DATA_DIR);
        if self.paths.data_dir.as_path() == data_dir {
            Ok(())
        } else {
            Err(format!(
                "the configuration's data_dir is {}, and the control plane's unit may write only \
                 {}",
                self.paths.data_dir.display(),
                data_dir.display()
            ))
        }
    }

    async fn log(&self) -> Result<Vec<LogEntry>, String> {
        self.store()
            .await?
            .log_entries()
            .await
            .map_err(|e| e.to_string())
    }

    async fn reads(&self) -> Result<PromotionReads, String> {
        freeze::promotion_reads(&self.store().await?)
            .await
            .map_err(|e| e.to_string())
    }

    async fn open_keys(&self, entries: &[LogEntry]) -> Result<PrimaryKeys, Vec<String>> {
        let opened = keys::open(&self.identity, entries, self.node_dir()).await?;
        let keys = PrimaryKeys::from_opened(opened).map_err(|e| vec![e])?;
        keys.check(&self.identity.fingerprint, &self.paths.data_dir)?;
        Ok(keys)
    }

    async fn check_log(&self, entries: &[FollowedEntry]) -> Result<(), String> {
        database::check(entries, &self.identity.cluster_id)
            .await
            .map(drop)
            .map_err(|e| e.to_string())
    }

    async fn votes(&self) -> Result<SpecVotes, String> {
        stored_votes(self.node_dir(), &self.identity).await
    }

    async fn seen(&self) -> Result<u64, String> {
        vote::current_term(&self.store().await?)
            .await
            .map_err(|e| e.to_string())
    }

    async fn check(&self, votes: &SpecVotes, seen: u64) -> Result<CheckOutcome, String> {
        self.client(votes)?
            .check(seen)
            .await
            .map_err(|e| e.to_string())
    }

    fn lock(&self) -> Result<PromotionLock, Refusal> {
        match PromotionLock::acquire(self.node_dir()) {
            Ok(lock) => Ok(lock),
            Err(AgentError::PromotionRunning) => Err(Refusal::AnotherPromotion),
            Err(e) => Err(Refusal::Unreadable(format!(
                "cannot take the promotion lock: {e}"
            ))),
        }
    }

    async fn freeze(&self) -> Result<(), String> {
        freeze::freeze(self.node_dir())
            .await
            .map_err(|e| e.to_string())
    }

    async fn vote(&self, votes: &SpecVotes, term: u64) -> Result<Round, String> {
        self.client(votes)?
            .vote(term)
            .await
            .map_err(|e| e.to_string())
    }

    /// The vote client's clock: `SystemMonotonic` reads [`Instant`].
    fn now(&self) -> Instant {
        Instant::now()
    }

    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    async fn build(&self, entries: &[FollowedEntry], term: u64) -> Result<Built, String> {
        database::build(entries, &self.identity.cluster_id, term, &self.database())
            .await
            .map_err(|e| e.to_string())
    }

    async fn write(&self, promotion: &Promotion) -> Result<Written, String> {
        let path = self.database();
        // `Store::open` would make an empty database where none is.
        if !path.is_file() {
            return Err(format!("{} is not a file", path.display()));
        }
        let store = Store::open(&path)
            .await
            .map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        let written = promotion::write(&store, promotion)
            .await
            .map_err(|e| e.to_string());
        store.pool().close().await;
        written
    }

    fn install_keys(&self, keys: &PrimaryKeys, out: &mut Out) -> Result<(), String> {
        install_keys::install_keys(
            keys,
            &self.identity.fingerprint,
            &self.paths.data_dir,
            &RealKeyFs,
            out,
        )
        .map(drop)
    }

    /// `prepare::prepare_host` blocks, so it runs on a blocking thread and
    /// prints as it goes; its lines are then kept in `out` too.
    async fn prepare_host(&self, out: &mut Out) -> Result<String, String> {
        let options = prepare::Options::host(PathBuf::from(crate::config::DEFAULT_PATH));
        let (prepared, lines) = tokio::task::spawn_blocking(move || {
            let mut printed = Out::printing();
            let prepared = prepare::prepare_host(&options, &prepare::Seams::REAL, &mut printed);
            (prepared, printed.lines)
        })
        .await
        .map_err(|e| format!("the host's preparation did not finish: {e}"))?;
        out.lines.extend(lines);
        prepared.map(|prepared| prepared.url)
    }

    async fn unfreeze(&self) -> Result<(), String> {
        freeze::unfreeze(self.node_dir())
            .await
            .map_err(|e| e.to_string())
    }

    async fn point_agent(&self, url: &str, out: &mut Out) -> Result<(), String> {
        let agent = prepare::NodeAgent {
            data_dir: self.paths.node_data_dir.clone(),
        };
        prepare::point_agent(url, &agent, &prepare::RealSystemd, out)
            .await
            .map(drop)
    }
}

/// The `spec.votes` of the newest desired state this machine's agent
/// stored, read through its store opened read-only with its node-local key,
/// and verified against the pinned cluster key as the agent verifies it.
async fn stored_votes(node_dir: &Path, identity: &Identity) -> Result<SpecVotes, String> {
    let store = NodeStore::open_read_only_with_local_key(node_dir)
        .await
        .map_err(|e| format!("cannot open the agent's store with its node-local key: {e}"))?;
    let Some((rev, body)) = store
        .latest_desired()
        .await
        .map_err(|e| format!("cannot read the agent's newest desired state: {e}"))?
    else {
        return Err(
            "the agent stored no desired state, so this machine knows no voter set".to_string(),
        );
    };
    let key = identity.verifying_key().map_err(|e| e.to_string())?;
    sign::verify_body(&key, &body).map_err(|e| {
        format!(
            "the agent's newest desired state, rev {rev}, does not verify against the pinned \
             cluster key: {e}"
        )
    })?;
    let state = DesiredState::from_value(&body)
        .map_err(|e| format!("the agent's newest desired state, rev {rev}, does not read: {e}"))?;
    state.spec.votes.ok_or_else(|| {
        format!(
            "the agent's newest desired state, rev {rev}, has no spec.votes: the primary gave \
             this machine no voter set (section 6.12)"
        )
    })
}
