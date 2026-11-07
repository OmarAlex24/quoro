// SPDX-License-Identifier: AGPL-3.0-only
//! Draining tunnel routes (`node-agent-protocol.md` section 5.11, ADR 0194).
//!
//! A `tunnel` hostname moves from one machine's tunnel to another's of the
//! same provider when its app crosses one replica: from its node to the edge,
//! and back. The gaining tunnel repoints the hostname's CNAME at once, but
//! Cloudflare keeps sending some requests to the losing tunnel for up to
//! about two minutes. So the losing machine keeps the route in its desired
//! state, marked `draining_until`, and its tunnel keeps the hostname in its
//! ingress rules, until [`DRAIN_WINDOW`] after the repoint.
//!
//! [`Drains`] is the registry of those routes. Two writers start a drain, and
//! both must, because whichever of them runs first after a change would
//! otherwise drop the hostname: the snapshot builder, when a `tunnel` route of
//! the snapshot it sent last is no longer one of the machine's routes, and the
//! tunnel manager, when a hostname left the set its last sync served. A drain
//! starts only when another machine's tunnel of the same provider serves the
//! hostname now and the losing machine still has a backend for it
//! ([`departed`]).
//!
//! The window counts from the repoint, which the tunnel manager records when
//! the gaining tunnel published the CNAME ([`Drains::record_cnames`]). The end
//! is therefore the same whichever writer started the drain and however often
//! it started: a drain whose window is already over does not start again.
//! Until the repoint the CNAME still points at the losing tunnel, so the drain
//! does not end on its own: at its provisional end [`sweep`] checks again that
//! the hostname still departed, and either gives it a new provisional end one
//! window later or ends it.
//!
//! The registry lives in memory. A primary restart forgets the open drains,
//! and the machines then drop those hostnames at once, as they did before
//! ADR 0194.

use super::desired::tunnel_hostnames;
use super::mesh::edge_node_id;
use sqlx::SqlitePool;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::sync::Notify;

/// How long a departed `tunnel` route stays on its machine after the CNAME
/// moved to another machine's tunnel (ADR 0194).
pub const DRAIN_WINDOW: Duration = Duration::from_secs(120);

/// The open drains, keyed by machine and hostname, and the owner of each
/// CNAME the tunnel manager published.
#[derive(Debug)]
pub struct Drains {
    state: Mutex<State>,
    changed: Notify,
}

#[derive(Debug)]
struct State {
    window: Duration,
    open: BTreeMap<(String, String), Drain>,
    /// Per hostname, the machine whose tunnel last published its CNAME and
    /// when it took the CNAME over.
    owners: HashMap<String, Owner>,
}

#[derive(Clone, Debug)]
struct Drain {
    app_id: String,
    /// When the drain started, to tell it apart from an older view of the
    /// machine's routes ([`Drains::clear_served`]).
    started: Instant,
    /// The end while the CNAME still points at this machine.
    provisional: OffsetDateTime,
}

#[derive(Clone, Debug)]
struct Owner {
    machine: String,
    since: OffsetDateTime,
}

/// What [`Drains::expire`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Expired {
    /// The machines whose drains ended.
    pub ended: BTreeSet<String>,
    /// The drains that reached their provisional end before their CNAME
    /// moved.
    pub waiting: Vec<Waiting>,
}

/// A drain that reached its provisional end before its CNAME moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Waiting {
    pub machine: String,
    pub hostname: String,
    pub app_id: String,
}

/// One open drain of a machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenDrain {
    pub hostname: String,
    pub app_id: String,
    pub until: OffsetDateTime,
}

impl Default for Drains {
    fn default() -> Self {
        Self::new(DRAIN_WINDOW)
    }
}

impl State {
    /// The end of `drain`, the drain of `hostname` on `machine`, and whether
    /// its window runs: the CNAME moved to another machine's tunnel.
    fn end_of(&self, machine: &str, hostname: &str, drain: &Drain) -> (OffsetDateTime, bool) {
        match self.owners.get(hostname) {
            Some(owner) if owner.machine != machine => (owner.since + self.window, true),
            _ => (drain.provisional, false),
        }
    }
}

impl Drains {
    /// An empty registry whose drains last `window` after the repoint.
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self {
            state: Mutex::new(State {
                window,
                open: BTreeMap::new(),
                owners: HashMap::new(),
            }),
            changed: Notify::new(),
        }
    }

    /// Test seam: a shorter window. It applies to the open drains too, since
    /// every end is computed from the repoint when it is read.
    pub fn set_window(&self, window: Duration) {
        self.state.lock().expect("drain lock").window = window;
        self.changed.notify_one();
    }

    /// Resolves once a drain started, ended or got a new end.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Starts draining `hostname` on `machine` for `app_id`, unless it
    /// already drains there or the window after its CNAME moved away from
    /// `machine` is already over. Returns whether it started.
    ///
    /// Refusing a drain that is already over keeps a caller that keeps
    /// seeing the same departure, such as a tunnel whose syncs fail, from
    /// starting and ending it again in a loop.
    pub fn start(&self, machine: &str, hostname: &str, app_id: &str, now: OffsetDateTime) -> bool {
        let mut state = self.state.lock().expect("drain lock");
        let key = (machine.to_string(), hostname.to_string());
        if state.open.contains_key(&key) {
            return false;
        }
        let drain = Drain {
            app_id: app_id.to_string(),
            started: Instant::now(),
            provisional: now + state.window,
        };
        let (until, moved) = state.end_of(machine, hostname, &drain);
        if moved && now >= until {
            return false;
        }
        state.open.insert(key, drain);
        drop(state);
        self.changed.notify_one();
        true
    }

    /// Ends the drains of `machine` for the hostnames in `served`, which it
    /// serves as usual again, if they started before `read_at`, the instant
    /// the caller began to read `served`. A drain that started later may
    /// come from a newer change than the one `served` reflects, so it stays.
    pub fn clear_served(&self, machine: &str, served: &BTreeSet<String>, read_at: Instant) {
        let mut state = self.state.lock().expect("drain lock");
        let before = state.open.len();
        state.open.retain(|(m, hostname), drain| {
            m != machine || !served.contains(hostname) || drain.started >= read_at
        });
        let cleared = state.open.len() != before;
        drop(state);
        if cleared {
            self.changed.notify_one();
        }
    }

    /// Records the CNAMEs of `machine`'s tunnel after a sync: `written`, the
    /// hostnames it serves and published, and `published`, every CNAME it
    /// still holds. A hostname in `written` whose CNAME another machine held
    /// moves to `machine` now, which starts the window of every drain of it
    /// on another machine. Returns those machines.
    pub fn record_cnames(
        &self,
        machine: &str,
        written: &BTreeSet<String>,
        published: &BTreeSet<String>,
        now: OffsetDateTime,
    ) -> BTreeSet<String> {
        let mut state = self.state.lock().expect("drain lock");
        let mut moved = Vec::new();
        for hostname in written {
            let takes_over = state
                .owners
                .get(hostname)
                .is_none_or(|owner| owner.machine != machine);
            if takes_over {
                state.owners.insert(
                    hostname.clone(),
                    Owner {
                        machine: machine.to_string(),
                        since: now,
                    },
                );
                moved.push(hostname);
            }
        }
        state
            .owners
            .retain(|hostname, owner| owner.machine != machine || published.contains(hostname));
        let affected: BTreeSet<String> = state
            .open
            .keys()
            .filter(|(m, hostname)| m != machine && moved.contains(&hostname))
            .map(|(m, _)| m.clone())
            .collect();
        drop(state);
        if !affected.is_empty() {
            self.changed.notify_one();
        }
        affected
    }

    /// The open drains of `machine` at `now`, ordered by hostname. A drain
    /// whose window is over is left out, even before [`Self::expire`]
    /// removes it.
    pub fn open_of(&self, machine: &str, now: OffsetDateTime) -> Vec<OpenDrain> {
        let state = self.state.lock().expect("drain lock");
        state
            .open
            .iter()
            .filter(|((m, _), _)| m == machine)
            .filter_map(|((m, hostname), drain)| {
                let (until, moved) = state.end_of(m, hostname, drain);
                (!(moved && now >= until)).then(|| OpenDrain {
                    hostname: hostname.clone(),
                    app_id: drain.app_id.clone(),
                    until,
                })
            })
            .collect()
    }

    /// The earliest end of an open drain.
    pub fn next_end(&self) -> Option<OffsetDateTime> {
        let state = self.state.lock().expect("drain lock");
        state
            .open
            .iter()
            .map(|((m, hostname), drain)| state.end_of(m, hostname, drain).0)
            .min()
    }

    /// Removes the drains whose window is over at `now`. Returns the machines
    /// they belonged to, and the drains that reached their provisional end
    /// while their CNAME has not moved: each needs [`Self::extend`] or
    /// [`Self::end`].
    pub fn expire(&self, now: OffsetDateTime) -> Expired {
        let mut state = self.state.lock().expect("drain lock");
        let mut over = Vec::new();
        let mut waiting = Vec::new();
        for ((m, hostname), drain) in &state.open {
            let (until, moved) = state.end_of(m, hostname, drain);
            if now >= until {
                if moved {
                    over.push((m.clone(), hostname.clone()));
                } else {
                    waiting.push(Waiting {
                        machine: m.clone(),
                        hostname: hostname.clone(),
                        app_id: drain.app_id.clone(),
                    });
                }
            }
        }
        let mut ended = BTreeSet::new();
        for key in over {
            state.open.remove(&key);
            ended.insert(key.0);
        }
        drop(state);
        if !ended.is_empty() {
            self.changed.notify_one();
        }
        Expired { ended, waiting }
    }

    /// Gives the drain of `hostname` on `machine` a new provisional end one
    /// window after `now`, if its CNAME still has not moved.
    pub fn extend(&self, machine: &str, hostname: &str, now: OffsetDateTime) {
        let mut state = self.state.lock().expect("drain lock");
        let window = state.window;
        let key = (machine.to_string(), hostname.to_string());
        let Some(drain) = state.open.get(&key).cloned() else {
            return;
        };
        if state.end_of(machine, hostname, &drain).1 {
            return;
        }
        if let Some(drain) = state.open.get_mut(&key) {
            drain.provisional = now + window;
        }
        drop(state);
        self.changed.notify_one();
    }

    /// Ends the drain of `hostname` on `machine` now.
    pub fn end(&self, machine: &str, hostname: &str) {
        let removed = self
            .state
            .lock()
            .expect("drain lock")
            .open
            .remove(&(machine.to_string(), hostname.to_string()))
            .is_some();
        if removed {
            self.changed.notify_one();
        }
    }

    /// Forgets every drain of `machine` and the CNAMEs its tunnel held, when
    /// the tunnel is gone.
    pub fn forget(&self, machine: &str) {
        let mut state = self.state.lock().expect("drain lock");
        state.open.retain(|(m, _), _| m != machine);
        state.owners.retain(|_, owner| owner.machine != machine);
        drop(state);
        self.changed.notify_one();
    }
}

/// The app of `hostname` when it left `machine`'s tunnel for another
/// machine's: it is a `tunnel` domain, `machine` has a tunnel, another
/// machine's tunnel of the same provider serves the hostname now, and
/// `machine` still has a backend for it. `None` otherwise: the hostname then
/// leaves the machine at once.
///
/// The caller has already seen that `machine` no longer serves the hostname
/// as usual.
///
/// # Errors
///
/// A database error.
pub(crate) async fn departed(
    pool: &SqlitePool,
    machine: &str,
    hostname: &str,
) -> Result<Option<String>, String> {
    let provider: Option<String> = sqlx::query_scalar(
        "SELECT provider_id FROM node_tunnels WHERE node_id = ? AND tunnel_id IS NOT NULL",
    )
    .bind(machine)
    .fetch_optional(pool)
    .await
    .map_err(|e| format!("cannot read the tunnel of {machine}: {e}"))?;
    let Some(provider) = provider else {
        return Ok(None);
    };
    let app_id: Option<String> =
        sqlx::query_scalar("SELECT app_id FROM domains WHERE hostname = ? AND tls_mode = 'tunnel'")
            .bind(hostname)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("cannot read the domain {hostname}: {e}"))?;
    let Some(app_id) = app_id else {
        return Ok(None);
    };
    let others: Vec<String> = sqlx::query_scalar(
        "SELECT node_id FROM node_tunnels \
         WHERE node_id <> ? AND provider_id = ? AND tunnel_id IS NOT NULL \
         ORDER BY node_id",
    )
    .bind(machine)
    .bind(&provider)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read node_tunnels: {e}"))?;
    let mut moved = false;
    for other in others {
        if tunnel_hostnames(pool, &other).await?.contains(hostname) {
            moved = true;
            break;
        }
    }
    if !moved {
        return Ok(None);
    }
    let is_edge = edge_node_id(pool).await?.as_deref() == Some(machine);
    if !has_backend(pool, machine, is_edge, &app_id).await? {
        return Ok(None);
    }
    Ok(Some(app_id))
}

/// Ends the drains whose window is over at `now`, and checks again each drain
/// that reached its provisional end before its CNAME moved: one whose
/// hostname still departed ([`departed`]) gets a new provisional end, and one
/// that no longer did ends. That covers a gaining tunnel that was removed, a
/// domain that was deleted and a backend that stopped. Returns the machines
/// whose drains changed.
pub(crate) async fn sweep(
    pool: &SqlitePool,
    drains: &Drains,
    now: OffsetDateTime,
) -> BTreeSet<String> {
    let expired = drains.expire(now);
    let mut changed = expired.ended;
    for Waiting {
        machine,
        hostname,
        app_id,
    } in expired.waiting
    {
        match departed(pool, &machine, &hostname).await {
            // Still the same app's hostname, still served elsewhere.
            Ok(Some(departed)) if departed == app_id => drains.extend(&machine, &hostname, now),
            Ok(_) => drains.end(&machine, &hostname),
            Err(error) => {
                // Checked again one window later.
                tracing::warn!(machine, hostname, %error, "cannot check a draining hostname");
                drains.extend(&machine, &hostname, now);
            }
        }
        changed.insert(machine);
    }
    changed
}

/// Whether `machine` can still serve `app_id`: on the edge, an instance runs
/// and has a guest address; on a node, one of the app's VMs here is meant to
/// run.
async fn has_backend(
    pool: &SqlitePool,
    machine: &str,
    is_edge: bool,
    app_id: &str,
) -> Result<bool, String> {
    let found: Option<i64> = if is_edge {
        sqlx::query_scalar(
            "SELECT 1 FROM vms WHERE app_id = ? AND desired_status = 'running' \
             AND ip IS NOT NULL LIMIT 1",
        )
        .bind(app_id)
        .fetch_optional(pool)
        .await
    } else {
        sqlx::query_scalar(
            "SELECT 1 FROM vms WHERE app_id = ? AND node_id = ? \
             AND desired_status = 'running' LIMIT 1",
        )
        .bind(app_id)
        .bind(machine)
        .fetch_optional(pool)
        .await
    }
    .map_err(|e| format!("cannot read the VMs of {app_id}: {e}"))?;
    Ok(found.is_some())
}

/// A drain to serve now: the domain is still a `tunnel` domain of the same
/// app and the machine still has a backend for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LiveDrain {
    pub hostname: String,
    pub domain_id: String,
    pub app_id: String,
    /// The end of the window, RFC 3339 like every protocol timestamp.
    pub until: String,
}

/// The open drains of `machine` that still have their domain and a backend,
/// ordered by hostname, leaving out the hostnames in `normal`, which the
/// machine serves as usual. `is_edge` says whether `machine` is the edge.
///
/// # Errors
///
/// A database error.
pub(crate) async fn live(
    pool: &SqlitePool,
    drains: &Drains,
    machine: &str,
    is_edge: bool,
    normal: &HashSet<String>,
    now: OffsetDateTime,
) -> Result<Vec<LiveDrain>, String> {
    let mut live = Vec::new();
    for drain in drains.open_of(machine, now) {
        if normal.contains(&drain.hostname) {
            continue;
        }
        let domain_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM domains WHERE hostname = ? AND app_id = ? AND tls_mode = 'tunnel'",
        )
        .bind(&drain.hostname)
        .bind(&drain.app_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("cannot read the domain {}: {e}", drain.hostname))?;
        let Some(domain_id) = domain_id else {
            continue;
        };
        if !has_backend(pool, machine, is_edge, &drain.app_id).await? {
            continue;
        }
        live.push(LiveDrain {
            hostname: drain.hostname,
            domain_id,
            app_id: drain.app_id,
            until: crate::store::format_rfc3339(drain.until),
        });
    }
    Ok(live)
}

/// The hostnames `machine`'s tunnel keeps in its ingress rules for its live
/// drains, beside the ones in `normal` it serves as usual.
///
/// # Errors
///
/// A database error.
pub(crate) async fn draining_hostnames(
    pool: &SqlitePool,
    drains: &Drains,
    machine: &str,
    normal: &BTreeSet<String>,
) -> Result<BTreeSet<String>, String> {
    let now = OffsetDateTime::now_utc();
    if drains.open_of(machine, now).is_empty() {
        return Ok(BTreeSet::new());
    }
    let is_edge = edge_node_id(pool).await?.as_deref() == Some(machine);
    let normal: HashSet<String> = normal.iter().cloned().collect();
    Ok(live(pool, drains, machine, is_edge, &normal, now)
        .await?
        .into_iter()
        .map(|drain| drain.hostname)
        .collect())
}

#[cfg(test)]
mod tests;
