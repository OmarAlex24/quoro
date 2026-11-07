// SPDX-License-Identifier: AGPL-3.0-only
//! The primary's side of warm copies (ADR 0172 to ADR 0174,
//! `node-agent-protocol.md` sections 5.3, 5.11 and 6.13).
//!
//! The volume of a `warm` app has two nodes:
//! - the **source**, which holds the volume and the app's lease and runs the
//!   app;
//! - the **warm node** (`apps.warm_node_id`), which holds the copy.
//!
//! The primary sends each of them its `spec.volumes[].warm` entry, and sends
//! the warm node the app's VM as a `standby` entry ([`crate::session::desired`]).
//! A node that reported a copy the database no longer gives it gets a
//! `deleted` entry for it ([`stale_copies`]).
//!
//! Nothing about a copy's progress is stored (migration 0011). The primary
//! keeps each node's newest `status_report.warm` in memory, as it does for the
//! edge's backends, and derives `App.warm` from it on every read ([`derive`]).
//! So the progress fields are null after a primary restart until the nodes
//! report again.
//!
//! Nothing sends `warm` yet: `updateApp` refuses `ha_policy: warm`, so no app
//! has a warm node.

use crate::AppState;
use crate::api::apps::{App, HaPolicy, WarmCopy, WarmState};
use crate::error::ApiError;
use quoro_proto::wire::{Spec, SpecVm, SpecVolumeState, SpecWarm, WarmRole, WarmStatus};
use sqlx::Row;
use sqlx::sqlite::SqlitePool;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// The TCP port of the warm node's replication listener on its own mesh
/// address (section 5.11). The contract lets an operator change it; nothing
/// does yet.
pub const REPLICATION_PORT: u16 = 51822;

/// The longest `WarmCopy.error` (`openapi.yaml`).
const ERROR_MAX: usize = 500;

/// One `warm` app whose two nodes can both be sent their entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Pair {
    pub app_id: String,
    pub volume_id: String,
    pub size_gb: u32,
    pub mount_path: String,
    /// The volume's node, which holds the app's lease.
    pub source: String,
    /// `apps.warm_node_id`.
    pub target: String,
    /// The source's mesh address, without its prefix length.
    pub source_address: String,
    /// The warm node's mesh address, without its prefix length.
    pub target_address: String,
    pub interval_s: u32,
    pub lease_id: String,
    pub fencing_token: u64,
}

impl Pair {
    /// This pair's `spec.volumes[].warm` entry for the node with `role`
    /// (section 5.11).
    pub(crate) fn entry(&self, role: WarmRole) -> SpecWarm {
        let (peer_node_id, peer_address) = match role {
            WarmRole::Source => (&self.target, &self.target_address),
            WarmRole::Target => (&self.source, &self.source_address),
        };
        SpecWarm {
            role,
            peer_node_id: peer_node_id.clone(),
            peer_address: peer_address.clone(),
            port: REPLICATION_PORT,
            interval_s: self.interval_s,
            lease_id: self.lease_id.clone(),
            fencing_token: self.fencing_token,
        }
    }
}

/// Why a `warm` app with a warm node and a `ready` volume gets no warm
/// entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Unpaired {
    /// The warm node cannot be one, or the pair cannot replicate
    /// (`openapi.yaml` `WarmCopy.node_id`, ADR 0172). `App.warm` is `failed`
    /// with this reason.
    Ineligible(&'static str),
    /// The volume's node holds no unrevoked lease of the app: the app is
    /// moving, as during a fence, a restore or a switchover. Nothing is
    /// shown, and the warm node keeps its copy ([`stale_copies`]).
    NoLease,
}

/// Every `warm` app with a warm node and a `ready` volume, ordered by app id,
/// with its pair or the reason it has none.
///
/// A pair needs every one of:
/// - a warm node other than the volume's node, and not removed;
/// - both nodes snapshotting with `reflink`: a `pause_copy` node is never
///   either side (section 6.13, ADR 0172);
/// - a mesh address on both nodes: the source dials the warm node's, and the
///   warm node admits only the source's (section 6.13);
/// - an unrevoked lease that the volume's node holds, whose token both sides
///   stamp and check (section 6.13).
///
/// An app with more than one ready volume pairs its first by id, the one its
/// VM attaches (section 5.11).
///
/// # Errors
///
/// A database error, or a stored number out of range.
pub(crate) async fn load_warm_apps(
    pool: &SqlitePool,
) -> Result<Vec<(String, Result<Pair, Unpaired>)>, String> {
    let rows = sqlx::query(
        "SELECT a.id AS app_id, a.warm_node_id, a.warm_interval_s, \
                v.id AS volume_id, v.node_id AS source, v.size_gb, v.mount_path, \
                l.id AS lease_id, l.fencing_token, l.holder_node_id, \
                ns.mesh_address AS source_address, nt.mesh_address AS target_address, \
                ns.volume_snapshot AS source_snapshot, nt.volume_snapshot AS target_snapshot, \
                ns.status AS source_status, nt.status AS target_status \
         FROM apps a \
         JOIN volumes v ON v.app_id = a.id AND v.state = 'ready' \
         LEFT JOIN leases l ON l.resource_type = 'app' AND l.resource_id = a.id \
              AND l.revoked_at IS NULL \
         JOIN nodes ns ON ns.id = v.node_id \
         JOIN nodes nt ON nt.id = a.warm_node_id \
         WHERE a.ha_policy = 'warm' AND a.warm_node_id IS NOT NULL \
         ORDER BY a.id, v.id",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| format!("cannot read the warm apps: {e}"))?;
    let mut apps: Vec<(String, Result<Pair, Unpaired>)> = Vec::new();
    for row in &rows {
        let app_id: String = row.try_get("app_id").map_err(|e| e.to_string())?;
        if apps.last().is_some_and(|(last, _)| *last == app_id) {
            continue;
        }
        let source: String = row.try_get("source").map_err(|e| e.to_string())?;
        let target: String = row.try_get("warm_node_id").map_err(|e| e.to_string())?;
        let holder: Option<String> = row.try_get("holder_node_id").map_err(|e| e.to_string())?;
        let lease_id: Option<String> = row.try_get("lease_id").map_err(|e| e.to_string())?;
        let token: Option<i64> = row.try_get("fencing_token").map_err(|e| e.to_string())?;
        let source_address: Option<String> =
            row.try_get("source_address").map_err(|e| e.to_string())?;
        let target_address: Option<String> =
            row.try_get("target_address").map_err(|e| e.to_string())?;
        let reflink = |column: &str| -> Result<bool, String> {
            let method: Option<String> = row.try_get(column).map_err(|e| e.to_string())?;
            Ok(method.as_deref() == Some("reflink"))
        };
        let source_status: String = row.try_get("source_status").map_err(|e| e.to_string())?;
        let target_status: String = row.try_get("target_status").map_err(|e| e.to_string())?;
        let unpaired = if target == source {
            Some(Unpaired::Ineligible("the warm node is the volume's node"))
        } else if source_status == "removed" {
            Some(Unpaired::Ineligible("the volume's node is removed"))
        } else if target_status == "removed" {
            Some(Unpaired::Ineligible("the warm node is removed"))
        } else if !reflink("target_snapshot")? {
            Some(Unpaired::Ineligible(
                "the warm node does not snapshot with reflink",
            ))
        } else if !reflink("source_snapshot")? {
            Some(Unpaired::Ineligible(
                "the volume's node does not snapshot with reflink",
            ))
        } else if source_address.is_none() || target_address.is_none() {
            Some(Unpaired::Ineligible(
                "a node of the pair has no mesh address yet",
            ))
        } else if holder.as_deref() != Some(source.as_str()) {
            Some(Unpaired::NoLease)
        } else {
            None
        };
        let pair = match (unpaired, lease_id, token, source_address, target_address) {
            (None, Some(lease_id), Some(token), Some(source_address), Some(target_address)) => {
                let size_gb: i64 = row.try_get("size_gb").map_err(|e| e.to_string())?;
                let interval_s: i64 = row.try_get("warm_interval_s").map_err(|e| e.to_string())?;
                Ok(Pair {
                    app_id: app_id.clone(),
                    volume_id: row.try_get("volume_id").map_err(|e| e.to_string())?,
                    size_gb: u32::try_from(size_gb).map_err(|e| format!("volumes.size_gb: {e}"))?,
                    mount_path: row.try_get("mount_path").map_err(|e| e.to_string())?,
                    source,
                    target,
                    source_address,
                    target_address,
                    interval_s: u32::try_from(interval_s)
                        .map_err(|e| format!("apps.warm_interval_s: {e}"))?,
                    lease_id,
                    fencing_token: u64::try_from(token)
                        .map_err(|e| format!("leases.fencing_token: {e}"))?,
                })
            }
            (unpaired, ..) => {
                let unpaired = unpaired.unwrap_or(Unpaired::NoLease);
                tracing::debug!(
                    app_id,
                    source,
                    target,
                    ?unpaired,
                    "the warm app gets no warm entries yet"
                );
                Err(unpaired)
            }
        };
        apps.push((app_id, pair));
    }
    Ok(apps)
}

/// Every `warm` app that can be sent its entries, ordered by app id
/// ([`load_warm_apps`]).
///
/// # Errors
///
/// A database error, or a stored number out of range.
pub(crate) async fn load_pairs(pool: &SqlitePool) -> Result<Vec<Pair>, String> {
    Ok(load_warm_apps(pool)
        .await?
        .into_iter()
        .filter_map(|(_, pair)| pair.ok())
        .collect())
}

/// The `(volume_id, app_id)` of each copy a node reported holding as a
/// target, from its newest `status_report.warm`, ordered by volume id. A
/// volume reported twice keeps its first app id.
#[must_use]
pub(crate) fn reported_targets(entries: &[WarmStatus]) -> Vec<(String, String)> {
    let mut targets: BTreeMap<String, String> = BTreeMap::new();
    for entry in entries
        .iter()
        .filter(|entry| entry.role == WarmRole::Target)
    {
        targets
            .entry(entry.volume_id.clone())
            .or_insert_with(|| entry.app_id.clone());
    }
    targets.into_iter().collect()
}

/// A copy a node reported holding that gets a `deleted` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StaleCopy {
    pub volume_id: String,
    pub app_id: String,
    /// The volume's size, or 1 once its row is gone. A `deleted` entry's
    /// size is not used.
    pub size_gb: u32,
}

/// What the primary stores about the volume of a reported copy.
#[derive(Clone, Debug, PartialEq, Eq)]
struct VolumeRow {
    node_id: String,
    app_id: String,
    state: String,
}

/// What the primary stores about the app of a reported copy.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AppRow {
    ha_policy: String,
    warm_node_id: Option<String>,
    /// The node that holds the app's unrevoked lease.
    lease_holder: Option<String>,
}

/// Whether `node_id`, which reported holding a copy of a volume for `app_id`,
/// gets a `deleted` entry for it. Only the database decides, and a copy that
/// a failover may still need is always kept: a copy left on disk costs
/// space, a copy deleted wrongly can lose the only good data.
/// - **Never** while the node has a volume row of its own for it, in any
///   state: that file is the node's own volume, which the reconciler keeps
///   or removes (section 6.7).
/// - **Yes** once the volume is being deleted (`deleting`), the volume and
///   the app are both gone, or the volume belongs to another app.
/// - **Yes** once the app left `warm`, but only while its source is
///   healthy: the volume is `ready` on the node that holds the app's
///   unrevoked lease, so the data lives there.
/// - **Yes** once the app's warm node is another node, under the same
///   condition. The new warm node starts its first copy then, so until it
///   completes, the source holds the only copy (section 6.13).
/// - **No** otherwise:
///   - a fence, a restore or a switchover revokes the lease;
///   - the source's volume may be `failed`, `detached` or `restoring`;
///   - the app may have no warm node;
///   - an app or a volume row may be missing while the other is there.
///
///   The copy may then be the data a failover starts from.
fn copy_is_stale(
    node_id: &str,
    app_id: &str,
    volume: Option<&VolumeRow>,
    app: Option<&AppRow>,
) -> bool {
    let (volume, app) = match (volume, app) {
        (Some(volume), _) if volume.node_id == node_id => return false,
        (Some(volume), _) if volume.state == "deleting" || volume.app_id != app_id => return true,
        (None, None) => return true,
        (Some(volume), Some(app)) => (volume, app),
        _ => return false,
    };
    let source_healthy =
        volume.state == "ready" && app.lease_holder.as_deref() == Some(volume.node_id.as_str());
    if !source_healthy {
        return false;
    }
    if app.ha_policy != "warm" {
        return true;
    }
    app.warm_node_id
        .as_deref()
        .is_some_and(|warm_node| warm_node != node_id)
}

/// The copies `node_id` reported holding (`reported`, from its newest report)
/// that no failover can need any more ([`copy_is_stale`]). The copy of a
/// pair whose warm node is `node_id` is never one of them.
///
/// # Errors
///
/// A database error.
pub(crate) async fn stale_copies(
    pool: &SqlitePool,
    node_id: &str,
    reported: &[(String, String)],
    pairs: &[Pair],
) -> Result<Vec<StaleCopy>, String> {
    let mut stale = Vec::new();
    for (volume_id, app_id) in reported {
        if pairs
            .iter()
            .any(|pair| pair.target == node_id && &pair.volume_id == volume_id)
        {
            continue;
        }
        let volume: Option<(String, String, String, i64)> =
            sqlx::query_as("SELECT node_id, app_id, state, size_gb FROM volumes WHERE id = ?")
                .bind(volume_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| format!("cannot read the volume of a warm copy {volume_id}: {e}"))?;
        let app: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT a.ha_policy, a.warm_node_id, l.holder_node_id FROM apps a \
             LEFT JOIN leases l ON l.resource_type = 'app' AND l.resource_id = a.id \
                  AND l.revoked_at IS NULL \
             WHERE a.id = ?",
        )
        .bind(app_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("cannot read the app of a warm copy {volume_id}: {e}"))?;
        let size_gb = volume
            .as_ref()
            .and_then(|(.., size)| u32::try_from(*size).ok())
            .unwrap_or(1);
        let volume = volume.map(|(node_id, app_id, state, _)| VolumeRow {
            node_id,
            app_id,
            state,
        });
        let app = app.map(|(ha_policy, warm_node_id, lease_holder)| AppRow {
            ha_policy,
            warm_node_id,
            lease_holder,
        });
        if copy_is_stale(node_id, app_id, volume.as_ref(), app.as_ref()) {
            stale.push(StaleCopy {
                volume_id: volume_id.clone(),
                app_id: app_id.clone(),
                size_gb,
            });
        }
    }
    Ok(stale)
}

/// What one side of a pair holds in a snapshot: its warm entry and, on the
/// source, the app's VMs, which the warm node's `standby` entry is built
/// from.
#[derive(Clone, Debug, PartialEq)]
struct PairFacts {
    warm: SpecWarm,
    /// On the source: every VM of the app other than standby ones, by id.
    vms: Vec<SpecVm>,
}

/// The warm facts of one snapshot, keyed by volume id ([`facts_of`]).
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WarmFacts(BTreeMap<String, PairFacts>);

impl WarmFacts {
    /// Whether the snapshot carries no warm entry.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The warm facts of `spec`: for each `present` volume with `warm`, its entry
/// and, on the source, the app's VMs.
#[must_use]
pub(crate) fn facts_of(spec: &Spec) -> WarmFacts {
    WarmFacts(
        spec.volumes
            .iter()
            .filter(|file| file.state == SpecVolumeState::Present)
            .filter_map(|file| {
                let warm = file.warm.clone()?;
                let mut vms: Vec<SpecVm> = if warm.role == WarmRole::Source {
                    spec.vms
                        .iter()
                        .filter(|vm| !vm.standby && vm.app_id.as_deref() == Some(&file.app_id))
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };
                vms.sort_by(|a, b| a.vm_id.cmp(&b.vm_id));
                Some((file.volume_id.clone(), PairFacts { warm, vms }))
            })
            .collect(),
    )
}

/// The other nodes of the pairs whose facts differ between the snapshot last
/// sent to a node (`old`, on any of its sessions) and the one just built for
/// it (`new`). The caller rebuilds their snapshots.
///
/// A rebuild of one side therefore reaches the other, whatever caused it:
/// - a new token or interval;
/// - a pair that appeared or went away, also while the node was away;
/// - a change to the source's VMs: a redeploy, a new digest, resources or
///   environment, which the warm node's standby entry follows.
///
/// It converges after one more rebuild of the peer, whose own facts then
/// match its last snapshot.
#[must_use]
pub(crate) fn peers_to_wake(old: Option<&WarmFacts>, new: &WarmFacts) -> BTreeSet<String> {
    let empty = WarmFacts::default();
    let before = old.unwrap_or(&empty);
    let mut peers = BTreeSet::new();
    for volume_id in before.0.keys().chain(new.0.keys()) {
        let (was, is) = (before.0.get(volume_id), new.0.get(volume_id));
        if was == is {
            continue;
        }
        peers.extend(
            [was, is]
                .into_iter()
                .flatten()
                .map(|facts| facts.warm.peer_node_id.clone()),
        );
    }
    peers
}

/// The settings `App.warm` shows as stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Settings {
    pub node_id: Option<String>,
    pub node_pinned: bool,
    pub interval_s: i64,
}

/// What the primary knows of the two sides of an app's copy.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Sides<'a> {
    /// The source's node, or `None` while the app has no ready volume.
    pub source_node: Option<&'a str>,
    /// Why the warm node cannot be one ([`Unpaired::Ineligible`]).
    pub ineligible: Option<&'a str>,
    pub source_connected: bool,
    pub target_connected: bool,
    /// The source's newest `role: source` entry for the volume.
    pub source: Option<&'a WarmStatus>,
    /// The warm node's newest `role: target` entry for the volume.
    pub target: Option<&'a WarmStatus>,
}

/// `App.warm` from the stored settings and the two sides' newest reports
/// (section 6.13, `openapi.yaml` `WarmCopy`):
/// - `last_snapshot_seq` and `last_shipped_at` are the warm node's
///   `snapshot_seq` and `completed_at`;
/// - `lag_s` is the time since the source took that copy's snapshot
///   (`snapshot_taken_at`), on the primary's clock;
/// - `state` is `failed` while no node is eligible, the warm node cannot be
///   one, either node is not connected or either side reports an error;
///   otherwise `syncing` until
///   the warm node holds a first complete copy, `in_sync` while `lag_s` is at
///   most twice `interval_s`, and `lagging` above that.
///
/// A copy whose snapshot time is missing or unreadable reads `lagging`: its
/// lag is unknown, so it is never shown in sync.
#[must_use]
pub(crate) fn derive(settings: &Settings, sides: &Sides<'_>, now: OffsetDateTime) -> WarmCopy {
    let target = sides.target;
    let last_snapshot_seq = target
        .and_then(|entry| entry.snapshot_seq)
        .and_then(|seq| i64::try_from(seq).ok());
    let lag_s = last_snapshot_seq
        .and(target)
        .and_then(|entry| entry.snapshot_taken_at.as_deref())
        .and_then(|taken| OffsetDateTime::parse(taken, &Rfc3339).ok())
        .map(|taken| (now - taken).whole_seconds().max(0));
    let failure = if settings.node_id.is_none() {
        Some("no node is eligible as the warm node".to_owned())
    } else if sides.source_node.is_none() {
        Some("the app has no ready volume".to_owned())
    } else if let Some(why) = sides.ineligible {
        Some(why.to_owned())
    } else if !sides.source_connected {
        Some("the source node is not connected".to_owned())
    } else if !sides.target_connected {
        Some("the warm node is not connected".to_owned())
    } else if let Some(error) = sides.source.and_then(|entry| entry.error.as_deref()) {
        Some(format!("source: {error}"))
    } else {
        target
            .and_then(|entry| entry.error.as_deref())
            .map(|error| format!("warm node: {error}"))
    };
    let state = match (&failure, last_snapshot_seq, lag_s) {
        (Some(_), _, _) => WarmState::Failed,
        (None, None, _) => WarmState::Syncing,
        (None, Some(_), Some(lag)) if lag <= 2 * settings.interval_s => WarmState::InSync,
        (None, Some(_), _) => WarmState::Lagging,
    };
    WarmCopy {
        node_id: settings.node_id.clone(),
        node_pinned: settings.node_pinned,
        interval_s: settings.interval_s,
        state,
        last_snapshot_seq,
        last_shipped_at: last_snapshot_seq
            .and(target)
            .and_then(|entry| entry.completed_at.clone()),
        lag_s: last_snapshot_seq.and(lag_s),
        extent_count: sides
            .source
            .and_then(|entry| entry.extent_count)
            .and_then(|count| i64::try_from(count).ok()),
        error: failure.map(|error| truncate(error, ERROR_MAX)),
    }
}

/// `text` cut to at most `max` bytes on a character boundary.
fn truncate(mut text: String, max: usize) -> String {
    if text.len() > max {
        let mut end = max;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// Fills `warm` on every `warm` app of `apps` (`openapi.yaml` `App.warm`);
/// every other app keeps `null`.
///
/// # Errors
///
/// A database error.
pub(crate) async fn attach(state: &AppState, apps: &mut [App]) -> Result<(), ApiError> {
    if !apps.iter().any(|app| app.ha_policy == HaPolicy::Warm) {
        return Ok(());
    }
    let reports = state.hub.warm_reports();
    let ineligible: HashMap<String, &'static str> = load_warm_apps(state.store.pool())
        .await
        .map_err(|e| ApiError::internal(&e))?
        .into_iter()
        .filter_map(|(app_id, pair)| match pair {
            Err(Unpaired::Ineligible(why)) => Some((app_id, why)),
            _ => None,
        })
        .collect();
    let now = OffsetDateTime::now_utc();
    for app in apps.iter_mut() {
        if app.ha_policy != HaPolicy::Warm {
            app.warm = None;
            continue;
        }
        // An app deleted since it was read shows no warm copy.
        let Some((node_id, pinned, interval_s)): Option<(Option<String>, i64, i64)> =
            sqlx::query_as(
                "SELECT warm_node_id, warm_node_pinned, warm_interval_s FROM apps WHERE id = ?",
            )
            .bind(&app.id)
            .fetch_optional(state.store.pool())
            .await?
        else {
            app.warm = None;
            continue;
        };
        let volume: Option<(String, String)> = sqlx::query_as(
            "SELECT id, node_id FROM volumes WHERE app_id = ? AND state = 'ready' \
             ORDER BY id LIMIT 1",
        )
        .bind(&app.id)
        .fetch_optional(state.store.pool())
        .await?;
        let entry = |node: Option<&str>, role: WarmRole| {
            let (node, (volume_id, _)) = (node?, volume.as_ref()?);
            reports.get(node)?.iter().find(|entry| {
                entry.role == role && &entry.volume_id == volume_id && entry.app_id == app.id
            })
        };
        let source_node = volume.as_ref().map(|(_, node)| node.as_str());
        let target_node = node_id.as_deref();
        let sides = Sides {
            source_node,
            ineligible: ineligible.get(&app.id).copied(),
            source_connected: source_node.is_some_and(|node| state.hub.is_connected(node)),
            target_connected: target_node.is_some_and(|node| state.hub.is_connected(node)),
            source: entry(source_node, WarmRole::Source),
            target: entry(target_node, WarmRole::Target),
        };
        let settings = Settings {
            node_id: node_id.clone(),
            node_pinned: pinned != 0,
            interval_s,
        };
        app.warm = Some(derive(&settings, &sides, now));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_proto::wire::{DesiredStatus, SpecVolumeFile};

    fn settings() -> Settings {
        Settings {
            node_id: Some("nod_t".to_owned()),
            node_pinned: false,
            interval_s: 30,
        }
    }

    fn status(role: WarmRole) -> WarmStatus {
        WarmStatus {
            volume_id: "vol_1".to_owned(),
            app_id: "app_1".to_owned(),
            role,
            snapshot_seq: None,
            snapshot_taken_at: None,
            completed_at: None,
            allocated_bytes: None,
            extent_count: None,
            frozen: None,
            rejected_token: None,
            rejected_at: None,
            error: None,
        }
    }

    fn copy(seq: u64, taken_at: &str) -> WarmStatus {
        WarmStatus {
            snapshot_seq: Some(seq),
            snapshot_taken_at: Some(taken_at.to_owned()),
            completed_at: Some("2026-11-07T10:00:02Z".to_owned()),
            ..status(WarmRole::Target)
        }
    }

    fn connected<'a>(source: Option<&'a WarmStatus>, target: Option<&'a WarmStatus>) -> Sides<'a> {
        Sides {
            source_node: Some("nod_s"),
            ineligible: None,
            source_connected: true,
            target_connected: true,
            source,
            target,
        }
    }

    fn at(text: &str) -> OffsetDateTime {
        OffsetDateTime::parse(text, &Rfc3339).unwrap()
    }

    #[test]
    fn a_copy_is_syncing_until_the_warm_node_holds_one() {
        let source = WarmStatus {
            extent_count: Some(12),
            ..status(WarmRole::Source)
        };
        let target = status(WarmRole::Target);
        let warm = derive(
            &settings(),
            &connected(Some(&source), Some(&target)),
            at("2026-11-07T10:00:00Z"),
        );
        assert_eq!(warm.state, WarmState::Syncing);
        assert_eq!(warm.last_snapshot_seq, None);
        assert_eq!(warm.last_shipped_at, None);
        assert_eq!(warm.lag_s, None);
        assert_eq!(warm.extent_count, Some(12));
        assert_eq!(warm.error, None);
        assert_eq!(warm.node_id.as_deref(), Some("nod_t"));
        assert_eq!(warm.interval_s, 30);
    }

    #[test]
    fn the_lag_is_measured_from_the_snapshot_on_the_primary_clock() {
        let target = copy(7, "2026-11-07T10:00:00Z");
        let sides = connected(None, Some(&target));
        let in_sync = derive(&settings(), &sides, at("2026-11-07T10:01:00Z"));
        assert_eq!(in_sync.state, WarmState::InSync);
        assert_eq!(in_sync.lag_s, Some(60));
        assert_eq!(in_sync.last_snapshot_seq, Some(7));
        assert_eq!(
            in_sync.last_shipped_at.as_deref(),
            Some("2026-11-07T10:00:02Z")
        );

        let lagging = derive(&settings(), &sides, at("2026-11-07T10:01:01Z"));
        assert_eq!(lagging.state, WarmState::Lagging, "61 s is above 2 x 30 s");
        assert_eq!(lagging.lag_s, Some(61));

        // A clock that reads before the snapshot shows no negative lag.
        let early = derive(&settings(), &sides, at("2026-11-07T09:59:00Z"));
        assert_eq!(early.lag_s, Some(0));
        assert_eq!(early.state, WarmState::InSync);
    }

    #[test]
    fn a_copy_without_a_readable_snapshot_time_is_never_in_sync() {
        let target = WarmStatus {
            snapshot_taken_at: Some("yesterday".to_owned()),
            ..copy(3, "")
        };
        let warm = derive(
            &settings(),
            &connected(None, Some(&target)),
            at("2026-11-07T10:00:00Z"),
        );
        assert_eq!(warm.state, WarmState::Lagging);
        assert_eq!(warm.lag_s, None);
        assert_eq!(warm.last_snapshot_seq, Some(3));
    }

    #[test]
    fn a_copy_fails_while_no_node_is_eligible_a_node_is_away_or_a_side_errs() {
        let target = copy(7, "2026-11-07T10:00:00Z");
        let now = at("2026-11-07T10:00:10Z");

        let none = Settings {
            node_id: None,
            ..settings()
        };
        let warm = derive(&none, &connected(None, None), now);
        assert_eq!(warm.state, WarmState::Failed);
        assert_eq!(
            warm.error.as_deref(),
            Some("no node is eligible as the warm node")
        );

        let away = Sides {
            target_connected: false,
            ..connected(None, Some(&target))
        };
        let warm = derive(&settings(), &away, now);
        assert_eq!(warm.state, WarmState::Failed);
        assert_eq!(
            warm.error.as_deref(),
            Some("the warm node is not connected")
        );
        // The last shipped snapshot is still shown.
        assert_eq!(warm.last_snapshot_seq, Some(7));

        let source_away = Sides {
            source_connected: false,
            ..connected(None, Some(&target))
        };
        assert_eq!(
            derive(&settings(), &source_away, now).error.as_deref(),
            Some("the source node is not connected")
        );

        let no_volume = Sides {
            source_node: None,
            ..connected(None, None)
        };
        assert_eq!(
            derive(&settings(), &no_volume, now).error.as_deref(),
            Some("the app has no ready volume")
        );

        // A warm node that cannot be one fails before connectivity is looked
        // at, and the last shipped snapshot is still shown.
        let ineligible = Sides {
            ineligible: Some("the warm node does not snapshot with reflink"),
            source_connected: false,
            ..connected(None, Some(&target))
        };
        let warm = derive(&settings(), &ineligible, now);
        assert_eq!(warm.state, WarmState::Failed);
        assert_eq!(
            warm.error.as_deref(),
            Some("the warm node does not snapshot with reflink")
        );
        assert_eq!(warm.last_snapshot_seq, Some(7));

        let source = WarmStatus {
            error: Some("connect timed out".to_owned()),
            ..status(WarmRole::Source)
        };
        let warm = derive(&settings(), &connected(Some(&source), Some(&target)), now);
        assert_eq!(warm.state, WarmState::Failed);
        assert_eq!(warm.error.as_deref(), Some("source: connect timed out"));

        let refused = WarmStatus {
            error: Some("x".repeat(600)),
            ..copy(7, "2026-11-07T10:00:00Z")
        };
        let warm = derive(&settings(), &connected(None, Some(&refused)), now);
        assert_eq!(warm.state, WarmState::Failed);
        assert_eq!(warm.error.as_ref().map(String::len), Some(500));
        assert!(warm.error.unwrap().starts_with("warm node: x"));
    }

    fn pair() -> Pair {
        Pair {
            app_id: "app_1".to_owned(),
            volume_id: "vol_1".to_owned(),
            size_gb: 4,
            mount_path: "/data".to_owned(),
            source: "nod_s".to_owned(),
            target: "nod_t".to_owned(),
            source_address: "10.43.0.2".to_owned(),
            target_address: "10.43.0.3".to_owned(),
            interval_s: 30,
            lease_id: "lse_1".to_owned(),
            fencing_token: 4,
        }
    }

    #[test]
    fn each_side_names_the_other_with_the_lease_and_the_listener_port() {
        let source = pair().entry(WarmRole::Source);
        assert_eq!(source.peer_node_id, "nod_t");
        assert_eq!(source.peer_address, "10.43.0.3");
        let target = pair().entry(WarmRole::Target);
        assert_eq!(target.peer_node_id, "nod_s");
        // The warm node compares it with the stream's source address.
        assert_eq!(target.peer_address, "10.43.0.2");
        for entry in [source, target] {
            assert_eq!(entry.port, 51822);
            assert_eq!(entry.interval_s, 30);
            assert_eq!(entry.lease_id, "lse_1");
            assert_eq!(entry.fencing_token, 4);
        }
    }

    fn file(volume_id: &str, warm: Option<SpecWarm>) -> SpecVolumeFile {
        SpecVolumeFile {
            volume_id: volume_id.to_owned(),
            app_id: "app_1".to_owned(),
            size_gb: 4,
            state: SpecVolumeState::Present,
            restore: None,
            warm,
        }
    }

    #[test]
    fn a_copy_is_deleted_only_once_no_failover_can_need_it() {
        let volume = |node_id: &str, app_id: &str, state: &str| VolumeRow {
            node_id: node_id.to_owned(),
            app_id: app_id.to_owned(),
            state: state.to_owned(),
        };
        let app = |ha_policy: &str, warm_node_id: Option<&str>, holder: Option<&str>| AppRow {
            ha_policy: ha_policy.to_owned(),
            warm_node_id: warm_node_id.map(str::to_owned),
            lease_holder: holder.map(str::to_owned),
        };
        let ready = volume("nod_s", "app_1", "ready");
        let held = Some("nod_s");
        let cases = [
            // Kept: a failover may still need the copy.
            (
                "an incomplete pair, as after a fence",
                Some(ready.clone()),
                Some(app("warm", Some("nod_t"), None)),
                false,
            ),
            (
                "a failed source volume",
                Some(volume("nod_s", "app_1", "failed")),
                Some(app("warm", Some("nod_t"), held)),
                false,
            ),
            (
                "no warm node yet",
                Some(ready.clone()),
                Some(app("warm", None, held)),
                false,
            ),
            (
                "another warm node while the source is fenced",
                Some(ready.clone()),
                Some(app("warm", Some("nod_x"), None)),
                false,
            ),
            (
                "another warm node while the source volume is detached",
                Some(volume("nod_s", "app_1", "detached")),
                Some(app("warm", Some("nod_x"), held)),
                false,
            ),
            (
                "the app left warm while its source is fenced",
                Some(ready.clone()),
                Some(app("none", None, None)),
                false,
            ),
            (
                "the app left warm while its source volume failed",
                Some(volume("nod_s", "app_1", "failed")),
                Some(app("none", None, held)),
                false,
            ),
            (
                "the app is gone but its volume is not",
                Some(ready.clone()),
                None,
                false,
            ),
            (
                "the volume is gone but its app is not",
                None,
                Some(app("warm", Some("nod_x"), held)),
                false,
            ),
            (
                "the node's own volume",
                Some(volume("nod_t", "app_1", "deleting")),
                None,
                false,
            ),
            (
                "the node's own failed volume",
                Some(volume("nod_t", "app_1", "failed")),
                Some(app("none", None, None)),
                false,
            ),
            // Deleted.
            (
                "the volume is being deleted",
                Some(volume("nod_s", "app_1", "deleting")),
                Some(app("warm", Some("nod_t"), None)),
                true,
            ),
            ("the volume and the app are gone", None, None, true),
            (
                "the volume is another app's",
                Some(volume("nod_s", "app_2", "ready")),
                Some(app("warm", Some("nod_t"), held)),
                true,
            ),
            (
                "the app left warm and its source is healthy",
                Some(ready.clone()),
                Some(app("cold", None, held)),
                true,
            ),
            (
                "another warm node and a healthy source",
                Some(ready.clone()),
                Some(app("warm", Some("nod_x"), held)),
                true,
            ),
        ];
        for (case, volume, app, stale) in cases {
            assert_eq!(
                copy_is_stale("nod_t", "app_1", volume.as_ref(), app.as_ref()),
                stale,
                "{case}"
            );
        }
    }

    #[test]
    fn reported_targets_skip_the_source_side_and_repeats() {
        let mut other = status(WarmRole::Target);
        other.volume_id = "vol_0".to_owned();
        let mut twice = status(WarmRole::Target);
        twice.app_id = "app_9".to_owned();
        let reported = reported_targets(&[
            status(WarmRole::Source),
            status(WarmRole::Target),
            status(WarmRole::Target),
            twice,
            other,
        ]);
        assert_eq!(
            reported,
            vec![
                ("vol_0".to_owned(), "app_1".to_owned()),
                ("vol_1".to_owned(), "app_1".to_owned())
            ]
        );
    }

    fn app_vm(digest: &str) -> SpecVm {
        SpecVm {
            vm_id: "vm_1".to_owned(),
            project_id: "prj_1".to_owned(),
            app_id: Some("app_1".to_owned()),
            deployment_id: None,
            name: "db".to_owned(),
            image: "img".to_owned(),
            image_digest: digest.to_owned(),
            vcpus: 1,
            memory_mb: 256,
            disk_gb: 2,
            port: Some(8080),
            env: BTreeMap::new(),
            health_check: None,
            desired_status: DesiredStatus::Running,
            standby: false,
            restart_generation: 0,
            stateful: true,
            lease_id: Some("lse_1".to_owned()),
            fencing_token: Some(4),
            volumes: Vec::new(),
            build_id: None,
        }
    }

    fn source_spec(token: u64, digest: &str) -> Spec {
        let mut entry = pair();
        entry.fencing_token = token;
        Spec {
            vms: vec![app_vm(digest)],
            volumes: vec![file("vol_1", Some(entry.entry(WarmRole::Source)))],
            ..Spec::default()
        }
    }

    #[test]
    fn a_changed_pair_wakes_its_peer_and_an_unchanged_one_does_not() {
        let wake = |old: Option<&Spec>, new: &Spec| {
            peers_to_wake(old.map(facts_of).as_ref(), &facts_of(new))
        };
        let spec = source_spec(4, "sha256:a");
        assert_eq!(wake(None, &spec), BTreeSet::from(["nod_t".to_owned()]));
        assert!(wake(Some(&spec), &source_spec(4, "sha256:a")).is_empty());
        assert_eq!(
            wake(Some(&spec), &source_spec(5, "sha256:a")).len(),
            1,
            "a new token"
        );
        assert_eq!(
            wake(Some(&spec), &source_spec(4, "sha256:b")).len(),
            1,
            "a source VM with a new digest"
        );

        // A redeploy adds a second VM before the first one goes.
        let mut redeployed = source_spec(4, "sha256:a");
        let mut newer = app_vm("sha256:c");
        newer.vm_id = "vm_0".to_owned();
        redeployed.vms.push(newer);
        assert_eq!(wake(Some(&spec), &redeployed).len(), 1, "a new source VM");

        let mut env = source_spec(4, "sha256:a");
        env.vms[0]
            .env
            .insert("DATABASE_URL".to_owned(), "x".to_owned());
        assert_eq!(wake(Some(&spec), &env).len(), 1, "a new environment value");

        // The standby entries of the warm node are not facts of the pair.
        let mut with_standby = source_spec(4, "sha256:a");
        let mut standby = app_vm("sha256:z");
        standby.standby = true;
        with_standby.vms.push(standby);
        assert!(wake(Some(&spec), &with_standby).is_empty());

        assert_eq!(
            wake(Some(&spec), &Spec::default()),
            BTreeSet::from(["nod_t".to_owned()]),
            "a pair that went away"
        );
        assert!(wake(Some(&Spec::default()), &Spec::default()).is_empty());
        assert!(facts_of(&Spec::default()).is_empty());
    }
}
