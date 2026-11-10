// SPDX-License-Identifier: AGPL-3.0-only
//! The N+1 indicator: `getClusterCapacity` simulates the loss of one node
//! (ADR 0021, ADR 0178, ADR 0217, `openapi.yaml` `ClusterCapacity`).
//!
//! The primary takes every app with an instance meant to run on the lost node
//! or a volume there:
//! - a stateless app, or one whose data is elsewhere, that keeps an instance
//!   on another node loses one replica (`keep`);
//! - a stateless app whose only instance was there needs a new one (`start`);
//! - a `warm` app starts on its warm node (`failover`), which already holds
//!   the disk, so it needs memory and vCPUs there and may go nowhere else; it
//!   cannot move without a complete copy there (`no_warm_copy`);
//! - any other stateful app needs a restore of its latest backup (`restore`);
//!   it cannot move without a succeeded, unpruned backup (`no_backup`).
//!
//! The apps that need a node are placed greedily, largest first by memory,
//! then vCPUs, then disk, on the remaining connected node with the `node`
//! role that has the most memory left and room for the whole app, within the
//! app's `placement.node_ids`. A node's room is its capacity minus what the
//! VMs placed on it use, as `Node.allocated` shows. Nothing is reserved and no
//! event is emitted.
//!
//! [`simulate`] is pure, so every case is tested without a database; [`load`]
//! reads its inputs. The panel's mock mode answers with the same rules
//! (`panel/src/mocks/capacity.ts`).

use crate::AppState;
use crate::api::apps::{APP_COLUMNS, HaPolicy, app_from_row};
use crate::api::cluster::{CapacityAction, CapacityReason, CapacityShortfall, ClusterCapacityApp};
use crate::api::nodes::{NODE_COLUMNS, NodeRole, NodeStatus, node_from_row};
use crate::error::ApiError;
use sqlx::{Row, SqliteConnection};
use std::collections::{HashMap, HashSet};

/// What a remaining node has left for the apps of the lost node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Room {
    pub node_id: String,
    pub cpus: i64,
    pub memory_mb: i64,
    pub disk_gb: i64,
}

/// What the simulation needs to know of one app.
#[derive(Clone, Debug, Default)]
pub(crate) struct AppFacts {
    pub app_id: String,
    pub project_id: String,
    pub policy: HaPolicy,
    pub stateful: bool,
    pub vcpus: i64,
    pub memory_mb: i64,
    pub disk_gb: i64,
    /// `placement.node_ids`; empty means any node.
    pub placement: Vec<String>,
    /// The nodes of the app's VMs that are not `deleted`.
    pub instance_nodes: Vec<String>,
    /// The node of the app's `creating`, `ready` or `restoring` volume.
    pub volume_node: Option<String>,
    /// `apps.warm_node_id`.
    pub warm_node: Option<String>,
    /// Whether the warm node reports a complete copy of the app's volume.
    pub warm_copy: bool,
    /// Whether the app has a succeeded backup that is not pruned.
    pub has_backup: bool,
}

/// One answer entry, with what the app would need: the needed resources
/// count in `missing` only while the entry is listed with `no_capacity`, so
/// [`summarize`] can work them out from the entries a caller may see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub app: ClusterCapacityApp,
    pub project_id: String,
    pub need: CapacityShortfall,
}

/// A need for a node: an app that must start somewhere.
struct Need<'a> {
    app: &'a AppFacts,
    action: CapacityAction,
    /// The only node it may go to: a warm app's warm node.
    only: Option<&'a str>,
}

/// Simulates the loss of `lost` against the `rooms` of the remaining
/// connected nodes with the `node` role. `apps` are taken in their order (by
/// id from [`load`]); the answer lists the kept apps, then the placed ones in
/// placement order, then the apps that cannot move for another reason than
/// capacity.
pub(crate) fn simulate(lost: &str, rooms: &[Room], apps: &[AppFacts]) -> Vec<Entry> {
    let mut free: Vec<Room> = rooms
        .iter()
        .filter(|room| room.node_id != lost)
        .cloned()
        .collect();
    let mut kept = Vec::new();
    let mut settled = Vec::new();
    let mut needs = Vec::new();

    for app in apps {
        let on_lost = app.instance_nodes.iter().any(|node| node == lost);
        let volume_there = app.volume_node.as_deref() == Some(lost);
        if !on_lost && !volume_there {
            continue;
        }
        // An app whose data is not on the lost node, with an instance
        // elsewhere, only loses a replica.
        if !app.stateful || !volume_there {
            if let Some(elsewhere) = app.instance_nodes.iter().find(|node| *node != lost) {
                kept.push(entry(
                    app,
                    CapacityAction::Keep,
                    Some(elsewhere.clone()),
                    None,
                ));
                continue;
            }
            if !app.stateful {
                needs.push(Need {
                    app,
                    action: CapacityAction::Start,
                    only: None,
                });
                continue;
            }
        }
        if app.policy == HaPolicy::Warm {
            match app.warm_node.as_deref() {
                Some(warm) if app.warm_copy => needs.push(Need {
                    app,
                    action: CapacityAction::Failover,
                    only: Some(warm),
                }),
                _ => settled.push(entry(
                    app,
                    CapacityAction::Failover,
                    None,
                    Some(CapacityReason::NoWarmCopy),
                )),
            }
        } else if app.has_backup {
            needs.push(Need {
                app,
                action: CapacityAction::Restore,
                only: None,
            });
        } else {
            settled.push(entry(
                app,
                CapacityAction::Restore,
                None,
                Some(CapacityReason::NoBackup),
            ));
        }
    }

    // Largest first; a stable sort keeps equal apps in id order.
    needs.sort_by(|left, right| {
        right
            .app
            .memory_mb
            .cmp(&left.app.memory_mb)
            .then(right.app.vcpus.cmp(&left.app.vcpus))
            .then(right.app.disk_gb.cmp(&left.app.disk_gb))
    });

    let mut placed = Vec::with_capacity(needs.len());
    for need in &needs {
        let app = need.app;
        // A warm copy already holds the disk on its node.
        let disk = if need.action == CapacityAction::Failover {
            0
        } else {
            app.disk_gb
        };
        let allowed = |room: &Room| match need.only {
            Some(only) => room.node_id == only,
            None => app.placement.is_empty() || app.placement.contains(&room.node_id),
        };
        // The node with the most memory left; the first in id order on a tie.
        let mut best: Option<usize> = None;
        for (index, room) in free.iter().enumerate() {
            let fits =
                room.cpus >= app.vcpus && room.memory_mb >= app.memory_mb && room.disk_gb >= disk;
            if allowed(room)
                && fits
                && best.is_none_or(|best| room.memory_mb > free[best].memory_mb)
            {
                best = Some(index);
            }
        }
        let mut placed_entry = match best {
            Some(index) => {
                let room = &mut free[index];
                room.cpus -= app.vcpus;
                room.memory_mb -= app.memory_mb;
                room.disk_gb -= disk;
                entry(app, need.action, Some(room.node_id.clone()), None)
            }
            None => entry(app, need.action, None, Some(CapacityReason::NoCapacity)),
        };
        placed_entry.need = CapacityShortfall {
            cpus: app.vcpus,
            memory_mb: app.memory_mb,
            disk_gb: disk,
        };
        placed.push(placed_entry);
    }

    kept.extend(placed);
    kept.extend(settled);
    kept
}

fn entry(
    app: &AppFacts,
    action: CapacityAction,
    node_id: Option<String>,
    reason: Option<CapacityReason>,
) -> Entry {
    Entry {
        app: ClusterCapacityApp {
            app_id: app.app_id.clone(),
            policy: app.policy,
            action,
            node_id,
            reason,
        },
        project_id: app.project_id.clone(),
        need: CapacityShortfall::default(),
    }
}

/// `fits` and `missing` of the listed `entries`: `fits` is false only when an
/// entry has `no_capacity`, and `missing` sums those entries' needs.
pub(crate) fn summarize(entries: &[Entry]) -> (bool, CapacityShortfall) {
    let mut missing = CapacityShortfall::default();
    let mut fits = true;
    for entry in entries {
        if entry.app.reason == Some(CapacityReason::NoCapacity) {
            fits = false;
            missing.cpus += entry.need.cpus;
            missing.memory_mb += entry.need.memory_mb;
            missing.disk_gb += entry.need.disk_gb;
        }
    }
    (fits, missing)
}

/// What [`load`] found.
pub(crate) enum Loaded {
    /// The node does not exist or was removed.
    UnknownNode,
    /// The inputs of [`simulate`].
    Inputs {
        rooms: Vec<Room>,
        apps: Vec<AppFacts>,
    },
}

/// Volume states that hold the app's data (`idx_volumes_app_attached`).
const ACTIVE_VOLUME: &str = "state IN ('creating', 'ready', 'restoring')";

/// Reads the inputs of [`simulate`] for the loss of `lost` through `conn`,
/// which should be a read transaction so every table is read at one point.
/// The warm copies come from the nodes' newest reports, which the primary
/// holds in memory only: after a restart a warm app reads `no_warm_copy` until
/// its warm node reports again.
///
/// # Errors
///
/// A database error.
pub(crate) async fn load(
    conn: &mut SqliteConnection,
    state: &AppState,
    lost: &str,
) -> Result<Loaded, ApiError> {
    let rows = sqlx::query(&format!("SELECT {NODE_COLUMNS} FROM nodes n ORDER BY n.id"))
        .fetch_all(&mut *conn)
        .await?;
    let nodes = rows
        .iter()
        .map(node_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    if !nodes
        .iter()
        .any(|node| node.id == lost && node.status != NodeStatus::Removed)
    {
        return Ok(Loaded::UnknownNode);
    }
    let rooms = nodes
        .iter()
        .filter(|node| {
            node.id != lost
                && node.status == NodeStatus::Connected
                && node.roles.contains(&NodeRole::Node)
        })
        .map(|node| Room {
            node_id: node.id.clone(),
            cpus: i64::from(node.capacity.cpus) - i64::from(node.allocated.cpus),
            memory_mb: signed(node.capacity.memory_mb) - signed(node.allocated.memory_mb),
            disk_gb: signed(node.capacity.disk_gb) - signed(node.allocated.disk_gb),
        })
        .collect();

    // Only the apps the lost node touches; the others' VMs are already in
    // the rooms.
    let rows = sqlx::query(&format!(
        "SELECT {APP_COLUMNS}, warm_node_id FROM apps WHERE id IN \
         (SELECT app_id FROM vms WHERE node_id = ? AND app_id IS NOT NULL \
            AND observed_status != 'deleted' \
          UNION SELECT app_id FROM volumes WHERE node_id = ? AND {ACTIVE_VOLUME}) \
         ORDER BY id"
    ))
    .bind(lost)
    .bind(lost)
    .fetch_all(&mut *conn)
    .await?;
    if rows.is_empty() {
        return Ok(Loaded::Inputs {
            rooms,
            apps: Vec::new(),
        });
    }

    let mut instances: HashMap<String, Vec<String>> = HashMap::new();
    for row in sqlx::query(
        "SELECT app_id, node_id FROM vms WHERE app_id IS NOT NULL \
         AND observed_status != 'deleted' ORDER BY node_id, id",
    )
    .fetch_all(&mut *conn)
    .await?
    {
        instances
            .entry(row.try_get("app_id")?)
            .or_default()
            .push(row.try_get("node_id")?);
    }
    let mut volumes: HashMap<String, (String, String)> = HashMap::new();
    for row in sqlx::query(&format!(
        "SELECT id, app_id, node_id FROM volumes WHERE {ACTIVE_VOLUME}"
    ))
    .fetch_all(&mut *conn)
    .await?
    {
        volumes.insert(
            row.try_get("app_id")?,
            (row.try_get("id")?, row.try_get("node_id")?),
        );
    }
    let backed_up: HashSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT app_id FROM backups WHERE source_kind = 'volume' \
         AND status = 'succeeded' AND pruned_at IS NULL AND app_id IS NOT NULL",
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    let reports = state.hub.warm_reports();

    let mut apps = Vec::with_capacity(rows.len());
    for row in &rows {
        let app = app_from_row(row)?;
        let warm_node: Option<String> = row.try_get("warm_node_id")?;
        let volume = volumes.get(&app.id);
        let warm_copy = match (&warm_node, volume) {
            (Some(warm), Some((volume_id, _))) => {
                crate::failover::complete_copy_in(&reports, warm, volume_id, &app.id).is_some()
            }
            _ => false,
        };
        apps.push(AppFacts {
            instance_nodes: instances.remove(&app.id).unwrap_or_default(),
            volume_node: volume.map(|(_, node_id)| node_id.clone()),
            warm_node,
            warm_copy,
            has_backup: backed_up.contains(&app.id),
            policy: app.ha_policy,
            stateful: app.stateful,
            vcpus: app.resources.vcpus,
            memory_mb: app.resources.memory_mb,
            disk_gb: app.resources.disk_gb,
            placement: app.placement.node_ids,
            project_id: app.project_id,
            app_id: app.id,
        });
    }
    Ok(Loaded::Inputs { rooms, apps })
}

/// A `u64` amount as `i64`, saturating.
fn signed(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOST: &str = "nod_LOST";
    const A: &str = "nod_A";
    const B: &str = "nod_B";

    fn room(node_id: &str, cpus: i64, memory_mb: i64, disk_gb: i64) -> Room {
        Room {
            node_id: node_id.to_string(),
            cpus,
            memory_mb,
            disk_gb,
        }
    }

    fn app(id: &str, memory_mb: i64) -> AppFacts {
        AppFacts {
            app_id: id.to_string(),
            project_id: "prj_1".to_string(),
            policy: HaPolicy::None,
            stateful: false,
            vcpus: 1,
            memory_mb,
            disk_gb: 10,
            instance_nodes: vec![LOST.to_string()],
            ..AppFacts::default()
        }
    }

    fn stateful(id: &str, policy: HaPolicy, memory_mb: i64) -> AppFacts {
        AppFacts {
            policy,
            stateful: true,
            volume_node: Some(LOST.to_string()),
            ..app(id, memory_mb)
        }
    }

    /// `(app_id, action, node_id, reason)` of each entry, in order.
    fn shape(
        entries: &[Entry],
    ) -> Vec<(&str, CapacityAction, Option<&str>, Option<CapacityReason>)> {
        entries
            .iter()
            .map(|entry| {
                (
                    entry.app.app_id.as_str(),
                    entry.app.action,
                    entry.app.node_id.as_deref(),
                    entry.app.reason,
                )
            })
            .collect()
    }

    #[test]
    fn an_app_the_lost_node_does_not_touch_is_not_listed() {
        let other = AppFacts {
            instance_nodes: vec![A.to_string()],
            ..app("app_other", 512)
        };
        assert!(simulate(LOST, &[room(A, 4, 4096, 100)], &[other]).is_empty());
    }

    #[test]
    fn a_stateless_app_with_another_instance_keeps_it() {
        let web = AppFacts {
            instance_nodes: vec![A.to_string(), LOST.to_string()],
            ..app("app_web", 512)
        };
        // No room anywhere: a kept app needs none.
        let entries = simulate(LOST, &[room(A, 0, 0, 0)], &[web]);
        assert_eq!(
            shape(&entries),
            [("app_web", CapacityAction::Keep, Some(A), None)]
        );
        assert_eq!(summarize(&entries), (true, CapacityShortfall::default()));
    }

    #[test]
    fn a_stateless_app_whose_only_instance_was_there_starts_on_the_node_with_most_memory() {
        let entries = simulate(
            LOST,
            &[room(A, 4, 2048, 100), room(B, 4, 4096, 100)],
            &[app("app_web", 1024)],
        );
        assert_eq!(
            shape(&entries),
            [("app_web", CapacityAction::Start, Some(B), None)]
        );
    }

    #[test]
    fn a_tie_on_memory_goes_to_the_first_node_by_id() {
        let entries = simulate(
            LOST,
            &[room(A, 4, 4096, 100), room(B, 4, 4096, 100)],
            &[app("app_web", 1024)],
        );
        assert_eq!(entries[0].app.node_id.as_deref(), Some(A));
    }

    #[test]
    fn larger_apps_are_placed_first_and_use_up_the_room() {
        // 3 GiB fits once; the larger app is placed first even though its id
        // comes later, and the smaller one is left without room.
        let small = app("app_a", 1024);
        let large = app("app_b", 3072);
        let entries = simulate(LOST, &[room(A, 4, 3072, 100)], &[small, large]);
        assert_eq!(
            shape(&entries),
            [
                ("app_b", CapacityAction::Start, Some(A), None),
                (
                    "app_a",
                    CapacityAction::Start,
                    None,
                    Some(CapacityReason::NoCapacity)
                ),
            ]
        );
        assert_eq!(
            summarize(&entries),
            (
                false,
                CapacityShortfall {
                    cpus: 1,
                    memory_mb: 1024,
                    disk_gb: 10
                }
            )
        );
    }

    #[test]
    fn memory_ties_are_broken_by_vcpus_then_disk() {
        let base = app("app_a", 1024);
        let more_cpus = AppFacts {
            vcpus: 2,
            ..app("app_b", 1024)
        };
        let more_disk = AppFacts {
            disk_gb: 20,
            ..app("app_c", 1024)
        };
        let entries = simulate(
            LOST,
            &[room(A, 64, 65_536, 1000)],
            &[base, more_disk, more_cpus],
        );
        let order: Vec<&str> = entries.iter().map(|e| e.app.app_id.as_str()).collect();
        assert_eq!(order, ["app_b", "app_c", "app_a"]);
    }

    #[test]
    fn every_resource_must_fit() {
        for (name, room) in [
            ("cpus", room(A, 0, 4096, 100)),
            ("memory", room(A, 4, 512, 100)),
            ("disk", room(A, 4, 4096, 5)),
        ] {
            let entries = simulate(LOST, &[room], &[app("app_web", 1024)]);
            assert_eq!(
                entries[0].app.reason,
                Some(CapacityReason::NoCapacity),
                "{name}"
            );
        }
    }

    #[test]
    fn a_room_already_overcommitted_never_fits() {
        // capacity minus allocated may be negative after an operator
        // shrank a node: it has no room, and nothing wraps around.
        let entries = simulate(LOST, &[room(A, -2, -1024, 100)], &[app("app_web", 1)]);
        assert_eq!(entries[0].app.reason, Some(CapacityReason::NoCapacity));
    }

    #[test]
    fn placement_limits_the_nodes() {
        let pinned = AppFacts {
            placement: vec![A.to_string()],
            ..app("app_web", 1024)
        };
        let entries = simulate(LOST, &[room(A, 0, 0, 0), room(B, 8, 8192, 100)], &[pinned]);
        assert_eq!(entries[0].app.reason, Some(CapacityReason::NoCapacity));
    }

    #[test]
    fn the_lost_node_is_never_a_room() {
        let entries = simulate(
            LOST,
            &[room(LOST, 64, 65_536, 1000)],
            &[app("app_web", 1024)],
        );
        assert_eq!(entries[0].app.reason, Some(CapacityReason::NoCapacity));
    }

    #[test]
    fn a_warm_app_starts_only_on_its_warm_node_and_needs_no_disk_there() {
        let db = AppFacts {
            warm_node: Some(A.to_string()),
            warm_copy: true,
            disk_gb: 50,
            ..stateful("app_db", HaPolicy::Warm, 2048)
        };
        // A has no disk left, B has more memory: the warm node all the same.
        let entries = simulate(
            LOST,
            &[room(A, 2, 2048, 0), room(B, 8, 8192, 500)],
            std::slice::from_ref(&db),
        );
        assert_eq!(
            shape(&entries),
            [("app_db", CapacityAction::Failover, Some(A), None)]
        );

        // Without memory on the warm node, the room elsewhere does not help.
        let entries = simulate(LOST, &[room(A, 2, 1024, 0), room(B, 8, 8192, 500)], &[db]);
        assert_eq!(
            shape(&entries),
            [(
                "app_db",
                CapacityAction::Failover,
                None,
                Some(CapacityReason::NoCapacity)
            )]
        );
        assert_eq!(
            summarize(&entries).1,
            CapacityShortfall {
                cpus: 1,
                memory_mb: 2048,
                disk_gb: 0
            }
        );
    }

    #[test]
    fn a_warm_app_whose_warm_node_is_not_connected_has_no_room() {
        // The warm node is not among the rooms: the app needs capacity it
        // cannot get, so `fits` is false.
        let db = AppFacts {
            warm_node: Some(B.to_string()),
            warm_copy: true,
            ..stateful("app_db", HaPolicy::Warm, 1024)
        };
        let entries = simulate(LOST, &[room(A, 8, 8192, 500)], &[db]);
        assert_eq!(entries[0].app.reason, Some(CapacityReason::NoCapacity));
        assert!(!summarize(&entries).0);
    }

    #[test]
    fn a_warm_app_without_a_complete_copy_cannot_move_and_does_not_count() {
        let no_copy = AppFacts {
            warm_node: Some(A.to_string()),
            warm_copy: false,
            ..stateful("app_a", HaPolicy::Warm, 1024)
        };
        let no_node = AppFacts {
            warm_node: None,
            ..stateful("app_b", HaPolicy::Warm, 1024)
        };
        let entries = simulate(LOST, &[room(A, 8, 8192, 500)], &[no_copy, no_node]);
        assert_eq!(
            shape(&entries),
            [
                (
                    "app_a",
                    CapacityAction::Failover,
                    None,
                    Some(CapacityReason::NoWarmCopy)
                ),
                (
                    "app_b",
                    CapacityAction::Failover,
                    None,
                    Some(CapacityReason::NoWarmCopy)
                ),
            ]
        );
        assert_eq!(summarize(&entries), (true, CapacityShortfall::default()));
    }

    #[test]
    fn a_stateful_app_is_restored_from_its_backup_or_cannot_move() {
        let backed = AppFacts {
            has_backup: true,
            ..stateful("app_a", HaPolicy::Cold, 1024)
        };
        let unbacked = stateful("app_b", HaPolicy::None, 1024);
        let entries = simulate(LOST, &[room(A, 8, 8192, 500)], &[backed, unbacked]);
        assert_eq!(
            shape(&entries),
            [
                ("app_a", CapacityAction::Restore, Some(A), None),
                (
                    "app_b",
                    CapacityAction::Restore,
                    None,
                    Some(CapacityReason::NoBackup)
                ),
            ]
        );
        assert_eq!(summarize(&entries), (true, CapacityShortfall::default()));
    }

    #[test]
    fn a_stateful_app_whose_volume_is_there_does_not_keep_an_instance_elsewhere() {
        // Its data is on the lost node, so another instance does not save it.
        let db = AppFacts {
            instance_nodes: vec![A.to_string(), LOST.to_string()],
            has_backup: true,
            ..stateful("app_db", HaPolicy::Cold, 1024)
        };
        let entries = simulate(LOST, &[room(A, 8, 8192, 500)], &[db]);
        assert_eq!(entries[0].app.action, CapacityAction::Restore);
    }

    #[test]
    fn a_stateful_app_whose_volume_is_elsewhere_keeps_its_other_instance() {
        let db = AppFacts {
            instance_nodes: vec![A.to_string(), LOST.to_string()],
            volume_node: Some(A.to_string()),
            ..stateful("app_db", HaPolicy::Cold, 1024)
        };
        let entries = simulate(LOST, &[room(A, 0, 0, 0)], &[db]);
        assert_eq!(
            shape(&entries),
            [("app_db", CapacityAction::Keep, Some(A), None)]
        );
    }

    #[test]
    fn a_volume_alone_on_the_lost_node_lists_the_app() {
        // No VM there (the app is stopped), but its data is.
        let db = AppFacts {
            instance_nodes: Vec::new(),
            has_backup: true,
            ..stateful("app_db", HaPolicy::Cold, 1024)
        };
        let entries = simulate(LOST, &[room(A, 8, 8192, 500)], &[db]);
        assert_eq!(
            shape(&entries),
            [("app_db", CapacityAction::Restore, Some(A), None)]
        );
    }

    #[test]
    fn entries_list_kept_then_placed_then_settled() {
        let kept = AppFacts {
            instance_nodes: vec![A.to_string(), LOST.to_string()],
            ..app("app_a", 512)
        };
        let settled = stateful("app_b", HaPolicy::None, 512);
        let placed = app("app_c", 512);
        let entries = simulate(LOST, &[room(A, 8, 8192, 500)], &[settled, placed, kept]);
        let order: Vec<&str> = entries.iter().map(|e| e.app.app_id.as_str()).collect();
        assert_eq!(order, ["app_a", "app_c", "app_b"]);
    }

    #[test]
    fn summarize_counts_only_the_entries_it_is_given() {
        let entries = simulate(LOST, &[], &[app("app_a", 1024), app("app_b", 2048)]);
        let (fits, missing) = summarize(&entries);
        assert!(!fits);
        assert_eq!(missing.memory_mb, 3072);
        // A caller that may see only app_a is told only about app_a.
        let visible: Vec<Entry> = entries
            .into_iter()
            .filter(|entry| entry.app.app_id == "app_a")
            .collect();
        assert_eq!(summarize(&visible).1.memory_mb, 1024);
        assert_eq!(summarize(&[]), (true, CapacityShortfall::default()));
    }
}
