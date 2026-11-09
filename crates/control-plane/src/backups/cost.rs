// SPDX-License-Identifier: AGPL-3.0-only
//! What each HA policy costs an app (`openapi.yaml` `AppHaCost`,
//! `HaPolicyCost`, ADR 0020).
//!
//! The primary computes these values from rows it already holds: the app's
//! resources and volume, the snapshot method of the volume's node (ADR 0146),
//! the manifests of the app's backups (ADR 0147) and, for `warm`, the warm
//! node's newest report (ADR 0174). Nothing is stored and no column is added
//! (`openapi.yaml` `AppHaCost`, "These are estimates, not reservations").
//!
//! The two rates below stand in for measurements until one exists: the restore
//! rate until the app has a volume a restore wrote, and the copy rate until a
//! `pause_copy` backup reported its pause.

use crate::AppState;
use crate::api::apps::{
    App, AppHaCost, HaCapacity, HaPolicy, HaPolicyCost, HaUnavailableReason, SnapshotMethod,
};
use crate::error::ApiError;
use crate::store::now_rfc3339;
use crate::warm::WarmAvailability;
use quoro_proto::wire::WarmRole;
use sqlx::Row;

/// Bytes per second a restore is estimated at before the first measured one:
/// 50 MiB/s. An estimate, not a measurement.
pub(crate) const DEFAULT_RESTORE_RATE: i64 = 50 * 1024 * 1024;

/// Bytes per second a `pause_copy` snapshot is estimated at before the first
/// measured pause: 80 MiB/s. An estimate, not a measurement.
pub(crate) const DEFAULT_COPY_RATE: i64 = 80 * 1024 * 1024;

/// Seconds a warm copy is estimated to take to start before the primary
/// observed a start of the app (`openapi.yaml` `HaPolicyCost.restore_estimate_s`,
/// ADR 0174). The primary does not time starts yet, so this is the estimate
/// every `warm` entry shows.
pub(crate) const DEFAULT_WARM_START_S: i64 = 10;

/// The app's attached volume, as far as the cost cares.
struct VolumeFacts {
    node_id: String,
    allocated_bytes: Option<i64>,
}

/// One succeeded, unpruned backup of the app, newest first.
struct BackupFacts {
    logical_bytes: i64,
    uploaded_bytes: i64,
    pause_ms: Option<i64>,
}

/// Fills `ha_cost` on every app of `apps` (`openapi.yaml` `App.ha_cost`).
///
/// # Errors
///
/// A database error.
pub(crate) async fn attach(state: &AppState, apps: &mut [App]) -> Result<(), ApiError> {
    for app in apps {
        app.ha_cost = app_ha_cost(state, app).await?;
    }
    Ok(())
}

/// The cost of each HA policy for `app` (`openapi.yaml` `AppHaCost`).
///
/// `None` only while the app has a volume that no node holds yet; a volume row
/// always names its node, so today that case does not arise.
///
/// # Errors
///
/// A database error.
pub(crate) async fn app_ha_cost(
    state: &AppState,
    app: &App,
) -> Result<Option<AppHaCost>, ApiError> {
    let volume = sqlx::query(
        "SELECT node_id, allocated_bytes FROM volumes \
         WHERE app_id = ? AND state IN ('creating', 'ready', 'restoring') \
         ORDER BY id DESC LIMIT 1",
    )
    .bind(&app.id)
    .fetch_optional(state.store.pool())
    .await?
    .map(|row| {
        Ok::<_, ApiError>(VolumeFacts {
            node_id: row.try_get("node_id")?,
            allocated_bytes: row.try_get("allocated_bytes")?,
        })
    })
    .transpose()?;
    let Some(volume) = volume else {
        let warm = warm_cost(state, app).await?;
        return Ok(Some(stateless(app, warm)));
    };
    if volume.node_id.is_empty() {
        return Ok(None);
    }

    let snapshot_method =
        sqlx::query_scalar::<_, Option<String>>("SELECT volume_snapshot FROM nodes WHERE id = ?")
            .bind(&volume.node_id)
            .fetch_optional(state.store.pool())
            .await?
            .flatten()
            .as_deref()
            .and_then(parse_snapshot_method);

    let rows = sqlx::query(
        "SELECT logical_bytes, uploaded_bytes, pause_ms FROM backups \
         WHERE app_id = ? AND source_kind = 'volume' AND status = 'succeeded' \
           AND pruned_at IS NULL ORDER BY id DESC",
    )
    .bind(&app.id)
    .fetch_all(state.store.pool())
    .await?;
    let backups = rows
        .iter()
        .map(|row| {
            Ok::<_, ApiError>(BackupFacts {
                logical_bytes: row.try_get::<Option<i64>, _>("logical_bytes")?.unwrap_or(0),
                uploaded_bytes: row
                    .try_get::<Option<i64>, _>("uploaded_bytes")?
                    .unwrap_or(0),
                pause_ms: row.try_get("pause_ms")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let repository: Option<(String,)> =
        sqlx::query_as("SELECT id FROM backup_repositories ORDER BY id LIMIT 1")
            .fetch_optional(state.store.pool())
            .await?;
    let rate = restore_rate(state, &app.id)
        .await?
        .unwrap_or(DEFAULT_RESTORE_RATE);

    let policies = vec![
        HaPolicyCost {
            policy: HaPolicy::None,
            available: true,
            unavailable_reason: None,
            reserved: zero_capacity(),
            storage_bytes: Some(0),
            restore_estimate_s: None,
            pause_estimate_ms: None,
        },
        cold_cost(
            &backups,
            &volume,
            snapshot_method,
            repository.is_some(),
            rate,
        ),
        warm_cost(state, app).await?,
        unavailable_cost(HaPolicy::Hot, &app.resources),
    ];
    Ok(Some(AppHaCost {
        snapshot_method,
        policies,
        computed_at: now_rfc3339(),
    }))
}

/// The cost of every policy for an app with no attached volume; `warm` is its
/// [`warm_cost`].
fn stateless(app: &App, warm: HaPolicyCost) -> AppHaCost {
    AppHaCost {
        snapshot_method: None,
        policies: vec![
            HaPolicyCost {
                policy: HaPolicy::None,
                available: true,
                unavailable_reason: None,
                reserved: zero_capacity(),
                storage_bytes: Some(0),
                restore_estimate_s: None,
                pause_estimate_ms: None,
            },
            HaPolicyCost {
                policy: HaPolicy::Cold,
                available: false,
                unavailable_reason: Some(HaUnavailableReason::Stateless),
                reserved: zero_capacity(),
                storage_bytes: Some(0),
                restore_estimate_s: None,
                pause_estimate_ms: None,
            },
            warm,
            unavailable_cost(HaPolicy::Hot, &app.resources),
        ],
        computed_at: now_rfc3339(),
    }
}

/// The `cold` entry: backups in object storage, the last restore's rate and
/// the pause a snapshot costs.
fn cold_cost(
    backups: &[BackupFacts],
    volume: &VolumeFacts,
    method: Option<SnapshotMethod>,
    has_repository: bool,
    rate: i64,
) -> HaPolicyCost {
    let available = has_repository;
    let unavailable_reason = if has_repository {
        None
    } else {
        Some(HaUnavailableReason::NoRepository)
    };
    let storage_bytes = if backups.is_empty() {
        volume.allocated_bytes.unwrap_or(0)
    } else {
        backups.iter().map(|backup| backup.uploaded_bytes).sum()
    };
    let restore_estimate_s = backups
        .first()
        .map(|latest| seconds_at_least(latest.logical_bytes, rate));
    let pause_estimate_ms = match method.unwrap_or(SnapshotMethod::PauseCopy) {
        SnapshotMethod::Reflink => Some(0),
        SnapshotMethod::PauseCopy => Some(
            backups
                .first()
                .and_then(|backup| backup.pause_ms)
                .unwrap_or_else(|| {
                    let allocated = volume.allocated_bytes.unwrap_or(0);
                    let rate = DEFAULT_COPY_RATE.max(1);
                    allocated.saturating_mul(1000).saturating_add(rate - 1) / rate
                }),
        ),
    };
    HaPolicyCost {
        policy: HaPolicy::Cold,
        available,
        unavailable_reason,
        reserved: zero_capacity(),
        storage_bytes: Some(storage_bytes.max(0)),
        restore_estimate_s,
        pause_estimate_ms,
    }
}

/// The `warm` entry (`openapi.yaml` `HaPolicyCost`, ADR 0174). A build that
/// refuses `warm` (ADR 0277) prices it as not in this release, as before.
/// Otherwise:
/// - `available` and `unavailable_reason` as `updateApp` decides
///   ([`crate::warm::warm_availability`]);
/// - `reserved` is disk only: the volume's size plus the app's root disk on
///   the warm node; nothing for an app with no volume;
/// - `storage_bytes` is the allocated bytes of the copy the warm node last
///   reported, or the volume's allocated bytes before that;
/// - `restore_estimate_s` is [`DEFAULT_WARM_START_S`], and
///   `pause_estimate_ms` is 0: a warm volume is only snapshotted with
///   `reflink` (ADR 0172); both are null with no volume.
async fn warm_cost(state: &AppState, app: &App) -> Result<HaPolicyCost, ApiError> {
    if !crate::DEV_WARM_POLICY {
        return Ok(unavailable_cost(HaPolicy::Warm, &app.resources));
    }
    let mut conn = state.store.pool().acquire().await?;
    let (available, unavailable_reason, volume) = match crate::warm::warm_availability(
        &mut conn,
        &app.id,
        app.source.is_some(),
        crate::DEV_WARM_POLICY,
    )
    .await?
    {
        WarmAvailability::Available { volume, .. } => (true, None, Some(volume)),
        WarmAvailability::Unavailable { reason, volume } => (false, Some(reason), volume),
    };
    let Some(volume) = volume else {
        return Ok(HaPolicyCost {
            policy: HaPolicy::Warm,
            available,
            unavailable_reason,
            reserved: zero_capacity(),
            storage_bytes: Some(0),
            restore_estimate_s: None,
            pause_estimate_ms: None,
        });
    };
    let warm_node: Option<Option<String>> =
        sqlx::query_scalar("SELECT warm_node_id FROM apps WHERE id = ? AND ha_policy = 'warm'")
            .bind(&app.id)
            .fetch_optional(&mut *conn)
            .await?;
    let reported = warm_node.flatten().and_then(|node_id| {
        state
            .hub
            .warm_reports()
            .get(&node_id)?
            .iter()
            .find_map(|entry| {
                (entry.role == WarmRole::Target
                    && entry.volume_id == volume.id
                    && entry.app_id == app.id)
                    .then_some(entry.allocated_bytes)
                    .flatten()
            })
    });
    let storage_bytes = reported
        .and_then(|bytes| i64::try_from(bytes).ok())
        .or(volume.allocated_bytes)
        .unwrap_or(0);
    Ok(HaPolicyCost {
        policy: HaPolicy::Warm,
        available,
        unavailable_reason,
        reserved: HaCapacity {
            vcpus: 0,
            memory_mb: 0,
            disk_gb: volume.size_gb.saturating_add(app.resources.disk_gb),
        },
        storage_bytes: Some(storage_bytes.max(0)),
        restore_estimate_s: Some(DEFAULT_WARM_START_S),
        pause_estimate_ms: Some(0),
    })
}

/// A policy that is not available in this release: `hot`, and `warm` in a
/// build that refuses it (ADR 0277).
fn unavailable_cost(policy: HaPolicy, resources: &crate::api::apps::AppResources) -> HaPolicyCost {
    HaPolicyCost {
        policy,
        available: false,
        unavailable_reason: Some(HaUnavailableReason::NotInThisRelease),
        reserved: HaCapacity {
            vcpus: resources.vcpus,
            memory_mb: resources.memory_mb,
            disk_gb: resources.disk_gb,
        },
        storage_bytes: None,
        restore_estimate_s: None,
        pause_estimate_ms: None,
    }
}

fn zero_capacity() -> HaCapacity {
    HaCapacity {
        vcpus: 0,
        memory_mb: 0,
        disk_gb: 0,
    }
}

/// The median download rate, in bytes per second, of the restores this app
/// already completed: each restored volume's backup size over the time the
/// restore took. `None` before the first one.
async fn restore_rate(state: &AppState, app_id: &str) -> Result<Option<i64>, ApiError> {
    let rows = sqlx::query(
        "SELECT b.logical_bytes, v.created_at, v.updated_at FROM volumes v \
         JOIN backups b ON b.id = v.restored_from_backup_id \
         WHERE v.app_id = ? AND v.restored_from_backup_id IS NOT NULL \
           AND v.state IN ('ready', 'detached', 'deleting')",
    )
    .bind(app_id)
    .fetch_all(state.store.pool())
    .await?;
    let mut rates = Vec::new();
    for row in &rows {
        let bytes: Option<i64> = row.try_get("logical_bytes")?;
        let created: String = row.try_get("created_at")?;
        let updated: String = row.try_get("updated_at")?;
        let (Some(bytes), Some(created), Some(updated)) = (
            bytes,
            super::parse_time(&created),
            super::parse_time(&updated),
        ) else {
            continue;
        };
        let seconds = (updated - created).as_seconds_f64();
        if bytes > 0 && seconds > 0.0 {
            rates.push(bytes as f64 / seconds);
        }
    }
    if rates.is_empty() {
        return Ok(None);
    }
    rates.sort_by(f64::total_cmp);
    let middle = rates.len() / 2;
    let median = if rates.len() % 2 == 0 {
        (rates[middle - 1] + rates[middle]) / 2.0
    } else {
        rates[middle]
    };
    Ok(Some((median.round() as i64).max(1)))
}

/// `ceil(bytes / rate)`, never negative.
fn seconds_at_least(bytes: i64, rate: i64) -> i64 {
    let rate = rate.max(1);
    bytes.max(0).saturating_add(rate - 1) / rate
}

/// The stored and wire form of a snapshot method.
fn parse_snapshot_method(value: &str) -> Option<SnapshotMethod> {
    match value {
        "reflink" => Some(SnapshotMethod::Reflink),
        "pause_copy" => Some(SnapshotMethod::PauseCopy),
        _ => None,
    }
}
