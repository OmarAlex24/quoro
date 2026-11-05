// SPDX-License-Identifier: AGPL-3.0-only
//! What is known about the packages of an app's current image (ADR 0150).
//!
//! [`inventory_status`] answers the `ImageInventoryStatus` of one image digest
//! and [`data_sources_for`] the data sources a report on it used. Both read the
//! primary's local cache and its `osv_mirror` rows; neither builds a URL or
//! touches the network.

use crate::AppState;
use crate::api::vulnerabilities::{
    ImageInventoryStatus, InventoryStatus, VulnerabilityDataSource, VulnerabilitySource,
};
use crate::error::ApiError;
use crate::vulns::{ecosystem, kev};
use quoro_proto::inventory::InventoryObject;
use quoro_proto::wire::InventoryOs;
use sqlx::Row;

/// `debian 13`, or just the id when the image names no release.
fn os_name(os: &InventoryOs) -> String {
    match &os.version_id {
        Some(version) => format!("{} {version}", os.id),
        None => os.id.clone(),
    }
}

/// One image's inventory, when the cache holds it.
async fn inventory(state: &AppState, digest: &str) -> Result<Option<InventoryObject>, ApiError> {
    let Some((_sha256, json)) = state.vulns.cache().inventory(digest).await? else {
        return Ok(None);
    };
    let object = serde_json::from_str(&json).map_err(|error| {
        ApiError::internal(&format!(
            "stored inventory {digest} is not readable: {error}"
        ))
    })?;
    Ok(Some(object))
}

/// What is known about the packages of one image (`ImageInventoryStatus`).
///
/// `None` is an app that runs no image, `pending` an image whose inventory has
/// not arrived, `not_inventoried` an inventory with no packages,
/// `pending_data` an inventory that needs an ecosystem the primary does not
/// hold, and `inventoried` everything else.
///
/// # Errors
///
/// A database error, or a stored inventory that is not readable.
pub async fn inventory_status(
    state: &AppState,
    image_digest: Option<&str>,
) -> Result<ImageInventoryStatus, ApiError> {
    let Some(digest) = image_digest else {
        return Ok(ImageInventoryStatus {
            status: InventoryStatus::NoImage,
            ..ImageInventoryStatus::default()
        });
    };
    let Some(object) = inventory(state, digest).await? else {
        return Ok(ImageInventoryStatus {
            status: InventoryStatus::Pending,
            image_digest: Some(digest.to_string()),
            ..ImageInventoryStatus::default()
        });
    };
    let mut status = InventoryStatus::Inventoried;
    if object.package_count == 0 {
        status = InventoryStatus::NotInventoried;
    } else {
        for name in &object.ecosystems {
            if let Some(bucket) = ecosystem::bucket_for(name)
                && state.vulns.cache().bucket(bucket).await?.is_none()
            {
                status = InventoryStatus::PendingData;
                break;
            }
        }
    }
    Ok(ImageInventoryStatus {
        status,
        image_digest: Some(digest.to_string()),
        os: object.os.as_ref().map(os_name),
        package_count: Some(i64::try_from(object.package_count).unwrap_or(i64::MAX)),
        ecosystems: Some(object.ecosystems.clone()),
        not_inventoried: Some(object.not_inventoried.clone()),
        inventoried_at: Some(object.inventoried_at.clone()),
    })
}

/// The OSV buckets an image's inventory needs that the primary holds data for,
/// plus KEV: the `data` list of the app report.
///
/// An ecosystem with no OSV bucket is left out: no data source can ever hold
/// it. Rows are ordered by ecosystem.
///
/// # Errors
///
/// A database error, or a stored inventory that is not readable.
pub async fn data_sources_for(
    state: &AppState,
    image_digest: Option<&str>,
) -> Result<Vec<VulnerabilityDataSource>, ApiError> {
    let mut buckets: Vec<&str> = Vec::new();
    if let Some(digest) = image_digest
        && let Some(object) = inventory(state, digest).await?
    {
        for name in &object.ecosystems {
            if let Some(bucket) = ecosystem::bucket_for(name)
                && !buckets.contains(&bucket)
            {
                buckets.push(bucket);
            }
        }
    }
    buckets.push(kev::KEV);
    let placeholders = vec!["?"; buckets.len()].join(", ");
    let sql = format!(
        "SELECT ecosystem, source, data_modified_at, refreshed_at, last_error \
         FROM osv_mirror WHERE ecosystem IN ({placeholders}) ORDER BY ecosystem"
    );
    let mut statement = sqlx::query(&sql);
    for bucket in &buckets {
        statement = statement.bind(*bucket);
    }
    let rows = statement.fetch_all(state.store.pool()).await?;
    rows.iter().map(source_from_row).collect()
}

/// Maps one `osv_mirror` row to the contract shape.
fn source_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<VulnerabilityDataSource, ApiError> {
    let source: String = row.try_get("source")?;
    let source = VulnerabilitySource::parse(&source)
        .ok_or_else(|| ApiError::internal(&format!("unknown osv_mirror source {source:?}")))?;
    Ok(VulnerabilityDataSource {
        ecosystem: row.try_get("ecosystem")?,
        source,
        data_modified_at: row.try_get("data_modified_at")?,
        refreshed_at: row.try_get("refreshed_at")?,
        last_error: row.try_get("last_error")?,
    })
}
