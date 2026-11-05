// SPDX-License-Identifier: AGPL-3.0-only
//! Image inventory objects and their wire parts (`node-agent-protocol.md`
//! sections 5.26 and 6.11; ADR 0150).
//!
//! [`InventoryObject`] is the object whose RFC 8785 JSON bytes are hashed into
//! `inventory_sha256`: every field of an `image_inventory` body other than
//! `inventory_id`, `part`, `parts` and `inventory_sha256` itself, with all
//! packages (section 6.11). [`split`] cuts one inventory into up to eight wire
//! parts and [`join`] reassembles and verifies them.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::jcs;
use crate::wire::{ImageInventoryBody, InventoryOs, InventoryPackageWire};

/// Most parts one inventory may be split into (section 5.26).
pub const MAX_PARTS: usize = 8;
/// Most packages one inventory may hold (section 6.11).
pub const MAX_PACKAGES: u64 = 20_000;

/// The object whose canonical JSON is hashed (section 6.11).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryObject {
    /// Manifest digest of the prepared image.
    pub image_digest: String,
    /// When the engine built the inventory.
    pub inventoried_at: String,
    /// `ID` and `VERSION_ID` of the image's os-release, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os: Option<InventoryOs>,
    /// OSV ecosystems the inventory covers.
    pub ecosystems: Vec<String>,
    /// Kinds of content seen and not read.
    pub not_inventoried: Vec<String>,
    /// Packages in the whole inventory.
    pub package_count: u64,
    /// Every package of the inventory, in the engine's order.
    pub packages: Vec<InventoryPackageWire>,
}

/// Why an inventory could not be split into eight parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooLarge;

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the inventory does not fit in eight parts")
    }
}

impl std::error::Error for TooLarge {}

/// The RFC 8785 bytes of the inventory object (section 6.11).
///
/// Every number in the object is a package count far below 2^53, so the
/// object always has a canonical form.
#[must_use]
pub fn canonical_bytes(object: &InventoryObject) -> Vec<u8> {
    let value = serde_json::to_value(object).expect("an inventory object serializes to JSON");
    jcs::canonicalize(&value)
        .expect("an inventory object is canonical JSON")
        .into_bytes()
}

/// Lowercase hex SHA-256 of [`canonical_bytes`] (section 6.11).
#[must_use]
pub fn sha256_hex(object: &InventoryObject) -> String {
    hex(&Sha256::digest(canonical_bytes(object)))
}

/// Splits an inventory into wire parts whose serialized bodies each fit
/// `max_part_bytes` (section 5.26).
///
/// Packages keep their order and are packed greedily. The caller passes a
/// budget below [`crate::wire::MAX_MESSAGE_BYTES`] so the envelope also fits.
///
/// # Errors
///
/// [`TooLarge`] when one package alone does not fit, or when the inventory
/// needs more than [`MAX_PARTS`] parts.
pub fn split(
    object: &InventoryObject,
    inventory_sha256: &str,
    inventory_id: &str,
    max_part_bytes: usize,
) -> Result<Vec<ImageInventoryBody>, TooLarge> {
    let total = object.packages.len();
    let mut slices: Vec<(usize, usize)> = Vec::new();
    let mut start = 0usize;
    while start < total {
        let part = slices.len() + 1;
        if part > MAX_PARTS {
            return Err(TooLarge);
        }
        // Find the largest end whose body fits. `parts` is measured with its
        // maximum, 8; every value 1..=8 has the same one-digit width, so the
        // measurement is an upper bound for the final value too.
        let mut low = start + 1;
        let mut high = total;
        let mut best = None;
        while low <= high {
            let mid = low + (high - low) / 2;
            let body = body(
                object,
                inventory_sha256,
                inventory_id,
                part,
                MAX_PARTS,
                start,
                mid,
            );
            if serialized_len(&body) <= max_part_bytes {
                best = Some(mid);
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        let Some(end) = best else {
            return Err(TooLarge);
        };
        slices.push((start, end));
        start = end;
    }
    // An inventory with no packages still travels as one empty part.
    if slices.is_empty() {
        slices.push((0, 0));
    }
    let parts = slices.len();
    let mut bodies = Vec::with_capacity(parts);
    for (index, (start, end)) in slices.into_iter().enumerate() {
        let body = body(
            object,
            inventory_sha256,
            inventory_id,
            index + 1,
            parts,
            start,
            end,
        );
        // The final `parts` value has the same one-digit width the packing
        // measured; this guards against a future change to that assumption.
        if serialized_len(&body) > max_part_bytes {
            return Err(TooLarge);
        }
        bodies.push(body);
    }
    Ok(bodies)
}

/// Reassembles and verifies the parts of one inventory (section 5.26).
///
/// Returns `None` when the parts disagree on their shared fields, do not cover
/// `1..=parts` exactly once, do not add up to `package_count`, or do not hash
/// to `inventory_sha256`.
#[must_use]
pub fn join(parts: &[ImageInventoryBody]) -> Option<InventoryObject> {
    let first = parts.first()?;
    // The parts come from a node: nothing below may trust their counts.
    if first.package_count > MAX_PACKAGES {
        return None;
    }
    let count = first.parts;
    if count == 0 || count as usize != parts.len() || count as usize > MAX_PARTS {
        return None;
    }
    let mut ordered: Vec<&ImageInventoryBody> = parts.iter().collect();
    ordered.sort_by_key(|part| part.part);
    for (index, part) in ordered.iter().enumerate() {
        if part.part as usize != index + 1 {
            return None;
        }
        if part.image_digest != first.image_digest
            || part.inventory_id != first.inventory_id
            || part.inventory_sha256 != first.inventory_sha256
            || part.parts != count
            || part.package_count != first.package_count
            || part.inventoried_at != first.inventoried_at
            || part.os != first.os
            || part.ecosystems != first.ecosystems
            || part.not_inventoried != first.not_inventoried
        {
            return None;
        }
    }
    let total: usize = ordered.iter().map(|part| part.packages.len()).sum();
    if total as u64 != first.package_count {
        return None;
    }
    let mut packages = Vec::with_capacity(total);
    for part in ordered {
        packages.extend(part.packages.iter().cloned());
    }
    if packages.len() as u64 != first.package_count {
        return None;
    }
    let object = InventoryObject {
        image_digest: first.image_digest.clone(),
        inventoried_at: first.inventoried_at.clone(),
        os: first.os.clone(),
        ecosystems: first.ecosystems.clone(),
        not_inventoried: first.not_inventoried.clone(),
        package_count: first.package_count,
        packages,
    };
    (sha256_hex(&object) == first.inventory_sha256).then_some(object)
}

fn body(
    object: &InventoryObject,
    inventory_sha256: &str,
    inventory_id: &str,
    part: usize,
    parts: usize,
    start: usize,
    end: usize,
) -> ImageInventoryBody {
    ImageInventoryBody {
        image_digest: object.image_digest.clone(),
        inventory_id: inventory_id.to_owned(),
        part: u32::try_from(part).unwrap_or(u32::MAX),
        parts: u32::try_from(parts).unwrap_or(u32::MAX),
        inventory_sha256: inventory_sha256.to_owned(),
        inventoried_at: object.inventoried_at.clone(),
        os: object.os.clone(),
        ecosystems: object.ecosystems.clone(),
        not_inventoried: object.not_inventoried.clone(),
        package_count: object.package_count,
        packages: object.packages[start..end].to_vec(),
    }
}

fn serialized_len(body: &ImageInventoryBody) -> usize {
    serde_json::to_vec(body).map_or(usize::MAX, |bytes| bytes.len())
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_refuses_a_package_count_over_the_bound() {
        let object = object(1);
        let sha = sha256_hex(&object);
        let mut parts = split(&object, &sha, "msg_x", 1 << 20).expect("split");
        parts[0].package_count = u64::MAX;
        assert!(join(&parts).is_none());
        parts[0].package_count = MAX_PACKAGES + 1;
        assert!(join(&parts).is_none());
    }
    use crate::wire::{MAX_MESSAGE_BYTES, NodeMessage};

    fn package(index: usize) -> InventoryPackageWire {
        InventoryPackageWire {
            ecosystem: "Debian:13".to_owned(),
            name: format!("package-{index}"),
            source_name: index.is_multiple_of(2).then(|| format!("source-{index}")),
            version: format!("1.{index}.0-1"),
            purl: format!("pkg:deb/debian/package-{index}@1.{index}.0-1"),
            path: index
                .is_multiple_of(3)
                .then(|| format!("usr/lib/package-{index}/file")),
        }
    }

    fn object(count: usize) -> InventoryObject {
        InventoryObject {
            image_digest: "sha256:aaaa".to_owned(),
            inventoried_at: "2026-11-05T06:09:41Z".to_owned(),
            os: Some(InventoryOs {
                id: "debian".to_owned(),
                version_id: Some("13".to_owned()),
            }),
            ecosystems: vec!["Debian:13".to_owned(), "npm".to_owned()],
            not_inventoried: vec!["static_binary".to_owned()],
            package_count: count as u64,
            packages: (0..count).map(package).collect(),
        }
    }

    fn split_object(object: &InventoryObject, budget: usize) -> Vec<ImageInventoryBody> {
        let sha = sha256_hex(object);
        split(object, &sha, "msg_inventory", budget).unwrap()
    }

    #[test]
    fn a_single_part_round_trips() {
        let object = object(2);
        let sha = sha256_hex(&object);
        let parts = split(
            &object,
            &sha,
            "msg_inventory",
            MAX_MESSAGE_BYTES - 16 * 1024,
        )
        .unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part, 1);
        assert_eq!(parts[0].parts, 1);
        assert_eq!(parts[0].inventory_id, "msg_inventory");
        assert_eq!(join(&parts).unwrap(), object);
    }

    #[test]
    fn an_empty_inventory_travels_as_one_part() {
        let object = object(0);
        let parts = split_object(&object, MAX_MESSAGE_BYTES - 16 * 1024);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].packages.is_empty());
        assert_eq!(join(&parts).unwrap(), object);
    }

    #[test]
    fn a_four_mib_inventory_splits_into_bounded_frames() {
        // Long names make the package array large without needing 20 000 rows.
        let mut object = object(4000);
        for (index, package) in object.packages.iter_mut().enumerate() {
            package.purl = format!("{}{index}", package.purl);
            package.path = Some("x".repeat(900));
        }
        let budget = MAX_MESSAGE_BYTES - 16 * 1024;
        let parts = split_object(&object, budget);
        assert!(parts.len() > 1, "the inventory needs several parts");
        assert!(parts.len() <= MAX_PARTS);
        for (index, part) in parts.iter().enumerate() {
            assert_eq!(part.part as usize, index + 1);
            assert_eq!(part.parts as usize, parts.len());
            assert!(serde_json::to_vec(part).unwrap().len() <= budget);
            // The whole message, envelope included, stays below the frame cap.
            let frame = NodeMessage::ImageInventory(part.clone())
                .to_frame(&format!("msg_part_{}", part.part), None)
                .to_text();
            assert!(frame.len() < MAX_MESSAGE_BYTES);
        }
        assert_eq!(join(&parts).unwrap(), object);
    }

    #[test]
    fn the_sha_is_stable_whatever_the_key_order() {
        let object = object(2);
        let first = serde_json::to_string(&object).unwrap();
        let value: serde_json::Value = serde_json::from_str(&first).unwrap();
        let mut reordered = serde_json::Map::new();
        for key in [
            "packages",
            "package_count",
            "not_inventoried",
            "ecosystems",
            "os",
            "inventoried_at",
            "image_digest",
        ] {
            reordered.insert(key.to_owned(), value[key].clone());
        }
        let reordered: InventoryObject =
            serde_json::from_value(serde_json::Value::Object(reordered)).unwrap();
        assert_eq!(canonical_bytes(&object), canonical_bytes(&reordered));
        assert_eq!(sha256_hex(&object), sha256_hex(&reordered));
    }

    #[test]
    fn join_rejects_an_inconsistent_sha() {
        let object = object(3);
        let mut parts = split_object(&object, MAX_MESSAGE_BYTES - 16 * 1024);
        parts[0].inventory_sha256 = "00".repeat(32);
        assert!(join(&parts).is_none());
    }

    #[test]
    fn join_rejects_missing_and_duplicate_parts() {
        let object = object(4000);
        let parts = split_object(&object, 256 * 1024);
        assert!(parts.len() >= 3);
        let mut missing = parts.clone();
        missing.remove(1);
        assert!(join(&missing).is_none());
        let mut duplicate = parts.clone();
        duplicate[1].part = 1;
        assert!(join(&duplicate).is_none());
    }

    #[test]
    fn join_rejects_mismatched_metadata() {
        let object = object(4000);
        let parts = split_object(&object, 256 * 1024);
        assert!(parts.len() >= 2);
        let mut mismatched = parts.clone();
        mismatched[1].image_digest = "sha256:bbbb".to_owned();
        assert!(join(&mismatched).is_none());
        let mut mismatched = parts.clone();
        mismatched[1].inventory_id = "msg_other".to_owned();
        assert!(join(&mismatched).is_none());
        let mut mismatched = parts.clone();
        mismatched[1].package_count += 1;
        assert!(join(&mismatched).is_none());
    }
}
