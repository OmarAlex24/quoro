// SPDX-License-Identifier: AGPL-3.0-only
//! Storing the image inventories nodes send.
//!
//! An inventory travels in up to eight `image_inventory` parts (section 5.26).
//! [`Assembler`] keeps the parts of one send until all of them arrived, then
//! [`join`] checks them against each other and the hash. A part that arrives
//! twice is ignored, a send whose parts disagree is dropped, at most two
//! incomplete sends are kept per session and a send older than ten minutes is
//! dropped.
//!
//! [`store`] writes a complete inventory into the cache, tells the mirror
//! which ecosystems its packages need and wakes the matcher.

use crate::vulns::Vulns;
use crate::vulns::findings;
use quoro_proto::inventory::{InventoryObject, canonical_bytes, join};
use quoro_proto::wire::ImageInventoryBody;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Most incomplete sends one session may hold at once.
const MAX_INCOMPLETE: usize = 2;

/// How long an incomplete send is kept before it is dropped.
const SEND_AGE: Duration = Duration::from_secs(10 * 60);

/// One complete inventory, with the `inventory_sha256` its parts carried.
#[derive(Clone, Debug)]
pub struct Assembled {
    /// The joined object whose canonical bytes were hashed.
    pub object: InventoryObject,
    /// The hex SHA-256 the parts named and [`join`] verified.
    pub sha256: String,
}

/// The parts of the sends a session is still assembling.
#[derive(Default)]
pub struct Assembler {
    sends: HashMap<String, Send>,
}

/// One incomplete send: its parts by 1-based number, plus the shared fields
/// that all of them must agree on.
struct Send {
    slots: Vec<Option<ImageInventoryBody>>,
    parts: u32,
    image_digest: String,
    sha256: String,
    started: Instant,
}

impl Send {
    fn complete(&self) -> bool {
        self.slots.iter().all(Option::is_some)
    }
}

impl Assembler {
    /// A new assembler with no send in progress.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one part, at the current instant.
    ///
    /// Returns the complete inventory when this part was the last one missing.
    pub fn push(&mut self, body: ImageInventoryBody) -> Option<Assembled> {
        self.push_at(body, Instant::now())
    }

    /// [`Assembler::push`] with an explicit clock, so a test can age a send.
    pub fn push_at(&mut self, body: ImageInventoryBody, now: Instant) -> Option<Assembled> {
        self.prune(now);

        // A send with an impossible part count can never join.
        if body.parts == 0 || usize::try_from(body.parts).map_or(true, |parts| parts > 8) {
            self.sends.remove(&body.inventory_id);
            return None;
        }
        // A part outside `1..=parts` is malformed; the whole send is dropped.
        if body.part == 0 || body.part > body.parts {
            self.sends.remove(&body.inventory_id);
            return None;
        }
        let Some(slot) = usize::try_from(body.part - 1).ok() else {
            self.sends.remove(&body.inventory_id);
            return None;
        };
        let id = body.inventory_id.clone();

        // Parts of one send must agree; a disagreement drops the send.
        if let Some(send) = self.sends.get(&id)
            && (send.parts != body.parts
                || send.image_digest != body.image_digest
                || send.sha256 != body.inventory_sha256)
        {
            self.sends.remove(&id);
            return None;
        }

        if let Some(send) = self.sends.get_mut(&id) {
            if slot >= send.slots.len() {
                self.sends.remove(&id);
                return None;
            }
            // A part that arrives twice is ignored: the first copy stands.
            if send.slots[slot].is_some() {
                return None;
            }
            send.slots[slot] = Some(body);
            if send.complete() {
                return self.finish(&id);
            }
            return None;
        }

        // A third send starts: the oldest incomplete send is dropped.
        if self.sends.len() >= MAX_INCOMPLETE
            && let Some(oldest) = self
                .sends
                .iter()
                .min_by_key(|(_, send)| send.started)
                .map(|(id, _)| id.clone())
        {
            self.sends.remove(&oldest);
        }
        let mut slots = Vec::with_capacity(usize::try_from(body.parts).unwrap_or(0));
        slots.resize_with(usize::try_from(body.parts).unwrap_or(0), || None);
        let sha256 = body.inventory_sha256.clone();
        let image_digest = body.image_digest.clone();
        let parts = body.parts;
        slots[slot] = Some(body);
        let send = Send {
            slots,
            parts,
            image_digest,
            sha256,
            started: now,
        };
        let complete = send.complete();
        self.sends.insert(id.clone(), send);
        if complete { self.finish(&id) } else { None }
    }

    /// Joins the completed send `id` and forgets it.
    fn finish(&mut self, id: &str) -> Option<Assembled> {
        let send = self.sends.remove(id)?;
        let parts: Vec<ImageInventoryBody> = send.slots.into_iter().flatten().collect();
        match join(&parts) {
            Some(object) => Some(Assembled {
                object,
                sha256: send.sha256,
            }),
            None => {
                tracing::debug!(inventory_id = id, "image inventory parts do not join");
                None
            }
        }
    }

    /// Drops every send older than [`SEND_AGE`].
    fn prune(&mut self, now: Instant) {
        self.sends
            .retain(|_, send| now.saturating_duration_since(send.started) < SEND_AGE);
    }
}

/// Writes one joined inventory into the cache and wakes the matcher.
///
/// The packages' ecosystems are handed to the mirror, deduplicated, so the
/// data the matcher needs is fetched even when the inventory's own
/// `ecosystems` list does not name them all.
///
/// # Errors
///
/// A message for the log when the cache cannot be written.
pub async fn store(vulns: &Vulns, object: &InventoryObject, sha256: &str) -> Result<(), String> {
    let json = String::from_utf8(canonical_bytes(object))
        .map_err(|error| format!("an inventory object is UTF-8: {error}"))?;
    vulns
        .cache()
        .put_inventory(&object.image_digest, sha256, &json)
        .await
        .map_err(|error| format!("cannot write the inventory cache: {error}"))?;
    let mut wanted = HashSet::new();
    for package in &object.packages {
        if wanted.insert(package.ecosystem.as_str()) {
            vulns.want(&package.ecosystem);
        }
    }
    findings::wake_matcher();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quoro_proto::inventory::{sha256_hex, split};
    use quoro_proto::wire::InventoryPackageWire;

    fn object(count: usize) -> InventoryObject {
        InventoryObject {
            image_digest: "sha256:aaaa".to_owned(),
            inventoried_at: "2026-11-05T06:09:41Z".to_owned(),
            os: None,
            ecosystems: vec!["Debian:13".to_owned()],
            not_inventoried: Vec::new(),
            package_count: count as u64,
            packages: (0..count)
                .map(|index| InventoryPackageWire {
                    ecosystem: "Debian:13".to_owned(),
                    name: format!("package-{index}"),
                    source_name: None,
                    version: "1.0".to_owned(),
                    purl: format!("pkg:deb/debian/package-{index}@1.0"),
                    path: None,
                })
                .collect(),
        }
    }

    fn parts(
        object: &InventoryObject,
        inventory_id: &str,
        budget: usize,
    ) -> Vec<ImageInventoryBody> {
        split(object, &sha256_hex(object), inventory_id, budget).expect("split")
    }

    fn sample() -> (InventoryObject, Vec<ImageInventoryBody>) {
        let object = object(4000);
        let parts = parts(&object, "msg_inventory", 128 * 1024);
        assert!(parts.len() >= 3, "the sample needs several parts");
        (object, parts)
    }

    #[test]
    fn parts_join_in_any_order() {
        let (object, parts) = sample();
        let sha = sha256_hex(&object);
        let mut assembler = Assembler::new();
        let last = parts.len() - 1;
        let mut assembled = assembler.push(parts[last].clone());
        assert!(assembled.is_none());
        for part in parts.iter().take(last).rev() {
            assembled = assembler.push(part.clone());
        }
        let assembled = assembled.expect("the last part completed the send");
        assert_eq!(assembled.object, object);
        assert_eq!(assembled.sha256, sha);
    }

    #[test]
    fn a_duplicate_part_is_ignored() {
        let (object, parts) = sample();
        let mut assembler = Assembler::new();
        assert!(assembler.push(parts[0].clone()).is_none());
        assert!(
            assembler.push(parts[0].clone()).is_none(),
            "a repeat is ignored"
        );
        let mut assembled = None;
        for part in parts.iter().skip(1) {
            assembled = assembler.push(part.clone());
        }
        let assembled = assembled.expect("the duplicate did not lose a part");
        assert_eq!(assembled.object, object);
    }

    #[test]
    fn mismatched_parts_are_dropped() {
        let (_, parts) = sample();
        let mut assembler = Assembler::new();
        assert!(assembler.push(parts[0].clone()).is_none());
        let mut other = parts[1].clone();
        other.inventory_sha256 = "00".repeat(32);
        assert!(assembler.push(other).is_none());
        // The send was dropped: putting the original part 1 starts a new send.
        assert!(assembler.push(parts[0].clone()).is_none());
    }

    #[test]
    fn a_third_send_evicts_the_oldest() {
        let (_, parts) = sample();
        let mut assembler = Assembler::new();
        let now = Instant::now();
        let mut first = parts[0].clone();
        first.inventory_id = "msg_first".to_owned();
        let mut second = parts[0].clone();
        second.inventory_id = "msg_second".to_owned();
        let mut third = parts[0].clone();
        third.inventory_id = "msg_third".to_owned();
        assembler.push_at(first.clone(), now);
        assembler.push_at(second.clone(), now + Duration::from_secs(1));
        assembler.push_at(third, now + Duration::from_secs(2));
        assert_eq!(assembler.sends.len(), 2);
        assert!(
            !assembler.sends.contains_key("msg_first"),
            "the oldest send was dropped"
        );
        assert!(assembler.sends.contains_key("msg_second"));
        assert!(assembler.sends.contains_key("msg_third"));
        // A part of the dropped send starts a new send, which evicts the next
        // oldest: nothing of the first send joins.
        assert!(
            assembler
                .push_at(first, now + Duration::from_secs(3))
                .is_none()
        );
        assert!(!assembler.sends.contains_key("msg_second"));
    }

    #[test]
    fn a_send_older_than_ten_minutes_is_dropped() {
        let (_, parts) = sample();
        let mut assembler = Assembler::new();
        let now = Instant::now();
        assert!(assembler.push_at(parts[0].clone(), now).is_none());
        // The second part arrives just after the send expired: the send is
        // forgotten, so this part starts a new send and nothing joins.
        let later = now + Duration::from_secs(10 * 60 + 1);
        assert!(assembler.push_at(parts[1].clone(), later).is_none());
        assert_eq!(assembler.sends.len(), 1);
        let send = assembler.sends.values().next().expect("one send");
        assert!(send.slots[0].is_none(), "the old part did not survive");
        assert!(send.slots[1].is_some(), "the new part started a send");
    }

    #[test]
    fn an_impossible_part_count_or_number_is_dropped() {
        let (_, parts) = sample();
        let mut assembler = Assembler::new();
        let mut zero_parts = parts[0].clone();
        zero_parts.parts = 0;
        assert!(assembler.push(zero_parts).is_none());
        assert!(assembler.sends.is_empty());

        let mut too_many = parts[0].clone();
        too_many.parts = 9;
        assert!(assembler.push(too_many).is_none());
        assert!(assembler.sends.is_empty());

        let mut out_of_range = parts[0].clone();
        out_of_range.part = out_of_range.parts + 1;
        assert!(assembler.push(out_of_range).is_none());
        assert!(assembler.sends.is_empty());
    }
}
