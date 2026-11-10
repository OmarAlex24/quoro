// SPDX-License-Identifier: AGPL-3.0-only
//! A returning source hands its volume over to the warm receiver
//! (`node-agent-protocol.md` section 9, ADR 0176, ADR 0215).
//!
//! A node that was the source of a warm volume and lost the lease while it
//! was cut off comes back to find itself the volume's target. It still holds
//! the volume as its own: its `volumes` row, the live file, the base and the
//! snapshot it was shipping, and its old VM, which the reconciler deletes
//! once it is stopped. Each pass of the receiver runs the hand-over of such a
//! volume, under the volume's lock, before any stream of it is accepted:
//!
//! 1. **Wait for the VM to go.** While a VM that exists attaches the volume
//!    the live file could change, so nothing happens and a later pass tries
//!    again. That is not an error. Neither is a refusal of the engine, which
//!    a later pass also waits out; once it has lasted
//!    [`REFUSALS_BEFORE_ERROR`] passes in a row it is reported as the warm
//!    copy's error.
//! 2. **Compare** the live file with the newest kept base, the snapshot the
//!    target confirmed complete (`volume_changed_ranges`). Only a comparison
//!    that is not `full` and finds no range proves that the file holds no
//!    write the target lacks. Anything else keeps the file: a `full`
//!    comparison, no base, an engine error, and a file that matches only the
//!    snapshot that was being shipped, which the target may never have
//!    completed. When the engine refuses the comparison or finds a side
//!    missing, the live file is compared alone, without the base: a refusal
//!    then is the file's (a VM the engine holds, another task, a path it does
//!    not take), and the hand-over waits; `NotFound` means the live file is
//!    missing, which is gone, with nothing to keep, only under the rule
//!    below; anything else means the base is unusable, and the file is
//!    quarantined.
//! 3. **Quarantine** a file not proved unchanged: a reflink clone
//!    (`snapshot_volume`), then in one store transaction its kept snapshot
//!    (purpose `quarantine`, so the start-up sweep keeps it), its
//!    `quarantine` record with a new `qrn_` id, and the mark on the own row
//!    that it is being handed over. A file proved unchanged gets the mark
//!    alone, in one transaction.
//! 4. **Then**, once the desired state still names the node as the target,
//!    delete the live file and, after it is gone, in one transaction drop
//!    the own row and leave the warm target row with no complete copy. The
//!    volume is an ordinary warm copy from then on, and the next stream is a
//!    full copy.
//!
//! **Crash safety.** The mark is written only once nothing in the live file
//! is lost without it, and it records why the file may go: the `qrn_` id of
//! the copy that keeps its writes, written in the transaction of the copy's
//! records; `unchanged`, on proof; or `gone`. A run that finds the mark skips
//! the comparison and the quarantine and goes on with step 4, once the copy
//! the mark names still has its clone.
//!
//! **A missing file proves nothing by itself**: a volume directory that did
//! not mount, or a new empty pool, looks exactly like it. So the live file
//! counts as gone, and a missing clone clears its mark, only while another
//! file the node records for the volume (a kept snapshot, a quarantined
//! copy's clone among them) is on disk and in the engine's current volume
//! directory. Without one, the run fails with the warm copy's error and
//! changes nothing, and a later pass tries again. A mark cleared
//! for a missing clone keeps the copy's record, reported without a hash: no
//! record is ever forgotten here, only by `quarantine_deletes`. The next pass
//! compares the live file again.
//!
//! The mark holds only while no VM writes to the file: a node that runs the
//! volume again before the hand-over finished clears it (the reconciler,
//! under the volume's lock, before any VM attaches the file), a run whose
//! desired state moved before the file went clears the mark it wrote itself,
//! and a later hand-over compares again. A crash before the record's commit
//! leaves no record and no mark, and the clone it took unrecorded, which the
//! start-up sweep removes; the next run compares again and takes one copy. A
//! crash after it finds the mark and takes none. A commit that fails may
//! still come back at the next start, so its clone is never deleted here:
//! the next run records the same copy under the same id while the live file
//! still matches it, and the start-up sweep removes a clone no record names.
//! So every re-run ends with exactly one new quarantined copy, or none. The
//! row is dropped only after the file is gone, so a later clean-up of the
//! row never deletes a file a stream has committed over.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt as _;

use quoro_engine::{Engine, EngineError, RangesOf, VmId, VolumeId, VolumeSnapshot};
use quoro_proto::wire::SpecVolumeFile;
use tracing::{debug, info, warn};

use super::Inner;
use crate::kept::{self, KeepPurpose};
use crate::store::{HandOverMark, KeptSnapshotRecord, VolumeQuarantineRecord};
use crate::util;

/// What one hand-over run did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum HandOver {
    /// Nothing to hand over: the node does not hold the volume as its own,
    /// or the desired state no longer names it as the target.
    Nothing,
    /// A stream or another task holds the volume's lock; a later pass tries
    /// again.
    Busy,
    /// A VM of the desired state that the engine still holds attaches the
    /// volume; a later pass tries again.
    VmAttached,
    /// The engine refused to compare the live file or to delete it, with its
    /// reason: a VM it holds attaches the file, another task holds it, or the
    /// path is not one it takes. A later pass tries again.
    Refused(String),
    /// A step failed, with this reason; a later pass tries again.
    Failed(String),
    /// The volume is a warm copy now; with the copy this run quarantined.
    Done(Option<String>),
}

/// After how many passes in a row with a refusal of the engine the
/// hand-over reports it as the warm copy's error.
pub(super) const REFUSALS_BEFORE_ERROR: u32 = 12;

/// What the comparison with the base proved.
enum Compared {
    /// No change since the base: the file holds no divergent write.
    Unchanged,
    /// Not proved unchanged, with why.
    NotProved(String),
    /// The engine refuses the live file, with its reason.
    Refused(String),
    /// The live file is gone from a volume directory shown to be the real
    /// one: there is nothing to keep.
    Gone,
    /// The live file is missing, and nothing shows the volume directory to
    /// be the real one; with why.
    Unproved(String),
}

/// What a quarantine kept.
enum Kept {
    /// The copy, with its id.
    Copy(String),
    /// Nothing: the live file is gone.
    NoFile,
}

impl<E: Engine + 'static> Inner<E> {
    /// Hands `volume_id` over when the node still holds it as its own (see
    /// the module documentation). Called by each pass for every `present`
    /// entry the node is the target of.
    pub(super) async fn hand_over(&self, volume_id: &str) -> HandOver {
        match self.own_volume(volume_id).await {
            Ok(true) => {}
            Ok(false) => return HandOver::Nothing,
            Err(e) => {
                warn!(volume_id, error = %e, "could not read the volume row");
                return HandOver::Failed(e);
            }
        }
        let Some(guard) = self.locks.try_lock(volume_id) else {
            debug!(
                volume_id,
                "the volume is busy; its hand-over waits for the next pass"
            );
            return HandOver::Busy;
        };
        let outcome = self.hand_over_locked(volume_id).await;
        let first_wait = {
            let mut waiting = self
                .waiting_for_vm
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if outcome == HandOver::VmAttached {
                waiting.insert(volume_id.to_owned())
            } else {
                waiting.remove(volume_id);
                false
            }
        };
        let refusals = count_refusals(
            &mut self
                .hand_over_refusals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            volume_id,
            &outcome,
        );
        match &outcome {
            HandOver::VmAttached if first_wait => info!(
                volume_id,
                "a VM still attaches the node's own volume; its hand-over waits until the VM is gone"
            ),
            HandOver::Refused(reason) if refusals >= REFUSALS_BEFORE_ERROR => {
                warn!(volume_id, refusals, error = %reason, "the engine still refuses the hand-over of the node's own volume");
                self.set_error(
                    volume_id,
                    Some(&format!(
                        "the engine has refused the hand-over of the node's own volume {refusals} passes in a row: {reason}"
                    )),
                )
                .await;
            }
            HandOver::Refused(reason) if refusals == 1 => info!(
                volume_id,
                error = %reason,
                "the engine refuses the hand-over of the node's own volume for now; a later pass tries again"
            ),
            HandOver::Refused(reason) => {
                debug!(volume_id, refusals, error = %reason, "the engine still refuses the hand-over")
            }
            HandOver::Failed(reason) => {
                warn!(volume_id, error = %reason, "could not hand the node's own volume over yet");
                self.set_error(
                    volume_id,
                    Some(&format!(
                        "could not hand the node's own volume over as the warm copy: {reason}"
                    )),
                )
                .await;
            }
            _ => {}
        }
        drop(guard);
        outcome
    }

    /// The steps of the hand-over; the caller holds the volume's lock.
    async fn hand_over_locked(&self, volume_id: &str) -> HandOver {
        // Under the lock: the entry and the row as they are now.
        let Some((file, _)) = self.target_entry(volume_id) else {
            return HandOver::Nothing;
        };
        match self.own_volume(volume_id).await {
            Ok(true) => {}
            Ok(false) => return HandOver::Nothing,
            Err(e) => return HandOver::Failed(e),
        }
        let mark = match self.store.volume_hand_over_mark(volume_id).await {
            Ok(mark) => mark,
            Err(e) => return HandOver::Failed(format!("cannot read the volume row: {e}")),
        };
        let marked = mark.is_some();
        let mut quarantined = None;
        // Also with the mark: the file goes only once no VM can write to it.
        if let Some(vm_id) = self.vm_attaching(volume_id).await {
            debug!(volume_id, vm_id, "a VM still attaches the volume");
            return HandOver::VmAttached;
        }
        if !marked {
            let gone = match self.compare_with_base(volume_id).await {
                Ok(Compared::Refused(reason)) => return HandOver::Refused(reason),
                Ok(Compared::Unproved(reason)) => return HandOver::Failed(reason),
                Ok(Compared::Unchanged) => {
                    info!(
                        volume_id,
                        "the live volume shows no change against the base: it holds no divergent write"
                    );
                    false
                }
                Ok(Compared::Gone) => true,
                Ok(Compared::NotProved(reason)) => match self.quarantine(&file, &reason).await {
                    Ok(Kept::Copy(id)) => {
                        quarantined = Some(id);
                        false
                    }
                    Ok(Kept::NoFile) => true,
                    Err(e) => return HandOver::Failed(e),
                },
                Err(e) => return HandOver::Failed(e),
            };
            if gone {
                warn!(
                    volume_id,
                    "the node's own volume has no live file: there is nothing to keep"
                );
            }
            // A quarantine wrote the mark with its records.
            let basis = if gone {
                HandOverMark::Gone
            } else {
                HandOverMark::Unchanged
            };
            if quarantined.is_none()
                && let Err(e) = self.store.mark_volume_handing_over(volume_id, &basis).await
            {
                return HandOver::Failed(format!("cannot mark the volume as handed over: {e}"));
            }
        }
        // The desired state may have moved meanwhile: the file goes only
        // while the node is still the volume's target. A mark from an earlier
        // run stays, and a later pass goes on from here; the reconciler
        // clears it under the lock once the node runs the volume again. A
        // mark from this run goes now: the reconciler may have read the row
        // before it, and a VM may write to the file next.
        if self.target_entry(volume_id).is_none() {
            if !marked && let Err(e) = self.store.clear_volume_handing_over(volume_id).await {
                warn!(volume_id, error = %e, "could not clear the hand-over mark of a volume no longer handed over");
            }
            return HandOver::Nothing;
        }
        if let Some(HandOverMark::Copy(quarantine_id)) = &mark
            && let Err(outcome) = self.copy_kept(volume_id, quarantine_id).await
        {
            return outcome;
        }
        match self
            .engine
            .delete_volume(&VolumeId(volume_id.to_owned()))
            .await
        {
            Ok(()) | Err(EngineError::NotFound) => {}
            Err(EngineError::Conflict(reason)) => {
                return HandOver::Refused(format!(
                    "the engine would not delete the live file: {reason}"
                ));
            }
            Err(e) => return HandOver::Failed(format!("cannot delete the live volume file: {e}")),
        }
        // Only now, the file being gone: the row never outlives a reason to
        // keep a file that a stream could later commit over.
        if let Err(e) = self.store.finish_hand_over(volume_id).await {
            return HandOver::Failed(format!("cannot drop the volume row: {e}"));
        }
        // A clone whose commit failed and that was never recorded again is
        // the start-up sweep's.
        self.unrecorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(volume_id);
        info!(
            volume_id,
            quarantine_id = ?quarantined,
            "handed the node's own volume over: it is a warm copy now, and the next stream is a full copy"
        );
        HandOver::Done(quarantined)
    }

    /// A VM of the desired state that attaches `volume_id` and that the
    /// engine may still hold: the node's old VM until the reconciler deletes
    /// it. An engine that cannot tell counts as holding it.
    async fn vm_attaching(&self, volume_id: &str) -> Option<String> {
        let vm_ids: Vec<String> = {
            let desired = self.desired.borrow();
            desired.as_ref().map_or_else(Vec::new, |snapshot| {
                snapshot
                    .state
                    .spec
                    .vms
                    .iter()
                    .filter(|vm| {
                        vm.volumes
                            .iter()
                            .any(|volume| volume.volume_id == volume_id)
                    })
                    .map(|vm| vm.vm_id.clone())
                    .collect()
            })
        };
        for vm_id in vm_ids {
            match self.engine.inspect(&VmId(vm_id.clone())).await {
                Err(EngineError::NotFound) => {}
                Ok(_) | Err(_) => return Some(vm_id),
            }
        }
        None
    }

    /// Before a marked file goes: the copy the mark stands for must still
    /// keep its writes. Only that copy is checked, and no record is ever
    /// forgotten here.
    ///
    /// - Its clone is there: the file may go.
    /// - Its clone is missing while another file the node records for the
    ///   volume is there, so the volume directory is the real one: the mark
    ///   goes, the record stays (reported without a hash, and hashed if the
    ///   file comes back), and the next pass compares the live file again.
    /// - Its clone is missing and so is every other recorded file of the
    ///   volume: a volume directory that did not mount looks exactly like
    ///   that. Nothing changes, and the run fails until the files are back.
    /// - An operator deleted the copy (`quarantine_deletes`) before the file
    ///   went: the live file is all that keeps its writes now, so the mark
    ///   goes and the next pass compares the file again.
    async fn copy_kept(&self, volume_id: &str, quarantine_id: &str) -> Result<(), HandOver> {
        let record = match self.store.volume_quarantine(quarantine_id).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                self.clear_mark(volume_id).await?;
                return Err(HandOver::Failed(format!(
                    "the quarantined copy {quarantine_id} the hand-over mark stood for was \
                     deleted; the next pass compares the live file again"
                )));
            }
            Err(e) => {
                return Err(HandOver::Failed(format!(
                    "cannot read the quarantined copy {quarantine_id}: {e}"
                )));
            }
        };
        match std::fs::symlink_metadata(&record.path) {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(HandOver::Failed(format!(
                    "cannot check the clone of the quarantined copy {quarantine_id}: {e}"
                )));
            }
        }
        if !self
            .volume_files_present(volume_id)
            .await
            .map_err(HandOver::Failed)?
        {
            return Err(HandOver::Failed(format!(
                "the clone of the quarantined copy {quarantine_id} is missing, and so is every \
                 other file the node records for the volume: the volume directory may not be \
                 the real one, so nothing changes until the files are back"
            )));
        }
        self.clear_mark(volume_id).await?;
        Err(HandOver::Failed(format!(
            "the clone of the quarantined copy {quarantine_id} the hand-over mark stood for is \
             missing; its record stays, and the next pass compares the live file again"
        )))
    }

    async fn clear_mark(&self, volume_id: &str) -> Result<(), HandOver> {
        self.store
            .clear_volume_handing_over(volume_id)
            .await
            .map_err(|e| HandOver::Failed(format!("cannot clear the hand-over mark: {e}")))
    }

    /// Whether a file the node records for `volume_id` is where the engine
    /// keeps the volume's files now: a kept snapshot of the volume (its base,
    /// the snapshot it shipped, or a quarantined copy's clone) that is on disk
    /// and that the engine takes as a snapshot of the volume in its current
    /// volume directory. That shows the directory to be the real one, so a
    /// file missing from it is gone; without it, a missing file proves
    /// nothing. The store and the engine alone do not count: they still name
    /// the files of a directory that did not mount. Neither does the disk
    /// alone: a file left where a moved directory was is not in the engine's.
    async fn volume_files_present(&self, volume_id: &str) -> Result<bool, String> {
        let kept = self
            .store
            .kept_snapshots_of(volume_id)
            .await
            .map_err(|e| format!("cannot read the kept snapshots: {e}"))?;
        for kept in kept {
            if std::fs::symlink_metadata(&kept.snapshot.location).is_err() {
                continue;
            }
            if self
                .engine
                .volume_changed_ranges(&RangesOf::Snapshot(kept.snapshot), None)
                .await
                .is_ok()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Compares the live file with the newest kept base (binding rule of
    /// section 9 step 1, ADR 0215). Every comparison also runs the engine's
    /// own check that no VM attaches the file.
    async fn compare_with_base(&self, volume_id: &str) -> Result<Compared, String> {
        let base = self
            .store
            .kept_snapshots_of(volume_id)
            .await
            .map_err(|e| format!("cannot read the kept snapshots: {e}"))?
            .into_iter()
            .filter(|kept| kept.purpose == KeepPurpose::WarmBase.as_str())
            .max_by_key(|kept| (kept.seq, kept.snapshot.created_at));
        let Some(base) = base else {
            return Ok(self
                .compare_alone(volume_id)
                .await
                .unwrap_or_else(|| NotProvedReason::NoBase.into()));
        };
        let ranges = self
            .engine
            .volume_changed_ranges(
                &RangesOf::Volume(VolumeId(volume_id.to_owned())),
                Some(&base.snapshot),
            )
            .await;
        Ok(match ranges {
            Ok(ranges) if ranges.full => NotProvedReason::Full.into(),
            Ok(ranges) if ranges.ranges.is_empty() => Compared::Unchanged,
            Ok(ranges) => Compared::NotProved(format!(
                "{} ranges changed since the base (snapshot_seq {})",
                ranges.ranges.len(),
                base.seq
                    .map_or_else(|| "unknown".to_owned(), |seq| seq.to_string())
            )),
            // Either side: the live file alone tells which.
            Err(e @ (EngineError::Conflict(_) | EngineError::NotFound)) => {
                self.compare_alone(volume_id).await.unwrap_or_else(|| {
                    Compared::NotProved(format!(
                        "the base cannot be compared with the live file: {e}"
                    ))
                })
            }
            Err(e) => Compared::NotProved(format!("the comparison with the base failed: {e}")),
        })
    }

    /// Compares the live file alone, without a base, which proves no file
    /// unchanged: what the engine's refusal or `NotFound` says of the live
    /// file, or `None` when it has neither. `NotFound` is the file gone only
    /// while another file the node records for the volume is on disk.
    async fn compare_alone(&self, volume_id: &str) -> Option<Compared> {
        match self
            .engine
            .volume_changed_ranges(&RangesOf::Volume(VolumeId(volume_id.to_owned())), None)
            .await
        {
            Err(EngineError::Conflict(reason)) => Some(Compared::Refused(reason)),
            Err(EngineError::NotFound) => Some(match self.volume_files_present(volume_id).await {
                Ok(true) => Compared::Gone,
                Ok(false) => Compared::Unproved(
                    "the live volume file is missing, and so is every other file the node \
                     records for the volume: the volume directory may not be the real one, so \
                     nothing changes until the file is back"
                        .to_owned(),
                ),
                Err(e) => Compared::Unproved(format!(
                    "the live volume file is missing, and the other files of the volume could \
                     not be checked: {e}"
                )),
            }),
            Ok(_) | Err(_) => None,
        }
    }

    /// Keeps the live file of `file` as a quarantined copy. Nothing relies on
    /// the clone until its records are committed. A commit that failed may
    /// still come back at the next start, so its clone stays: the next run
    /// records it again under the same id while the live file still matches
    /// it, and the start-up sweep removes it when no record names it.
    async fn quarantine(&self, file: &SpecVolumeFile, reason: &str) -> Result<Kept, String> {
        let pending = self.take_unrecorded(&file.volume_id).await;
        let (kept, record) = match pending {
            Some(pending) => pending,
            None => match self.clone_live(file, reason).await? {
                Some(pending) => pending,
                None => return Ok(Kept::NoFile),
            },
        };
        if let Err(e) = self.store.record_volume_quarantine(&kept, &record).await {
            self.unrecorded
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(file.volume_id.clone(), (kept, record));
            return Err(format!("cannot record the quarantined copy: {e}"));
        }
        warn!(
            volume_id = %file.volume_id,
            quarantine_id = %record.quarantine_id,
            size_bytes = record.size_bytes,
            reason = %record.reason,
            "kept the live volume as a quarantined copy: it may hold writes no other node has"
        );
        self.quarantines.added(&record).await;
        Ok(Kept::Copy(record.quarantine_id))
    }

    /// The copy of `volume_id` whose records could not be committed, while
    /// the live file still matches it and no record has its id. One that no
    /// longer qualifies is left to the start-up sweep.
    async fn take_unrecorded(
        &self,
        volume_id: &str,
    ) -> Option<(KeptSnapshotRecord, VolumeQuarantineRecord)> {
        let (kept, record) = self
            .unrecorded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(volume_id)?;
        let unchanged = matches!(
            self.engine
                .volume_changed_ranges(
                    &RangesOf::Volume(VolumeId(volume_id.to_owned())),
                    Some(&kept.snapshot),
                )
                .await,
            Ok(ranges) if !ranges.full && ranges.ranges.is_empty()
        );
        let unrecorded = matches!(
            self.store.volume_quarantine(&record.quarantine_id).await,
            Ok(None)
        );
        if unchanged && unrecorded {
            Some((kept, record))
        } else {
            debug!(
                volume_id,
                snapshot_id = %kept.snapshot.snapshot_id,
                "an unrecorded quarantine clone no longer matches; the start-up sweep removes it"
            );
            None
        }
    }

    /// Clones the live file of `file` and builds its records; `None` when
    /// the live file is gone.
    async fn clone_live(
        &self,
        file: &SpecVolumeFile,
        reason: &str,
    ) -> Result<Option<(KeptSnapshotRecord, VolumeQuarantineRecord)>, String> {
        let id = VolumeId(file.volume_id.clone());
        let snapshot = match self.engine.snapshot_volume(&id).await {
            Ok(snapshot) => snapshot,
            // Only when the live file alone is missing too, from a volume
            // directory shown to be the real one.
            Err(EngineError::NotFound) => match self.compare_alone(&file.volume_id).await {
                Some(Compared::Gone) => return Ok(None),
                Some(Compared::Unproved(reason)) => return Err(reason),
                _ => {
                    return Err(format!(
                        "cannot take the quarantined copy: {}",
                        EngineError::NotFound
                    ));
                }
            },
            Err(e) => return Err(format!("cannot take the quarantined copy: {e}")),
        };
        let size_bytes = match std::fs::metadata(&snapshot.location) {
            Ok(meta) => meta.blocks().saturating_mul(512),
            Err(e) => {
                // No record was tried: nothing can bring one back.
                self.drop_unrecorded(&snapshot).await;
                return Err(format!("cannot read the quarantined copy's size: {e}"));
            }
        };
        let record = VolumeQuarantineRecord {
            quarantine_id: util::quarantine_id(),
            volume_id: file.volume_id.clone(),
            app_id: file.app_id.clone(),
            created_at: util::now_rfc3339(),
            size_bytes,
            path: snapshot.location.clone(),
            snapshot_id: snapshot.snapshot_id.clone(),
            reason: reason.to_owned(),
            sha256: None,
        };
        let kept = KeptSnapshotRecord {
            app_id: file.app_id.clone(),
            snapshot,
            purpose: KeepPurpose::Quarantine.as_str().to_owned(),
            seq: None,
            cells: None,
        };
        Ok(Some((kept, record)))
    }

    /// Deletes a clone no record was tried for; one the engine keeps is
    /// removed by the next start-up sweep, since nothing records it.
    async fn drop_unrecorded(&self, snapshot: &VolumeSnapshot) {
        if let Err(e) = self.engine.delete_volume_snapshot(snapshot).await {
            warn!(
                volume_id = %snapshot.volume_id.0,
                error = %e,
                "could not delete an unrecorded quarantine clone; the next start-up sweep removes it"
            );
        }
    }

    /// Releases the warm snapshots this node kept of `volume_id` as its
    /// source, once a complete stream has landed here as the target (section
    /// 9 step 3): the node then holds the source's newest complete copy, and
    /// they are no candidate any more. A quarantined copy is never one of
    /// them. One the engine would not delete stays recorded, and the next
    /// complete stream tries again. The caller holds the volume's lock.
    pub(super) async fn release_returning_snapshots(&self, volume_id: &str) {
        let kept_any = match self.store.kept_snapshot_purposes().await {
            Ok(purposes) => purposes
                .iter()
                .any(|(volume, purpose)| volume == volume_id && KeepPurpose::is_warm(purpose)),
            Err(e) => {
                warn!(volume_id, error = %e, "could not read the kept snapshots");
                return;
            }
        };
        if !kept_any {
            return;
        }
        let records = match self.store.kept_snapshots_of(volume_id).await {
            Ok(records) => records,
            Err(e) => {
                warn!(volume_id, error = %e, "could not read the kept snapshots");
                return;
            }
        };
        for record in records
            .iter()
            .filter(|record| KeepPurpose::is_warm(&record.purpose))
        {
            match kept::release(&*self.engine, &self.store, &record.snapshot).await {
                Ok(()) => info!(
                    volume_id,
                    snapshot_id = %record.snapshot.snapshot_id,
                    purpose = %record.purpose,
                    "released a snapshot kept as the volume's source: a complete copy has landed"
                ),
                Err(e) => warn!(
                    volume_id,
                    snapshot_id = %record.snapshot.snapshot_id,
                    error = %e,
                    "could not release a snapshot kept as the volume's source; the next complete stream tries again"
                ),
            }
        }
    }
}

/// The reasons a comparison proves nothing, apart from a changed range.
enum NotProvedReason {
    NoBase,
    Full,
}

impl From<NotProvedReason> for Compared {
    fn from(reason: NotProvedReason) -> Self {
        Self::NotProved(
            match reason {
                NotProvedReason::NoBase => "no base is kept to compare with",
                NotProvedReason::Full => "the comparison with the base was full",
            }
            .to_owned(),
        )
    }
}

/// Counts the passes in a row whose hand-over of `volume_id` the engine
/// refused, and returns the count after `outcome`: any other outcome ends
/// the run.
fn count_refusals(counts: &mut HashMap<String, u32>, volume_id: &str, outcome: &HandOver) -> u32 {
    match outcome {
        HandOver::Refused(_) => {
            let count = counts.entry(volume_id.to_owned()).or_default();
            *count = count.saturating_add(1);
            *count
        }
        _ => {
            counts.remove(volume_id);
            0
        }
    }
}
