// SPDX-License-Identifier: AGPL-3.0-only
import type { WarmCopy } from '../api/generated/model';

/*
 * Mock mode only: a simulation of how a warm copy's progress moves with the
 * clock, so the mock answers what the primary's `derive` would at the moment
 * of the read rather than what was true when the mock seeded. Deterministic:
 * it depends only on the seeded copy, when that copy was measured, and now.
 *
 * - `in_sync`: the source takes a snapshot every `interval_s`, and the warm
 *   node completes each copy the same few seconds after it was taken (the
 *   seeded `lag_s` minus the seeded copy's age). The newest shipped snapshot
 *   moves on by one each interval; `lag_s` is the time since it was taken,
 *   so it never exceeds `interval_s` plus those few seconds, at most twice
 *   `interval_s`.
 * - `lagging`, and `failed` with progress: nothing new ships, so the
 *   snapshot and its time stay, and `lag_s` grows with the time since that
 *   snapshot was taken.
 * - `syncing`, and anything without a snapshot or a lag: as seeded.
 *
 * In every case the copy's age (now − `last_shipped_at`) stays at most
 * `lag_s`, since a copy completes after its snapshot is taken.
 */
export const warmCopyAt = (warm: WarmCopy, measuredAt: number, now: number): WarmCopy => {
  if (warm.last_snapshot_seq === null || warm.last_shipped_at === null || warm.lag_s === null) return warm;
  const shippedAt = Date.parse(warm.last_shipped_at);
  if (Number.isNaN(shippedAt)) return warm;
  // When the source took the seeded snapshot, on the primary's clock.
  const takenAt = measuredAt - warm.lag_s * 1_000;
  const lagAt = (taken: number): number => Math.max(0, Math.floor((now - taken) / 1_000));

  if (warm.state === 'in_sync') {
    const intervalMs = warm.interval_s * 1_000;
    // How long a copy takes to complete once its snapshot is taken.
    const shipMs = Math.min(Math.max(1_000, shippedAt - takenAt), intervalMs);
    const shipped = Math.max(0, Math.floor((now - takenAt - shipMs) / intervalMs));
    const taken = takenAt + shipped * intervalMs;
    return {
      ...warm,
      last_snapshot_seq: warm.last_snapshot_seq + shipped,
      last_shipped_at: new Date(taken + shipMs).toISOString(),
      lag_s: lagAt(taken),
    };
  }
  if (warm.state === 'lagging' || warm.state === 'failed') {
    return { ...warm, lag_s: Math.max(warm.lag_s, lagAt(takenAt)) };
  }
  return warm;
};
