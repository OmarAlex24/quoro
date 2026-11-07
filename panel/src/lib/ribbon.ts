// SPDX-License-Identifier: AGPL-3.0-only
import { clamp } from './dither';

/*
 * The hour ribbon over an incident, as pure numbers: where each grant window,
 * each call, each wait for a person and the "now" line sit, in percent of the
 * lane. A call is a point at the time it was recorded; the ribbon never
 * invents how long it took.
 */

/** The spans the ribbon picks from, in minutes: the first that holds what happened. */
export const RIBBON_SPANS = [10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 720, 1_440, 2_880, 10_080] as const;

/** The furthest right "now" may sit, so the label after it still fits. */
const NOW_AT_MOST = 0.9;

/** Tick labels closer than this to the "now" line are hidden under its own label. */
const LABEL_GAP = 9;

export interface RibbonGrant {
  readonly id: string;
  readonly created_at: string;
  readonly expires_at: string;
  readonly revoked_at?: string | null;
}

export interface RibbonCall {
  readonly seq: number;
  readonly time: string;
  /** Refused or failed: drawn red with a cross. */
  readonly refused: boolean;
  /** A record by Quoro itself or a person, not by the agent: drawn grey. */
  readonly other: boolean;
}

export interface RibbonWait {
  readonly id: string;
  /** When the agent asked. */
  readonly from: string;
  /** When a person decided, or when the request lapsed; none while it still waits. */
  readonly to?: string | null;
}

export interface RibbonInput {
  /** When the incident opened. */
  readonly openedAt: string;
  /** When it was resolved; the ribbon stops there and draws no "now". */
  readonly resolvedAt?: string | null;
  readonly now: number;
  readonly grants: readonly RibbonGrant[];
  readonly calls: readonly RibbonCall[];
  readonly waits: readonly RibbonWait[];
}

export interface RibbonTick {
  readonly at: number;
  readonly left: number;
  /** Too close to "now" to print its own label. */
  readonly hidden: boolean;
}

export interface RibbonWindow {
  readonly id: string;
  readonly left: number;
  readonly width: number;
  /** The grant runs on past the right edge. */
  readonly open: boolean;
}

export interface RibbonPoint {
  readonly seq: number;
  readonly left: number;
  readonly tone: 'call' | 'refused' | 'other';
  /** Whether the call happened while one of the grants was in force. */
  readonly inGrant: boolean;
  /** Recorded outside the ribbon's span, so pinned to its edge. */
  readonly pinned: boolean;
}

export interface RibbonBand {
  readonly id: string;
  readonly left: number;
  readonly width: number;
  /** Still waiting: the band runs up to "now". */
  readonly live: boolean;
}

export interface RibbonLayout {
  readonly start: number;
  readonly end: number;
  readonly ticks: readonly RibbonTick[];
  readonly windows: readonly RibbonWindow[];
  readonly points: readonly RibbonPoint[];
  readonly waits: readonly RibbonBand[];
  /** Where "now" sits, or null once the incident is resolved. */
  readonly now: number | null;
}

const MINUTE = 60_000;

const ms = (iso: string | null | undefined): number | null => {
  if (!iso) return null;
  const at = Date.parse(iso);
  return Number.isNaN(at) ? null : at;
};

/** The shortest span, in minutes, that fits `elapsed` ms with "now" at most 90 % across. */
export const ribbonSpan = (elapsed: number): number => {
  const needed = Math.max(0, elapsed) / NOW_AT_MOST / MINUTE;
  return RIBBON_SPANS.find((span) => span >= needed) ?? RIBBON_SPANS[RIBBON_SPANS.length - 1]!;
};

export function ribbonLayout(input: RibbonInput): RibbonLayout {
  const opened = ms(input.openedAt) ?? input.now;
  const resolved = ms(input.resolvedAt);
  const callTimes = input.calls.map((call) => ms(call.time)).filter((at): at is number => at !== null);

  // The lane starts on the minute the incident opened, or earlier when a
  // record of the incident is older; it ends at the first span that fits.
  const first = Math.min(opened, ...callTimes);
  const start = Math.floor(first / MINUTE) * MINUTE;
  const last = Math.max(resolved ?? input.now, ...callTimes);
  const end = start + ribbonSpan(last - start) * MINUTE;
  const span = end - start;

  const at = (time: number) => (clamp((time - start) / span) * 100);

  const windows: RibbonWindow[] = [];
  const inForce: Array<[number, number]> = [];
  for (const grant of input.grants) {
    const from = ms(grant.created_at);
    const until = ms(grant.revoked_at) ?? ms(grant.expires_at);
    if (from === null || until === null || until <= from) continue;
    inForce.push([from, until]);
    if (until < start || from > end) continue;
    const left = at(from);
    windows.push({ id: grant.id, left, width: at(until) - left, open: until > end });
  }

  const points: RibbonPoint[] = [];
  for (const call of input.calls) {
    const time = ms(call.time);
    if (time === null) continue;
    points.push({
      seq: call.seq,
      left: at(time),
      tone: call.refused ? 'refused' : call.other ? 'other' : 'call',
      inGrant: inForce.some(([from, until]) => time >= from && time <= until),
      pinned: time < start || time > end,
    });
  }

  const nowMark = resolved === null ? input.now : resolved;
  const waits: RibbonBand[] = [];
  for (const wait of input.waits) {
    const from = ms(wait.from);
    if (from === null) continue;
    const closed = ms(wait.to);
    const until = closed ?? nowMark;
    if (until <= from) continue;
    const left = at(from);
    waits.push({ id: wait.id, left, width: at(until) - left, live: closed === null });
  }

  const nowLeft = resolved === null ? at(input.now) : null;
  const ticks: RibbonTick[] = [0, 1, 2, 3].map((quarter) => {
    const time = start + (span * quarter) / 4;
    const left = at(time);
    return { at: time, left, hidden: nowLeft !== null && quarter > 0 && Math.abs(left - nowLeft) < LABEL_GAP };
  });

  return { start, end, ticks, windows, points, waits, now: nowLeft };
}
