// SPDX-License-Identifier: AGPL-3.0-only
import type { WarmCopy } from '../api/generated/model';
import type { TagTone } from '../components/ui/tag';
import { relativeTime } from './app-status';
import { localTime } from './audit';
import { secondsWords } from './metric-area';

/*
 * A warm app's copy in words, from `App.warm` (openapi.yaml `WarmCopy`). The
 * Storage tab's Warm copy box and the canvas card's footer row both read
 * from here. Every word comes from the fields as the primary gives them: the
 * state is never derived again from `lag_s` against `interval_s`, and the lag
 * is never computed from `last_shipped_at` and the browser's clock. Pure, so
 * every state can be tested without a DOM.
 */

export interface WarmTag {
  tone: TagTone;
  words: string;
  /** The copy is still being made: the tag carries a pulsing lamp. */
  pulse: boolean;
}

const WARM_TAG: Record<WarmCopy['state'], WarmTag> = {
  syncing: { tone: 'shift', words: 'Syncing', pulse: true },
  in_sync: { tone: 'run', words: 'In sync', pulse: false },
  lagging: { tone: 'shift', words: 'Lagging', pulse: false },
  failed: { tone: 'fault', words: 'Failed', pulse: false },
};

/** The copy's state as a tag: the word, its tone, and whether it pulses. */
export const warmTag = (warm: Pick<WarmCopy, 'state'>): WarmTag => WARM_TAG[warm.state];

/** A node's name, or the words `VolumeBox` uses for a node the list does not hold. */
const nodeName = (nodeId: string, names: Readonly<Record<string, string>>): string | null => {
  const name = names[nodeId];
  return name === undefined || name === '' ? null : name;
};

export interface WarmNodeLine {
  /** The warm node to link to, when the node list knows it. */
  node: { id: string; name: string } | null;
  /** The whole line: `On lab-4 · picked by the scheduler`, or `No eligible node`. */
  text: string;
  /** What follows the name: `picked by the scheduler` or `pinned`; null without a node. */
  how: string | null;
}

/**
 * Where the copy lives and who chose it: `On lab-4 · pinned`. A node the list
 * does not hold is `an unknown node`, as the volume box says; with no warm
 * node at all, `No eligible node`.
 */
export const warmNodeLine = (
  warm: Pick<WarmCopy, 'node_id' | 'node_pinned'>,
  names: Readonly<Record<string, string>>,
): WarmNodeLine => {
  if (warm.node_id === null) return { node: null, text: 'No eligible node', how: null };
  const name = nodeName(warm.node_id, names);
  const how = warm.node_pinned ? 'pinned' : 'picked by the scheduler';
  return {
    node: name === null ? null : { id: warm.node_id, name },
    text: `On ${name ?? 'an unknown node'} · ${how}`,
    how,
  };
};

export interface WarmShippedLine {
  text: string;
  /** The exact time, for the hover title; null when there is none. */
  title: string | null;
}

/**
 * The newest complete copy on the warm node: `Snapshot 4182, 2 min ago`,
 * with the exact local time for the hover title. Before the first one, and
 * after a primary restart until the warm node reports again, the panel
 * cannot tell the two apart, so both say `No complete copy reported yet`.
 */
export const warmShippedLine = (
  warm: Pick<WarmCopy, 'last_snapshot_seq' | 'last_shipped_at'>,
  now: number = Date.now(),
): WarmShippedLine => {
  if (warm.last_snapshot_seq === null) return { text: 'No complete copy reported yet', title: null };
  const snapshot = `Snapshot ${warm.last_snapshot_seq}`;
  if (warm.last_shipped_at === null) return { text: snapshot, title: null };
  return {
    text: `${snapshot}, ${relativeTime(warm.last_shipped_at, now)}`,
    title: localTime(warm.last_shipped_at),
  };
};

/**
 * What a failover now would cost, from `lag_s` as the primary measured it on
 * its own clock: `A failover now would lose up to 12 s of writes`. A lag of
 * 0 means the newest snapshot is under a second old. Null when `lag_s` is,
 * which a `lagging` copy can be when the snapshot's time is unreadable.
 */
export const warmLagSentence = (warm: Pick<WarmCopy, 'lag_s'>): string | null => {
  if (warm.lag_s === null) return null;
  if (warm.lag_s <= 0) return 'A failover now would lose under a second of writes';
  return `A failover now would lose up to ${secondsWords(warm.lag_s)} of writes`;
};

/** How often the source snapshots the volume: `A snapshot every 30 s`, `A snapshot every 5 min`. */
export const warmIntervalWords = (warm: Pick<WarmCopy, 'interval_s'>): string =>
  `A snapshot every ${secondsWords(warm.interval_s)}`;

/**
 * The source's newest snapshot, as reported, in extents: `1,284 extents in
 * the newest snapshot`. A fragmented volume ships more slowly. Null when not
 * reported; it can be set before the warm node holds any copy.
 */
export const warmExtentWords = (warm: Pick<WarmCopy, 'extent_count'>): string | null => {
  if (warm.extent_count === null) return null;
  const count = warm.extent_count.toLocaleString('en-US');
  return `${count} ${warm.extent_count === 1 ? 'extent' : 'extents'} in the newest snapshot`;
};

/**
 * A lag short enough for the card's footer, rounded up to one unit so it
 * never says less than is at risk: `12 s` under a minute (`under 1 s` at 0),
 * whole minutes under an hour (`7 min` for 417 s), whole hours above.
 */
export const warmLagShort = (lagSeconds: number): string => {
  const seconds = Math.ceil(lagSeconds);
  if (seconds <= 0) return 'under 1 s';
  if (seconds < 60) return `${seconds} s`;
  if (seconds < 3_600) return `${Math.ceil(seconds / 60)} min`;
  return `${Math.ceil(seconds / 3_600)} h`;
};

/**
 * The first line of an error, and whether more follows. A node's error can
 * span several lines; the card shows only the first, so a cut is marked.
 */
const firstLine = (error: string): { line: string; more: boolean } => {
  const lines = error
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line !== '');
  return { line: lines[0] ?? '', more: lines.length > 1 };
};

/**
 * The canvas card's footer row for a warm app:
 * - `in_sync` and `lagging`: `Warm copy on lab-4, 12 s behind`, the lag
 *   rounded up to one unit so it fits the card and never understates the
 *   data at risk (`4 min` for 190 s; `warmLagShort`). The Storage tab gives
 *   the exact lag (`warmLagSentence`). Without a lag, the `, 12 s behind`
 *   part is left out, and a `lagging` copy says `, lagging` instead;
 * - `syncing`: `Warm copy syncing to lab-4`;
 * - `failed`: `Warm copy failed: ` and the first line of the error. When the
 *   error has more lines, the line ends with `…`; a long first line is cut
 *   by the row's own CSS truncation. Without an error, `Warm copy failed`.
 *
 * A node the list does not hold is `an unknown node`. With no warm node, the
 * line names the state instead of a node (`Warm copy in sync`).
 */
export const warmCardLine = (
  warm: Pick<WarmCopy, 'state' | 'node_id' | 'lag_s' | 'error'>,
  names: Readonly<Record<string, string>>,
): string => {
  const node = warm.node_id === null ? null : (nodeName(warm.node_id, names) ?? 'an unknown node');
  const behind = warm.lag_s === null ? '' : `, ${warmLagShort(warm.lag_s)} behind`;
  switch (warm.state) {
    case 'syncing':
      return node === null ? 'Warm copy syncing' : `Warm copy syncing to ${node}`;
    case 'in_sync':
      return `${node === null ? 'Warm copy in sync' : `Warm copy on ${node}`}${behind}`;
    case 'lagging':
      // Without a lag, the word keeps it from reading like a copy in sync.
      return node === null ? `Warm copy lagging${behind}` : `Warm copy on ${node}${behind || ', lagging'}`;
    case 'failed': {
      if (warm.error === null) return 'Warm copy failed';
      const { line, more } = firstLine(warm.error);
      if (line === '') return 'Warm copy failed';
      return `Warm copy failed: ${line}${more ? '…' : ''}`;
    }
  }
};
