// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The build log: the SSE frames the stream sends, ordering by `seq` across a
 * resume, and the failing line inside a Dockerfile excerpt. Pure, so the
 * parser and the excerpt rule are tested without an `EventSource`.
 */
import type { Build, BuildLogLine, BuildStatus } from '../api/generated/model';
import { problemViewFromEvent, type SseFrame } from './log-buffer';
import type { ProblemView } from './problem';

/** The tape never keeps more than this many lines; the oldest are dropped. */
export const BUILD_LOG_CAP = 5_000;

/** A single line is bounded so a hostile log cannot freeze the tab. */
export const BUILD_LOG_LINE_MAX = 8_000;

const STREAM_NAMES: ReadonlySet<string> = new Set(['stdout', 'stderr']);

const BUILD_STATUSES: ReadonlySet<string> = new Set<BuildStatus>([
  'queued',
  'preparing',
  'building',
  'exporting',
  'importing',
  'succeeded',
  'failed',
  'cancelled',
]);

export const isBuildStatus = (value: unknown): value is BuildStatus =>
  typeof value === 'string' && BUILD_STATUSES.has(value);

/**
 * Merge the lines we already show with the ones just received. The stream
 * resumes after the last `seq` the browser saw, so a resumed stream may repeat
 * a tail; `seq` is the identity and the order is always ascending.
 */
export const orderBuildLog = (
  current: readonly BuildLogLine[],
  incoming: readonly BuildLogLine[],
  cap: number = BUILD_LOG_CAP,
): BuildLogLine[] => {
  if (incoming.length === 0) return current as BuildLogLine[];
  const bySeq = new Map<number, BuildLogLine>();
  for (const line of current) {
    if (Number.isInteger(line.seq) && line.seq >= 1) bySeq.set(line.seq, line);
  }
  for (const line of incoming) {
    if (Number.isInteger(line.seq) && line.seq >= 1) bySeq.set(line.seq, line);
  }
  const merged = [...bySeq.values()].sort((left, right) => left.seq - right.seq);
  if (cap <= 0) return [];
  return merged.length > cap ? merged.slice(merged.length - cap) : merged;
};

const parseBuildLogLine = (data: string): BuildLogLine | null => {
  try {
    const parsed = JSON.parse(data) as Record<string, unknown>;
    const seq = parsed.seq;
    const time = parsed.time;
    const stream = parsed.stream;
    const line = parsed.line;
    if (typeof seq !== 'number' || !Number.isInteger(seq) || seq < 1) return null;
    if (typeof time !== 'string' || time === '') return null;
    if (typeof stream !== 'string' || !STREAM_NAMES.has(stream)) return null;
    if (typeof line !== 'string') return null;
    const vertex = parsed.vertex;
    return {
      seq,
      time,
      stream: stream as BuildLogLine['stream'],
      line: line.slice(0, BUILD_LOG_LINE_MAX),
      vertex:
        typeof vertex === 'number' && Number.isInteger(vertex) && vertex >= 0
          ? vertex
          : null,
    };
  } catch {
    return null;
  }
};

const parseBuildStatus = (data: string): Build | null => {
  try {
    const parsed = JSON.parse(data) as Record<string, unknown>;
    if (typeof parsed.id !== 'string' || parsed.id === '') return null;
    if (!isBuildStatus(parsed.status)) return null;
    return parsed as unknown as Build;
  } catch {
    return null;
  }
};

export type BuildStreamEvent =
  | { kind: 'log'; line: BuildLogLine }
  | { kind: 'status'; build: Build }
  | { kind: 'error'; problem: ProblemView }
  | { kind: 'end' }
  | { kind: 'unknown' };

/**
 * One parsed SSE frame as the build stream means it: `log` carries a
 * `BuildLogLine`, `status` a `Build`, `error` a `Problem`, `end` closes.
 * Malformed frames are dropped, never thrown on.
 */
export const parseBuildStreamEvent = (frame: SseFrame): BuildStreamEvent => {
  const data = frame.data ?? '';
  if (frame.event === 'log') {
    const line = parseBuildLogLine(data);
    return line === null ? { kind: 'unknown' } : { kind: 'log', line };
  }
  if (frame.event === 'status') {
    const build = parseBuildStatus(data);
    return build === null ? { kind: 'unknown' } : { kind: 'status', build };
  }
  if (frame.event === 'error') {
    return { kind: 'error', problem: problemViewFromEvent(data, 'read build logs') };
  }
  if (frame.event === 'end') return { kind: 'end' };
  return { kind: 'unknown' };
};

/** The SSE address of one build's log; `tail` is ignored when resuming. */
export const buildLogStreamUrl = (buildId: string, tail = 2_000): string =>
  `/api/v1/builds/${encodeURIComponent(buildId)}/logs/stream?tail=${tail}`;

const DOCKERFILE_MARKER = /^\s*(?:\d+\s*\|\s*)?>>>/;
const DOCKERFILE_LINE = /^\s*(\d+)\s*\|/;

/**
 * The index of the failing line in a BuildKit excerpt: the line BuildKit marks
 * with `>>>` when it names one, otherwise the line whose Dockerfile number is
 * `dockerfileLine`. -1 when the excerpt does not point at a single line.
 */
export const failingLineIndex = (
  excerpt: readonly string[],
  dockerfileLine: number | null,
): number => {
  const marked = excerpt.findIndex((line) => DOCKERFILE_MARKER.test(line));
  if (marked >= 0) return marked;
  if (dockerfileLine === null) return -1;
  return excerpt.findIndex((line) => {
    const match = DOCKERFILE_LINE.exec(line);
    return match !== null && Number(match[1]) === dockerfileLine;
  });
};
