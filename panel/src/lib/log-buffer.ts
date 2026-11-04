import { ApiProblem } from '../api/fetcher';
import type { Problem } from '../api/generated/model';
import { problemView, type ProblemView } from './problem';

/** The tape never keeps more than this many lines; the oldest are dropped. */
export const LOG_LINE_CAP = 2_000;

/** Append lines and keep only the newest `cap`, dropping the oldest first. */
export const appendLogLines = <T>(
  current: readonly T[],
  incoming: readonly T[],
  cap: number = LOG_LINE_CAP,
): T[] => {
  if (incoming.length === 0) return current as T[];
  const next = [...current, ...incoming];
  if (cap <= 0) return [];
  return next.length > cap ? next.slice(next.length - cap) : next;
};

export interface SseFrame {
  id?: string;
  event?: string;
  data?: string;
}

/** One SSE frame as the server writes it: `id`, `event`, `data`, blank line. */
export const formatSseFrame = ({
  id,
  event,
  data,
}: {
  id?: string | number;
  event?: string;
  data: string;
}): string => {
  const lines: string[] = [];
  if (id !== undefined) lines.push(`id: ${id}`);
  if (event !== undefined) lines.push(`event: ${event}`);
  for (const line of String(data).split('\n')) lines.push(`data: ${line}`);
  return `${lines.join('\n')}\n\n`;
};

/** Parse the fields of one SSE frame block. Comments and unknown fields go. */
export const parseSseFrame = (block: string): SseFrame | null => {
  const frame: SseFrame = {};
  const data: string[] = [];

  for (const rawLine of block.split(/\r?\n/)) {
    if (rawLine === '' || rawLine.startsWith(':')) continue;
    const colon = rawLine.indexOf(':');
    const field = colon === -1 ? rawLine : rawLine.slice(0, colon);
    let value = colon === -1 ? '' : rawLine.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);

    if (field === 'id') frame.id = value;
    else if (field === 'event') frame.event = value;
    else if (field === 'data') data.push(value);
  }

  if (data.length > 0) frame.data = data.join('\n');
  return Object.keys(frame).length > 0 ? frame : null;
};

/**
 * Feed a chunk of an SSE stream and get the complete frames plus the tail that
 * is still waiting for its blank line.
 */
export const parseSseChunk = (
  buffered: string,
  chunk: string,
): { frames: SseFrame[]; rest: string } => {
  const text = `${buffered}${chunk}`.replace(/\r\n/g, '\n');
  const parts = text.split('\n\n');
  const rest = parts.pop() ?? '';
  const frames = parts
    .map(parseSseFrame)
    .filter((frame): frame is SseFrame => frame !== null);
  return { frames, rest };
};

const pad = (value: number, width = 2): string => String(value).padStart(width, '0');

/** Local time for the tape's left column: HH:MM:SS.mmm. */
export const formatLogTime = (iso: string): string => {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return '--:--:--.---';
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(
    date.getMilliseconds(),
    3,
  )}`;
};

/** Problems that are worth reconnecting through; anything else closes the source. */
const TRANSIENT_CODES: ReadonlySet<string> = new Set([
  'node_unreachable',
  'rate_limited',
  'internal',
]);

export const isTransientProblem = (view: ProblemView): boolean =>
  view.code !== undefined && TRANSIENT_CODES.has(view.code);

const KNOWN_CODES: ReadonlySet<string> = new Set([
  'unauthenticated',
  'forbidden',
  'approval_required',
  'not_found',
  'conflict',
  'validation_failed',
  'rate_limited',
  'not_primary',
  'node_unreachable',
  'internal',
]);

/** Turn an SSE `error` event's Problem JSON into the panel's plain words. */
export const problemViewFromEvent = (
  data: string,
  action = 'read app logs',
): ProblemView => {
  try {
    const parsed = JSON.parse(data) as Partial<Problem>;
    if (typeof parsed.code !== 'string' || !KNOWN_CODES.has(parsed.code)) {
      throw new Error('missing code');
    }
    return problemView(
      new ApiProblem({
        status: typeof parsed.status === 'number' ? parsed.status : 500,
        code: parsed.code as Problem['code'],
        reason: parsed.reason,
        detail: typeof parsed.detail === 'string' ? parsed.detail : undefined,
        scope: typeof parsed.scope === 'string' ? parsed.scope : undefined,
        title: typeof parsed.title === 'string' ? parsed.title : undefined,
      }),
      action,
    );
  } catch {
    return {
      message: 'The log stream stopped. Reload the page to start it again.',
    };
  }
};
