// SPDX-License-Identifier: AGPL-3.0-only
import { useEffect, useRef, useState } from 'react';
import type { Build, BuildLogLine } from '../api/generated/model';
import { getBuildLogs } from '../api/generated/builds/builds';
import { isApiProblem } from '../api/fetcher';
import {
  buildLogStreamUrl,
  orderBuildLog,
  parseBuildStreamEvent,
} from '../lib/build-log';
import { appendLogLines, formatLogTime } from '../lib/log-buffer';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';

/** A request to scroll the tape to the first line of one BuildKit step. */
export interface BuildJumpRequest {
  readonly vertex: number;
  /** Changes on every click, so clicking the same step again jumps again. */
  readonly nonce: number;
}

type StreamState =
  | 'connecting'
  | 'live'
  | 'reconnecting'
  | 'reading-pages'
  | 'stopped'
  | 'unreachable'
  | 'error';

const statusWords: Record<StreamState, string> = {
  connecting: 'Connecting',
  live: 'Live',
  reconnecting: 'Reconnecting',
  'reading-pages': 'Reading the log',
  stopped: 'End of log',
  unreachable: 'Node unreachable',
  error: 'Stream stopped',
};

const statusTone = (state: StreamState) => {
  if (state === 'live') return 'run' as const;
  if (state === 'stopped') return 'idle' as const;
  if (state === 'connecting' || state === 'reconnecting' || state === 'reading-pages') {
    return 'shift' as const;
  }
  return 'fault' as const;
};

const RULING =
  'repeating-linear-gradient(to bottom, transparent 0, transparent 19px, rgb(20 32 48 / 0.045) 19px, rgb(20 32 48 / 0.045) 20px)';

const PAGE_LIMIT = 1_000;
const MAX_PAGES = 20;

/** Read every page the node still keeps when the live stream is unavailable. */
const readAllPages = async (buildId: string): Promise<BuildLogLine[]> => {
  const collected: BuildLogLine[] = [];
  let cursor: string | undefined;
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const answer = await getBuildLogs(buildId, { limit: PAGE_LIMIT, cursor });
    collected.push(...answer.items);
    if (answer.next_cursor === null) break;
    cursor = answer.next_cursor;
  }
  return collected;
};

/**
 * The build's log, live over SSE. The browser resumes with `Last-Event-ID`
 * by itself; `status` events move the rail, `end` closes the source, and an
 * `error` event is said in plain words. A finished build replays and ends;
 * when the stream cannot open (the node is unreachable) the same log is read
 * from the node's pages instead, and the failing step above stays visible
 * because it is replicated with the build.
 */
export function BuildLog({
  buildId,
  failingVertex,
  jumpRequest,
  onBuildStatus,
}: {
  buildId: string;
  failingVertex: number | null;
  jumpRequest: BuildJumpRequest | null;
  /** Every `status` event, so the rail moves without another read. */
  onBuildStatus: (build: Build) => void;
}) {
  const [lines, setLines] = useState<BuildLogLine[]>([]);
  const [state, setState] = useState<StreamState>('connecting');
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [follow, setFollow] = useState(true);
  const [paused, setPaused] = useState(false);
  const [attempt, setAttempt] = useState(0);

  const tapeRef = useRef<HTMLDivElement | null>(null);
  const lineRefs = useRef(new Map<number, HTMLDivElement>());
  const handledJump = useRef(-1);
  const pausedRef = useRef(false);
  const pendingRef = useRef<BuildLogLine[]>([]);
  const closedRef = useRef(false);

  useEffect(() => {
    setLines([]);
    setProblem(null);
    setNote(null);
    setState('connecting');
    setFollow(true);
    setPaused(false);
    pausedRef.current = false;
    pendingRef.current = [];
    closedRef.current = false;
    lineRefs.current.clear();
    handledJump.current = -1;

    let source: EventSource | null = null;
    let disposed = false;
    let opened = false;
    let fallbackTried = false;

    const close = () => {
      if (!source) return;
      source.removeEventListener('open', onOpen);
      source.removeEventListener('log', onLog);
      source.removeEventListener('status', onStatus);
      source.removeEventListener('error', onError);
      source.removeEventListener('end', onEnd);
      source.close();
      source = null;
    };

    const readPages = async () => {
      if (fallbackTried || disposed) return;
      fallbackTried = true;
      setState('reading-pages');
      try {
        const pageLines = await readAllPages(buildId);
        if (disposed) return;
        setProblem(null);
        setLines((current) => orderBuildLog(current, pageLines));
        setNote(
          'The live stream is closed, so these lines were read from the node. The full log stays on the node.',
        );
        setState('stopped');
      } catch (error) {
        if (disposed) return;
        const view = problemView(error, 'read build logs');
        setProblem(view);
        if (isApiProblem(error) && error.status === 504) {
          setState('unreachable');
          setNote(
            'The failing step above is replicated with the build; the full log stays on the node.',
          );
        } else {
          setState('error');
        }
      }
    };

    function onOpen() {
      opened = true;
      setState('live');
      setProblem(null);
    }

    const onLog = (event: Event) => {
      const message = event as MessageEvent<string>;
      const parsed = parseBuildStreamEvent({
        id: message.lastEventId,
        event: 'log',
        data: message.data,
      });
      if (parsed.kind !== 'log') return;
      if (pausedRef.current) {
        pendingRef.current = appendLogLines(pendingRef.current, [parsed.line]);
        return;
      }
      setLines((current) => orderBuildLog(current, [parsed.line]));
    };

    const onStatus = (event: Event) => {
      const message = event as MessageEvent<string>;
      const parsed = parseBuildStreamEvent({
        id: message.lastEventId,
        event: 'status',
        data: message.data,
      });
      if (parsed.kind === 'status') {
        setState('live');
        onBuildStatus(parsed.build);
      }
    };

    function onError(event: Event) {
      if (event instanceof MessageEvent && typeof event.data === 'string' && event.data !== '') {
        const parsed = parseBuildStreamEvent({ event: 'error', data: event.data });
        if (parsed.kind !== 'error') return;
        setProblem(parsed.problem);
        if (parsed.problem.code === 'node_unreachable') {
          close();
          setNote(
            'The failing step above is replicated with the build; the full log stays on the node.',
          );
          void readPages();
          return;
        }
        setState('error');
        closedRef.current = true;
        close();
        return;
      }
      if (closedRef.current || disposed) return;
      if (!opened) {
        close();
        void readPages();
        return;
      }
      setState('reconnecting');
      if (source && source.readyState === EventSource.CLOSED) {
        close();
        void readPages();
      }
    }

    function onEnd() {
      closedRef.current = true;
      setState('stopped');
      close();
    }

    source = new EventSource(buildLogStreamUrl(buildId), { withCredentials: true });
    source.addEventListener('open', onOpen);
    source.addEventListener('log', onLog);
    source.addEventListener('status', onStatus);
    source.addEventListener('error', onError);
    source.addEventListener('end', onEnd);

    return () => {
      disposed = true;
      close();
    };
  }, [buildId, attempt]);

  useEffect(() => {
    if (!follow || paused) return;
    const tape = tapeRef.current;
    if (tape) tape.scrollTop = tape.scrollHeight;
  }, [lines, follow, paused]);

  useEffect(() => {
    if (jumpRequest === null || jumpRequest.nonce === handledJump.current) return;
    const target = lines.find((line) => line.vertex === jumpRequest.vertex);
    if (!target) return;
    const element = lineRefs.current.get(target.seq);
    if (!element) return;
    handledJump.current = jumpRequest.nonce;
    setFollow(false);
    element.scrollIntoView({ block: 'center' });
  }, [jumpRequest, lines]);

  const togglePause = () => {
    const next = !paused;
    pausedRef.current = next;
    setPaused(next);
    if (!next && pendingRef.current.length > 0) {
      const queued = pendingRef.current;
      pendingRef.current = [];
      setLines((current) => orderBuildLog(current, queued));
    }
  };

  const jumpToNewest = () => {
    setFollow(true);
    const tape = tapeRef.current;
    if (tape) tape.scrollTop = tape.scrollHeight;
  };

  const caption = statusWords[state];

  return (
    <section>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        <div className="flex items-center gap-2">
          <ToneLamp
            tone={statusTone(state)}
            pulse={state === 'connecting' || state === 'reading-pages'}
            size={18}
            label={statusWords[state]}
          />
          <span className="type-body-medium text-[13px] text-ink" aria-live="polite">
            {caption}
          </span>
        </div>

        <div className="ml-auto flex flex-wrap items-center gap-1.5">
          {!follow ? (
            <Button variant="quiet" className="px-2.5" onClick={jumpToNewest}>
              Jump to newest
            </Button>
          ) : null}
          <Button
            variant="quiet"
            className="px-2.5"
            aria-pressed={follow}
            onClick={() => setFollow((current) => !current)}
          >
            Follow
          </Button>
          <Button
            variant="quiet"
            className="px-2.5"
            aria-pressed={paused}
            onClick={togglePause}
          >
            {paused ? 'Resume' : 'Pause'}
          </Button>
          <span className="type-data ml-1 text-[13px] text-ink-muted">
            {lines.length} {lines.length === 1 ? 'line' : 'lines'}
          </span>
        </div>
      </div>

      {problem ? (
        <div className="mt-2 flex flex-wrap items-start gap-2">
          <ProblemBanner view={problem} className="min-w-0 flex-1" />
          {state === 'error' ? (
            <Button variant="quiet" className="px-2.5" onClick={() => setAttempt((n) => n + 1)}>
              Reconnect
            </Button>
          ) : null}
        </div>
      ) : null}
      {note ? (
        <p className="mt-2 text-[13px] leading-snug text-ink-muted">{note}</p>
      ) : null}

      <div className="mt-2 rounded-[6px] border border-rail bg-recess p-1.5 shadow-[inset_0_2px_6px_rgb(20_32_48_/_0.18)]">
        <div
          ref={tapeRef}
          role="log"
          aria-live={paused ? 'off' : 'polite'}
          aria-label="Build log"
          onScroll={(event) => {
            const element = event.currentTarget;
            const atBottom =
              element.scrollHeight - element.scrollTop - element.clientHeight < 24;
            setFollow(atBottom);
          }}
          className="h-[320px] overflow-y-auto bg-panel px-2 py-2 sm:h-[420px]"
          style={{ backgroundImage: RULING }}
        >
          {lines.length === 0 ? (
            <p className="px-1 py-2 text-[13px] leading-snug text-ink-muted">
              {state === 'unreachable'
                ? 'The node keeps this log; it cannot be read while the node is unreachable.'
                : 'Waiting for build output. BuildKit lines appear here as the node writes them.'}
            </p>
          ) : (
            lines.map((entry) => {
              const failing =
                failingVertex !== null && entry.vertex === failingVertex;
              return (
                <div
                  key={entry.seq}
                  ref={(element) => {
                    if (element) lineRefs.current.set(entry.seq, element);
                    else lineRefs.current.delete(entry.seq);
                  }}
                  className={cn(
                    'relative grid grid-cols-[76px_34px_minmax(0,1fr)] items-baseline gap-2 py-px leading-5',
                    entry.stream === 'stderr' && 'pl-2',
                    failing && 'bg-lamp-fault/[0.08]',
                  )}
                >
                  {entry.stream === 'stderr' || failing ? (
                    <span
                      aria-hidden="true"
                      className={cn(
                        'absolute bottom-0 left-0 top-0 w-[3px]',
                        failing ? 'bg-lamp-fault' : 'bg-lamp-fault/50',
                      )}
                    />
                  ) : null}
                  <span
                    className="type-data truncate text-[12px] text-ink-muted"
                    title={entry.time}
                  >
                    {formatLogTime(entry.time)}
                  </span>
                  <span className="type-data text-[11px] text-ink-muted/80">
                    {entry.vertex === null || entry.vertex === undefined
                      ? ' '
                      : `#${entry.vertex}`}
                  </span>
                  <span className="whitespace-pre-wrap break-words font-mono text-[12.5px] text-ink">
                    {entry.line}
                  </span>
                </div>
              );
            })
          )}
        </div>
      </div>
    </section>
  );
}
