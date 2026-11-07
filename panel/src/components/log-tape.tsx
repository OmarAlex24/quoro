import { useEffect, useRef, useState } from 'react';
import type { LogEntry } from '../api/generated/model';
import {
  appendLogLines,
  formatLogTime,
  isTransientProblem,
  problemViewFromEvent,
} from '../lib/log-buffer';
import type { ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';

type TapeStatus = 'connecting' | 'live' | 'reconnecting' | 'error' | 'stopped';

const statusTone = (status: TapeStatus) => {
  if (status === 'live') return 'run' as const;
  if (status === 'connecting' || status === 'reconnecting') return 'shift' as const;
  return 'fault' as const;
};

const statusWords: Record<TapeStatus, string> = {
  connecting: 'Connecting',
  live: 'Live',
  reconnecting: 'Reconnecting',
  error: 'Stream stopped',
  stopped: 'Stopped',
};

const RULING =
  'repeating-linear-gradient(to bottom, transparent 0, transparent 19px, color-mix(in oklab, var(--line) 55%, transparent) 19px, color-mix(in oklab, var(--line) 55%, transparent) 20px)';

/**
 * The recorder tape: a recessed bay holding light paper with a faint ruling
 * per line. The time column stays in the panel's data face; only the log line
 * itself uses a monospace stack.
 */
export function LogTape({ appId }: { appId: string }) {
  const [lines, setLines] = useState<LogEntry[]>([]);
  const [status, setStatus] = useState<TapeStatus>('connecting');
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [follow, setFollow] = useState(true);
  const [paused, setPaused] = useState(false);

  const tapeRef = useRef<HTMLDivElement | null>(null);
  const pausedRef = useRef(false);
  const pendingRef = useRef<LogEntry[]>([]);
  const closedRef = useRef(false);

  useEffect(() => {
    setLines([]);
    setProblem(null);
    setStatus('connecting');
    setFollow(true);
    setPaused(false);
    pausedRef.current = false;
    pendingRef.current = [];
    closedRef.current = false;

    // The browser retries a dropped stream by itself, but gives up for good
    // when a retry fails before any response (for example while the control
    // plane restarts). Then the tape opens a new stream with a backoff of its
    // own and drops the replayed tail it already shows.
    let source: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let retryDelay = 1000;
    let lastTime = '';
    let disposed = false;

    const onOpen = () => {
      closedRef.current = false;
      retryDelay = 1000;
      setStatus('live');
      setProblem(null);
    };

    const onLog = (event: Event) => {
      const message = event as MessageEvent<string>;
      try {
        const entry = JSON.parse(message.data) as LogEntry;
        // Times are RFC 3339 UTC with nine fractional digits, so they order
        // as strings; anything at or before the last shown line is a replay.
        if (entry.time <= lastTime) return;
        lastTime = entry.time;
        if (pausedRef.current) {
          pendingRef.current = appendLogLines(pendingRef.current, [entry]);
          return;
        }
        setLines((current) => appendLogLines(current, [entry]));
      } catch {
        // A malformed line is dropped; the stream itself keeps going.
      }
    };

    const close = () => {
      if (!source) return;
      source.removeEventListener('open', onOpen);
      source.removeEventListener('log', onLog);
      source.removeEventListener('error', onError);
      source.removeEventListener('end', onEnd);
      source.close();
      source = null;
    };

    function onError(event: Event) {
      if (event instanceof MessageEvent && typeof event.data === 'string' && event.data !== '') {
        const view = problemViewFromEvent(event.data);
        setProblem(view);
        setStatus('error');
        if (!isTransientProblem(view)) {
          closedRef.current = true;
          close();
        }
        return;
      }
      if (closedRef.current || disposed) return;
      setStatus('reconnecting');
      if (source && source.readyState === EventSource.CLOSED) {
        close();
        retryTimer = setTimeout(connect, retryDelay);
        retryDelay = Math.min(retryDelay * 2, 15000);
      }
    }

    function onEnd() {
      closedRef.current = true;
      setStatus('stopped');
      close();
    }

    function connect() {
      if (disposed) return;
      source = new EventSource(`/api/v1/apps/${appId}/logs/stream?tail=200`, {
        withCredentials: true,
      });
      source.addEventListener('open', onOpen);
      source.addEventListener('log', onLog);
      source.addEventListener('error', onError);
      source.addEventListener('end', onEnd);
    }

    connect();

    return () => {
      disposed = true;
      clearTimeout(retryTimer);
      close();
    };
  }, [appId]);

  useEffect(() => {
    if (!follow || paused) return;
    const tape = tapeRef.current;
    if (tape) tape.scrollTop = tape.scrollHeight;
  }, [lines, follow, paused]);

  const togglePause = () => {
    const next = !paused;
    pausedRef.current = next;
    setPaused(next);
    if (!next && pendingRef.current.length > 0) {
      const queued = pendingRef.current;
      pendingRef.current = [];
      setLines((current) => appendLogLines(current, queued));
    }
  };

  const jumpToNewest = () => {
    setFollow(true);
    const tape = tapeRef.current;
    if (tape) tape.scrollTop = tape.scrollHeight;
  };

  const caption = status === 'error' && problem ? problem.message : statusWords[status];

  return (
    <section>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        <div className="flex items-center gap-2">
          <ToneLamp
            tone={statusTone(status)}
            pulse={status === 'connecting'}
            size={18}
            label={statusWords[status]}
          />
          <span className="type-body-medium text-[13px] text-ink">{caption}</span>
        </div>

        <div className="ml-auto flex flex-wrap items-center gap-1.5">
          {!follow ? (
            <Button variant="ghost" className="px-2.5" onClick={jumpToNewest}>
              Jump to newest
            </Button>
          ) : null}
          <Button
            variant="ghost"
            className="px-2.5"
            aria-pressed={follow}
            onClick={() => setFollow((current) => !current)}
          >
            Follow
          </Button>
          <Button
            variant="ghost"
            className="px-2.5"
            aria-pressed={paused}
            onClick={togglePause}
          >
            {paused ? 'Resume' : 'Pause'}
          </Button>
          <span className="type-data ml-1 text-[13px] text-ink-2">
            {lines.length} {lines.length === 1 ? 'line' : 'lines'}
          </span>
        </div>
      </div>

      <div className="mt-2 rounded-[10px] border border-line bg-sunk p-1.5">
        <div
          ref={tapeRef}
          role="log"
          aria-live={paused ? 'off' : 'polite'}
          aria-label="Live app logs"
          onScroll={(event) => {
            const element = event.currentTarget;
            const atBottom =
              element.scrollHeight - element.scrollTop - element.clientHeight < 24;
            setFollow(atBottom);
          }}
          className="h-[280px] overflow-y-auto bg-surface px-2 py-2 sm:h-[360px]"
          style={{ backgroundImage: RULING }}
        >
          {lines.length === 0 ? (
            <p className="px-1 py-2 text-[13px] leading-snug text-ink-2">
              Waiting for output. Lines appear here as the app writes to stdout or stderr.
            </p>
          ) : (
            lines.map((entry, index) => (
              <div
                key={`${entry.time}-${entry.vm_id}-${index}`}
                className={cn(
                  'relative grid grid-cols-[84px_minmax(0,1fr)] items-baseline gap-2 leading-5',
                  entry.stream === 'stderr' && 'pl-2',
                )}
              >
                {entry.stream === 'stderr' ? (
                  <span
                    aria-hidden="true"
                    className="absolute bottom-0 left-0 top-0 w-[3px] bg-fault"
                  />
                ) : null}
                <span
                  className="type-data truncate text-[12px] text-ink-2"
                  title={entry.time}
                >
                  {formatLogTime(entry.time)}
                </span>
                <span className="type-code whitespace-pre-wrap break-words text-[11.5px] text-ink">
                  {entry.line}
                </span>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
  );
}
