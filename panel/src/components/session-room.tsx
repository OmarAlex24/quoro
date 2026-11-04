import { useMutation } from '@tanstack/react-query';
import { useState, type ReactNode } from 'react';
import { endInteractiveSession } from '../api/generated/interactive/interactive';
import type { InteractiveSessionTicket } from '../api/generated/model';
import { isApiProblem } from '../api/fetcher';
import {
  isSessionLive,
  sessionStateText,
  type SessionViewState,
} from '../lib/interactive';
import type { LampTone } from '../lib/lamp';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';
import { ToneLamp } from './tone-lamp';
import { XtermView } from './xterm-view';

const stateTone = (state: SessionViewState | null): LampTone => {
  if (state === null) return 'idle';
  if (state.phase === 'live') return 'run';
  if (state.phase === 'connecting') return 'shift';
  if (state.phase === 'error') return 'fault';
  return 'idle';
};

const shortSessionId = (id: string) => `${id.slice(0, 10)}…`;

export interface SessionRoomProps {
  kind: InteractiveSessionTicket['kind'];
  /** "Web terminal" or "Serial console", engraved on the plate. */
  eyebrow: string;
  /** The app or VM name. */
  title: string;
  description: string;
  /** The breadcrumb back to the app or the VM rack. */
  back: ReactNode;
  /** POST the open call; it resolves with a one-shot ticket. */
  connect: () => Promise<InteractiveSessionTicket>;
  canConnect: boolean;
  /** Why this identity cannot open it, shown in place of the controls. */
  deniedMessage: string;
}

/**
 * The room around one terminal or console session: the plate with the name and
 * the two actions, the dark bezel, and one status line. The ticket stays inside
 * this component: it is passed to `XtermView` and never rendered.
 */
export function SessionRoom({
  kind,
  eyebrow,
  title,
  description,
  back,
  connect,
  canConnect,
  deniedMessage,
}: SessionRoomProps) {
  const [ticket, setTicket] = useState<InteractiveSessionTicket | null>(null);
  const [view, setView] = useState<SessionViewState | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const open = useMutation({
    mutationFn: connect,
    onSuccess: (opened) => {
      setProblem(null);
      setView({ phase: 'connecting' });
      setTicket(opened);
    },
    onError: (error) => {
      setTicket(null);
      setView(null);
      setProblem(problemView(error, kind === 'terminal' ? 'open a terminal' : 'open the console'));
    },
  });

  const end = useMutation({
    mutationFn: (sessionId: string) => endInteractiveSession(sessionId),
    onSuccess: () => {
      setTicket(null);
      setProblem(null);
      setView({ phase: 'closed', reason: 'ended_by_request' });
    },
    onError: (error) => {
      // A session that already ended answers 404; that is the state we wanted.
      if (isApiProblem(error) && error.status === 404) {
        setTicket(null);
        setProblem(null);
        setView({ phase: 'closed', reason: 'ended_by_request' });
        return;
      }
      setProblem(problemView(error, 'end the session'));
    },
  });

  const live = isSessionLive(view);
  const status = sessionStateText(view);
  const actionWords = view === null || live ? 'Connect' : 'Reconnect';

  return (
    <main className="mx-auto w-full max-w-5xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="mb-3">{back}</div>

      <header className="rounded-[6px] border border-rail bg-panel px-4 py-4 bay-shadow sm:px-5">
        <div className="flex flex-wrap items-start gap-x-4 gap-y-3">
          <div className="min-w-0 flex-1">
            <p className="type-display-light text-[12px] leading-none text-ink-muted">
              {eyebrow}
            </p>
            <div className="mt-1.5 flex flex-wrap items-center gap-2.5">
              <ToneLamp tone={stateTone(view)} pulse={view?.phase === 'connecting'} label={status} />
              <h1 className="type-display nameplate truncate text-[24px] leading-none">
                {title}
              </h1>
            </div>
            <p className="mt-1.5 max-w-2xl text-[13px] leading-snug text-ink-muted">
              {description}
            </p>
          </div>

          {canConnect ? (
            <div className="flex flex-wrap items-center gap-2">
              {ticket && live ? (
                <Button
                  variant="secondary"
                  disabled={end.isPending}
                  onClick={() => end.mutate(ticket.id)}
                >
                  {end.isPending ? 'Ending…' : 'End session'}
                </Button>
              ) : (
                <Button
                  variant="primary"
                  disabled={open.isPending}
                  onClick={() => open.mutate()}
                >
                  {open.isPending ? 'Opening…' : actionWords}
                </Button>
              )}
            </div>
          ) : null}
        </div>
      </header>

      {!canConnect ? (
        <p className="mt-3 rounded-[6px] border border-rail bg-panel px-3.5 py-2.5 text-[13px] leading-snug text-ink">
          {deniedMessage}
        </p>
      ) : null}

      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      <section className="mt-4">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-0.5">
          <span
            aria-hidden="true"
            className={cn(
              'inline-block h-2.5 w-2.5 border-2',
              view?.phase === 'live'
                ? 'border-lamp-run bg-lamp-run'
                : view?.phase === 'error'
                  ? 'border-lamp-fault bg-lamp-fault'
                  : view?.phase === 'connecting'
                    ? 'border-lamp-shift'
                    : 'border-lamp-idle',
            )}
          />
          <span className="type-body-medium text-[13px] text-ink">{status}</span>
          {ticket ? (
            <span className="type-data ml-auto text-[12px] text-ink-muted" title={ticket.id}>
              {kind === 'terminal' ? 'terminal' : 'console'} {shortSessionId(ticket.id)}
            </span>
          ) : null}
        </div>

        <div className="mt-2 overflow-hidden rounded-[6px] border border-rail bg-[#0f1a26] p-1.5 shadow-[inset_0_2px_8px_rgb(0_0_0_/_0.35)]">
          {ticket ? (
            <XtermView ticket={ticket} kind={kind} onState={setView} />
          ) : (
            <div className="flex h-[300px] flex-col items-center justify-center gap-2 px-6 text-center sm:h-[420px]">
              <p className="text-[15px] text-[#dce1e5]">
                {view?.phase === 'closed' || view?.phase === 'exited'
                  ? 'The session has ended.'
                  : kind === 'terminal'
                    ? 'No terminal is open.'
                    : 'No console is attached.'}
              </p>
              <p className="max-w-md text-[13px] leading-snug text-[#7d8b9a]">
                {kind === 'terminal'
                  ? 'Connect to run a shell inside the app’s guest. The guest renders only through xterm.js.'
                  : 'Connect to follow the VM’s serial console. It is read-only: kernel and init output, no input.'}
              </p>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
