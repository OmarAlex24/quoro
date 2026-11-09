// SPDX-License-Identifier: AGPL-3.0-only
import { Check, X } from 'lucide-react';
import { useCallback, type CSSProperties } from 'react';
import {
  formatClock,
  formatSpan,
  TAPE_STEP_LABELS,
  type BuildkitStep,
  type Estimate,
  type MiniSegment,
  type NodeLineSpan,
  type RunTimeline,
  type StepSpan,
  type StepState,
} from '../lib/deploy-tape';
import {
  drawStepBar,
  drawTapeSegment,
  type DitherSurface,
  type TapeTone,
  type TokenReader,
} from '../lib/dither-draw';
import { cn } from '../lib/utils';
import { DitherCanvas } from './dither-canvas';
import { LampDot } from './lamp';
import { Tag } from './ui/tag';

const STATE_WORDS: Record<StepState, string> = {
  done: 'done',
  live: 'in progress',
  failed: 'failed',
  stopped: 'stopped',
  pending: 'not reached yet',
  skipped: 'no record',
};

const STEP_CLASS: Record<StepState, string> = {
  done: 'stp-done',
  live: 'stp-live',
  failed: 'stp-fail',
  stopped: 'stp-stop',
  pending: 'stp-next',
  skipped: 'stp-skip',
};

const stepValue = (step: StepSpan, typical: number | undefined): string => {
  switch (step.state) {
    case 'done':
    case 'failed':
    case 'stopped':
      return step.seconds === null ? '—' : formatSpan(step.seconds);
    case 'live':
      return formatClock(step.seconds ?? 0);
    case 'pending':
      return typical === undefined ? 'Waiting' : `about ${formatSpan(typical)}`;
    case 'skipped':
      return '—';
  }
};

/**
 * The run's steps side by side, equal widths, so their words never collide:
 * done ones with a check and their span, the running one lit amber with its
 * clock, the ones still to come with what they usually take.
 */
export function Stepper({ timeline, estimate }: { timeline: RunTimeline; estimate: Estimate | null }) {
  const steps = timeline.steps;
  return (
    <ol
      className="stepper"
      data-n={steps.length}
      style={{ '--n': steps.length } as CSSProperties}
      aria-label="Steps"
    >
      {steps.map((step) => {
        const label = TAPE_STEP_LABELS[step.step];
        const value = stepValue(step, estimate?.steps[step.step]);
        return (
          <li
            key={step.step}
            className={cn('stp', STEP_CLASS[step.state])}
            aria-current={step.state === 'live' ? 'step' : undefined}
          >
            {step.state === 'live' ? <span className="sweep" aria-hidden="true" /> : null}
            <span className="stp-n">
              {step.state === 'done' ? <Check strokeWidth={2} aria-hidden="true" /> : null}
              {step.state === 'failed' ? <X strokeWidth={2} aria-hidden="true" /> : null}
              {step.state === 'live' ? (
                <LampDot ring="shift" core="shift" pulse size={8} />
              ) : null}
              {label}
              <span className="sr-only">, {STATE_WORDS[step.state]}</span>
            </span>
            <span className="stp-v">{value}</span>
          </li>
        );
      })}
    </ol>
  );
}

const tapeTone = (state: StepState): TapeTone =>
  state === 'live' ? 'live' : state === 'failed' ? 'failed' : state === 'stopped' ? 'stopped' : 'done';

/** Shares are rounded so a running tape repaints its neighbours only when they move. */
const round = (value: number) => Math.round(value * 500) / 500;

function TapeSegment({ tone, from, to, grow }: { tone: TapeTone; from: number; to: number; grow: number }) {
  const start = round(from);
  const end = round(to);
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawTapeSegment(surface, tone, start, end, read),
    [tone, start, end],
  );
  return (
    <span className="tape-seg" style={{ flexGrow: grow }}>
      <DitherCanvas paint={paint} />
    </span>
  );
}

/**
 * Where the time went, to scale: one dithered segment per recorded step, and
 * dotted after it what the estimate says is left. Nothing is drawn for a run
 * that recorded no time.
 */
export function Tape({
  timeline,
  remaining,
  estimate,
}: {
  timeline: RunTimeline;
  remaining: number | null;
  estimate: Estimate | null;
}) {
  const segments = timeline.steps.filter((step) => step.seconds !== null && step.seconds > 0);
  const recorded = segments.reduce((total, step) => total + (step.seconds ?? 0), 0);
  const left = remaining !== null && remaining > 0 ? remaining : 0;
  const total = recorded + left;
  if (recorded <= 0) return null;

  const summary = segments
    .map((step) => `${TAPE_STEP_LABELS[step.step]} ${formatSpan(step.seconds ?? 0)}`)
    .join(', ');
  let cursor = 0;
  return (
    <>
      <div
        className="tape"
        role="img"
        aria-label={`Where the time went: ${summary}${left > 0 ? `, about ${formatSpan(left)} left` : ''}`}
      >
        {segments.map((step) => {
          const from = cursor / total;
          cursor += step.seconds ?? 0;
          return (
            <TapeSegment
              key={step.step}
              tone={tapeTone(step.state)}
              from={from}
              to={cursor / total}
              grow={step.seconds ?? 0}
            />
          );
        })}
        {left > 0 ? <span className="tape-ghost d25" style={{ flexGrow: left }} /> : null}
      </div>
      <div className="tape-k">
        <span>Where the time went, to scale</span>
        {left > 0 && estimate ? (
          <span>
            Dotted: what is left, from the last {estimate.samples}{' '}
            {estimate.samples === 1 ? 'deploy' : 'deploys'}
          </span>
        ) : null}
      </div>
    </>
  );
}

function StepBar({ share, tone }: { share: number; tone: TapeTone }) {
  const value = round(share);
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawStepBar(surface, value, tone, read),
    [value, tone],
  );
  return <DitherCanvas paint={paint} />;
}

/**
 * Where the build's time went: its Dockerfile steps, and in between, in the
 * order they ran, the node's own lines that took a second or more (pulling
 * the builder image, starting the builder VM), each with its share of the
 * build and its span. The node's lines are its own words, in the text face.
 */
export function BuildSteps({
  steps,
  nodeLines = [],
}: {
  steps: readonly BuildkitStep[];
  nodeLines?: readonly NodeLineSpan[];
}) {
  if (steps.length === 0 && nodeLines.length === 0) return null;
  const rows = [
    ...nodeLines.map((span) => ({ kind: 'node' as const, start: span.start, span })),
    ...steps.map((step) => ({ kind: 'step' as const, start: step.start, step })),
  ].sort((left, right) => left.start - right.start);
  return (
    <div className="bsteps">
      <span className="bsteps-h">Build steps</span>
      {rows.map((row) =>
        row.kind === 'node' ? (
          <div key={`line-${row.span.seq}`} className="bs">
            <span className="bs-name bs-node" title={row.span.line}>
              {row.span.line.charAt(0).toUpperCase() + row.span.line.slice(1)}
            </span>
            <StepBar share={row.span.share} tone={tapeTone(row.span.state)} />
            <span className="bs-d">
              {row.span.state === 'live' ? formatClock(row.span.seconds) : formatSpan(row.span.seconds)}
            </span>
          </div>
        ) : (
          <div key={row.step.vertex} className="bs">
            <span className="bs-name" title={row.step.name}>
              {row.step.name}
            </span>
            <StepBar share={row.step.share} tone={tapeTone(row.step.state)} />
            <span className={cn('bs-d', row.step.state === 'failed' && 'text-fault')}>
              {row.step.cached ? (
                <Tag tone="plain">cached</Tag>
              ) : row.step.state === 'live' ? (
                formatClock(row.step.seconds)
              ) : (
                formatSpan(row.step.seconds)
              )}
            </span>
          </div>
        ),
      )}
    </div>
  );
}

const MINI_CLASS: Record<MiniSegment['tone'], string | undefined> = {
  done: undefined,
  live: 'mt-l',
  failed: 'mt-f',
  stopped: 'mt-s',
  rest: 'mt-g',
};

/** A history row's tape: its steps on the scale every row of the list shares. */
export function MiniTape({ segments, label }: { segments: readonly MiniSegment[]; label: string }) {
  if (segments.length === 0) return <span className="minitape" aria-hidden="true" />;
  return (
    <span className="minitape" role="img" aria-label={label} title={label}>
      {segments.map((segment, index) => (
        <i key={index} className={MINI_CLASS[segment.tone]} style={{ flexGrow: segment.grow }} />
      ))}
    </span>
  );
}
