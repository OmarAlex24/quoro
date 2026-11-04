// SPDX-License-Identifier: AGPL-3.0-only
import type { Build } from '../api/generated/model';
import { failingLineIndex } from '../lib/build-log';
import { BUILD_FAILED_PHASE_LABELS } from '../lib/build-status';
import { cn } from '../lib/utils';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';

/**
 * The failing step, taken from the build record itself (ADR 0119): the phase,
 * the BuildKit step, its error and the Dockerfile excerpt with the failing
 * line at full contrast. It stays readable while the node that ran the build
 * is down, so it sits above the log and never depends on it.
 */
export function BuildFailure({
  build,
  onJump,
}: {
  build: Build;
  onJump: (vertex: number) => void;
}) {
  const step = build.failed_step;
  const phase = build.failed_phase;
  const excerpt = (step?.excerpt ?? [])
    .slice(0, 20)
    .map((line) => line.slice(0, 500));
  const failing = failingLineIndex(excerpt, step?.dockerfile_line ?? null);
  const error = build.error === null ? null : build.error.slice(0, 2_000);
  const stepName = step === null ? null : step.name.slice(0, 500);

  return (
    <section
      aria-labelledby="build-failure-title"
      className="mt-4 rounded-[6px] border border-rail border-l-4 border-l-lamp-fault bg-panel px-4 py-4 bay-shadow sm:px-5"
    >
      <div className="flex flex-wrap items-center gap-2.5">
        <ToneLamp tone="fault" size={20} label="The build failed" />
        <h2
          id="build-failure-title"
          className="type-display-light text-[15px] leading-none text-ink"
        >
          {phase === null
            ? 'The build failed'
            : `Failed while ${BUILD_FAILED_PHASE_LABELS[phase]}`}
        </h2>
      </div>

      {error ? (
        <p className="mt-3 text-[15px] leading-snug text-ink">{error}</p>
      ) : (
        <p className="mt-3 text-[15px] leading-snug text-ink-muted">
          The build stopped without a one-line reason.
        </p>
      )}

      {step ? (
        <>
          <dl className="type-data mt-3 flex flex-wrap gap-x-6 gap-y-1 text-[13px]">
            <div className="flex min-w-0 gap-2">
              <dt className="text-ink-muted">Step</dt>
              <dd className="min-w-0 break-words text-ink">{stepName}</dd>
            </div>
            <div className="flex gap-2">
              <dt className="text-ink-muted">Dockerfile line</dt>
              <dd className="text-ink">
                {step.dockerfile_line === null
                  ? 'not named'
                  : step.dockerfile_line}
              </dd>
            </div>
          </dl>

          {excerpt.length > 0 ? (
            <div className="mt-3 overflow-x-auto rounded-[4px] border border-rail bg-recess/60 px-3 py-2">
              <pre className="m-0 font-mono text-[12.5px] leading-5 text-ink-muted">
                {excerpt.map((line, index) => (
                  <div
                    key={index}
                    className={cn(
                      'whitespace-pre',
                      index === failing &&
                        'relative -mx-1 my-0.5 bg-lamp-fault/10 px-1 font-medium text-ink',
                    )}
                  >
                    {index === failing ? (
                      <span
                        aria-hidden="true"
                        className="absolute bottom-0 left-0 top-0 w-[3px] bg-lamp-fault"
                      />
                    ) : null}
                    {line}
                    {index === failing ? (
                      <span className="sr-only"> (the failing line)</span>
                    ) : null}
                  </div>
                ))}
              </pre>
            </div>
          ) : (
            <p className="mt-3 text-[13px] text-ink-muted">
              BuildKit kept no excerpt of the step.
            </p>
          )}

          <div className="mt-3 flex flex-wrap items-center gap-2">
            <Button variant="quiet" className="px-2.5" onClick={() => onJump(step.vertex)}>
              Jump to step #{step.vertex}
            </Button>
            <span className="text-[13px] text-ink-muted">
              Scrolls the log to the first line of that step.
            </span>
          </div>
        </>
      ) : (
        <p className="mt-3 text-[13px] leading-snug text-ink-muted">
          BuildKit named no failing step, so there is no Dockerfile excerpt.
          {build.failed_phase === 'preparing'
            ? ' The failure happened before the build started.'
            : null}
        </p>
      )}
    </section>
  );
}
