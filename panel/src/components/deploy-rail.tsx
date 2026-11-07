import type { Deployment } from '../api/generated/model';
import {
  DEPLOY_PHASE_LABELS,
  deploymentRail,
  shortDeploymentId,
  stationTone,
} from '../lib/app-status';
import { ImageRef } from './image-ref';
import { ToneLamp } from './tone-lamp';

const stationWords = {
  done: 'Done',
  current: 'In progress',
  failed: 'Failed',
  'not-reached': 'Not reached',
} as const;

/**
 * The four stations of a rollout joined by one rail. They are a real sequence,
 * so the order carries the meaning and the stations are never numbered.
 */
export function DeployRail({
  deployment,
  error,
}: {
  deployment: Deployment;
  error?: string | null;
}) {
  const rail = deploymentRail(deployment);

  return (
    <div>
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <p className="type-data text-[13px] text-ink-2">
          Deployment{' '}
          <span className="type-code" title={deployment.id}>
            {shortDeploymentId(deployment.id)}
          </span>{' '}
          ·{' '}
          <ImageRef image={deployment.image} />
        </p>
        <p className="type-data text-[13px] text-ink-2">{deployment.status}</p>
      </div>

      <div className="relative mt-4 pb-1">
        <div
          aria-hidden="true"
          className="absolute left-0 right-0 top-[10px] h-px bg-line"
        />
        <ol className="relative grid grid-cols-4 gap-2">
          {rail.stations.map((station) => (
            <li key={station.phase} className="flex min-w-0 flex-col items-start gap-1.5">
              <ToneLamp
                tone={stationTone(station.state)}
                pulse={station.state === 'current'}
                size={21}
                label={`${DEPLOY_PHASE_LABELS[station.phase]}: ${stationWords[station.state]}`}
              />
              <span className="type-body-medium text-[13px] leading-tight text-ink">
                {DEPLOY_PHASE_LABELS[station.phase]}
              </span>
              <span className="text-[12px] leading-tight text-ink-2">
                {stationWords[station.state]}
              </span>
              {station.state === 'failed' && error ? (
                <p className="text-[12px] leading-snug text-fault">{error}</p>
              ) : null}
            </li>
          ))}
        </ol>
      </div>

      {rail.caption ? (
        <p className="mt-3 text-[13px] text-ink-2">{rail.caption}</p>
      ) : null}
      {deployment.status === 'failed' && !error ? (
        <p className="mt-3 text-[13px] text-fault">
          The deployment failed before it reported which step broke.
        </p>
      ) : null}
    </div>
  );
}
