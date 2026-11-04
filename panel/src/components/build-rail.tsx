// SPDX-License-Identifier: AGPL-3.0-only
import type { Build } from '../api/generated/model';
import { stationTone } from '../lib/app-status';
import {
  BUILD_PHASE_LABELS,
  buildRail,
} from '../lib/build-status';
import { ToneLamp } from './tone-lamp';

const stationWords = {
  done: 'Done',
  current: 'In progress',
  failed: 'Failed',
  'not-reached': 'Not reached',
} as const;

/** One station of a build rail, named so the phase is not colour alone. */
export function BuildStationList({ build }: { build: Build }) {
  const rail = buildRail(build);

  return (
    <div>
      <div className="relative pb-1">
        <div
          aria-hidden="true"
          className="absolute left-0 right-0 top-[10px] hidden h-px bg-rail sm:block"
        />
        <ol className="relative grid grid-cols-3 gap-x-2 gap-y-4 sm:grid-cols-6">
          {rail.stations.map((station) => (
            <li
              key={station.phase}
              className="flex min-w-0 flex-col items-start gap-1.5"
              aria-current={station.state === 'current' ? 'step' : undefined}
            >
              <ToneLamp
                tone={stationTone(station.state)}
                pulse={station.state === 'current'}
                size={21}
                label={`${BUILD_PHASE_LABELS[station.phase]}: ${stationWords[station.state]}`}
              />
              <span className="type-body-medium text-[13px] leading-tight text-ink">
                {BUILD_PHASE_LABELS[station.phase]}
              </span>
              <span className="text-[12px] leading-tight text-ink-muted">
                {stationWords[station.state]}
              </span>
            </li>
          ))}
        </ol>
      </div>

      {rail.caption ? (
        <p className="mt-3 text-[13px] text-ink-muted">{rail.caption}</p>
      ) : null}
    </div>
  );
}
