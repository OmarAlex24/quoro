// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useRef, useState } from 'react';
import type { Build } from '../api/generated/model';
import { cancelBuild } from '../api/generated/builds/builds';
import {
  buildDurationLabel,
  buildLampState,
  builtImageLabel,
  isBuildTerminal,
  shortBuildId,
  shortCommit,
} from '../lib/build-status';
import { shortDeploymentId } from '../lib/app-status';
import { problemView, type ProblemView } from '../lib/problem';
import { BuildFailure } from './build-failure';
import { BuildLog, type BuildJumpRequest } from './build-log';
import { BuildStationList } from './build-rail';
import { ImageRef } from './image-ref';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';

/**
 * One build end to end: its status plate, the phase rail, the failing step
 * (replicated with the build) and the live log read from the node. `status`
 * events from the stream move the plate without waiting for a poll.
 */
export function BuildView({
  build,
  appId,
  appName,
  canWrite,
  onBuildChanged,
}: {
  build: Build;
  appId: string;
  appName: string;
  canWrite: boolean;
  onBuildChanged: (build: Build) => void;
}) {
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [jumpRequest, setJumpRequest] = useState<BuildJumpRequest | null>(null);
  const jumpNonce = useRef(0);

  const lamp = buildLampState(build.status);
  const terminal = isBuildTerminal(build.status);
  const builtImage = build.image === null ? null : builtImageLabel(build.image);

  const cancel = useMutation({
    mutationFn: () => cancelBuild(build.id),
    onSuccess: (next) => {
      setProblem(null);
      setNotice('Cancel sent to the node');
      onBuildChanged(next);
    },
    onError: (error) => {
      setNotice(null);
      setProblem(problemView(error, 'cancel builds'));
    },
  });

  const jumpToStep = (vertex: number) => {
    jumpNonce.current += 1;
    setJumpRequest({ vertex, nonce: jumpNonce.current });
  };

  return (
    <div>
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2.5">
            <ToneLamp tone={lamp.tone} pulse={lamp.pulse} label={lamp.statusText} />
            <span className="type-body-medium text-[15px] text-ink" aria-live="polite">
              {lamp.statusText}
            </span>
            <span className="text-[13px] text-ink-2">{lamp.caption}</span>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {!terminal && canWrite ? (
            <Button
              variant="danger"
              disabled={cancel.isPending}
              onClick={() => cancel.mutate()}
            >
              {cancel.isPending ? 'Cancelling…' : 'Cancel build'}
            </Button>
          ) : null}
          <Button variant="ghost" asChild>
            <Link to="/apps/$appId" params={{ appId }}>
              Open the app
            </Link>
          </Button>
        </div>
      </div>

      {notice ? <div className="mt-2">{<NoticeLine>{notice}</NoticeLine>}</div> : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      <dl className="type-data mt-4 grid grid-cols-1 gap-x-6 gap-y-1 border-t border-line pt-3 text-[13px] sm:grid-cols-2">
        <div className="flex gap-2">
          <dt className="text-ink-2">App</dt>
          <dd className="min-w-0 truncate text-ink">{appName}</dd>
        </div>
        <div className="flex gap-2">
          <dt className="text-ink-2">Build</dt>
          <dd className="type-code min-w-0 truncate text-ink" title={build.id}>
            {shortBuildId(build.id)}
          </dd>
        </div>
        <div className="flex gap-2">
          <dt className="text-ink-2">Commit</dt>
          <dd className="type-code text-ink">{shortCommit(build.commit)}</dd>
        </div>
        <div className="flex gap-2">
          <dt className="text-ink-2">Duration</dt>
          <dd className="text-ink">{buildDurationLabel(build)}</dd>
        </div>
        <div className="flex gap-2">
          <dt className="text-ink-2">Trigger</dt>
          <dd className="text-ink">{build.trigger}</dd>
        </div>
        <div className="flex gap-2">
          <dt className="text-ink-2">Started by</dt>
          <dd className="min-w-0 truncate text-ink" title={build.created_by_id}>
            {build.created_by_type === 'system'
              ? 'system'
              : `${build.created_by_type} ${build.created_by_id.slice(0, 10)}…`}
          </dd>
        </div>
        <div className="flex gap-2 sm:col-span-2">
          <dt className="text-ink-2">Image</dt>
          <dd className="min-w-0 truncate text-ink">
            {build.image !== null && builtImage !== null ? (
              <ImageRef image={build.image} />
            ) : build.status === 'succeeded' ? (
              'built'
            ) : (
              'Not built yet'
            )}
          </dd>
        </div>
        <div className="flex min-w-0 gap-2 sm:col-span-2">
          <dt className="text-ink-2">Source</dt>
          <dd className="type-code min-w-0 truncate text-ink" title={build.source.repo_url}>
            {build.source.repo_url} @ {build.source.ref}
          </dd>
        </div>
      </dl>

      <div className="mt-4">
        <BuildStationList build={build} />
      </div>

      {build.status === 'failed' ? (
        <BuildFailure build={build} onJump={jumpToStep} />
      ) : null}

      {build.status === 'succeeded' && build.deployment_id !== null ? (
        <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-2 rounded-[10px] border border-line bg-surface px-4 py-3">
          <ToneLamp tone="run" size={18} label="Build succeeded" />
          <p className="text-[13px] text-ink">
            The build started deployment{' '}
            <span className="type-code">{shortDeploymentId(build.deployment_id)}</span>.
          </p>
          <Button variant="ghost" className="px-2.5" asChild>
            <Link to="/apps/$appId" params={{ appId }}>
              Open the deployment
            </Link>
          </Button>
        </div>
      ) : null}

      {build.status === 'cancelled' ? (
        <p className="mt-4 rounded-[10px] border border-line bg-surface px-4 py-3 text-[13px] text-ink-2">
          The build was cancelled before it finished. Nothing was deployed.
        </p>
      ) : null}

      <div className="mt-5">
        <BuildLog
          buildId={build.id}
          failingVertex={build.failed_step?.vertex ?? null}
          jumpRequest={jumpRequest}
          onBuildStatus={onBuildChanged}
        />
      </div>
    </div>
  );
}
