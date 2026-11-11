// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueries } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { ChevronLeft, GitBranch, Box, Server } from 'lucide-react';
import { useState, type ReactNode } from 'react';
import type { AppDetail, Build, Deployment, DeploymentLogEntry } from '../api/generated/model';
import { cancelBuild, startBuild, useGetBuild } from '../api/generated/builds/builds';
import {
  createDeployment,
  getGetDeploymentLogsQueryOptions,
  useGetDeploymentLogs,
} from '../api/generated/deployments/deployments';
import { isApiProblem } from '../api/fetcher';
import { DEPLOY_PHASE_LABELS, relativeTime } from '../lib/app-status';
import { isBuildTerminal, shortBuildId, shortCommit } from '../lib/build-status';
import {
  activeDeploymentId,
  buildkitSteps,
  estimateRun,
  formatClock,
  formatSpan,
  imageLabel,
  isRunLive,
  miniTape,
  miniTapeScale,
  nodeLineSpans,
  remainingSeconds,
  rolloutView,
  runSentence,
  runTag,
  runTimeline,
  type DeployRun,
  type Estimate,
  type RunTimeline,
} from '../lib/deploy-tape';
import { formatLogTime } from '../lib/log-buffer';
import { problemView, type ProblemView } from '../lib/problem';
import { useAppRuns } from '../lib/use-app-runs';
import { useBuildLines } from '../lib/use-build-lines';
import { appKind } from '../lib/services';
import { useNow } from '../lib/use-now';
import { cn } from '../lib/utils';
import { BuildView } from './build-view';
import { GlyphTile } from './glyph-tile';
import { BuildSteps, MiniTape, Stepper, Tape } from './deploy-tape';
import { StartedBy } from './identity-name';
import { RebuildWithRefreshedBase } from './app-vulnerabilities';
import { Button } from './ui/button';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';
import { Tag } from './ui/tag';
import { EmptyState } from './empty-state';
import { SkeletonCards, SkeletonLines } from './skeleton';

/** History rows read their progress lines; older ones keep to the list. */
const HISTORY_ROWS = 10;

/** `github.com/example/api` from `https://github.com/example/api.git`. */
const repoLabel = (url: string): string =>
  url.replace(/^https?:\/\//, '').replace(/\.git$/, '');

const runTitle = (run: DeployRun): string => {
  if (run.build) {
    const ref = run.build.source.ref;
    return run.build.commit === null ? `Build of ${ref}` : `Build ${shortCommit(run.build.commit)} on ${ref}`;
  }
  const deployment = run.deployment!;
  switch (deployment.trigger) {
    case 'redeploy':
      return 'Redeploy';
    case 'rollback':
      return 'Rollback';
    case 'build':
      return 'Deploy of a built image';
    case 'api':
      return `Deploy ${imageLabel(deployment.image)}`;
  }
};

const startedBy = (run: DeployRun) => {
  const source = run.build ?? run.deployment!;
  return <StartedBy type={source.created_by_type} id={source.created_by_id} avatar={false} />;
};

/** `2 replicas, one on lab-3 and one on lab-4`, from where the instances run. */
const replicasLine = (app: AppDetail, nodeNames: Record<string, string>): string => {
  const nodes = [...new Set(app.instances.map((vm) => nodeNames[vm.node_id] ?? vm.node_id))];
  const count = `${app.replicas} ${app.replicas === 1 ? 'replica' : 'replicas'}`;
  if (nodes.length === 0) return count;
  if (app.replicas === 2 && nodes.length === 2) return `${count}, one on ${nodes[0]} and one on ${nodes[1]}`;
  return `${count} on ${nodes.join(', ')}`;
};

/**
 * The Deployments tab: the newest run as one sentence and a clock, its steps,
 * where its time went and its BuildKit steps; under it the history, each row
 * with a mini tape on one shared scale. `?build=` opens one build instead.
 */
export function DeploymentsTab({
  app,
  projectId,
  buildId,
  canWrite,
  nodeNames,
  onChanged,
  onRedeploy,
  redeploying,
}: {
  app: AppDetail;
  projectId: string;
  buildId?: string;
  canWrite: boolean;
  nodeNames: Record<string, string>;
  onChanged: () => void;
  onRedeploy: () => void;
  redeploying: boolean;
}) {
  if (buildId) {
    return (
      <HostedBuild
        key={buildId}
        app={app}
        projectId={projectId}
        buildId={buildId}
        canWrite={canWrite}
      />
    );
  }
  return (
    <RunsView
      app={app}
      projectId={projectId}
      canWrite={canWrite}
      nodeNames={nodeNames}
      onChanged={onChanged}
      onRedeploy={onRedeploy}
      redeploying={redeploying}
    />
  );
}

function RunsView({
  app,
  projectId,
  canWrite,
  nodeNames,
  onChanged,
  onRedeploy,
  redeploying,
}: {
  app: AppDetail;
  projectId: string;
  canWrite: boolean;
  nodeNames: Record<string, string>;
  onChanged: () => void;
  onRedeploy: () => void;
  redeploying: boolean;
}) {
  const source = app.source ?? null;
  // The sheet's header reads the same two lists with the same rules.
  const { runs, error: listError, loading } = useAppRuns(app);
  const top = runs[0];
  const history = runs.slice(1, 1 + HISTORY_ROWS);
  const live = isRunLive(top);

  const topLogs = useGetDeploymentLogs(top?.deployment?.id ?? '', {
    query: {
      enabled: top?.deployment != null,
      refetchInterval: live ? 2_000 : false,
      retry: false,
    },
  });
  const historyLogs = useQueries({
    queries: history.map((run) =>
      getGetDeploymentLogsQueryOptions(run.deployment?.id ?? '', {
        query: {
          enabled: run.deployment !== null,
          staleTime: isRunLive(run) ? 0 : Infinity,
          retry: false,
        },
      }),
    ),
  });
  const now = useNow(live);

  const topTimeline = top ? runTimeline(top, topLogs.data?.items, now) : null;
  const historyTimelines = history.map((run, index) =>
    runTimeline(run, historyLogs[index]?.data?.items, now),
  );
  const estimate = topTimeline ? estimateRun(historyTimelines, topTimeline.kind) : null;
  // Which row is Active comes from the same two lists the rows are drawn
  // from, never from the app read, which lands at another moment.
  const activeId = activeDeploymentId(topTimeline ? [topTimeline, ...historyTimelines] : historyTimelines);

  return (
    <>
      <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
        <div className="metaline">
          {source ? (
            <span className="min-w-0">
              <GitBranch strokeWidth={1.7} aria-hidden="true" />
              <span className="truncate">
                {repoLabel(source.repo_url)} · {source.ref}
              </span>
            </span>
          ) : (
            <span className="min-w-0">
              <Box strokeWidth={1.7} aria-hidden="true" />
              <span className="type-code truncate text-[11.5px]">{app.image}</span>
            </span>
          )}
          <span>
            <Server strokeWidth={1.7} aria-hidden="true" />
            {replicasLine(app, nodeNames)}
          </span>
        </div>
      </div>

      {canWrite ? (
        <DeployActions
          app={app}
          liveBuild={top?.build && !isBuildTerminal(top.build.status) ? top.build : undefined}
          onChanged={onChanged}
          onRedeploy={onRedeploy}
          redeploying={redeploying}
        />
      ) : null}

      {listError ? <ProblemBanner view={problemView(listError, 'see deployments')} /> : null}

      {loading ? (
        <SkeletonCards label="Loading deployments" height={240} lines={4} />
      ) : !top || !topTimeline ? (
        <EmptyState title="No deployment yet">
          {source !== null
            ? 'A deployment boots the built image on a node. A successful build starts one.'
            : 'A deployment boots the image on a node. Redeploy to roll out the current image.'}
        </EmptyState>
      ) : (
        <RunBox
          key={top.key}
          app={app}
          projectId={projectId}
          timeline={topTimeline}
          activeId={activeId}
          estimate={estimate}
          entries={topLogs.data?.items}
          entriesError={topLogs.error}
          now={now}
          canWrite={canWrite}
          nodeNames={nodeNames}
          onChanged={onChanged}
        />
      )}

      {historyTimelines.length > 0 ? (
        <History app={app} projectId={projectId} timelines={historyTimelines} activeId={activeId} />
      ) : null}
    </>
  );
}

/** The newest run, live or not: its sentence and clock, steps, tape and BuildKit steps. */
function RunBox({
  app,
  projectId,
  timeline,
  activeId,
  estimate,
  entries,
  entriesError,
  now,
  canWrite,
  nodeNames,
  onChanged,
}: {
  app: AppDetail;
  projectId: string;
  timeline: RunTimeline;
  activeId: string | null;
  estimate: Estimate | null;
  entries: readonly DeploymentLogEntry[] | undefined;
  entriesError: unknown;
  now: number;
  canWrite: boolean;
  nodeNames: Record<string, string>;
  onChanged: () => void;
}) {
  const { run } = timeline;
  const build = run.build;
  const live = timeline.state === 'live';
  const tag = runTag(timeline, activeId);
  const remaining = remainingSeconds(timeline, estimate);
  const buildLive = build !== null && !isBuildTerminal(build.status);
  const lines = useBuildLines(build?.id ?? null, buildLive);
  const steps = build ? buildkitSteps(lines, build, now) : [];
  const nodeLines = build ? nodeLineSpans(lines, build, now) : [];
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const cancel = useMutation({
    mutationFn: (id: string) => cancelBuild(id),
    onSuccess: () => {
      setProblem(null);
      onChanged();
    },
    onError: (error) => setProblem(problemView(error, 'cancel builds')),
  });

  const inBuild = timeline.steps.some(
    (step) => (step.step === 'queue' || step.step === 'build') && step.state === 'live',
  );
  // Once its build succeeded, a Git run is drawn as its rollout, the build in words.
  const rollout = rolloutView(timeline);
  const usually = live && estimate ? `usually about ${formatClock(estimate.total)}` : null;
  const sub = live
    ? [inBuild && build ? `${repoLabel(build.source.repo_url)} at ${build.source.ref}` : replicasLine(app, nodeNames), usually]
    : [
        timeline.end !== null ? `Finished ${relativeTime(new Date(timeline.end).toISOString(), now)}` : null,
        rollout.before,
      ];
  const subLine = sub.filter(Boolean).join(' · ');
  const failure =
    timeline.state === 'failed'
      ? (build?.status === 'failed' ? build.error : run.deployment?.error) ?? null
      : null;

  return (
    <section className={cn('box run-box', live && 'run-box-live')} aria-label="Newest deployment">
      <div className="box-h">
        <Tag tone={tag.tone}>{tag.word}</Tag>
        <div className="box-t">
          <b>{runTitle(run)}</b>
          <small>
            {startedBy(run)}
            <span aria-hidden="true">·</span>
            {live ? `started ${relativeTime(new Date(timeline.start).toISOString(), now)}` : relativeTime(new Date(timeline.start).toISOString(), now)}
          </small>
        </div>
        {buildLive && canWrite ? (
          <Button
            busy={cancel.isPending}
            variant="danger"
            size="sm"
            disabled={cancel.isPending}
            onClick={() => cancel.mutate(build.id)}
          >
            {cancel.isPending ? 'Cancelling…' : 'Cancel build'}
          </Button>
        ) : null}
        {build ? (
          <Button size="sm" asChild>
            <Link
              to="/projects/$projectId"
              params={{ projectId }}
              search={{ app: app.id, tab: 'deployments', build: build.id }}
            >
              View logs
            </Link>
          </Button>
        ) : null}
      </div>
      <div className="box-b">
        {problem ? <ProblemBanner view={problem} /> : null}
        <div className="phase-h">
          <div className="min-w-0" aria-live="polite">
            <b>{runSentence(timeline, app.replicas)}</b>
            {subLine ? <small>{subLine}</small> : null}
          </div>
          <div className="elapsed">
            <b>{formatClock(timeline.elapsed)}</b>
            <span className="sr-only">{live ? ' elapsed' : ' in total'}</span>
          </div>
        </div>
        <Stepper steps={rollout.steps} live={live} />
        <Tape steps={rollout.steps} remaining={remaining} estimate={estimate} />
        {failure ? <p className="text-[12.5px] leading-snug text-fault">{failure}</p> : null}
        <BuildSteps steps={steps} nodeLines={nodeLines} />
        {run.deployment ? <ProgressLines entries={entries} error={entriesError} /> : null}
      </div>
    </section>
  );
}

const phaseWords = (phase: DeploymentLogEntry['phase']): string =>
  phase in DEPLOY_PHASE_LABELS ? DEPLOY_PHASE_LABELS[phase as keyof typeof DEPLOY_PHASE_LABELS] : 'Build';

/** The deployment's progress lines, folded under the tape. */
function ProgressLines({
  entries,
  error,
}: {
  entries: readonly DeploymentLogEntry[] | undefined;
  error: unknown;
}) {
  const message =
    error !== null && error !== undefined
      ? isApiProblem(error) && error.status === 504
        ? "The node is unreachable, so its progress lines can't be read right now."
        : problemView(error, 'read progress lines').message
      : null;
  return (
    <details className="progress-lines">
      <summary>
        Progress lines{entries ? ` (${entries.length})` : ''}
      </summary>
      {message ? (
        <p className="mt-2 text-[12.5px] text-fault">{message}</p>
      ) : !entries ? (
        <SkeletonLines label="Loading progress lines" lines={3} heading={false} className="mt-2" />
      ) : entries.length === 0 ? (
        <p className="mt-2 text-[12.5px] text-ink-2">No progress lines have been written yet.</p>
      ) : (
        <div className="logs mt-2">
          {entries.map((entry, index) => (
            <div key={`${entry.time}-${index}`}>
              <span className="ts" title={entry.time}>
                {formatLogTime(entry.time)}
              </span>
              <span className="ts">{phaseWords(entry.phase)}</span>
              {entry.message}
            </div>
          ))}
        </div>
      )}
    </details>
  );
}

/** Earlier runs, newest first, each with its mini tape on one shared scale. */
function History({
  app,
  projectId,
  timelines,
  activeId,
}: {
  app: AppDetail;
  projectId: string;
  timelines: readonly RunTimeline[];
  activeId: string | null;
}) {
  const scale = miniTapeScale(timelines);
  return (
    <section className="hist" aria-labelledby="hist-title">
      <h3 id="hist-title" className="sh-lbl">
        History
      </h3>
      <div className="box hist-box">
        {timelines.map((timeline) => (
          <HistoryRow
            key={timeline.run.key}
            app={app}
            projectId={projectId}
            timeline={timeline}
            activeId={activeId}
            scale={scale}
          />
        ))}
      </div>
    </section>
  );
}

const exitCode = (error: string | null): string | null => {
  const match = error ? /exit code:?\s*(\d+)/i.exec(error) : null;
  return match ? `exit code ${match[1]}` : null;
};

/**
 * The run's state in its row's words. An old deploy that simply gave way to
 * a newer one says nothing; every other state is written, so a red tape is
 * never the only sign a run failed.
 */
const rowWord = (word: string): string | null => (word === 'Removed' ? null : word);

function HistoryRow({
  app,
  projectId,
  timeline,
  activeId,
  scale,
}: {
  app: AppDetail;
  projectId: string;
  timeline: RunTimeline;
  activeId: string | null;
  scale: number;
}) {
  const { run } = timeline;
  const tag = runTag(timeline, activeId);
  const word = rowWord(tag.word);
  const when = relativeTime(new Date(timeline.start).toISOString());
  const failedBuild: Build | null = run.build?.status === 'failed' ? run.build : null;
  const label = `${formatSpan(timeline.recorded)} recorded${timeline.state === 'failed' ? ', failed' : ''}`;

  let small: ReactNode;
  if (failedBuild) {
    small = (
      <span className="min-w-0 truncate">
        {[word, failedBuild.failed_step?.name, exitCode(failedBuild.error), when].filter(Boolean).join(' · ')}
      </span>
    );
  } else {
    small = (
      <>
        {word ? (
          <>
            <span className={cn('hist-word', `hist-word-${tag.tone}`)}>{word}</span>
            <span aria-hidden="true">·</span>
          </>
        ) : null}
        {startedBy(run)}
        <span aria-hidden="true">·</span>
        {when}
        {timeline.state === 'live' ? null : (
          <>
            <span aria-hidden="true">·</span>
            {formatSpan(timeline.elapsed)}
          </>
        )}
      </>
    );
  }

  const body = (
    <>
      <GlyphTile kind={appKind(app)} size="sm" />
      <div className="box-t">
        <b>{runTitle(run)}</b>
        <small>{small}</small>
      </div>
      <MiniTape segments={miniTape(timeline, scale)} label={label} />
    </>
  );

  if (run.build) {
    return (
      <Link
        to="/projects/$projectId"
        params={{ projectId }}
        search={{ app: app.id, tab: 'deployments', build: run.build.id }}
        className="box-row"
        aria-label={`${runTitle(run)}, ${tag.word}, open the build`}
      >
        {body}
      </Link>
    );
  }
  return <div className="box-row">{body}</div>;
}

/**
 * What can be rolled out from here: a redeploy of the current image, a new
 * build of the Git source (with another ref, or with fresh base images), or a
 * new image for an app run from one.
 */
function DeployActions({
  app,
  liveBuild,
  onChanged,
  onRedeploy,
  redeploying,
}: {
  app: AppDetail;
  liveBuild: Build | undefined;
  onChanged: () => void;
  onRedeploy: () => void;
  redeploying: boolean;
}) {
  const source = app.source ?? null;
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [value, setValue] = useState('');
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const build = useMutation({
    mutationFn: () =>
      startBuild(app.id, value.trim() === '' ? undefined : { ref: value.trim() }, {
        headers: { 'Idempotency-Key': crypto.randomUUID() },
      }),
    onSuccess: () => {
      setProblem(null);
      setValue('');
      setOpen(false);
      onChanged();
      void navigate({
        to: '/projects/$projectId',
        params: { projectId: app.project_id },
        search: { app: app.id, tab: 'deployments' },
      });
    },
    onError: (error) => {
      setProblem(problemView(error, 'start builds'));
      onChanged();
    },
  });

  const deploy = useMutation({
    mutationFn: () =>
      createDeployment(
        app.id,
        { image: value.trim() },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: () => {
      setProblem(null);
      setValue('');
      setOpen(false);
      onChanged();
    },
    onError: (error) => {
      setProblem(problemView(error, 'deploy apps'));
      onChanged();
    },
  });

  const busy = build.isPending || deploy.isPending;
  const blocked =
    source !== null && liveBuild !== undefined
      ? `Build ${shortBuildId(liveBuild.id)} is still running. Wait for it to end.`
      : null;

  return (
    <div className="grid gap-3">
      <div className="flex flex-wrap items-center gap-2">
        {source === null || app.image !== '' ? (
          <Button size="sm" disabled={redeploying} onClick={onRedeploy}>
            Redeploy
          </Button>
        ) : null}
        <Button
          size="sm"
          variant="ghost"
          aria-expanded={open}
          onClick={() => {
            setProblem(null);
            setOpen((current) => !current);
          }}
        >
          {source !== null ? 'New build' : 'Deploy an image'}
        </Button>
        {blocked && !open ? <span className="text-[12.5px] text-ink-2">{blocked}</span> : null}
      </div>

      {open ? (
        <form
          className="box grid gap-3 px-4 py-4"
          onSubmit={(event) => {
            event.preventDefault();
            setProblem(null);
            if (source !== null) {
              build.mutate();
            } else if (value.trim() === '') {
              setProblem({ message: 'Enter the image to deploy.' });
            } else {
              deploy.mutate();
            }
          }}
        >
          {source !== null ? (
            <Field
              label="Build ref"
              htmlFor="sheet-build-ref"
              hint={`Optional. Leave it empty to build ${source.ref}.`}
            >
              <Input
                id="sheet-build-ref"
                value={value}
                onChange={(event) => setValue(event.target.value)}
                placeholder={source.ref}
                spellCheck={false}
              />
            </Field>
          ) : (
            <Field
              label="Image"
              htmlFor="sheet-deploy-image"
              hint="A public OCI image. The app keeps its port, variables and domains."
            >
              <Input
                id="sheet-deploy-image"
                value={value}
                onChange={(event) => setValue(event.target.value)}
                placeholder={app.image}
                spellCheck={false}
              />
            </Field>
          )}
          {problem ? <ProblemBanner view={problem} /> : null}
          {blocked ? <p className="text-[12.5px] text-ink-2">{blocked}</p> : null}
          <div className="flex flex-wrap items-start gap-2">
            <Button busy={busy} type="submit" variant="primary" disabled={busy || blocked !== null}>
              {busy ? 'Starting…' : source !== null ? 'Build now' : 'Deploy'}
            </Button>
            {source !== null ? (
              <RebuildWithRefreshedBase
                appId={app.id}
                disabled={blocked !== null}
                onStarted={onChanged}
              />
            ) : null}
            <Button variant="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
          </div>
        </form>
      ) : null}
    </div>
  );
}

/**
 * `?build=`: one build in full, as the build page showed it, inside the tab.
 * Its live `status` events move the plate without waiting for a poll.
 */
function HostedBuild({
  app,
  projectId,
  buildId,
  canWrite,
}: {
  app: AppDetail;
  projectId: string;
  buildId: string;
  canWrite: boolean;
}) {
  const [streamBuild, setStreamBuild] = useState<Build | null>(null);
  const buildQuery = useGetBuild(buildId, {
    query: {
      retry: false,
      refetchInterval: (query) =>
        query.state.data !== undefined && isBuildTerminal(query.state.data.status) ? false : 2_000,
    },
  });
  const build = streamBuild ?? buildQuery.data;

  return (
    <>
      <Link
        to="/projects/$projectId"
        params={{ projectId }}
        search={{ app: app.id, tab: 'deployments' }}
        className="inline-flex w-fit items-center gap-1 text-[13px] text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
      >
        <ChevronLeft className="size-4" strokeWidth={1.7} aria-hidden="true" />
        All deployments
      </Link>
      {buildQuery.isPending && !build ? (
        <SkeletonCards label="Loading the build" height={180} />
      ) : !build ? (
        <ProblemBanner view={problemView(buildQuery.error, 'see this build')} />
      ) : (
        <section className="box px-4 py-4" aria-label={`Build ${shortBuildId(build.id)}`}>
          <h3 className="mb-3 text-[15px] font-semibold leading-none text-ink">
            Build <span className="type-code text-[13px]">{shortBuildId(build.id)}</span>
          </h3>
          <BuildView
            build={build}
            appId={app.id}
            appName={app.name}
            canWrite={canWrite}
            onBuildChanged={setStreamBuild}
            sheet={{ projectId }}
          />
        </section>
      )}
    </>
  );
}
