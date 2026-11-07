import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useState } from 'react';
import {
  deleteApp,
  getGetAppEnvQueryKey,
  getGetAppQueryKey,
  getListAppsQueryKey,
  restartApp,
  redeployApp,
  startApp,
  stopApp,
  useGetApp,
  useGetAppEnv,
} from '../api/generated/apps/apps';
import {
  getGetDeploymentQueryKey,
  getListAppDeploymentsQueryKey,
  useGetDeployment,
  useGetDeploymentLogs,
  useListAppDeployments,
} from '../api/generated/deployments/deployments';
import {
  getListAppBuildsQueryKey,
  startBuild,
  useListAppBuilds,
} from '../api/generated/builds/builds';
import type { App, AppEnvVar, Deployment } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { isApiProblem } from '../api/fetcher';
import {
  appLampState,
  DEPLOY_PHASE_LABELS,
  relativeTime,
  shortDeploymentId,
} from '../lib/app-status';
import {
  buildDurationLabel,
  buildLampState,
  builtImageLabel,
  isBuildTerminal,
  lastBuiltCommit,
  shortBuildId,
  shortCommit,
} from '../lib/build-status';
import { canOpenTerminal } from '../lib/capabilities';
import { formatLogTime } from '../lib/log-buffer';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { AppDomains } from '../components/app-domains';
import { AppEdgeBackends } from '../components/app-edge';
import { AppEnvDialog } from '../components/app-env-dialog';
import { AppInstances } from '../components/app-instances';
import { ImageRef } from '../components/image-ref';
import { AppStorage } from '../components/app-storage';
import {
  AppVulnerabilities,
  RebuildWithRefreshedBase,
} from '../components/app-vulnerabilities';
import { DeployRail } from '../components/deploy-rail';
import { LogTape } from '../components/log-tape';
import { AppMetricsBay } from '../components/metrics-bay';
import { ToneLamp } from '../components/tone-lamp';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

type ActionKind = 'start' | 'stop' | 'restart' | 'redeploy';

const actionPhrase: Record<ActionKind, string> = {
  start: 'start apps',
  stop: 'stop apps',
  restart: 'restart apps',
  redeploy: 'redeploy apps',
};

const actionDone: Record<ActionKind, string> = {
  start: 'Starting the app',
  stop: 'Stopping the app',
  restart: 'Restarting the app',
  redeploy: 'Redeploying the current image',
};

const deploymentPhaseWords = (phase: string): string =>
  phase in DEPLOY_PHASE_LABELS
    ? DEPLOY_PHASE_LABELS[phase as keyof typeof DEPLOY_PHASE_LABELS]
    : 'Build';

/** One app with everything about it; the service sheet replaces it later. */
export function AppScreen({ appId }: { appId: string }) {
  const { me, user, loading, unauthenticated, canWrite } = useSession();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [envDialogOpen, setEnvDialogOpen] = useState(false);
  const [refOverride, setRefOverride] = useState('');
  const [buildProblem, setBuildProblem] = useState<ProblemView | null>(null);

  const appQuery = useGetApp(appId, {
    query: {
      enabled: appId !== '',
      refetchInterval: (query) =>
        query.state.data?.status === 'deploying' ? 2_000 : 10_000,
    },
  });

  const deploymentsQuery = useListAppDeployments(appId, undefined, {
    query: {
      enabled: appId !== '',
      refetchInterval: (query) =>
        query.state.data?.items[0]?.status === 'pending' ||
        query.state.data?.items[0]?.status === 'preparing' ||
        query.state.data?.items[0]?.status === 'starting'
          ? 2_000
          : 10_000,
    },
  });

  const newest = deploymentsQuery.data?.items[0];
  const liveDeployment =
    newest?.status === 'pending' ||
    newest?.status === 'preparing' ||
    newest?.status === 'starting';

  const buildsQuery = useListAppBuilds(appId, undefined, {
    query: {
      enabled: appId !== '',
      refetchInterval: (query) => {
        const newestBuild = query.state.data?.items[0];
        return newestBuild !== undefined && !isBuildTerminal(newestBuild.status)
          ? 2_000
          : 10_000;
      },
    },
  });

  const builds = buildsQuery.data?.items ?? [];
  const liveBuild =
    builds[0] !== undefined && !isBuildTerminal(builds[0].status)
      ? builds[0]
      : undefined;

  const deploymentQuery = useGetDeployment(newest?.id ?? '', {
    query: {
      enabled: newest !== undefined,
      refetchInterval: liveDeployment ? 1_000 : false,
    },
  });

  const newestIsSettled =
    newest?.status === 'succeeded' ||
    newest?.status === 'failed' ||
    newest?.status === 'superseded';
  const deployment = newestIsSettled ? newest : deploymentQuery.data ?? newest;

  const logsQuery = useGetDeploymentLogs(deployment?.id ?? '', {
    query: {
      enabled: deployment !== undefined,
      refetchInterval: liveDeployment ? 2_000 : false,
    },
  });

  const envQuery = useGetAppEnv(appId, { query: { enabled: appId !== '' } });
  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: getGetAppQueryKey(appId) });
    void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
    void queryClient.invalidateQueries({
      queryKey: getListAppDeploymentsQueryKey(appId),
    });
    void queryClient.invalidateQueries({ queryKey: getListAppBuildsQueryKey(appId) });
    void queryClient.invalidateQueries({ queryKey: getGetAppEnvQueryKey(appId) });
    if (deployment) {
      void queryClient.invalidateQueries({
        queryKey: getGetDeploymentQueryKey(deployment.id),
      });
    }
  };

  const action = useMutation<App | Deployment, unknown, ActionKind>({
    mutationFn: (kind: ActionKind) => {
      if (kind === 'start') return startApp(appId);
      if (kind === 'stop') return stopApp(appId);
      if (kind === 'restart') return restartApp(appId);
      return redeployApp(appId);
    },
    onSuccess: (_data, kind) => {
      setProblem(null);
      setNotice(actionDone[kind]);
      invalidate();
    },
    onError: (error, kind) => {
      setNotice(null);
      setProblem(problemView(error, actionPhrase[kind]));
      invalidate();
    },
  });

  const buildNow = useMutation({
    mutationFn: () =>
      startBuild(
        appId,
        refOverride.trim() === '' ? undefined : { ref: refOverride.trim() },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: (build) => {
      setBuildProblem(null);
      setRefOverride('');
      invalidate();
      void navigate({
        to: '/apps/$appId/builds/$buildId',
        params: { appId, buildId: build.id },
      });
    },
    onError: (error) => {
      setBuildProblem(problemView(error, 'start builds'));
      invalidate();
    },
  });

  const remove = useMutation({
    mutationFn: () => deleteApp(appId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
      const projectId = appQuery.data?.project_id;
      void navigate(
        projectId ? { to: '/projects/$projectId', params: { projectId } } : { to: '/projects' },
      );
    },
    onError: (error) => {
      setConfirmDelete(false);
      setProblem(problemView(error, 'delete apps'));
    },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (appQuery.isPending) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <div className="h-[140px] rounded-[10px] border border-line bg-surface/60" />
      </main>
    );
  }

  if (appQuery.error !== null && appQuery.error !== undefined) {
    const view = problemView(appQuery.error, 'see this app');
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
        <ProblemBanner view={view} />
        <Button variant="default" className="mt-4" asChild>
          <Link to="/projects">Back to projects</Link>
        </Button>
      </main>
    );
  }

  const app = appQuery.data;
  if (!app) return null;

  const lamp = appLampState(app.status);
  const nodeName =
    nodesQuery.data?.items.find((node) => node.id === app.placement.node_ids[0])?.name ??
    'the scheduler';
  const nodeNames = Object.fromEntries(
    (nodesQuery.data?.items ?? []).map((node) => [node.id, node.name]),
  );
  const nodeStatuses = Object.fromEntries(
    (nodesQuery.data?.items ?? []).map((node) => [node.id, node.status]),
  );

  const env: AppEnvVar[] = envQuery.data?.items ?? [];
  const canStart = app.status === 'stopped' || app.status === 'down';
  const canStop = app.status !== 'stopped';
  const source = app.source ?? null;
  const imageLabel = builtImageLabel(app.image);
  const lastCommit = lastBuiltCommit(builds);
  const canBuildNow = canWrite && source !== null && liveBuild === undefined;
  const buildDisabledReason =
    source === null
      ? null
      : !canWrite
        ? 'Your role can read builds but not start them.'
        : liveBuild !== undefined
          ? `Build ${shortBuildId(liveBuild.id)} is still running. Wait for it to end.`
          : null;

  const progressError =
    logsQuery.error !== null && logsQuery.error !== undefined
      ? isApiProblem(logsQuery.error) && logsQuery.error.status === 504
        ? "The node is unreachable, so its progress lines can't be read right now."
        : problemView(logsQuery.error, 'read progress lines').message
      : null;

  return (
    <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="mb-3">
        <Link
          to="/projects/$projectId"
          params={{ projectId: app.project_id }}
          className="text-[13px] text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
        >
          All apps in this project
        </Link>
      </div>

      <header className="rounded-[10px] border border-line bg-surface px-4 py-4 card-sheen sm:px-5">
        <div className="flex flex-wrap items-start gap-x-4 gap-y-3">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2.5">
              <ToneLamp tone={lamp.tone} pulse={lamp.pulse} label={lamp.statusText} />
              <h1 className="type-display truncate text-[26px] leading-none">
                {app.name}
              </h1>
            </div>
            <p className="mt-1.5 text-[13px] text-ink-2">{lamp.caption}</p>
          </div>

          {canWrite ? (
            <div className="flex flex-wrap items-center gap-2">
              {canOpenTerminal(me) ? (
                <Button variant="default" asChild>
                  <Link to="/apps/$appId/terminal" params={{ appId: app.id }}>
                    Terminal
                  </Link>
                </Button>
              ) : null}
              {canStart ? (
                <Button
                  variant="primary"
                  disabled={action.isPending}
                  onClick={() => {
                    setNotice(null);
                    action.mutate('start');
                  }}
                >
                  Start
                </Button>
              ) : null}
              {canStop ? (
                <Button
                  variant="default"
                  disabled={action.isPending}
                  onClick={() => {
                    setNotice(null);
                    action.mutate('stop');
                  }}
                >
                  Stop
                </Button>
              ) : null}
              <Button
                variant="default"
                disabled={action.isPending}
                onClick={() => {
                  setNotice(null);
                  action.mutate('restart');
                }}
              >
                Restart
              </Button>
              {source === null || app.image !== '' ? (
                <Button
                  variant="default"
                  disabled={action.isPending}
                  onClick={() => {
                    setNotice(null);
                    action.mutate('redeploy');
                  }}
                >
                  Redeploy
                </Button>
              ) : null}
              <Button
                variant="danger"
                disabled={action.isPending || remove.isPending}
                onClick={() => setConfirmDelete(true)}
              >
                Delete
              </Button>
            </div>
          ) : null}
        </div>

        <dl className="type-data mt-4 grid grid-cols-1 gap-x-6 gap-y-1 border-t border-line pt-3 text-[13px] sm:grid-cols-2">
          <div className="flex gap-2">
            <dt className="text-ink-2">Image</dt>
            <dd
              className="min-w-0 truncate text-ink"
              title={source === null ? app.image : imageLabel ?? undefined}
            >
              {source !== null && imageLabel === null ? (
                'Not built yet'
              ) : (
                <ImageRef image={app.image} />
              )}
            </dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Port</dt>
            <dd className="text-ink">{app.port}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Replicas</dt>
            <dd className="text-ink">{app.replicas}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Node</dt>
            <dd className="text-ink">{nodeName}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-2">Deployment</dt>
            <dd className="type-code text-ink" title={app.current_deployment_id ?? undefined}>
              {app.current_deployment_id
                ? shortDeploymentId(app.current_deployment_id)
                : 'None yet'}
            </dd>
          </div>
        </dl>
      </header>

      {notice ? <div className="mt-2">{<NoticeLine>{notice}</NoticeLine>}</div> : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      {source !== null ? (
        <>
          <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
            <h2 className="type-display-light text-[15px] leading-none text-ink">Source</h2>
            <dl className="type-data mt-3 grid grid-cols-1 gap-x-6 gap-y-1 text-[13px] sm:grid-cols-2">
              <div className="flex min-w-0 gap-2 sm:col-span-2">
                <dt className="text-ink-2">Repository</dt>
                <dd className="type-code min-w-0 truncate text-ink" title={source.repo_url}>
                  {source.repo_url}
                </dd>
              </div>
              <div className="flex gap-2">
                <dt className="text-ink-2">Ref</dt>
                <dd className="type-code text-ink">{source.ref}</dd>
              </div>
              <div className="flex gap-2">
                <dt className="text-ink-2">Dockerfile</dt>
                <dd className="type-code min-w-0 truncate text-ink">{source.dockerfile_path}</dd>
              </div>
              <div className="flex gap-2">
                <dt className="text-ink-2">Context</dt>
                <dd className="type-code min-w-0 truncate text-ink">{source.context_dir}</dd>
              </div>
              <div className="flex gap-2">
                <dt className="text-ink-2">Target</dt>
                <dd className="text-ink">{source.target ?? 'last stage'}</dd>
              </div>
              <div className="flex gap-2">
                <dt className="text-ink-2">Last built commit</dt>
                <dd className="text-ink" title={lastCommit ?? undefined}>
                  {lastCommit === null ? 'Not built yet' : shortCommit(lastCommit)}
                </dd>
              </div>
            </dl>

            <div className="mt-4 flex flex-col gap-3 border-t border-line pt-3 sm:flex-row sm:items-end">
              <Field
                label="Build ref"
                htmlFor="build-ref"
                hint={`Optional. Leave it empty to build ${source.ref}.`}
                className="flex-1"
              >
                <Input
                  id="build-ref"
                  value={refOverride}
                  onChange={(event) => setRefOverride(event.target.value)}
                  placeholder={source.ref}
                  spellCheck={false}
                />
              </Field>
              <div className="flex flex-col gap-1.5">
                <Button
                  variant="primary"
                  disabled={!canBuildNow || buildNow.isPending}
                  onClick={() => {
                    setBuildProblem(null);
                    buildNow.mutate();
                  }}
                >
                  {buildNow.isPending ? 'Starting…' : 'Build now'}
                </Button>
                {buildDisabledReason ? (
                  <p className="max-w-[260px] text-[13px] leading-snug text-ink-2">
                    {buildDisabledReason}
                  </p>
                ) : null}
                <RebuildWithRefreshedBase
                  appId={app.id}
                  disabled={!canBuildNow}
                  onStarted={invalidate}
                />
              </div>
            </div>
            {buildProblem ? <ProblemBanner view={buildProblem} className="mt-3" /> : null}
          </section>

          <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h2 className="type-display-light text-[15px] leading-none text-ink">Builds</h2>
              <span className="type-data text-[13px] text-ink-2">
                {builds.length} {builds.length === 1 ? 'build' : 'builds'}
              </span>
            </div>

            {buildsQuery.error !== null && buildsQuery.error !== undefined ? (
              <ProblemBanner
                view={problemView(buildsQuery.error, 'see builds')}
                className="mt-3"
              />
            ) : builds.length === 0 ? (
              <p className="mt-3 text-[13px] text-ink-2">
                No builds yet. Build now to make the first image from this source.
              </p>
            ) : (
              <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
                {builds.map((build) => {
                  const buildLamp = buildLampState(build.status);
                  return (
                    <li key={build.id}>
                      <Link
                        to="/apps/$appId/builds/$buildId"
                        params={{ appId, buildId: build.id }}
                        className="flex flex-wrap items-center gap-x-3 gap-y-1.5 rounded-[4px] px-1.5 py-2.5 hover:bg-sunk/40 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
                      >
                        <ToneLamp
                          tone={buildLamp.tone}
                          pulse={buildLamp.pulse}
                          size={18}
                          label={buildLamp.statusText}
                        />
                        <span className="type-body-medium min-w-[86px] text-[14px] text-ink">
                          {buildLamp.statusText}
                        </span>
                        <span className="type-data text-[13px] text-ink-2">
                          commit {shortCommit(build.commit)}
                        </span>
                        <span className="type-data text-[13px] text-ink-2">
                          {buildDurationLabel(build)}
                        </span>
                        <span className="type-data text-[13px] text-ink-2">
                          trigger {build.trigger}
                        </span>
                        <span className="type-data min-w-0 truncate text-[13px] text-ink-2">
                          {build.created_by_type === 'system'
                            ? 'system'
                            : `${build.created_by_type} ${build.created_by_id.slice(0, 10)}…`}
                        </span>
                        <span className="type-code ml-auto text-[12px] text-ink-2" title={build.id}>
                          {shortBuildId(build.id)}
                        </span>
                      </Link>
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
        </>
      ) : null}

      <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
        <h2 className="type-display-light text-[15px] leading-none text-ink">Deployment</h2>
        <div className="mt-3">
          {deployment ? (
            <DeployRail
              deployment={deployment}
              error={deployment.status === 'failed' ? deployment.error : null}
            />
          ) : (
            <p className="text-[13px] text-ink-2">
              {source !== null
                ? 'No deployment yet. A successful build starts one.'
                : 'No deployment yet. Redeploy to roll out the current image.'}
            </p>
          )}
        </div>

        {deployment ? (
          <details className="mt-4 border-t border-line pt-3">
            <summary className="cursor-pointer select-none text-[13px] text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent">
              Progress lines
              {logsQuery.data ? ` (${logsQuery.data.items.length})` : ''}
            </summary>
            {progressError ? (
              <p className="mt-2 text-[13px] text-fault">{progressError}</p>
            ) : logsQuery.isPending ? (
              <p className="mt-2 text-[13px] text-ink-2">Reading progress lines…</p>
            ) : logsQuery.data && logsQuery.data.items.length > 0 ? (
              <ul className="mt-2 flex flex-col gap-0.5">
                {logsQuery.data.items.map((entry, index) => (
                  <li
                    key={`${entry.time}-${index}`}
                    className="grid grid-cols-[80px_110px_minmax(0,1fr)] gap-2 text-[12.5px] leading-5"
                  >
                    <span className="type-data text-ink-2" title={entry.time}>
                      {formatLogTime(entry.time)}
                    </span>
                    <span className="type-data text-ink-2">
                      {deploymentPhaseWords(entry.phase)}
                    </span>
                    <span className="whitespace-pre-wrap break-words text-ink">
                      {entry.message}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="mt-2 text-[13px] text-ink-2">
                No progress lines have been written yet.
              </p>
            )}
          </details>
        ) : null}
      </section>

      <AppVulnerabilities
        app={app}
        canWrite={canWrite}
        onRebuilt={invalidate}
      />

      <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
        <AppDomains
          appId={app.id}
          domains={app.domains}
          canWrite={canWrite}
          onChanged={invalidate}
        />
      </section>

      <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="type-display-light text-[15px] leading-none text-ink">
            Environment
          </h2>
          {canWrite ? (
            <Button variant="ghost" className="px-2.5" onClick={() => setEnvDialogOpen(true)}>
              Edit variables
            </Button>
          ) : null}
        </div>
        {env.length === 0 ? (
          <p className="mt-3 text-[13px] text-ink-2">
            No variables yet. Add one to pass configuration to the app.
          </p>
        ) : (
          <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
            {env.map((item) => (
              <li key={item.name} className="flex flex-wrap items-baseline gap-x-3 gap-y-1 py-2">
                <span className="type-code text-[12.5px] text-ink">{item.name}</span>
                <span className="text-[13px] text-ink-2">
                  changed {relativeTime(item.updated_at)}
                </span>
              </li>
            ))}
          </ul>
        )}
        <p className="mt-2 text-[13px] leading-snug text-ink-2">
          Values are write-only: Quoro stores them encrypted and never shows them again.
        </p>
      </section>

      <AppStorage
        app={app}
        canWrite={canWrite}
        nodeNames={nodeNames}
        onChanged={invalidate}
      />

      <AppInstances
        instances={app.instances}
        nodeNames={nodeNames}
        nodeStatuses={nodeStatuses}
        hasSource={source !== null}
      />

      <AppEdgeBackends backends={app.edge_backends ?? []} nodeNames={nodeNames} />

      <AppMetricsBay appId={app.id} nodeNames={nodeNames} className="mt-5" />

      <section className="mt-5">
        <h2 className="type-display-light mb-2 text-[15px] leading-none text-ink">
          Live logs
        </h2>
        <LogTape appId={app.id} />
      </section>

      <Dialog open={confirmDelete} onOpenChange={setConfirmDelete}>
        <DialogContent
          title="Delete app"
          description={`Delete ${app.name}? Its VM, domains and environment are removed.`}
        >
          <div className="flex flex-wrap justify-end gap-2">
            <Button variant="default" onClick={() => setConfirmDelete(false)}>
              Keep the app
            </Button>
            <Button
              variant="danger-solid"
              disabled={remove.isPending}
              onClick={() => remove.mutate()}
            >
              {remove.isPending ? 'Deleting…' : 'Delete'}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <AppEnvDialog
        open={envDialogOpen}
        onOpenChange={setEnvDialogOpen}
        appId={app.id}
        env={env}
        onSaved={invalidate}
      />
    </main>
  );
}
