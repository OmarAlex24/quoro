import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link, useNavigate, useParams } from '@tanstack/react-router';
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
import type { App, AppEnvVar, Deployment } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { isApiProblem } from '../api/fetcher';
import {
  appLampState,
  DEPLOY_PHASE_LABELS,
  relativeTime,
  shortDeploymentId,
} from '../lib/app-status';
import { formatLogTime } from '../lib/log-buffer';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { AppDomains } from '../components/app-domains';
import { AppEnvDialog } from '../components/app-env-dialog';
import { DeployRail } from '../components/deploy-rail';
import { Lamp } from '../components/lamp';
import { LogTape } from '../components/log-tape';
import { ToneLamp } from '../components/tone-lamp';
import { Button } from '../components/ui/button';
import { Dialog, DialogContent } from '../components/ui/dialog';
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

export function AppScreen() {
  const { user, loading, unauthenticated, canWrite } = useSession();
  const params = useParams({ strict: false });
  const appId = params.appId ?? '';
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [envDialogOpen, setEnvDialogOpen] = useState(false);

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

  const remove = useMutation({
    mutationFn: () => deleteApp(appId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
      void navigate({ to: '/' });
    },
    onError: (error) => {
      setConfirmDelete(false);
      setProblem(problemView(error, 'delete apps'));
    },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-muted">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  if (appQuery.isPending) {
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-8 sm:px-6">
        <div className="h-[140px] rounded-[6px] border border-rail bg-panel/60" />
      </main>
    );
  }

  if (appQuery.error !== null && appQuery.error !== undefined) {
    const view = problemView(appQuery.error, 'see this app');
    return (
      <main className="mx-auto w-full max-w-4xl px-4 py-6 sm:px-6 sm:py-8">
        <ProblemBanner view={view} />
        <Button variant="secondary" className="mt-4" asChild>
          <Link to="/">Back to apps</Link>
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

  const env: AppEnvVar[] = envQuery.data?.items ?? [];
  const canStart = app.status === 'stopped' || app.status === 'down';
  const canStop = app.status !== 'stopped';

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
          to="/"
          className="text-[13px] text-ink-muted underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
        >
          Apps
        </Link>
      </div>

      <header className="rounded-[6px] border border-rail bg-panel px-4 py-4 bay-shadow sm:px-5">
        <div className="flex flex-wrap items-start gap-x-4 gap-y-3">
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2.5">
              <ToneLamp tone={lamp.tone} pulse={lamp.pulse} label={lamp.statusText} />
              <h1 className="type-display nameplate truncate text-[26px] leading-none">
                {app.name}
              </h1>
            </div>
            <p className="mt-1.5 text-[13px] text-ink-muted">{lamp.caption}</p>
          </div>

          {canWrite ? (
            <div className="flex flex-wrap items-center gap-2">
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
                  variant="secondary"
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
                variant="secondary"
                disabled={action.isPending}
                onClick={() => {
                  setNotice(null);
                  action.mutate('restart');
                }}
              >
                Restart
              </Button>
              <Button
                variant="secondary"
                disabled={action.isPending}
                onClick={() => {
                  setNotice(null);
                  action.mutate('redeploy');
                }}
              >
                Redeploy
              </Button>
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

        <dl className="type-data mt-4 grid grid-cols-1 gap-x-6 gap-y-1 border-t border-rail pt-3 text-[13px] sm:grid-cols-2">
          <div className="flex gap-2">
            <dt className="text-ink-muted">Image</dt>
            <dd className="min-w-0 truncate text-ink">{app.image}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-muted">Port</dt>
            <dd className="text-ink">{app.port}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-muted">Node</dt>
            <dd className="text-ink">{nodeName}</dd>
          </div>
          <div className="flex gap-2">
            <dt className="text-ink-muted">Deployment</dt>
            <dd className="text-ink" title={app.current_deployment_id ?? undefined}>
              {app.current_deployment_id
                ? shortDeploymentId(app.current_deployment_id)
                : 'None yet'}
            </dd>
          </div>
        </dl>
      </header>

      {notice ? <div className="mt-2">{<NoticeLine>{notice}</NoticeLine>}</div> : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      <section className="mt-5 rounded-[6px] border border-rail bg-panel px-4 py-4 sm:px-5">
        <h2 className="type-display-light text-[15px] leading-none text-ink">Deployment</h2>
        <div className="mt-3">
          {deployment ? (
            <DeployRail
              deployment={deployment}
              error={deployment.status === 'failed' ? deployment.error : null}
            />
          ) : (
            <p className="text-[13px] text-ink-muted">
              No deployment yet. Redeploy to roll out the current image.
            </p>
          )}
        </div>

        {deployment ? (
          <details className="mt-4 border-t border-rail pt-3">
            <summary className="cursor-pointer select-none text-[13px] text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt">
              Progress lines
              {logsQuery.data ? ` (${logsQuery.data.items.length})` : ''}
            </summary>
            {progressError ? (
              <p className="mt-2 text-[13px] text-lamp-fault">{progressError}</p>
            ) : logsQuery.isPending ? (
              <p className="mt-2 text-[13px] text-ink-muted">Reading progress lines…</p>
            ) : logsQuery.data && logsQuery.data.items.length > 0 ? (
              <ul className="mt-2 flex flex-col gap-0.5">
                {logsQuery.data.items.map((entry, index) => (
                  <li
                    key={`${entry.time}-${index}`}
                    className="grid grid-cols-[80px_110px_minmax(0,1fr)] gap-2 text-[12.5px] leading-5"
                  >
                    <span className="type-data text-ink-muted" title={entry.time}>
                      {formatLogTime(entry.time)}
                    </span>
                    <span className="type-data text-ink-muted">
                      {deploymentPhaseWords(entry.phase)}
                    </span>
                    <span className="whitespace-pre-wrap break-words text-ink">
                      {entry.message}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="mt-2 text-[13px] text-ink-muted">
                No progress lines have been written yet.
              </p>
            )}
          </details>
        ) : null}
      </section>

      <section className="mt-5 rounded-[6px] border border-rail bg-panel px-4 py-4 sm:px-5">
        <AppDomains
          appId={app.id}
          domains={app.domains}
          canWrite={canWrite}
          onChanged={invalidate}
        />
      </section>

      <section className="mt-5 rounded-[6px] border border-rail bg-panel px-4 py-4 sm:px-5">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="type-display-light text-[15px] leading-none text-ink">
            Environment
          </h2>
          {canWrite ? (
            <Button variant="quiet" className="px-2.5" onClick={() => setEnvDialogOpen(true)}>
              Edit variables
            </Button>
          ) : null}
        </div>
        {env.length === 0 ? (
          <p className="mt-3 text-[13px] text-ink-muted">
            No variables yet. Add one to pass configuration to the app.
          </p>
        ) : (
          <ul className="mt-3 flex flex-col divide-y divide-rail border-y border-rail">
            {env.map((item) => (
              <li key={item.name} className="flex flex-wrap items-baseline gap-x-3 gap-y-1 py-2">
                <span className="type-data text-[14px] text-ink">{item.name}</span>
                <span className="text-[13px] text-ink-muted">
                  changed {relativeTime(item.updated_at)}
                </span>
              </li>
            ))}
          </ul>
        )}
        <p className="mt-2 text-[13px] leading-snug text-ink-muted">
          Values are write-only: Quoro stores them encrypted and never shows them again.
        </p>
      </section>

      <section className="mt-5 rounded-[6px] border border-rail bg-panel px-4 py-4 sm:px-5">
        <h2 className="type-display-light text-[15px] leading-none text-ink">Instances</h2>
        {app.instances.length === 0 ? (
          <p className="mt-3 text-[13px] text-ink-muted">
            No instance is running. Deploy the app to boot one.
          </p>
        ) : (
          <ul className="mt-3 flex flex-col divide-y divide-rail border-y border-rail">
            {app.instances.map((instance) => (
              <li
                key={instance.id}
                className="flex flex-wrap items-center gap-x-3 gap-y-2 py-2.5"
              >
                <Lamp vm={instance} size={18} />
                <span className="type-data min-w-0 flex-1 truncate text-[14px] text-ink">
                  {instance.name}
                </span>
                <span className="type-data text-[13px] text-ink-muted">
                  {instance.observed_status}
                </span>
                <span className="type-data text-[13px] text-ink-muted">
                  {instance.ip ?? 'No IP yet'}
                </span>
                <span className="type-data text-[13px] text-ink-muted">
                  {instance.restart_count} restarts
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>

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
            <Button variant="secondary" onClick={() => setConfirmDelete(false)}>
              Keep the app
            </Button>
            <Button
              variant="danger"
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
