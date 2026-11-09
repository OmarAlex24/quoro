// SPDX-License-Identifier: AGPL-3.0-only
import * as DialogPrimitive from '@radix-ui/react-dialog';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { Lock, MoreHorizontal, Play, RefreshCw, RotateCcw, Square, SquareTerminal, X } from 'lucide-react';
import { useLayoutEffect, useRef, useState, type ReactNode } from 'react';
import {
  getGetAppEnvQueryKey,
  getGetAppQueryKey,
  getListAppsQueryKey,
  redeployApp,
  restartApp,
  startApp,
  stopApp,
  useGetApp,
  useGetAppEnv,
} from '../api/generated/apps/apps';
import { getListAppBuildsQueryKey, useListAppBuilds } from '../api/generated/builds/builds';
import { getListAppDeploymentsQueryKey } from '../api/generated/deployments/deployments';
import type { App, AppDetail, Deployment } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useGetAppVulnerabilities } from '../api/generated/vulnerabilities/vulnerabilities';
import { appsPollingInterval, relativeTime, serviceLampState } from '../lib/app-status';
import { canOpenTerminal } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { PROJECT_TABS, type ProjectTab } from '../lib/routes';
import { appAddress, appKind } from '../lib/services';
import { securityParams, securityTabCount } from '../lib/vulnerabilities';
import { cachedBuildLive, useRecentBuilds } from '../lib/use-builds';
import { useSession } from '../session';
import { AppEnvDialog } from './app-env-dialog';
import { GlyphTile } from './glyph-tile';
import { LampDot } from './lamp';
import { LogTape } from './log-tape';
import { MenuPanel, useMenu } from './shell/menu';
import { DeploymentsTab, RUN_PAGE } from './sheet-deployments';
import { MetricsTab } from './sheet-metrics';
import { SecurityTab } from './sheet-security';
import { SettingsTab } from './sheet-settings';
import { StorageTab } from './sheet-storage';
import { Button } from './ui/button';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { Tag } from './ui/tag';

const TAB_WORDS: Record<ProjectTab, string> = {
  deployments: 'Deployments',
  variables: 'Variables',
  metrics: 'Metrics',
  logs: 'Logs',
  storage: 'Storage',
  security: 'Security',
  settings: 'Settings',
};

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

/**
 * An app opened from the canvas: a sheet at the right with the app's name,
 * its lamp and its tabs. The canvas stays behind it and stays live, so
 * another card opens in the same sheet. Escape or the close button return to
 * the canvas; the caller puts focus back on the card. The tab and an open
 * build live in the URL.
 */
export function ServiceSheet({
  projectId,
  appId,
  tab,
  buildId,
  onClose,
}: {
  projectId: string;
  appId: string;
  tab: ProjectTab;
  buildId?: string;
  onClose: () => void;
}) {
  const contentRef = useRef<HTMLDivElement>(null);
  const queryClient = useQueryClient();
  // The canvas draws this app's card from this same read, with the same
  // refresh rule as the list: every 2 s while it deploys or builds.
  const appQuery = useGetApp(appId, {
    query: {
      retry: false,
      refetchInterval: (query) =>
        appsPollingInterval(
          query.state.data ? [query.state.data] : [],
          query.state.data?.source != null && cachedBuildLive(queryClient, appId),
        ),
    },
  });
  const app = appQuery.data;

  return (
    <DialogPrimitive.Root open modal={false} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPrimitive.Content
        ref={contentRef}
        className="sheet svc-sheet"
        aria-describedby={undefined}
        // The canvas stays usable behind the sheet: a click on another card
        // opens it here instead of closing the sheet.
        onInteractOutside={(event) => event.preventDefault()}
        // Focus lands on the sheet itself; Tab walks its header, tabs and body.
        onOpenAutoFocus={(event) => {
          event.preventDefault();
          contentRef.current?.focus();
        }}
        // The caller returns focus to the card that opened the sheet.
        onCloseAutoFocus={(event) => event.preventDefault()}
        onEscapeKeyDown={(event) => {
          // Escape inside a menu closes the menu, and on a chart its
          // crosshair, not the sheet.
          if ((event.target as Element | null)?.closest?.('[role="menu"], [data-crosshair]')) event.preventDefault();
        }}
      >
        {app ? (
          <SheetBody key={app.id} app={app} projectId={projectId} tab={tab} buildId={buildId} />
        ) : (
          <>
            <div className="sh-head">
              <div className="sh-title">
                <DialogPrimitive.Title className="sh-name">
                  {appQuery.isPending ? 'Opening the service…' : 'Service not available'}
                </DialogPrimitive.Title>
                <span className="flex-1" />
                <CloseButton />
              </div>
            </div>
            <div className="sh-body">
              {appQuery.isPending ? (
                <div className="box h-[220px] opacity-60" aria-hidden="true" />
              ) : (
                <ProblemBanner view={problemView(appQuery.error, 'see this app')} />
              )}
            </div>
          </>
        )}
      </DialogPrimitive.Content>
    </DialogPrimitive.Root>
  );
}

/** The sheet's close button; the app and the VM sheet share it. */
export function CloseButton() {
  return (
    <DialogPrimitive.Close className="iconbtn" aria-label="Close">
      <X strokeWidth={1.7} aria-hidden="true" />
    </DialogPrimitive.Close>
  );
}

function SheetBody({
  app,
  projectId,
  tab,
  buildId,
}: {
  app: AppDetail;
  projectId: string;
  tab: ProjectTab;
  buildId?: string;
}) {
  const { me, canWrite } = useSession();
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const nodes = nodesQuery.data?.items ?? [];
  const nodeNames = Object.fromEntries(nodes.map((node) => [node.id, node.name]));
  const nodeStatuses = Object.fromEntries(nodes.map((node) => [node.id, node.status]));
  const source = app.source ?? null;

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: getGetAppQueryKey(app.id) });
    void queryClient.invalidateQueries({ queryKey: getListAppsQueryKey() });
    void queryClient.invalidateQueries({ queryKey: getListAppDeploymentsQueryKey(app.id) });
    void queryClient.invalidateQueries({ queryKey: getListAppBuildsQueryKey(app.id) });
    void queryClient.invalidateQueries({ queryKey: getGetAppEnvQueryKey(app.id) });
  };

  const action = useMutation<App | Deployment, unknown, ActionKind>({
    mutationFn: (kind) => {
      if (kind === 'start') return startApp(app.id);
      if (kind === 'stop') return stopApp(app.id);
      if (kind === 'restart') return restartApp(app.id);
      return redeployApp(app.id);
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
  const run = (kind: ActionKind) => {
    setNotice(null);
    action.mutate(kind);
  };

  // The card reads the same two things the same way (`serviceLampState`).
  const builds = useRecentBuilds([app]);
  const lamp = serviceLampState(app.status, builds[app.id]);
  const terminal = canWrite && canOpenTerminal(me);
  // The open findings on the Security tab: the same request the tab makes
  // with no filter, so opening the tab reads it from the cache.
  const vulnerabilities = useGetAppVulnerabilities(app.id, securityParams(), {
    query: { refetchInterval: 30_000 },
  });
  const openFindings = securityTabCount(vulnerabilities.data);

  return (
    <>
      <div className="sh-head">
        <div className="sh-title">
          <GlyphTile kind={appKind(app)} size="lg" label />
          <DialogPrimitive.Title className="sh-name">{app.name}</DialogPrimitive.Title>
          <span className="flex-1" />
          {terminal ? (
            <Button size="sm" asChild className="max-[479.98px]:hidden">
              <Link to="/apps/$appId/terminal" params={{ appId: app.id }}>
                <SquareTerminal className="size-3.5" strokeWidth={1.7} aria-hidden="true" />
                Terminal
              </Link>
            </Button>
          ) : null}
          {canWrite ? (
            <ActionsMenu app={app} busy={action.isPending} onAction={run} />
          ) : null}
          <CloseButton />
        </div>
        <div className="sh-meta">
          <span className="sh-state">
            <LampDot ring={lamp.tone} core={lamp.tone} pulse={lamp.pulse} size={10} />
            {lamp.statusText}
          </span>
          <span>{appAddress(app, app.domains[0]?.hostname)}</span>
        </div>
      </div>

      <SheetTabs label={`${app.name} sections`} tab={tab}>
        {PROJECT_TABS.map((key) => (
          <Link
            key={key}
            to="/projects/$projectId"
            params={{ projectId }}
            search={{ app: app.id, tab: key }}
            className={key === tab ? 'on' : undefined}
            aria-current={key === tab ? 'page' : undefined}
          >
            {TAB_WORDS[key]}
            {key === 'security' && openFindings !== null ? (
              <>
                <span className="count count-soft" aria-hidden="true">
                  {openFindings}
                </span>
                <span className="sr-only">
                  , {openFindings} open {openFindings === 1 ? 'finding' : 'findings'}
                </span>
              </>
            ) : null}
          </Link>
        ))}
      </SheetTabs>

      <div className="sh-body">
        {notice ? <NoticeLine>{notice}</NoticeLine> : null}
        {problem ? <ProblemBanner view={problem} /> : null}

        {tab === 'deployments' ? (
          <DeploymentsTab
            app={app}
            projectId={projectId}
            buildId={buildId}
            canWrite={canWrite}
            nodeNames={nodeNames}
            onChanged={invalidate}
            onRedeploy={() => run('redeploy')}
            redeploying={action.isPending}
          />
        ) : null}
        {tab === 'variables' ? (
          <VariablesTab app={app} canWrite={canWrite} onSaved={invalidate} />
        ) : null}
        {tab === 'metrics' ? <MetricsTab app={app} nodeNames={nodeNames} /> : null}
        {tab === 'logs' ? <LogTape appId={app.id} tall /> : null}
        {tab === 'storage' ? (
          <StorageTab app={app} canWrite={canWrite} nodeNames={nodeNames} onChanged={invalidate} />
        ) : null}
        {tab === 'security' ? <SecurityTab app={app} canWrite={canWrite} onRebuilt={invalidate} /> : null}
        {tab === 'settings' ? (
          <SettingsWithBuilds
            app={app}
            canWrite={canWrite}
            terminal={terminal}
            nodeNames={nodeNames}
            nodeStatuses={nodeStatuses}
            onChanged={invalidate}
            hasSource={source !== null}
          />
        ) : null}
      </div>
    </>
  );
}

/**
 * The tab row. On a narrow sheet it scrolls sideways inside its own box: the
 * edge with more tabs behind it fades, and the active tab is scrolled into
 * view whenever it changes.
 */
export function SheetTabs({ label, tab, children }: { label: string; tab: string; children: ReactNode }) {
  const ref = useRef<HTMLElement>(null);
  const [edges, setEdges] = useState<'none' | 'start' | 'end' | 'both'>('none');

  useLayoutEffect(() => {
    const nav = ref.current;
    if (!nav) return;
    const measure = () => {
      const before = nav.scrollLeft > 1;
      const after = nav.scrollLeft + nav.clientWidth < nav.scrollWidth - 1;
      setEdges(before && after ? 'both' : before ? 'start' : after ? 'end' : 'none');
    };
    measure();
    nav.addEventListener('scroll', measure, { passive: true });
    const observer = new ResizeObserver(measure);
    observer.observe(nav);
    return () => {
      nav.removeEventListener('scroll', measure);
      observer.disconnect();
    };
  }, []);

  // Scroll the row itself, not the page: scrollIntoView could also move the
  // sheet's body.
  useLayoutEffect(() => {
    const nav = ref.current;
    const active = nav?.querySelector<HTMLElement>('a.on');
    if (!nav || !active) return;
    // The row is positioned, so a tab's offset is measured from the row itself.
    const pad = 32;
    const left = active.offsetLeft;
    const right = left + active.offsetWidth;
    if (left - pad < nav.scrollLeft) nav.scrollLeft = Math.max(0, left - pad);
    else if (right + pad > nav.scrollLeft + nav.clientWidth) nav.scrollLeft = right + pad - nav.clientWidth;
  }, [tab]);

  return (
    <nav ref={ref} className="tabs sh-tabs" aria-label={label} data-fade={edges}>
      {children}
    </nav>
  );
}

/** Start, stop, restart and redeploy, as they were on the app page. Delete lives in Settings. */
function ActionsMenu({
  app,
  busy,
  onAction,
}: {
  app: AppDetail;
  busy: boolean;
  onAction: (kind: ActionKind) => void;
}) {
  const menu = useMenu();
  const source = app.source ?? null;
  const canStart = app.status === 'stopped' || app.status === 'down';
  const canStop = app.status !== 'stopped';
  const canRedeploy = source === null || app.image !== '';
  return (
    <div ref={menu.rootRef} className="relative">
      <button
        ref={menu.triggerRef}
        type="button"
        className="iconbtn"
        aria-label={`Actions for ${app.name}`}
        {...menu.triggerProps}
      >
        <MoreHorizontal strokeWidth={1.7} aria-hidden="true" />
      </button>
      <MenuPanel menu={menu} label={`Actions for ${app.name}`} align="end">
        {canStart ? (
          <button type="button" role="menuitem" className="menu-item" disabled={busy} onClick={() => onAction('start')}>
            <Play strokeWidth={1.7} aria-hidden="true" />
            Start
          </button>
        ) : null}
        {canStop ? (
          <button type="button" role="menuitem" className="menu-item" disabled={busy} onClick={() => onAction('stop')}>
            <Square strokeWidth={1.7} aria-hidden="true" />
            Stop
          </button>
        ) : null}
        <button type="button" role="menuitem" className="menu-item" disabled={busy} onClick={() => onAction('restart')}>
          <RotateCcw strokeWidth={1.7} aria-hidden="true" />
          Restart
        </button>
        {canRedeploy ? (
          <button type="button" role="menuitem" className="menu-item" disabled={busy} onClick={() => onAction('redeploy')}>
            <RefreshCw strokeWidth={1.7} aria-hidden="true" />
            Redeploy
          </button>
        ) : null}
      </MenuPanel>
    </div>
  );
}

function SettingsWithBuilds({
  hasSource,
  ...props
}: Omit<Parameters<typeof SettingsTab>[0], 'builds'> & { hasSource: boolean }) {
  const buildsQuery = useListAppBuilds(props.app.id, RUN_PAGE, { query: { enabled: hasSource } });
  return <SettingsTab {...props} builds={hasSource ? (buildsQuery.data?.items ?? []) : []} />;
}

/**
 * The app's variables by name, with when each was set. Values are
 * write-only, so the list never shows one; editing replaces the whole set
 * through the same dialog the app page used.
 */
function VariablesTab({
  app,
  canWrite,
  onSaved,
}: {
  app: AppDetail;
  canWrite: boolean;
  onSaved: () => void;
}) {
  const [open, setOpen] = useState(false);
  const envQuery = useGetAppEnv(app.id);
  const env = envQuery.data?.items ?? [];
  const secrets = new Set(app.source?.secrets ?? []);

  return (
    <>
      <div className="flex items-center gap-2.5">
        <b className="text-[14px] font-semibold text-ink">
          {envQuery.isPending ? 'Variables' : `${env.length} ${env.length === 1 ? 'variable' : 'variables'}`}
        </b>
        <span className="flex-1" />
        {canWrite ? (
          <Button size="sm" onClick={() => setOpen(true)} disabled={envQuery.isPending}>
            Edit variables
          </Button>
        ) : null}
      </div>
      <p className="notice">
        <Lock strokeWidth={1.7} aria-hidden="true" />
        <span>Values are write-only: Quoro stores them encrypted and never shows them again.</span>
      </p>
      {envQuery.error ? (
        <ProblemBanner view={problemView(envQuery.error, 'see variables')} />
      ) : envQuery.isPending ? (
        <div className="box h-[120px] opacity-60" aria-hidden="true" />
      ) : env.length === 0 ? (
        <p className="box px-4 py-4 text-[13px] text-ink-2">
          No variables yet. Add one to pass configuration to the app.
        </p>
      ) : (
        <ul className="box" aria-label="Variables">
          {env.map((item) => (
            <li key={item.name} className="keyrow">
              <span className="keyrow-name type-code" title={item.name}>
                {item.name}
              </span>
              <span className="keyrow-when">Set {relativeTime(item.updated_at)}</span>
              {secrets.has(item.name) ? <Tag tone="plain">Also a build secret</Tag> : null}
            </li>
          ))}
        </ul>
      )}
      <AppEnvDialog open={open} onOpenChange={setOpen} appId={app.id} env={env} onSaved={onSaved} />
    </>
  );
}
