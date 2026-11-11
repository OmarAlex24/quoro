// SPDX-License-Identifier: AGPL-3.0-only
import { useQueries } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { getListAppDomainsQueryOptions } from '../api/generated/domains/domains';
import { useListApps } from '../api/generated/apps/apps';
import { useListGrants } from '../api/generated/grants/grants';
import { useListVms } from '../api/generated/vms/vms';
import type { App, Build, Deployment, Domain, Node, Project, Vm } from '../api/generated/model';
import { SERVICE_LIST_PARAMS } from '../lib/projects';
import {
  deployBarWidth,
  deployRows,
  deployWhen,
  domainRows,
  placement,
  projectGrants,
  projectSummary,
  type AgentAccess,
  type DeployRow,
  type PlacementCell,
} from '../lib/canvas-rail';
import { drawMeter, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { grantKindWords, grantMeter } from '../lib/grants';
import { useNow } from '../lib/use-now';
import { cn } from '../lib/utils';
import { Avatar } from './avatar';
import { DitherCanvas } from './dither-canvas';
import { GlyphTile } from './glyph-tile';
import { AgentName, UserName } from './identity-name';
import { LampDot } from './lamp';
import { SkelBar, Skeleton } from './skeleton';

/** The service a rail cell or a card points at, shared so each lights the other. */
export interface Highlight {
  key: string | null;
  set: (key: string | null) => void;
}

/**
 * The project rail beside the canvas: where each instance runs, the latest
 * deploys, which agents hold a grant here and the domains. Pointing at a
 * cell, or focusing it, lights its card, and pointing at a card lights its
 * cells.
 */
export function ProjectRail({
  projectId,
  project,
  apps,
  vms,
  nodes,
  builds,
  deployments,
  loading,
  highlight,
}: {
  projectId: string;
  project: Pick<Project, 'name' | 'slug'> | undefined;
  apps: readonly App[];
  /** Every VM of the project, an app's instances and the standalone ones. */
  vms: readonly Vm[];
  nodes: readonly Node[];
  builds: Readonly<Record<string, readonly Build[]>>;
  deployments: Readonly<Record<string, readonly Deployment[]>>;
  /** The apps or the VMs have not come back yet. */
  loading: boolean;
  highlight: Highlight;
}) {
  const now = useNow(true, 30_000);
  const groups = useMemo(
    () => placement([...vms], apps, nodes, project?.name ?? 'this project'),
    [vms, apps, nodes, project?.name],
  );
  const rows = useMemo(() => deployRows(apps, builds, deployments, now), [apps, builds, deployments, now]);
  const grants = useListGrants(
    { status: 'active', limit: 500 },
    { query: { staleTime: 30_000, refetchInterval: 60_000, retry: false } },
  );
  const access = useMemo(
    () => (project ? projectGrants(grants.data?.items ?? [], project, apps, now) : []),
    [grants.data, project, apps, now],
  );
  // The same reads the cards make (same query keys), so no second request.
  const domainReads = useQueries({
    queries: apps.map((app) =>
      getListAppDomainsQueryOptions(app.id, undefined, { query: { staleTime: 10_000, retry: false } }),
    ),
  });
  const domains = domainRows(
    domainReads.flatMap((read): Domain[] => read.data?.items ?? []),
    now,
  );
  const domainsLoading = domainReads.some((read) => read.isPending && read.fetchStatus !== 'idle');

  return (
    <aside className="card crail" aria-label="Project summary">
      <section className="crail-s" aria-labelledby="crail-placement">
        <div className="psec-h">
          <h2 id="crail-placement">Placement</h2>
          <Link to="/nodes" className="psec-link">
            Open nodes
          </Link>
        </div>
        {loading ? (
          <Skeleton label="Loading where the instances run" className="crail-skel">
            <SkelBar width="40%" />
            <SkelBar height={22} />
            <SkelBar width="40%" />
            <SkelBar height={22} />
          </Skeleton>
        ) : groups.length === 0 ? (
          <p className="crail-none">No instance runs yet.</p>
        ) : (
          <div className="crail-rack">
            {groups.map((group) => (
              <div key={group.nodeId} className="crail-node">
                <div className="crail-node-h">
                  <LampDot ring={group.tone} core={group.tone} size={10} label={group.state} />
                  <b>{group.name}</b>
                  <span>{group.share}</span>
                </div>
                <ul className="vcells crail-cells" aria-label={`Instances on ${group.name}`}>
                  {group.cells.map((cell) => (
                    <RailCell key={cell.key} cell={cell} projectId={projectId} highlight={highlight} />
                  ))}
                </ul>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="crail-s" aria-labelledby="crail-deploys">
        <div className="psec-h">
          <h2 id="crail-deploys">Recent deployments</h2>
        </div>
        {rows.length === 0 && (loading || apps.some((app) => !deployments[app.id])) ? (
          <Skeleton label="Loading the latest deploys" className="crail-skel">
            <SkelBar width="70%" />
            <SkelBar width="55%" />
            <SkelBar width="62%" />
          </Skeleton>
        ) : rows.length === 0 ? (
          <p className="crail-none">Nothing deployed yet.</p>
        ) : (
          <DeployList rows={rows} now={now} projectId={projectId} />
        )}
      </section>

      <section className="crail-s" aria-labelledby="crail-access">
        <div className="psec-h">
          <h2 id="crail-access">Agents with access</h2>
          <Link to="/agents" search={{ tab: 'grants', status: 'active' }} className="psec-link">
            Grants
          </Link>
        </div>
        {grants.isPending && grants.fetchStatus !== 'idle' ? (
          <Skeleton label="Loading the grants" className="crail-skel">
            <SkelBar width="80%" />
            <SkelBar height={8} />
          </Skeleton>
        ) : access.length === 0 ? (
          <p className="crail-none">{grants.isError ? 'The grants could not be read.' : 'No agent holds a grant here.'}</p>
        ) : (
          <ul className="crail-list">
            {access.map((entry) => (
              <AccessRow key={entry.agentId} entry={entry} now={now} />
            ))}
          </ul>
        )}
      </section>

      <section className="crail-s" aria-labelledby="crail-domains">
        <div className="psec-h">
          <h2 id="crail-domains">Domains</h2>
        </div>
        {domainsLoading && domains.length === 0 ? (
          <Skeleton label="Loading the domains" className="crail-skel">
            <SkelBar width="75%" />
          </Skeleton>
        ) : domains.length === 0 ? (
          <p className="crail-none">No domain points here.</p>
        ) : (
          <ul className="crail-list">
            {domains.map((domain) => (
              <li key={domain.id} className="crail-dom">
                <LampDot ring={domain.tone} core={domain.tone} size={10} />
                <p>
                  <b>{domain.hostname}</b> · {domain.words}
                </p>
              </li>
            ))}
          </ul>
        )}
      </section>
    </aside>
  );
}

/**
 * One VM: as wide as its vCPUs, a link to it, and the pointer and focus
 * light its card. A cell is narrow, so a state worth saying is its edge
 * (amber, red) instead of a lamp; the words are in its title and for
 * screen readers.
 */
function RailCell({ cell, projectId, highlight }: { cell: PlacementCell; projectId: string; highlight: Highlight }) {
  const on = () => highlight.set(cell.service);
  const off = () => highlight.set(null);
  return (
    <li className="crail-cell" style={{ '--u': cell.units } as CSSProperties}>
      <Link
        to="/projects/$projectId"
        params={{ projectId }}
        search={{ vm: cell.key }}
        className="vcell"
        data-k={cell.kind}
        data-tone={cell.tone ?? undefined}
        data-hl={highlight.key === cell.service ? '' : undefined}
        title={cell.title}
        onPointerEnter={on}
        onPointerLeave={off}
        onFocus={on}
        onBlur={off}
      >
        <span aria-hidden="true">{cell.label}</span>
        <span className="sr-only">{cell.title}</span>
      </Link>
    </li>
  );
}

/**
 * The latest deploys; a run that starts while the list is open slides in at
 * the top. A row counts as new only when its run started after the list
 * mounted: a row whose key changes because its build was read late (a
 * deployment becomes its build) is not new.
 */
function DeployList({ rows, now, projectId }: { rows: readonly DeployRow[]; now: number; projectId: string }) {
  const [mountedAt] = useState(() => Date.now());
  const seen = useRef<Set<string> | null>(null);
  const first = seen.current;
  useEffect(() => {
    seen.current = new Set(rows.map((row) => row.key));
  }, [rows]);
  const longest = Math.max(0, ...rows.map((row) => row.seconds ?? 0));
  return (
    <ul className="crail-list">
      {rows.map((row) => (
        <li key={row.key} className={cn(first !== null && !first.has(row.key) && row.start > mountedAt && 'crail-new')}>
          <Link
            to="/projects/$projectId"
            params={{ projectId }}
            search={{ app: row.appId, tab: 'deployments' }}
            className="crail-dep"
          >
            <GlyphTile kind={row.kind} className="glyph-sm" />
            <span className="crail-dep-t">
              <b>{row.name}</b>
              <small>
                {row.title.text}
                {row.title.code ? <span className="type-code">{row.title.code}</span> : null}
                {row.actor ? (
                  <>
                    {' · '}
                    {row.actor.type === 'user' ? <UserName id={row.actor.id} plain /> : <AgentName id={row.actor.id} link={false} />}
                  </>
                ) : null}
                {' · '}
                {deployWhen(row, now)}
              </small>
            </span>
            <span
              className="crail-bar"
              data-s={row.state}
              style={{ width: deployBarWidth(row.seconds, longest) }}
              aria-hidden="true"
            />
          </Link>
        </li>
      ))}
    </ul>
  );
}

/** An agent that holds a grant here: what it may do, and how much of its grant is left. */
function AccessRow({ entry, now }: { entry: AgentAccess; now: number }) {
  const meter = grantMeter(entry.grant, now);
  const share = meter?.share ?? 0;
  const paint = useCallback((surface: DitherSurface, read: TokenReader) => drawMeter(surface, share, read), [share]);
  return (
    <li className="crail-acc">
      <p className="crail-acc-who">
        <Avatar kind="agent" seed={entry.agentId} />
        <span>
          <b>
            <AgentName id={entry.agentId} link={false} />
          </b>{' '}
          can {entry.phrase} {entry.where}
        </span>
      </p>
      {meter ? <DitherCanvas paint={paint} label={meter.words} className="crail-meter" /> : null}
      <p className="crail-acc-f">
        <span>
          {grantKindWords[entry.grant.kind]}
          {entry.more > 0 ? ` · ${entry.more} more` : ''}
        </span>
        {meter ? <span>{meter.words}</span> : null}
      </p>
    </li>
  );
}

/**
 * The top bar's line while the canvas is open: how many services run, deploy
 * or are down, and how many instances run on how many nodes. It reads the
 * canvas's own lists (same query keys), so it adds no request.
 */
export function ProjectSummaryLine({ projectId }: { projectId: string }) {
  const apps = useListApps({ project_id: projectId, ...SERVICE_LIST_PARAMS }, { query: { staleTime: 2_000 } });
  const vms = useListVms({ project_id: projectId, ...SERVICE_LIST_PARAMS }, { query: { staleTime: 2_000 } });
  if (!apps.data || !vms.data) return null;
  const summary = projectSummary(
    apps.data.items.filter((app) => app.project_id === projectId),
    vms.data.items.filter((vm) => vm.project_id === projectId),
  );
  const first = summary.states[0];
  if (!first && !summary.instances) return null;
  return (
    <p className="top-sum">
      {first ? (
        <span>
          <LampDot ring={first.tone} core={first.tone} size={10} />
          {summary.states.map((state) => state.words).join(' · ')}
        </span>
      ) : null}
      {summary.instances ? <span>{summary.instances}</span> : null}
    </p>
  );
}
