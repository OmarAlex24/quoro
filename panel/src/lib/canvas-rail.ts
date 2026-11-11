// SPDX-License-Identifier: AGPL-3.0-only
import type { App, Build, Deployment, Domain, Grant, Node, Project, Vm } from '../api/generated/model';
import { accessPhrase } from './access';
import { relativeTime, servingTone, servingWords, tlsWords } from './app-status';
import { imageLabel, isRunLive, joinRuns, runTimeline, type RunState } from './deploy-tape';
import type { LampTone } from './lamp';
import { nodeReadout } from './nodes';
import { vmCells, type VmCell } from './nodes-page';
import { appKey, appKind, isStandaloneVm, vmKey } from './services';

/*
 * The project rail beside the canvas, as pure data: where each instance runs
 * (`GET /vms` grouped by `node_id`), the latest deploys (each app's builds and
 * deployments), which agents hold an active grant here (`GET /grants`
 * filtered by project in the client) and the domains (`GET /apps/{id}/domains`).
 * Also the top bar's one-line summary of the project.
 */

/* ---------- the top bar summary ---------- */

export interface ProjectSummary {
  /** `5 running`, `1 deploying`, `1 down`: only the states some service is in. */
  states: Array<{ tone: LampTone; words: string }>;
  /** `7 instances on 2 nodes`; null with no instance. */
  instances: string | null;
}

/**
 * How the project's services stand, counted as the canvas shows them: an app
 * by its status, a standalone VM by what its node reports. Instances are every
 * VM of the project, an app's and the standalone ones alike.
 */
export const projectSummary = (
  apps: ReadonlyArray<Pick<App, 'status'>>,
  vms: ReadonlyArray<Pick<Vm, 'app_id' | 'node_id' | 'observed_status'>>,
): ProjectSummary => {
  const counts = { running: 0, deploying: 0, degraded: 0, down: 0 };
  for (const app of apps) {
    if (app.status in counts) counts[app.status as keyof typeof counts] += 1;
  }
  const live = vms.filter((vm) => vm.observed_status !== 'deleted');
  for (const vm of live) {
    if (!isStandaloneVm(vm)) continue;
    if (vm.observed_status === 'running') counts.running += 1;
    else if (vm.observed_status === 'crashed') counts.down += 1;
  }
  const tones: Record<keyof typeof counts, LampTone> = { running: 'run', deploying: 'shift', degraded: 'shift', down: 'fault' };
  const states = (Object.keys(counts) as Array<keyof typeof counts>)
    .filter((state) => counts[state] > 0)
    .map((state) => ({ tone: tones[state], words: `${counts[state]} ${state}` }));
  const nodes = new Set(live.map((vm) => vm.node_id)).size;
  return {
    states,
    instances:
      live.length === 0
        ? null
        : `${live.length} ${live.length === 1 ? 'instance' : 'instances'} on ${nodes} ${nodes === 1 ? 'node' : 'nodes'}`,
  };
};

/* ---------- placement ---------- */

export interface PlacementCell extends VmCell {
  /** The service the cell belongs to, as the canvas keys it: its app's, or the VM's own. */
  service: string;
}

export interface PlacementNode {
  nodeId: string;
  name: string;
  tone: LampTone;
  /** The node's state in a word, for the lamp's label. */
  state: string;
  /** `6 of 12 vCPU to Atlas`: what the project's VMs reserve there, of what the node has. */
  share: string;
  cells: PlacementCell[];
}

/**
 * Where each of the project's instances runs: one group per node that holds
 * any, sorted by name, with a cell per VM as wide as its vCPUs (`vmCells`,
 * the same strip the Nodes page draws). A node the list does not hold keeps
 * its id as its name.
 */
export const placement = (
  vms: readonly Vm[],
  apps: ReadonlyArray<Pick<App, 'id' | 'name' | 'source' | 'status'>>,
  nodes: ReadonlyArray<Pick<Node, 'id' | 'name' | 'status' | 'capacity'>>,
  projectName: string,
): PlacementNode[] => {
  const live = vms.filter((vm) => vm.observed_status !== 'deleted');
  const byId = new Map(live.map((vm) => [vm.id, vm]));
  const nodeIds = [...new Set(live.map((vm) => vm.node_id))];
  return nodeIds
    .map((nodeId) => {
      const node = nodes.find((candidate) => candidate.id === nodeId);
      const readout = node ? nodeReadout(node.status) : { tone: 'unknown' as const, words: 'Not listed' };
      const reserved = live.filter((vm) => vm.node_id === nodeId).reduce((total, vm) => total + vm.vcpus, 0);
      const of = node ? ` of ${node.capacity.cpus}` : '';
      return {
        nodeId,
        name: node?.name ?? nodeId,
        tone: readout.tone,
        state: readout.words,
        share: `${reserved}${of} vCPU to ${projectName}`,
        cells: vmCells(nodeId, live, apps).map((cell) => {
          const vm = byId.get(cell.key);
          return { ...cell, service: vm && vm.app_id !== null ? appKey(vm.app_id) : vmKey(cell.key) };
        }),
      };
    })
    .sort((left, right) => left.name.localeCompare(right.name));
};

/* ---------- recent deployments ---------- */

export interface DeployRow {
  key: string;
  appId: string;
  service: string;
  name: string;
  kind: 'git' | 'image';
  /** What ran: `Build` and its sha (`code`, set in Martian Mono), `Redeploy`, or the image deployed. */
  title: { text: string; code?: string };
  /** Who started a deployment that no build started; null for a build or the system. */
  actor: { type: Deployment['created_by_type']; id: string } | null;
  /** Epoch milliseconds the run started. */
  start: number;
  state: RunState;
  /** Seconds from start to end; null while it runs. */
  seconds: number | null;
}

/**
 * The project's latest runs across its apps, newest first: each app's builds
 * joined to the deployments they started (`joinRuns`, as the Deployments tab
 * reads them). A run whose lists have not been read is not shown.
 */
export const deployRows = (
  apps: ReadonlyArray<Pick<App, 'id' | 'name' | 'source'>>,
  builds: Readonly<Record<string, readonly Build[]>>,
  deployments: Readonly<Record<string, readonly Deployment[]>>,
  now: number,
  limit = 5,
): DeployRow[] =>
  apps
    .flatMap((app) => {
      const listed = deployments[app.id];
      if (!listed) return [];
      const kind = appKind(app);
      return joinRuns(kind === 'git' ? (builds[app.id] ?? []) : [], listed).map((run): DeployRow => {
        const timeline = runTimeline(run, undefined, now);
        const deployment = run.deployment;
        let title: DeployRow['title'];
        if (run.build) {
          title = run.build.commit ? { text: 'Build ', code: run.build.commit.slice(0, 7) } : { text: `Build of ${run.build.source.ref}` };
        } else if (deployment!.trigger === 'redeploy') {
          title = { text: 'Redeploy' };
        } else if (deployment!.trigger === 'rollback') {
          title = { text: 'Rollback' };
        } else if (deployment!.trigger === 'build') {
          title = { text: 'Deploy of a built image' };
        } else {
          title = { text: imageLabel(deployment!.image) };
        }
        const actor =
          !run.build && deployment && deployment.created_by_type !== 'system'
            ? { type: deployment.created_by_type, id: deployment.created_by_id }
            : null;
        return {
          key: run.key,
          appId: app.id,
          service: appKey(app.id),
          name: app.name,
          kind,
          title,
          actor,
          start: timeline.start,
          state: timeline.state,
          seconds: isRunLive(run) ? null : timeline.elapsed,
        };
      });
    })
    .sort((left, right) => right.start - left.start)
    .slice(0, limit);

/** `34 s`, `1 m 47 s`, `1 h 02 m`: how long a run took, as the rail says it. */
export const spanWords = (seconds: number): string => {
  const whole = Math.max(0, Math.round(seconds));
  if (whole < 60) return `${whole} s`;
  const minutes = Math.floor(whole / 60);
  if (minutes < 60) return `${minutes} m ${whole % 60} s`;
  return `${Math.floor(minutes / 60)} h ${String(minutes % 60).padStart(2, '0')} m`;
};

/** The width of a row's bar in pixels: its length against the longest row's, at least 6. */
export const deployBarWidth = (seconds: number | null, longest: number, max = 54): number =>
  seconds === null || longest <= 0 ? 6 : Math.max(6, Math.round((seconds / longest) * max));

/** `40 min ago · 1 m 47 s`, or `just now` while it runs. */
export const deployWhen = (row: Pick<DeployRow, 'start' | 'seconds'>, now: number): string => {
  const when = relativeTime(new Date(row.start).toISOString(), now);
  return row.seconds === null ? when : `${when} · ${spanWords(row.seconds)}`;
};

/* ---------- agents with access ---------- */

export interface AgentAccess {
  agentId: string;
  /** The grant the row speaks for: an incident grant first, then the one that ends first. */
  grant: Grant;
  /** `read and restart apps`. */
  phrase: string;
  /** `here` for the project or every resource; `on blog` for a grant on some of its apps. */
  where: string;
  /** Other active grants the same agent holds here. */
  more: number;
}

/**
 * Which agents hold an active grant on the project, from `GET /grants?status=active`
 * filtered here: a grant on every resource, on the project by its slug, or on
 * any of its apps. A grant whose scopes do nothing inside a project, or that
 * expired since it was read, is left out. One row per agent.
 */
export const projectGrants = (
  grants: readonly Grant[],
  project: Pick<Project, 'slug'>,
  apps: ReadonlyArray<Pick<App, 'id' | 'name'>>,
  now: number,
): AgentAccess[] => {
  const appNames = new Map(apps.map((app) => [app.id, app.name]));
  const covering = grants.flatMap((grant) => {
    if (grant.status !== 'active') return [];
    const expires = Date.parse(grant.expires_at);
    if (Number.isNaN(expires) || expires <= now) return [];
    const phrase = accessPhrase(grant.scopes);
    if (phrase === null) return [];
    const whole = grant.selector.all === true || (grant.selector.projects ?? []).includes(project.slug);
    const named = (grant.selector.apps ?? []).flatMap((id) => {
      const name = appNames.get(id);
      return name ? [name] : [];
    });
    if (!whole && named.length === 0) return [];
    return [{ grant, phrase, where: whole ? 'here' : `on ${named.sort().join(', ')}` }];
  });
  const byAgent = new Map<string, typeof covering>();
  for (const entry of covering) {
    byAgent.set(entry.grant.agent_id, [...(byAgent.get(entry.grant.agent_id) ?? []), entry]);
  }
  return [...byAgent.entries()]
    .map(([agentId, entries]) => {
      const first = [...entries].sort(
        (left, right) =>
          Number(right.grant.kind === 'incident') - Number(left.grant.kind === 'incident') ||
          Date.parse(left.grant.expires_at) - Date.parse(right.grant.expires_at),
      )[0]!;
      return { agentId, grant: first.grant, phrase: first.phrase, where: first.where, more: entries.length - 1 };
    })
    .sort((left, right) => Date.parse(left.grant.expires_at) - Date.parse(right.grant.expires_at));
};

/* ---------- domains ---------- */

export interface DomainRow {
  id: string;
  hostname: string;
  tone: LampTone;
  /** `serving · TLS by Cloudflare Tunnel`, `serving · certificate valid 81 d`. */
  words: string;
}

const DAY = 86_400_000;

/** The certificate in a few words: who terminates TLS, or the ACME certificate's state and expiry. */
export const certPhrase = (
  domain: Pick<Domain, 'tls_mode' | 'cert_status' | 'cert_expires_at'>,
  now: number,
): string => {
  if (domain.tls_mode === 'tunnel') return `TLS by ${tlsWords.tunnel}`;
  if (domain.cert_status !== 'issued') {
    return domain.cert_status === 'not_applicable' ? tlsWords.acme : `certificate ${domain.cert_status}`;
  }
  const expires = domain.cert_expires_at ? Date.parse(domain.cert_expires_at) : Number.NaN;
  if (Number.isNaN(expires)) return 'certificate issued';
  const left = expires - now;
  if (left <= 0) return 'certificate expired';
  if (left < DAY) return `certificate valid ${Math.max(1, Math.round(left / 3_600_000))} h`;
  return `certificate valid ${Math.floor(left / DAY)} d`;
};

/** The project's domains, by hostname, each with whether a machine serves it and its certificate. */
export const domainRows = (domains: readonly Domain[], now: number): DomainRow[] =>
  [...domains]
    .sort((left, right) => left.hostname.localeCompare(right.hostname))
    .map((domain) => ({
      id: domain.id,
      hostname: domain.hostname,
      tone: servingTone(domain.serving_status),
      words: `${servingWords[domain.serving_status].toLowerCase()} · ${certPhrase(domain, now)}`,
    }));
