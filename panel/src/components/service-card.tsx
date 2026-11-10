import { Link } from '@tanstack/react-router';
import { CalendarClock, Copy, GitBranch, HardDrive, Server, type LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import type { App, Build, Vm } from '../api/generated/model';
import { useListAppBackups } from '../api/generated/backups/backups';
import { useListAppDomains } from '../api/generated/domains/domains';
import { useListAppVolumes } from '../api/generated/volumes/volumes';
import { serviceLampState } from '../lib/app-status';
import type { RunPhase } from '../lib/deploy-tape';
import { lampState } from '../lib/lamp';
import {
  appAddress,
  appKey,
  appKind,
  commitLine,
  lastBuildFailed,
  nextBackupLine,
  nodesLine,
  replicasMark,
  vmKey,
  vmSize,
  volumeLine,
  type ServiceKind,
} from '../lib/services';
import { warmCardLine } from '../lib/warm-words';
import { GlyphTile } from './glyph-tile';
import { LampDot } from './lamp';

interface FootRow {
  key: string;
  Icon: LucideIcon;
  text: string;
  /**
   * Said in the fault colour. Only the text: a failed build is also why the
   * card glows red, but a failed warm copy is not, since the lamp and the
   * aura stay the app's status (`appFailed` reads neither row).
   */
  fault?: boolean;
}

/**
 * The card face both kinds share: a kind tile and the name, one line of
 * address, the lamp with one word, and footer rows when there is something
 * to say. The whole card is the link that opens the service.
 */
function CardFace({
  kind,
  name,
  replicas,
  sub,
  lamp,
  word,
  foot,
  deploying,
  link,
  selected = false,
}: {
  kind: ServiceKind;
  name: string;
  replicas?: number;
  sub: string;
  lamp: ReactNode;
  word: string;
  foot: readonly FootRow[];
  deploying: boolean;
  link: { search: { app: string; tab: 'deployments' } | { vm: string }; projectId: string };
  /** Open in the service sheet. */
  selected?: boolean;
}) {
  const mark = replicas !== undefined ? replicasMark({ replicas }) : null;
  return (
    <Link
      to="/projects/$projectId"
      params={{ projectId: link.projectId }}
      search={link.search}
      className="svc"
      draggable={false}
      data-service={'app' in link.search ? appKey(link.search.app) : vmKey(link.search.vm)}
      data-selected={selected ? '' : undefined}
    >
      {deploying ? <span className="sweep" aria-hidden="true" /> : null}
      <span className="svc-body">
        <span className="svc-name">
          <GlyphTile kind={kind} label />
          <b>{name}</b>
          {mark ? (
            <span className="svc-x">
              <span aria-hidden="true">{mark}</span>
              <span className="sr-only">, {replicas} replicas</span>
            </span>
          ) : null}
        </span>
        <span className="svc-sub">{sub}</span>
        <span className="svc-state">
          {lamp}
          {word}
        </span>
      </span>
      {foot.map(({ key, Icon, text, fault }) => (
        <span key={key} className={fault ? 'svc-foot svc-foot-fault' : 'svc-foot'}>
          <Icon strokeWidth={1.7} aria-hidden="true" />
          <span className="min-w-0 truncate">{text}</span>
        </span>
      ))}
    </Link>
  );
}

/**
 * An app on the canvas. Its domain, volume and backups are read here, each
 * only when the app can have one, and a refusal just leaves that line out.
 */
export function AppCard({
  app,
  builds,
  run,
  nodeIds,
  nodeNames,
  selected = false,
}: {
  app: App;
  /** Recent builds, newest first; only for an app with a Git source. */
  builds?: readonly Build[];
  /** For the app open in the sheet: what its newest run is doing (`useAppRuns`). */
  run?: RunPhase | null;
  /** Nodes the app's instances run on. */
  nodeIds: readonly string[];
  nodeNames: Readonly<Record<string, string>>;
  /** Open in the service sheet. */
  selected?: boolean;
}) {
  // The sheet's header reads the same things the same way.
  const lamp = serviceLampState(app.status, builds, run);
  const domains = useListAppDomains(app.id, undefined, {
    query: { staleTime: 10_000, retry: false },
  });
  const volumes = useListAppVolumes(app.id, {
    query: { enabled: app.stateful, staleTime: 30_000, retry: false },
  });
  const backups = useListAppBackups(app.id, undefined, {
    query: { enabled: app.ha_policy === 'cold', staleTime: 60_000, retry: false },
  });

  const foot: FootRow[] = [];
  const newest = builds?.[0];
  if (builds && newest && lastBuildFailed(builds)) {
    const text = newest.commit ? `Build of ${newest.commit.slice(0, 7)} failed` : 'Last build failed';
    foot.push({ key: 'build', Icon: GitBranch, text, fault: true });
  } else {
    const commit = builds ? commitLine(builds) : null;
    if (commit) foot.push({ key: 'commit', Icon: GitBranch, text: commit });
  }
  const volume = app.stateful ? volumeLine(volumes.data?.items ?? []) : null;
  if (volume) foot.push({ key: 'volume', Icon: HardDrive, text: volume });
  const backup = backups.data ? nextBackupLine(app, backups.data.items) : null;
  if (backup) foot.push({ key: 'backup', Icon: CalendarClock, text: backup });
  // A warm app's copy, where a cold app's next backup goes; the full error is
  // in the Storage tab.
  if (app.warm) {
    foot.push({
      key: 'warm',
      Icon: Copy,
      text: warmCardLine(app.warm, nodeNames),
      fault: app.warm.state === 'failed',
    });
  }
  if (foot.length === 0) {
    const nodes = nodesLine(nodeIds, nodeNames);
    if (nodes) foot.push({ key: 'nodes', Icon: Server, text: nodes });
  }

  return (
    <CardFace
      kind={appKind(app)}
      name={app.name}
      replicas={app.replicas}
      sub={appAddress(app, domains.data?.items[0]?.hostname)}
      lamp={<LampDot ring={lamp.tone} core={lamp.tone} pulse={lamp.pulse} size={15} />}
      word={lamp.statusText}
      foot={foot}
      deploying={lamp.pulse}
      link={{ projectId: app.project_id, search: { app: app.id, tab: 'deployments' } }}
      selected={selected}
    />
  );
}

/** A standalone VM on the canvas: its size, its lamp and the node it runs on. */
export function VmCard({
  vm,
  nodeNames,
  selected = false,
}: {
  vm: Vm;
  nodeNames: Readonly<Record<string, string>>;
  /** Open in the VM sheet. */
  selected?: boolean;
}) {
  const lamp = lampState(vm);
  const nodes = nodesLine([vm.node_id], nodeNames);
  return (
    <CardFace
      kind="vm"
      name={vm.name}
      sub={vmSize(vm)}
      lamp={<LampDot ring={lamp.ringTone} core={lamp.coreTone} pulse={lamp.pulse} size={15} />}
      word={lamp.statusText}
      foot={nodes ? [{ key: 'nodes', Icon: Server, text: nodes }] : []}
      deploying={false}
      link={{ projectId: vm.project_id, search: { vm: vm.id } }}
      selected={selected}
    />
  );
}
