import { Link } from '@tanstack/react-router';
import { Box, CalendarClock, Copy, GitBranch, HardDrive, Server, type LucideIcon } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import type { App, Build, Deployment, Node, Vm } from '../api/generated/model';
import { useListAppBackups } from '../api/generated/backups/backups';
import { useListAppDomains } from '../api/generated/domains/domains';
import { useGetAppMetrics } from '../api/generated/metrics/metrics';
import { useListAppVolumes } from '../api/generated/volumes/volumes';
import { serviceLampState } from '../lib/app-status';
import {
  appFootRows,
  cardFigures,
  cpuFigure,
  deployAge,
  deployProgress,
  justDeployed,
  memoryFigure,
  memoryShare,
  nodeChips,
  vmFootRows,
  type DeployProgress,
  type FootKey,
  type FootRow,
  type NodeChip,
} from '../lib/canvas-card';
import { drawMeter, drawMetricArea, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { isDeploymentLive, type RunPhase } from '../lib/deploy-tape';
import { lampState } from '../lib/lamp';
import { areaPlot, seriesSummary, toSeries } from '../lib/metric-area';
import { formatPercent } from '../lib/metrics';
import { appAddress, appKey, appKind, memoryWords, replicasMark, vmKey, vmSize, type ServiceKind } from '../lib/services';
import { useNow } from '../lib/use-now';
import { DitherCanvas } from './dither-canvas';
import { GlyphTile } from './glyph-tile';
import { LampDot } from './lamp';

/** A footer row's icon, by what the row says. */
const FOOT_ICONS: Record<FootKey, LucideIcon> = {
  build: GitBranch,
  commit: GitBranch,
  volume: HardDrive,
  backup: CalendarClock,
  warm: Copy,
  nodes: Server,
  image: Box,
  host: Server,
};

/**
 * How often a card reads its app's metrics: every other sample
 * (`AppMetrics.interval_s` is 5 s). A canvas holds many cards, each its own
 * request, so the cards read at half the stream's pace.
 */
const METRICS_EVERY_MS = 10_000;

/** An app with nothing running has no new sample to read: its card reads once and stops. */
const metricsIdle = (status: App['status']): boolean => status === 'down' || status === 'stopped';

/**
 * The card face both kinds share: a kind tile and the name, one line of
 * address, the lamp with its word and how long it has run, what the service
 * uses or reserves and where it runs (or, while a redeploy rolls out, its
 * phase tape), and footer rows when there is something to say. The whole
 * card is the link that opens the service.
 */
function CardFace({
  kind,
  name,
  replicas,
  sub,
  lamp,
  word,
  since,
  body,
  foot,
  deploying,
  confirmed = false,
  link,
  selected = false,
  highlighted = false,
}: {
  kind: ServiceKind;
  name: string;
  replicas?: number;
  sub: string;
  lamp: ReactNode;
  word: string;
  /** `for 40 min`: the age of the last deploy. */
  since?: string | null;
  /** The figures and the nodes, or the phase tape. */
  body: ReactNode;
  foot: readonly FootRow[];
  deploying: boolean;
  /** A redeploy just succeeded: one short ring, once. */
  confirmed?: boolean;
  link: { search: { app: string; tab: 'deployments' } | { vm: string }; projectId: string };
  /** Open in the service sheet. */
  selected?: boolean;
  /** Pointed at from the project rail. */
  highlighted?: boolean;
}) {
  const mark = replicas !== undefined ? replicasMark({ replicas }) : null;
  return (
    <Link
      to="/projects/$projectId"
      params={{ projectId: link.projectId }}
      search={link.search}
      className="card card-lift svc"
      draggable={false}
      data-service={'app' in link.search ? appKey(link.search.app) : vmKey(link.search.vm)}
      data-selected={selected ? '' : undefined}
      data-hl={highlighted ? '' : undefined}
    >
      {deploying ? <span className="sweep" aria-hidden="true" /> : null}
      {confirmed ? <span className="svc-ok" aria-hidden="true" /> : null}
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
          <span className="min-w-0 truncate">{word}</span>
          {since ? <span className="svc-since">{since}</span> : null}
        </span>
        {body}
      </span>
      {foot.map(({ key, parts, fault }) => {
        const Icon = FOOT_ICONS[key];
        return (
          <span key={key} className={fault ? 'svc-foot svc-foot-fault' : 'svc-foot'}>
            <Icon strokeWidth={1.7} aria-hidden="true" />
            <span className="min-w-0 truncate">
              {parts.map((part, index) =>
                part.code ? (
                  <span key={index} className="type-code">
                    {part.text}
                  </span>
                ) : (
                  <span key={index}>{part.text}</span>
                ),
              )}
            </span>
          </span>
        );
      })}
    </Link>
  );
}

/**
 * An app on the canvas. Its domain, volume, backups and metrics are read
 * here, each only when the app can have one, and a refusal just leaves that
 * part out. Its deployments come from the canvas, which shares them with the
 * project rail.
 */
export function AppCard({
  app,
  builds,
  run,
  instances,
  deployments,
  nodeNames,
  selected = false,
  highlighted = false,
  dimmed = false,
  onDeployed,
}: {
  app: App;
  /** Recent builds, newest first; only for an app with a Git source. */
  builds?: readonly Build[];
  /** For the app open in the sheet: what its newest run is doing (`useAppRuns`). */
  run?: RunPhase | null;
  /** The app's VMs, from the project's VM list. */
  instances: readonly Vm[];
  /** The app's deployments, newest first; undefined until read. */
  deployments?: readonly Deployment[];
  nodeNames: Readonly<Record<string, string>>;
  /** Open in the service sheet. */
  selected?: boolean;
  /** Pointed at from the project rail. */
  highlighted?: boolean;
  /** Stepped back while another service's sheet is open: its figures stop reading until it is back. */
  dimmed?: boolean;
  /** A redeploy this card watched roll out has succeeded. */
  onDeployed?: (name: string) => void;
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

  const newest = deployments?.[0];
  const live = newest !== undefined && isDeploymentLive(newest.status);
  // The clock ticks once a second only while a redeploy rolls out; the age once a minute.
  const now = useNow(true, live ? 1_000 : 30_000);
  const progress = deployProgress(live ? newest : undefined, app.replicas, now);
  const confirmed = useDeployConfirm(deployments, () => onDeployed?.(app.name));

  const foot = appFootRows({
    app,
    builds,
    volumes: volumes.data?.items,
    backups: backups.data?.items,
    nodeIds: instances.map((vm) => vm.node_id),
    nodeNames,
    now,
  });
  const chips = nodeChips(instances, nodeNames);

  return (
    <CardFace
      kind={appKind(app)}
      name={app.name}
      replicas={app.replicas}
      sub={appAddress(app, domains.data?.items[0]?.hostname)}
      lamp={<LampDot ring={lamp.tone} core={lamp.tone} pulse={lamp.pulse} size={15} />}
      word={progress && lamp.statusText === 'Deploying' ? progress.word : lamp.statusText}
      since={deployments ? deployAge(app, deployments, now) : null}
      body={progress ? <PhaseTape progress={progress} /> : <AppFigures appId={app.id} chips={chips} poll={!dimmed && !metricsIdle(app.status)} />}
      foot={foot}
      deploying={lamp.pulse}
      confirmed={confirmed}
      link={{ projectId: app.project_id, search: { app: app.id, tab: 'deployments' } }}
      selected={selected}
      highlighted={highlighted}
    />
  );
}

/**
 * True for one short moment after a deployment this card saw rolling out
 * succeeds. Nothing on a first load, a failure or a superseded rollout.
 */
function useDeployConfirm(deployments: readonly Deployment[] | undefined, onConfirm: () => void): boolean {
  const watched = useRef<string | null>(null);
  const confirm = useRef(onConfirm);
  confirm.current = onConfirm;
  const [confirmed, setConfirmed] = useState(false);
  useEffect(() => {
    if (!deployments) return;
    if (justDeployed(watched.current, deployments)) {
      watched.current = null;
      setConfirmed(true);
      confirm.current();
      return;
    }
    const newest = deployments[0];
    watched.current = newest && isDeploymentLive(newest.status) ? newest.id : null;
  }, [deployments]);
  useEffect(() => {
    if (!confirmed) return;
    // Long enough for the ring (styles.css, at most 300 ms) to play once.
    const timer = window.setTimeout(() => setConfirmed(false), 600);
    return () => window.clearTimeout(timer);
  }, [confirmed]);
  return confirmed;
}

/** The redeploy on the card: four segments (pull, rootfs, boot, health), what it does now and its clock. */
function PhaseTape({ progress }: { progress: DeployProgress }) {
  const spoken = progress.steps.map((step) => `${step.step} ${step.state === 'live' ? 'running' : step.state}`).join(', ');
  return (
    <>
      <span className="svc-tape" role="img" aria-label={spoken}>
        {progress.steps.map((step) => (
          <i key={step.step} data-s={step.state} />
        ))}
      </span>
      <span className="svc-tape-l">
        <span className="min-w-0 truncate">{progress.sentence}</span>
        <span className="svc-clock">{progress.clock}</span>
      </span>
    </>
  );
}

/**
 * CPU and memory from `GET /apps/{id}/metrics`, read once per sample: the
 * figures, a small CPU trace and a memory meter, each redrawn as a sample
 * lands, without a tween. Then the nodes its instances run on.
 */
function AppFigures({ appId, chips, poll }: { appId: string; chips: readonly NodeChip[]; poll: boolean }) {
  const metrics = useGetAppMetrics(appId, { query: { refetchInterval: poll ? METRICS_EVERY_MS : false, retry: false } });
  const figures = useMemo(() => cardFigures(metrics.data?.instances ?? []), [metrics.data]);
  const intervalMs = (metrics.data?.interval_s ?? 5) * 1_000;
  return (
    <>
      <span className="svc-m">
        <span className="svc-m-c">
          <span className="svc-k">
            <span>CPU</span>
            <b>{figures ? cpuFigure(figures.cpu) : '—'}</b>
          </span>
          <CpuTrace trace={figures?.cpuTrace ?? []} intervalMs={intervalMs} />
        </span>
        <span className="svc-m-c">
          <span className="svc-k">
            <span>Memory</span>
            <b>{figures ? memoryFigure(figures.memoryBytes) : '—'}</b>
          </span>
          <Meter
            share={figures ? memoryShare(figures) : 0}
            label={
              figures
                ? `Memory: ${memoryFigure(figures.memoryBytes)} of ${memoryFigure(figures.memoryTotalBytes)} in use`
                : 'Memory: no sample yet'
            }
          />
        </span>
      </span>
      <NodeChips chips={chips} />
    </>
  );
}

const TRACE_LOOK = { labels: false, dot: false, lineWidth: 1 } as const;
const TRACE_PAD = { left: 0, right: 0, top: 2, bottom: 1 } as const;

/** The app's CPU over the last ten minutes, as a small dithered area. Scaled to 80 % unless it goes higher. */
function CpuTrace({ trace, intervalMs }: { trace: ReadonlyArray<{ time: string; percent: number }>; intervalMs: number }) {
  const series = useMemo(() => toSeries(trace, (point) => point.percent), [trace]);
  const end = series.times[series.times.length - 1] ?? 0;
  const max = Math.max(80, ...series.values.map((value) => value * 1.1));
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) =>
      drawMetricArea(
        surface,
        areaPlot({
          series,
          end,
          width: surface.w,
          height: surface.h,
          axis: { max, ticks: [] },
          tickLabel: String,
          intervalMs,
          pad: TRACE_PAD,
        }),
        read,
        TRACE_LOOK,
      ),
    [series, end, max, intervalMs],
  );
  const label =
    series.values.length > 0
      ? seriesSummary({ label: 'CPU', values: series.values, format: (value) => formatPercent(value / 100) })
      : 'CPU: no sample yet';
  return <DitherCanvas paint={paint} label={label} className="svc-trace" />;
}

/** A dithered meter drawn at its value, without a tween: a live reading redraws per sample. */
function Meter({ share, label }: { share: number; label: string }) {
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, share, read),
    [share],
  );
  return <DitherCanvas paint={paint} label={label} className="svc-meter" />;
}

/** The nodes a service runs on, each with the lamp of its instance there. */
function NodeChips({ chips }: { chips: readonly NodeChip[] }) {
  if (chips.length === 0) return null;
  return (
    <span className="svc-on">
      {chips.map((chip) => (
        <span key={chip.nodeId} className="tag tag-plain svc-chip">
          <LampDot ring={chip.tone} core={chip.tone} size={10} />
          <span aria-hidden="true">{chip.name}</span>
          <span className="sr-only">{chip.spoken}</span>
        </span>
      ))}
    </span>
  );
}

/**
 * A standalone VM on the canvas: its size, its lamp, what it reserves of its
 * node (the contract serves no metrics for a single VM), the node and its address.
 */
export function VmCard({
  vm,
  node,
  nodeNames,
  selected = false,
  highlighted = false,
}: {
  vm: Vm;
  /** The node it runs on, for the share it reserves; undefined while the list loads. */
  node?: Pick<Node, 'capacity'>;
  nodeNames: Readonly<Record<string, string>>;
  /** Open in the VM sheet. */
  selected?: boolean;
  /** Pointed at from the project rail. */
  highlighted?: boolean;
}) {
  const lamp = lampState(vm);
  const share = (part: number, whole: number | undefined) => (whole && whole > 0 ? Math.min(1, part / whole) : 0);
  const chips = nodeChips([vm], nodeNames);
  const memory = memoryWords(vm.memory_mb);
  return (
    <CardFace
      kind="vm"
      name={vm.name}
      sub={vmSize(vm)}
      lamp={<LampDot ring={lamp.ringTone} core={lamp.coreTone} pulse={lamp.pulse} size={15} />}
      word={lamp.statusText}
      body={
        <>
          <span className="svc-m">
            <span className="svc-m-c">
              <span className="svc-k">
                <span>Reserved</span>
                <b>{vm.vcpus} vCPU</b>
              </span>
              <Meter
                share={share(vm.vcpus, node?.capacity.cpus)}
                label={node ? `Reserves ${vm.vcpus} of the node’s ${node.capacity.cpus} vCPU` : `Reserves ${vm.vcpus} vCPU`}
              />
            </span>
            <span className="svc-m-c">
              <span className="svc-k">
                <span>Memory</span>
                <b>{memory}</b>
              </span>
              <Meter
                share={share(vm.memory_mb, node?.capacity.memory_mb)}
                label={node ? `Reserves ${memory} of the node’s ${memoryWords(node.capacity.memory_mb)}` : `Reserves ${memory}`}
              />
            </span>
          </span>
          <NodeChips chips={chips} />
        </>
      }
      foot={vmFootRows(vm, nodeNames)}
      deploying={false}
      link={{ projectId: vm.project_id, search: { vm: vm.id } }}
      selected={selected}
      highlighted={highlighted}
    />
  );
}
