import { Link } from '@tanstack/react-router';
import { AlertTriangle, ChevronRight, GitBranch } from 'lucide-react';
import { useCallback, useMemo } from 'react';
import type { Approval, Vm } from '../api/generated/model';
import { useGetAppMetrics } from '../api/generated/metrics/metrics';
import { shortId } from '../lib/agency-format';
import { appLampState } from '../lib/app-status';
import { lampState } from '../lib/lamp';
import { appCpuSeries, type TileFoot, type TileStatus } from '../lib/board';
import { drawMetricArea, drawPreview, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import { areaPlot, BARE_PAD, seriesSummary, toSeries } from '../lib/metric-area';
import { formatPercent } from '../lib/metrics';
import type { ProjectTileData } from '../lib/projects';
import { nodesLine, replicasMark, servicesCount, type ServiceItem } from '../lib/services';
import { cn } from '../lib/utils';
import { ApprovalCallout } from './approval-callout';
import { DitherCanvas } from './dither-canvas';
import { GlyphTile } from './glyph-tile';
import { Lamp, LampDot } from './lamp';
import { SkelBar, Skeleton } from './skeleton';
import { Tag } from './ui/tag';

/** The field shows this many service tiles; beyond it, the last place says how many more. */
const PREVIEW_TILES = 8;

/** How long the app sparks wait between two reads of `GET /apps/{id}/metrics`. */
const SPARK_REFRESH_MS = 15_000;

export interface TileContext {
  /** Node names by id, for the tags on each row. */
  nodeNames: Readonly<Record<string, string>>;
  /** Every VM, so an app's row can say where its instances run. */
  vms: readonly Vm[];
}

/**
 * One project on the board: its name, size and state; the glyph field of its
 * services; a request that waits on a person, when one acts in it; one row per
 * app and standalone VM; and at its foot what happened last. The name links to
 * the project and its box takes the click; each row opens its service.
 */
export function ProjectTile({
  tile,
  status,
  foot,
  footWhen,
  ask,
  context,
}: {
  tile: ProjectTileData;
  status: TileStatus;
  foot: TileFoot | null;
  /** When the foot's event happened, in words: `40 min ago`, or `16 min` for an incident still open. */
  footWhen: string;
  /** The pending request that acts in this project, with its target's name. */
  ask?: { approval: Approval; targetName?: string };
  context: TileContext;
}) {
  const count = tile.services.length;
  const field = status.field;
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawPreview(surface, count, read, field),
    [count, field],
  );
  const overflow = count > PREVIEW_TILES;
  const shown = overflow ? tile.services.slice(0, PREVIEW_TILES - 1) : tile.services;
  const name = tile.project?.name ?? shortId(tile.projectId);

  return (
    <article className={cn('card card-lift ptile', status.warn && 'ptile-warn')} aria-label={name}>
      <div className="ptile-h">
        <Link to="/projects/$projectId" params={{ projectId: tile.projectId }} className="ptile-name">
          {name}
        </Link>
        <span className="ptile-count">{servicesCount(count)}</span>
        <span className="ptile-st">
          <LampDot ring={status.tone} core={status.tone} pulse={status.pulse} size={15} />
          <span>{status.words}</span>
          <ChevronRight strokeWidth={1.7} aria-hidden="true" className="ptile-go" />
        </span>
      </div>
      <div className="pprev" aria-hidden="true">
        <DitherCanvas paint={paint} />
        <span className="ptiles">
          {shown.map((service) => (
            <GlyphTile key={service.key} kind={service.kind} className="glyph-preview" />
          ))}
          {overflow ? <span className="ptiles-more">+{count - shown.length}</span> : null}
        </span>
      </div>
      {ask ? <ApprovalCallout approval={ask.approval} targetName={ask.targetName} /> : null}
      {count > 0 ? (
        <ul className="prows" aria-label={`Services of ${name}`}>
          {tile.services.map((service) => (
            <ServiceRow key={service.key} service={service} projectId={tile.projectId} context={context} />
          ))}
        </ul>
      ) : null}
      {foot ? <TileFootLine foot={foot} when={footWhen} /> : null}
    </article>
  );
}

function ServiceRow({
  service,
  projectId,
  context,
}: {
  service: ServiceItem;
  projectId: string;
  context: TileContext;
}) {
  const nodeIds =
    service.kind === 'vm'
      ? [service.vm.node_id]
      : context.vms.filter((vm) => vm.app_id === service.app.id && vm.observed_status !== 'deleted').map((vm) => vm.node_id);
  const nodes = (nodesLine(nodeIds, context.nodeNames) ?? '').split(' · ').filter((node) => node !== '');
  const replicas = service.kind === 'vm' ? null : replicasMark(service.app);
  const lamp = service.kind === 'vm' ? null : appLampState(service.app.status);
  const spoken = [
    service.name,
    service.kind === 'vm' ? `VM, ${service.vm.vcpus} vCPU` : replicas ? `${replicas.slice(1)} replicas` : null,
    nodes.length > 0 ? `on ${nodes.join(' and ')}` : null,
    service.kind === 'vm' ? lampState(service.vm).text : lamp!.statusText,
  ]
    .filter(Boolean)
    .join(', ');

  return (
    <li>
      <Link
        to="/projects/$projectId"
        params={{ projectId }}
        search={service.kind === 'vm' ? { vm: service.vm.id } : { app: service.app.id }}
        className="prow"
        aria-label={spoken}
      >
        <GlyphTile kind={service.kind} className="glyph-sm" />
        <span className="prow-n">
          <span>{service.name}</span>
          {replicas ? <small aria-label={`${replicas.slice(1)} replicas`}>{replicas}</small> : null}
        </span>
        <span className="prow-on">
          {nodes.map((node) => (
            <Tag key={node} tone="plain">
              {node}
            </Tag>
          ))}
        </span>
        {service.kind === 'vm' ? (
          <span className="prow-cpu">{service.vm.vcpus} vCPU</span>
        ) : (
          <AppSpark appId={service.app.id} />
        )}
        {service.kind === 'vm' ? (
          <Lamp vm={service.vm} size={12} />
        ) : (
          <LampDot ring={lamp!.tone} core={lamp!.tone} pulse={lamp!.pulse} size={12} label={lamp!.statusText} />
        )}
      </Link>
    </li>
  );
}

const SPARK_LOOK = { labels: false, dot: false, lineWidth: 1 } as const;

/** An app's CPU over the last ten minutes, as a small dithered area on a 0 to 100 % scale. */
function AppSpark({ appId }: { appId: string }) {
  const metrics = useGetAppMetrics(appId, { query: { refetchInterval: SPARK_REFRESH_MS, retry: false } });
  const series = useMemo(
    () => toSeries(appCpuSeries(metrics.data?.instances ?? []), (sample) => sample.cpu_usage * 100),
    [metrics.data],
  );
  const end = series.times[series.times.length - 1] ?? 0;
  const intervalMs = (metrics.data?.interval_s ?? 5) * 1_000;
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) =>
      drawMetricArea(
        surface,
        areaPlot({
          series,
          end,
          width: surface.w,
          height: surface.h,
          axis: { max: 100, ticks: [] },
          tickLabel: String,
          intervalMs,
          pad: { left: 0, right: 0, top: 2, bottom: 1 },
        }),
        read,
        SPARK_LOOK,
      ),
    [series, end, intervalMs],
  );
  const label = seriesSummary({ label: 'CPU', values: series.values, format: (value) => formatPercent(value / 100) });
  return <DitherCanvas paint={paint} label={label} className="prow-spark" />;
}

function TileFootLine({ foot, when }: { foot: TileFoot; when: string }) {
  const Icon = foot.kind === 'incident' ? AlertTriangle : GitBranch;
  return (
    <p className={cn('pfoot', foot.kind === 'failed' && 'pfoot-fault')}>
      <Icon strokeWidth={1.7} aria-hidden="true" />
      <span className="pfoot-t">
        {foot.segments.map((segment, index) =>
          segment.code ? (
            <span key={index} className="type-code">
              {segment.text}
            </span>
          ) : (
            <span key={index}>{segment.text}</span>
          ),
        )}
      </span>
      <time dateTime={foot.at} className="pfoot-at">
        {when}
      </time>
    </p>
  );
}

/** Name widths of the skeleton rows, so the field does not read as a table of equal bars. */
const SKELETON_ROWS = [
  [34, 2],
  [26, 2],
  [42, 1],
] as const;

/**
 * Project tiles on their way: each in a tile's shape, with the name and its
 * state, the glyph field as a dither field, three service rows and the foot.
 */
export function ProjectTilesSkeleton({ count = 2 }: { count?: number }) {
  return (
    <Skeleton label="Loading projects" className="ptiles-grid">
      {Array.from({ length: count }, (_, tile) => (
        <div key={tile} className="ptiles-col">
          <div className="card ptile ptile-skel">
            <div className="ptile-h">
              <SkelBar width={tile === 0 ? '30%' : '24%'} height={16} />
              <SkelBar width="22%" className="ptile-skel-st" />
            </div>
            <div className="pprev">
              <SkelBar className="ptile-skel-field" />
            </div>
            <div className="prows">
              {SKELETON_ROWS.slice(0, 3 - tile).map(([name, tags], row) => (
                <div key={row} className="prow">
                  <SkelBar className="ptile-skel-glyph" />
                  <SkelBar width={`${name}%`} />
                  <span className="ptile-skel-tags">
                    {Array.from({ length: tags }, (_, index) => (
                      <SkelBar key={index} width="38px" height={18} />
                    ))}
                  </span>
                  <SkelBar width="56px" height={8} />
                  <SkelBar className="ptile-skel-lamp" />
                </div>
              ))}
            </div>
            <div className="pfoot">
              <SkelBar width="52%" />
            </div>
          </div>
        </div>
      ))}
    </Skeleton>
  );
}
