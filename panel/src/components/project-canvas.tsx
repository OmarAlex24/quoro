import { Maximize2, Minus, Plus } from 'lucide-react';
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent as ReactMouseEvent,
  type PointerEvent as ReactPointerEvent,
} from 'react';
import { useQueries, useQueryClient } from '@tanstack/react-query';
import { useGetApp, useListApps } from '../api/generated/apps/apps';
import { getListAppDeploymentsQueryOptions } from '../api/generated/deployments/deployments';
import type { Deployment, Project } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListVms } from '../api/generated/vms/vms';
import { appsPollingInterval, serviceLampState, warmListInterval, withOpenApp } from '../lib/app-status';
import { appFailed, chooseAuras, vmFailed, waitsOnApproval } from '../lib/canvas-aura';
import {
  autoLayout,
  boardZoom,
  bounds,
  browserStorage,
  CARD_H,
  CARD_W,
  fitView,
  layoutFrame,
  moved,
  readingOrder,
  readPositions,
  writePositions,
  zoomIn,
  zoomOut,
  type Point,
  type Positions,
} from '../lib/canvas-layout';
import { isDeploymentLive } from '../lib/deploy-tape';
import { listIsConverging } from '../lib/lamp';
import { problemView } from '../lib/problem';
import { SERVICE_LIST_PARAMS } from '../lib/projects';
import { appIdOfKey, lastBuildFailed, projectServices, type ServiceItem } from '../lib/services';
import { RUN_PAGE, useAppRuns } from '../lib/use-app-runs';
import { cachedBuildLive, useRecentBuilds } from '../lib/use-builds';
import { useMedia } from '../lib/use-media';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { Aura } from './aura';
import { EmptyState } from './empty-state';
import { ProjectRail } from './project-rail';
import { AppCard, VmCard } from './service-card';
import { usePendingApprovals } from './shell/shell-data';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';

/** How far a press must travel before it is a drag rather than a click. */
const DRAG_THRESHOLD = 4;
/** Room past the last card, so a card can be dragged further out. */
const WORLD_PAD = 160;
/** From this width the cards float on the field beside the rail; below it they stack. */
const FLOATING = '(min-width: 1101px)';
/** Up to this width the rail is narrower. */
const NARROW_RAIL = '(max-width: 1180px)';
/** The rail and its margins at the right of the field: 316 px wide (280 when narrow), 12 px on each side. */
const RAIL_SPACE = { wide: 340, narrow: 304 } as const;

interface Press {
  key: string;
  pointerId: number;
  x: number;
  y: number;
  origin: Point;
  dragging: boolean;
}

interface Pan {
  pointerId: number;
  x: number;
  y: number;
  left: number;
  top: number;
}

/**
 * A project's canvas: one card per app and per standalone VM on a dotted
 * field, laid out by kind until someone moves a card, and the project rail
 * at its right. Wide, the cards drag with a mouse or a pen and keep their
 * place in this browser; the field pans by dragging its background and zooms
 * from the bottom-left, starting at the zoom that fits the board beside the
 * rail. Narrower than 1101 px the cards stack in two columns (one on a
 * phone) with the rail under them. Each card is a link in reading order, so
 * Tab walks the cards and Enter opens one. While a sheet is open the rail
 * gives way and the other cards dim. No line joins two services.
 */
export function ProjectCanvas({
  projectId,
  project,
  selected,
  onAdd,
}: {
  projectId: string;
  project?: Project;
  /** The service open in the sheet, by its key; its card is marked. */
  selected?: string;
  /** Opens the Add menu; absent for a session that cannot write. */
  onAdd?: () => void;
}) {
  const { canWrite } = useSession();
  const queryClient = useQueryClient();
  const appsQuery = useListApps(
    { project_id: projectId, ...SERVICE_LIST_PARAMS },
    {
      query: {
        // A warm app's copy moves every `interval_s`, so the list is read at
        // least that often (never under 5 s). `refetchIntervalInBackground`
        // stays unset (false): nothing is read while the browser tab is hidden.
        refetchInterval: (query) => {
          const items = query.state.data?.items ?? [];
          return warmListInterval(
            appsPollingInterval(
              items,
              items.some((app) => app.source != null && cachedBuildLive(queryClient, app.id)),
            ),
            items,
          );
        },
      },
    },
  );
  // The app open in the sheet is drawn from the sheet's own read (same query
  // key, so no second request), so its card never lags the sheet's header.
  const openAppId = appIdOfKey(selected);
  const openApp = useGetApp(openAppId ?? '', { query: { enabled: openAppId !== undefined, retry: false } });
  // Its card says what the sheet's header says while a run moves, from the
  // Deployments tab's own two lists (same query keys, so no second request).
  const openRun = useAppRuns(openApp.data).phase;
  const runOf = useCallback(
    (appId: string) => (appId === openAppId ? openRun : undefined),
    [openAppId, openRun],
  );
  const vmsQuery = useListVms(
    { project_id: projectId, ...SERVICE_LIST_PARAMS },
    {
      query: {
        refetchInterval: (query) =>
          listIsConverging(query.state.data?.items ?? []) ? 2_000 : 10_000,
      },
    },
  );
  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const apps = useMemo(
    () => withOpenApp(appsQuery.data?.items ?? [], openApp.data),
    [appsQuery.data, openApp.data],
  );
  const vms = useMemo(() => vmsQuery.data?.items ?? [], [vmsQuery.data]);
  const builds = useRecentBuilds(apps);
  const approvals = usePendingApprovals();
  // Each app's runs, on the page the Deployments tab reads (same query keys,
  // so the open app's sheet adds no request): every 2 s while one rolls out.
  const deployments = useQueries({
    queries: apps.map((app) =>
      getListAppDeploymentsQueryOptions(app.id, RUN_PAGE, {
        query: {
          retry: false,
          refetchInterval: (query) => {
            const newest = query.state.data?.items[0];
            return (newest && isDeploymentLive(newest.status)) || app.status === 'deploying' ? 2_000 : 10_000;
          },
        },
      }),
    ),
    combine: (results) => {
      const out: Record<string, Deployment[]> = {};
      results.forEach((result, index) => {
        const app = apps[index];
        if (app && result.data) out[app.id] = result.data.items;
      });
      return out;
    },
  });
  const nodes = useMemo(() => nodesQuery.data?.items ?? [], [nodesQuery.data]);
  const [hl, setHl] = useState<string | null>(null);
  const highlight = useMemo(() => ({ key: hl, set: setHl }), [hl]);
  const [said, setSaid] = useState('');
  // Cleared first, then set a moment later, so a second success of the same
  // app changes the live region and is announced again.
  const sayTimer = useRef<number | undefined>(undefined);
  const onDeployed = useCallback((name: string) => {
    window.clearTimeout(sayTimer.current);
    setSaid('');
    sayTimer.current = window.setTimeout(() => setSaid(`${name} deployed`), 120);
  }, []);
  useEffect(() => () => window.clearTimeout(sayTimer.current), []);

  const nodeNames = useMemo(() => Object.fromEntries(nodes.map((node) => [node.id, node.name])), [nodes]);
  const services = useMemo(() => projectServices(projectId, apps, vms), [projectId, apps, vms]);
  const keys = useMemo(() => services.map((service) => service.key), [services]);

  /* ---------- layout ---------- */

  const scrollRef = useRef<HTMLDivElement>(null);
  const slotRefs = useRef(new Map<string, HTMLDivElement>());
  const [viewport, setViewport] = useState({ w: 0, h: 0 });
  const floating = useMedia(FLOATING);
  const narrowRail = useMedia(NARROW_RAIL);
  // Set once someone zooms or fits; until then the zoom follows the room.
  const [userZoom, setUserZoom] = useState<number | null>(null);
  const [saved, setSaved] = useState<Positions>(() => readPositions(browserStorage(), projectId));
  const [drag, setDrag] = useState<{ key: string; point: Point } | null>(null);
  const press = useRef<Press | null>(null);
  const pan = useRef<Pan | null>(null);
  const swallowClick = useRef(false);
  const pendingScroll = useRef<{ left: number; top: number } | null>(null);

  useLayoutEffect(() => {
    const element = scrollRef.current;
    if (!element) return;
    const measure = () => setViewport({ w: element.clientWidth, h: element.clientHeight });
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  // The room left of the rail. The board is laid out for it at the zoom that
  // fits it, so a narrower window keeps its columns, only smaller.
  const room = Math.max(CARD_W + 32, (viewport.w || CARD_W + 32) - (narrowRail ? RAIL_SPACE.narrow : RAIL_SPACE.wide));
  const autoZoom = boardZoom(room);
  const zoom = floating ? (userZoom ?? autoZoom) : 1;
  const frame = useMemo(() => layoutFrame(room / autoZoom), [room, autoZoom]);
  const positions = useMemo(() => autoLayout(services, saved, frame), [services, saved, frame]);
  const shown: Positions = drag ? { ...positions, [drag.key]: drag.point } : positions;
  const order = useMemo(() => readingOrder(positions, keys), [positions, keys]);
  const byKey = useMemo(
    () => new Map(services.map((service) => [service.key, service])),
    [services],
  );

  const box = bounds(shown);
  const worldW = Math.max((box ? box.x + box.w : 0) + WORLD_PAD, viewport.w / zoom);
  const worldH = Math.max((box ? box.y + box.h : 0) + WORLD_PAD, viewport.h / zoom);

  useLayoutEffect(() => {
    const element = scrollRef.current;
    const target = pendingScroll.current;
    if (!element || !target) return;
    pendingScroll.current = null;
    element.scrollLeft = target.left;
    element.scrollTop = target.top;
  }, [zoom]);

  /** Zoom about the middle of what is in view. */
  const zoomTo = useCallback(
    (next: number) => {
      const element = scrollRef.current;
      if (!element || next === zoom) return;
      const middleX = (element.scrollLeft + element.clientWidth / 2) / zoom;
      const middleY = (element.scrollTop + element.clientHeight / 2) / zoom;
      pendingScroll.current = {
        left: Math.max(0, middleX * next - element.clientWidth / 2),
        top: Math.max(0, middleY * next - element.clientHeight / 2),
      };
      setUserZoom(next);
    },
    [zoom],
  );

  const fit = useCallback(() => {
    const element = scrollRef.current;
    if (!element) return;
    const sizes: Record<string, { w: number; h: number }> = {};
    for (const [key, slot] of slotRefs.current) {
      sizes[key] = { w: slot.offsetWidth, h: slot.offsetHeight };
    }
    const all = bounds(positions, sizes);
    if (!all) return;
    // Fit into the room left of the rail, centred there, so no card lands under it.
    const railSpace = narrowRail ? RAIL_SPACE.narrow : RAIL_SPACE.wide;
    const view = fitView(all, { w: Math.max(CARD_W + 32, element.clientWidth - railSpace), h: element.clientHeight });
    if (view.zoom === zoom) {
      setUserZoom(view.zoom);
      element.scrollLeft = view.left;
      element.scrollTop = view.top;
      return;
    }
    pendingScroll.current = { left: view.left, top: view.top };
    setUserZoom(view.zoom);
  }, [positions, zoom, narrowRail]);

  /* ---------- auras ---------- */

  const auras = useMemo(
    () =>
      chooseAuras(
        order.flatMap((key) => {
          const service = byKey.get(key);
          if (!service) return [];
          if (service.kind === 'vm') {
            return [
              {
                key,
                waiting: waitsOnApproval(approvals, { type: 'vm', id: service.vm.id }),
                deploying: false,
                failed: vmFailed(service.vm),
              },
            ];
          }
          const appBuilds = builds[service.app.id];
          return [
            {
              key,
              waiting: waitsOnApproval(approvals, { type: 'app', id: service.app.id }),
              deploying: serviceLampState(service.app.status, appBuilds, runOf(service.app.id)).pulse,
              failed: appFailed(service.app, appBuilds ? lastBuildFailed(appBuilds) : false),
            },
          ];
        }),
      ),
    [order, byKey, approvals, builds, runOf],
  );

  /* ---------- dragging a card ---------- */

  const onCardPointerDown = (key: string) => (event: ReactPointerEvent<HTMLDivElement>) => {
    if (!floating || event.pointerType === 'touch' || event.button !== 0) return;
    const origin = positions[key];
    if (!origin) return;
    press.current = {
      key,
      pointerId: event.pointerId,
      x: event.clientX,
      y: event.clientY,
      origin,
      dragging: false,
    };
  };

  const onCardPointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const current = press.current;
    if (!current || current.pointerId !== event.pointerId) return;
    const dx = event.clientX - current.x;
    const dy = event.clientY - current.y;
    if (!current.dragging) {
      if (Math.hypot(dx, dy) < DRAG_THRESHOLD) return;
      current.dragging = true;
      event.currentTarget.setPointerCapture(event.pointerId);
    }
    setDrag({ key: current.key, point: moved(current.origin, dx / zoom, dy / zoom) });
  };

  const onCardPointerUp = (event: ReactPointerEvent<HTMLDivElement>) => {
    const current = press.current;
    if (!current || current.pointerId !== event.pointerId) return;
    press.current = null;
    if (!current.dragging) return;
    swallowClick.current = true;
    const point = moved(
      current.origin,
      (event.clientX - current.x) / zoom,
      (event.clientY - current.y) / zoom,
    );
    const next = { ...positions, [current.key]: point };
    setSaved(next);
    setDrag(null);
    writePositions(browserStorage(), projectId, next, keys);
  };

  const onCardPointerCancel = () => {
    press.current = null;
    setDrag(null);
  };

  /** A drag ends with a click on the card; that click must not open it. */
  const onCardClickCapture = (event: ReactMouseEvent<HTMLDivElement>) => {
    if (!swallowClick.current) return;
    swallowClick.current = false;
    event.preventDefault();
    event.stopPropagation();
  };

  /* ---------- panning the field ---------- */

  const onFieldPointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.pointerType === 'touch' || event.button !== 0) return;
    if ((event.target as Element).closest('.canvas-slot')) return;
    const element = scrollRef.current;
    if (!element) return;
    pan.current = {
      pointerId: event.pointerId,
      x: event.clientX,
      y: event.clientY,
      left: element.scrollLeft,
      top: element.scrollTop,
    };
    element.setPointerCapture(event.pointerId);
  };

  const onFieldPointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    const current = pan.current;
    const element = scrollRef.current;
    if (!current || !element || current.pointerId !== event.pointerId) return;
    element.scrollLeft = current.left - (event.clientX - current.x);
    element.scrollTop = current.top - (event.clientY - current.y);
  };

  const onFieldPointerUp = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (pan.current?.pointerId === event.pointerId) pan.current = null;
  };

  // A service that disappears takes its stored place with it.
  useEffect(() => {
    if (!appsQuery.isSuccess || !vmsQuery.isSuccess) return;
    if (Object.keys(saved).every((key) => keys.includes(key))) return;
    const kept = Object.fromEntries(Object.entries(saved).filter(([key]) => keys.includes(key)));
    setSaved(kept);
    writePositions(browserStorage(), projectId, kept, keys);
  }, [appsQuery.isSuccess, vmsQuery.isSuccess, keys, saved, projectId]);

  const error = appsQuery.error ?? vmsQuery.error;
  const listError = error ? problemView(error, 'see this project') : null;
  const loaded = appsQuery.isSuccess && vmsQuery.isSuccess;
  const empty = loaded && services.length === 0;
  const add = canWrite && onAdd ? onAdd : undefined;

  const card = (service: ServiceItem) =>
    service.kind === 'vm' ? (
      <VmCard
        selected={selected === service.key}
        highlighted={hl === service.key}
        vm={service.vm}
        node={nodes.find((node) => node.id === service.vm.node_id)}
        nodeNames={nodeNames}
      />
    ) : (
      <AppCard
        selected={selected === service.key}
        highlighted={hl === service.key}
        dimmed={Boolean(selected) && selected !== service.key}
        app={service.app}
        builds={builds[service.app.id]}
        run={runOf(service.app.id)}
        instances={vms.filter((vm) => vm.app_id === service.app.id)}
        deployments={deployments[service.app.id]}
        nodeNames={nodeNames}
        onDeployed={onDeployed}
      />
    );

  /** Pointing at a card, or focusing it, lights its cells in the rail. */
  const pointAt = (key: string) => ({
    onPointerEnter: () => setHl(key),
    onPointerLeave: () => setHl(null),
    onFocus: () => setHl(key),
    onBlur: () => setHl(null),
  });

  const tools =
    add && !empty ? (
      <div className={cn('canvas-tools', !floating && 'canvas-tools-flow')}>
        <Button variant="default" onClick={add} aria-keyshortcuts="Meta+K Control+K">
          <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
          Add
        </Button>
      </div>
    ) : null;

  const rail =
    services.length > 0 ? (
      <ProjectRail
        projectId={projectId}
        project={project}
        apps={apps}
        vms={vms}
        nodes={nodes}
        builds={builds}
        deployments={deployments}
        loading={!loaded}
        highlight={highlight}
      />
    ) : null;

  return (
    <div className="canvas-stage" data-sheet={selected ? '' : undefined} data-layout={floating ? 'float' : 'stack'}>
      {floating ? (
        <div
          ref={scrollRef}
          className="canvas-scroll"
          style={{ backgroundSize: `${22 * zoom}px ${22 * zoom}px` }}
          onPointerDown={onFieldPointerDown}
          onPointerMove={onFieldPointerMove}
          onPointerUp={onFieldPointerUp}
          onPointerCancel={onFieldPointerUp}
        >
          <div className="canvas-sizer" style={{ width: worldW * zoom, height: worldH * zoom }}>
            <div
              className="canvas-world"
              style={{ width: worldW, height: worldH, transform: `scale(${zoom})` }}
            >
              {order.map((key) => {
                const service = byKey.get(key);
                const point = shown[key];
                if (!service || !point) return null;
                const aura = auras[key];
                return (
                  <div
                    key={key}
                    ref={(element) => {
                      if (element) slotRefs.current.set(key, element);
                      else slotRefs.current.delete(key);
                    }}
                    className="canvas-slot"
                    data-dragging={drag?.key === key ? '' : undefined}
                    style={{ left: point.x, top: point.y, width: CARD_W, minHeight: CARD_H / 2 }}
                    onPointerDown={onCardPointerDown(key)}
                    onPointerMove={onCardPointerMove}
                    onPointerUp={onCardPointerUp}
                    onPointerCancel={onCardPointerCancel}
                    onClickCapture={onCardClickCapture}
                    {...pointAt(key)}
                  >
                    {aura ? <Aura tone={aura.tone} breathe={aura.breathe} /> : null}
                    {card(service)}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      ) : (
        <div ref={scrollRef} className="canvas-scroll canvas-flow">
          {tools}
          <div className="canvas-stack">
            {order.map((key) => {
              const service = byKey.get(key);
              if (!service) return null;
              const aura = auras[key];
              return (
                <div key={key} className="canvas-cell" {...pointAt(key)}>
                  {aura ? <Aura tone={aura.tone} breathe={aura.breathe} /> : null}
                  {card(service)}
                </div>
              );
            })}
          </div>
          {rail}
        </div>
      )}

      {empty ? (
        <div className="canvas-empty">
          <EmptyState
            title="No apps in this project"
            action={
              add ? (
                <Button variant="primary" onClick={add}>
                  <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
                  Add a service
                </Button>
              ) : undefined
            }
          >
            Deploy from a Git repository or an image.
          </EmptyState>
        </div>
      ) : null}

      {floating ? tools : null}
      {floating ? rail : null}

      {listError ? <ProblemBanner view={listError} className="canvas-problem" /> : null}

      {floating && services.length > 0 ? (
        <div className="zoom" role="group" aria-label="Zoom">
          <button type="button" onClick={() => zoomTo(zoomIn(zoom))} aria-label="Zoom in">
            <Plus strokeWidth={1.7} aria-hidden="true" />
          </button>
          <button type="button" onClick={() => zoomTo(zoomOut(zoom))} aria-label="Zoom out">
            <Minus strokeWidth={1.7} aria-hidden="true" />
          </button>
          <button type="button" onClick={fit} aria-label="Fit every card in view">
            <Maximize2 strokeWidth={1.7} aria-hidden="true" />
          </button>
        </div>
      ) : null}

      <p className="sr-only" role="status">
        {said}
      </p>
    </div>
  );
}
