import type { ServiceKind } from './services';
import { SERVICE_KINDS } from './services';

/*
 * Where each card sits on a project's canvas, as pure data. A first layout
 * puts the cards on a grid, one band of rows per kind. A card someone moved
 * keeps its place: the whole layout is stored per project in this browser's
 * localStorage, and a card added later takes the first free cell.
 */

export interface Point {
  readonly x: number;
  readonly y: number;
}

/** Card positions in canvas pixels at zoom 1, keyed by service key (`app:<id>`, `vm:<id>`). */
export type Positions = Readonly<Record<string, Point>>;

/** A ServiceCard's width; the canvas CSS uses the same number. */
export const CARD_W = 264;
/** The height the layout reserves for a card; most cards are shorter. */
export const CARD_H = 200;
/** One grid cell: a card and the gap after it. */
export const CELL_W = CARD_W + 56;
export const CELL_H = CARD_H + 40;

export interface LayoutFrame {
  /** How many cards fit side by side. */
  columns: number;
  /** Top-left corner of the first cell. */
  origin: Point;
}

/** The frame for a canvas this wide: room at the top for the tools, at most four columns. */
export const layoutFrame = (width: number): LayoutFrame => {
  const narrow = width < 600;
  const pad = narrow ? 16 : 48;
  const columns = Math.max(1, Math.min(4, Math.floor((width - 2 * pad + (CELL_W - CARD_W)) / CELL_W)));
  return { columns, origin: { x: pad, y: narrow ? 72 : 80 } };
};

const overlaps = (a: Point, b: Point): boolean =>
  Math.abs(a.x - b.x) < CARD_W && Math.abs(a.y - b.y) < CARD_H;

/**
 * Every card's position: a saved one where there is one, otherwise the next
 * free cell of the grid. The grid starts a new band of rows for each kind
 * (Git apps, image apps, VMs), in the order the items come, and skips a cell
 * that a saved card covers.
 */
export const autoLayout = (
  items: ReadonlyArray<{ key: string; kind: ServiceKind }>,
  saved: Positions,
  frame: LayoutFrame,
): Record<string, Point> => {
  const out: Record<string, Point> = {};
  const taken: Point[] = [];
  for (const item of items) {
    const point = saved[item.key];
    if (point) {
      out[item.key] = point;
      taken.push(point);
    }
  }

  let row = 0;
  for (const kind of SERVICE_KINDS) {
    let col = 0;
    let used = false;
    for (const item of items) {
      if (item.kind !== kind || out[item.key]) continue;
      for (;;) {
        const cell = {
          x: frame.origin.x + col * CELL_W,
          y: frame.origin.y + row * CELL_H,
        };
        col += 1;
        if (col >= frame.columns) {
          col = 0;
          row += 1;
        }
        if (!taken.some((point) => overlaps(point, cell))) {
          out[item.key] = cell;
          taken.push(cell);
          used = true;
          break;
        }
      }
    }
    if (used && col > 0) {
      row += 1;
    }
  }
  return out;
};

/** A position moved by a drag, never left of or above the canvas's corner. */
export const moved = (from: Point, dx: number, dy: number): Point => ({
  x: Math.max(0, Math.round(from.x + dx)),
  y: Math.max(0, Math.round(from.y + dy)),
});

/** The order cards take focus in: row by row, then left to right. */
export const readingOrder = (positions: Positions, keys: readonly string[]): string[] => {
  const row = (key: string) => Math.round((positions[key]?.y ?? 0) / (CARD_H / 2));
  return [...keys].sort(
    (left, right) =>
      row(left) - row(right) || (positions[left]?.x ?? 0) - (positions[right]?.x ?? 0),
  );
};

/* ---------- storage ---------- */

export const positionsKey = (projectId: string): string => `quoro.canvas.${projectId}`;

const isPoint = (value: unknown): value is Point =>
  typeof value === 'object' &&
  value !== null &&
  Number.isFinite((value as Point).x) &&
  Number.isFinite((value as Point).y);

type Reader = Pick<Storage, 'getItem'>;
type Writer = Pick<Storage, 'setItem'>;

/**
 * The positions saved for a project in this browser. Storage can be missing
 * or refuse (private windows, blocked site data), and its content can be
 * anything; each of those reads as no saved positions.
 */
export const readPositions = (storage: Reader | null, projectId: string): Positions => {
  if (!storage) return {};
  try {
    const raw = storage.getItem(positionsKey(projectId));
    if (raw === null) return {};
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return {};
    const out: Record<string, Point> = {};
    for (const [key, value] of Object.entries(parsed)) {
      if (isPoint(value)) out[key] = { x: Math.max(0, value.x), y: Math.max(0, value.y) };
    }
    return out;
  } catch {
    return {};
  }
};

/**
 * Saves the positions of the services that still exist; a removed service's
 * place is forgotten. A refusal leaves the layout for this page only.
 */
export const writePositions = (
  storage: Writer | null,
  projectId: string,
  positions: Positions,
  keys: readonly string[],
): void => {
  if (!storage) return;
  const kept: Record<string, Point> = {};
  for (const key of keys) {
    const point = positions[key];
    if (point) kept[key] = { x: Math.round(point.x), y: Math.round(point.y) };
  }
  try {
    storage.setItem(positionsKey(projectId), JSON.stringify(kept));
  } catch {
    // Storage refused: the layout lasts as long as the page.
  }
};

/** This browser's localStorage, or null where reading it throws. */
export const browserStorage = (): Storage | null => {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
};

/* ---------- zoom ---------- */

export const ZOOM_MIN = 0.4;
export const ZOOM_MAX = 1.5;

const roundZoom = (zoom: number): number => Math.round(zoom * 100) / 100;
const clampZoom = (zoom: number): number => Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, zoom));

export const zoomIn = (zoom: number): number => clampZoom(roundZoom(zoom + 0.1));
export const zoomOut = (zoom: number): number => clampZoom(roundZoom(zoom - 0.1));

export interface Box {
  readonly x: number;
  readonly y: number;
  readonly w: number;
  readonly h: number;
}

/** The box around every card; sizes default to the reserved card size. */
export const bounds = (
  positions: Positions,
  sizes: Readonly<Record<string, { w: number; h: number }>> = {},
): Box | null => {
  const keys = Object.keys(positions);
  if (keys.length === 0) return null;
  let left = Infinity;
  let top = Infinity;
  let right = -Infinity;
  let bottom = -Infinity;
  for (const key of keys) {
    const point = positions[key]!;
    const size = sizes[key] ?? { w: CARD_W, h: CARD_H };
    left = Math.min(left, point.x);
    top = Math.min(top, point.y);
    right = Math.max(right, point.x + size.w);
    bottom = Math.max(bottom, point.y + size.h);
  }
  return { x: left, y: top, w: right - left, h: bottom - top };
};

/**
 * Fit: the zoom that shows every card in a viewport (never larger than 1),
 * and the scroll offset that centres them in it.
 */
export const fitView = (
  box: Box,
  viewport: { w: number; h: number },
  pad = 32,
): { zoom: number; left: number; top: number } => {
  const zoom = clampZoom(
    roundZoom(
      Math.min(1, (viewport.w - 2 * pad) / box.w, (viewport.h - 2 * pad) / box.h),
    ),
  );
  const left = Math.max(0, Math.round(box.x * zoom - (viewport.w - box.w * zoom) / 2));
  const top = Math.max(0, Math.round(box.y * zoom - (viewport.h - box.h * zoom) / 2));
  return { zoom, left, top };
};
