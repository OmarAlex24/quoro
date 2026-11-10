import { Link } from '@tanstack/react-router';
import { useCallback } from 'react';
import { shortId } from '../lib/agency-format';
import { drawPreview, type DitherSurface, type TokenReader } from '../lib/dither-draw';
import type { ProjectSummary, ProjectTileData } from '../lib/projects';
import { servicesCount } from '../lib/services';
import { DitherCanvas } from './dither-canvas';
import { GlyphTile } from './glyph-tile';
import { LampDot } from './lamp';

/** The preview shows this many service tiles; beyond it, the last place says how many more. */
const PREVIEW_TILES = 8;

/**
 * One project on the projects grid: its name and size, a dithered preview of
 * its services in their kind colours, and its state in one line.
 */
export function ProjectTile({
  tile,
  summary,
}: {
  tile: ProjectTileData;
  summary: ProjectSummary;
}) {
  const count = tile.services.length;
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawPreview(surface, count, read),
    [count],
  );
  const overflow = count > PREVIEW_TILES;
  const shown = overflow ? tile.services.slice(0, PREVIEW_TILES - 1) : tile.services;

  return (
    <Link to="/projects/$projectId" params={{ projectId: tile.projectId }} className="card card-lift ptile">
      <span className="ptile-h">
        <b>{tile.project?.name ?? shortId(tile.projectId)}</b>
        <span className="ptile-count">{servicesCount(count)}</span>
      </span>
      <span className="pprev">
        <DitherCanvas paint={paint} />
        <span className="ptiles">
          {shown.map((service) => (
            <GlyphTile key={service.key} kind={service.kind} className="glyph-preview" />
          ))}
          {overflow ? (
            <span className="ptiles-more" aria-hidden="true">
              +{count - shown.length}
            </span>
          ) : null}
        </span>
      </span>
      <span className="pfoot">
        <LampDot ring={summary.tone} core={summary.tone} pulse={summary.pulse} size={15} />
        <span className="min-w-0 truncate">{summary.text}</span>
      </span>
    </Link>
  );
}
