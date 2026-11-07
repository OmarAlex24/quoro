import { Link } from '@tanstack/react-router';
import type { App } from '../api/generated/model';
import { useListAppDomains } from '../api/generated/domains/domains';
import { appLampState } from '../lib/app-status';
import { builtImageLabel } from '../lib/build-status';
import { GlyphTile } from './glyph-tile';
import { ImageRef } from './image-ref';
import { ToneLamp } from './tone-lamp';

/** One app as a rack bay: lamp well on the left, equipment plate on the right. */
export function AppBay({ app }: { app: App }) {
  const lamp = appLampState(app.status);
  const domains = useListAppDomains(app.id, undefined, {
    query: { staleTime: 10_000, retry: false },
  });
  const hostname = domains.data?.items[0]?.hostname;
  const notBuilt = app.source != null && builtImageLabel(app.image) === null;

  return (
    <article className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[10px] border border-line bg-surface card-sheen">
      <div className="flex items-center justify-center border-r border-line bg-sunk">
        <ToneLamp tone={lamp.tone} pulse={lamp.pulse} label={lamp.statusText} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="min-w-0 flex-1 basis-[min(100%,320px)]">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <GlyphTile kind={app.source != null ? 'git' : 'image'} />
            <Link
              to="/projects/$projectId"
              params={{ projectId: app.project_id }}
              search={{ app: app.id, tab: 'deployments' }}
              className="type-bay-title truncate text-[18px] leading-tight text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
            >
              {app.name}
            </Link>
            <span className="type-data text-[13px] text-ink-2">{lamp.statusText}</span>
          </div>
          {lamp.caption !== lamp.statusText ? (
            <p className="mt-0.5 text-[13px] leading-snug text-ink-2">{lamp.caption}</p>
          ) : null}
          <p className="type-data mt-2 truncate text-[13px] text-ink">
            {notBuilt ? 'Not built yet' : <ImageRef image={app.image} />} · port {app.port}
          </p>
          <p className="type-data mt-0.5 truncate text-[13px] text-ink-2">
            {hostname ?? 'No domain yet'}
          </p>
        </div>
      </div>
    </article>
  );
}
