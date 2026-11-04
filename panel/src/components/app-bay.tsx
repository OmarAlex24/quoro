import { Link } from '@tanstack/react-router';
import type { App } from '../api/generated/model';
import { useListAppDomains } from '../api/generated/domains/domains';
import { appLampState } from '../lib/app-status';
import { builtImageLabel } from '../lib/build-status';
import { ToneLamp } from './tone-lamp';

/** One app as a rack bay: lamp well on the left, equipment plate on the right. */
export function AppBay({ app }: { app: App }) {
  const lamp = appLampState(app.status);
  const domains = useListAppDomains(app.id, undefined, {
    query: { staleTime: 10_000, retry: false },
  });
  const hostname = domains.data?.items[0]?.hostname;
  const image =
    app.source != null ? (builtImageLabel(app.image) ?? 'Not built yet') : app.image;

  return (
    <article className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[6px] border border-rail bg-panel bay-shadow">
      <div className="flex items-center justify-center border-r border-rail bg-recess">
        <ToneLamp tone={lamp.tone} pulse={lamp.pulse} label={lamp.statusText} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="min-w-0 flex-1 basis-[min(100%,320px)]">
          <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <Link
              to="/apps/$appId"
              params={{ appId: app.id }}
              className="type-bay-title truncate text-[18px] leading-tight text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
            >
              {app.name}
            </Link>
            <span className="type-data text-[13px] text-ink-muted">{lamp.statusText}</span>
          </div>
          {lamp.caption !== lamp.statusText ? (
            <p className="mt-0.5 text-[13px] leading-snug text-ink-muted">{lamp.caption}</p>
          ) : null}
          <p className="type-data mt-2 truncate text-[13px] text-ink">
            {image} · port {app.port}
          </p>
          <p className="type-data mt-0.5 truncate text-[13px] text-ink-muted">
            {hostname ?? 'No domain yet'}
          </p>
        </div>
      </div>
    </article>
  );
}
