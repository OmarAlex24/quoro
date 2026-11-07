import type { EdgeBackend } from '../api/generated/model';
import { relativeTime } from '../lib/app-status';
import { ToneLamp } from './tone-lamp';

/** The short form of a vm id, enough to tell two instances apart. */
const shortVmId = (id: string): string =>
  id.length > 12 ? `${id.slice(0, 10)}…` : id;

const reasonWords = (reason: string): string => {
  switch (reason) {
    case 'health_check':
      return 'Health check failed';
    case 'connect_failed':
      return 'Connect failed';
    default:
      return reason;
  }
};

/**
 * How the edge that serves an app sees its instances. Empty while no edge
 * serves the app, so the whole section stays out of the screen.
 */
export function AppEdgeBackends({
  backends,
  nodeNames,
}: {
  backends: readonly EdgeBackend[];
  nodeNames: Record<string, string>;
}) {
  if (backends.length === 0) return null;

  return (
    <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
      <h2 className="type-display-light text-[15px] leading-none text-ink">
        Edge backends
      </h2>
      <p className="mt-1.5 text-[13px] leading-snug text-ink-2">
        How the edge that serves this app sees each instance.
      </p>
      <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
        {backends.map((backend) => {
          const state = backend.in_rotation ? 'In rotation' : 'Out of rotation';
          return (
            <li
              key={`${backend.edge_node_id}-${backend.vm_id}`}
              className="flex flex-wrap items-center gap-x-3 gap-y-2 py-2.5"
            >
              <ToneLamp
                tone={backend.in_rotation ? 'run' : 'fault'}
                size={18}
                label={state}
              />
              <span
                className="type-code min-w-0 flex-1 truncate text-[12.5px] text-ink"
                title={backend.vm_id}
              >
                {shortVmId(backend.vm_id)}
              </span>
              <span className="type-data text-[13px] text-ink-2">
                node {nodeNames[backend.node_id] ?? backend.node_id}
              </span>
              <span className="type-data text-[13px] text-ink">{state}</span>
              <span className="type-data text-[13px] text-ink-2">
                since {relativeTime(backend.since)}
              </span>
              {!backend.in_rotation && backend.reason ? (
                <span
                  className="type-data text-[13px] text-ink-2"
                  title={backend.reason}
                >
                  {reasonWords(backend.reason)}
                </span>
              ) : null}
              {!backend.in_rotation && backend.last_error ? (
                <p className="basis-full text-[13px] leading-snug text-fault">
                  {backend.last_error}
                </p>
              ) : null}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
