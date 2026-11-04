import type { Vm } from '../api/generated/model';
import { Lamp } from './lamp';

/** The app's instances, one row each, with the node that runs it. */
export function AppInstances({
  instances,
  nodeNames,
  hasSource,
}: {
  instances: readonly Vm[];
  nodeNames: Record<string, string>;
  hasSource: boolean;
}) {
  return (
    <section className="mt-5 rounded-[6px] border border-rail bg-panel px-4 py-4 sm:px-5">
      <h2 className="type-display-light text-[15px] leading-none text-ink">Instances</h2>
      {instances.length === 0 ? (
        <p className="mt-3 text-[13px] text-ink-muted">
          {hasSource
            ? 'No instance is running. A successful build deploys the app and boots one.'
            : 'No instance is running. Deploy the app to boot one.'}
        </p>
      ) : (
        <ul className="mt-3 flex flex-col divide-y divide-rail border-y border-rail">
          {instances.map((instance) => (
            <li
              key={instance.id}
              className="flex flex-wrap items-center gap-x-3 gap-y-2 py-2.5"
            >
              <Lamp vm={instance} size={18} />
              <span className="type-data min-w-0 flex-1 truncate text-[14px] text-ink">
                {instance.name}
              </span>
              <span className="type-data text-[13px] text-ink-muted">
                {instance.observed_status}
              </span>
              <span className="type-data text-[13px] text-ink-muted">
                {instance.ip ?? 'No IP yet'}
              </span>
              <span className="type-data text-[13px] text-ink-muted">
                {instance.restart_count} restarts
              </span>
              <span className="type-data text-[13px] text-ink-muted">
                node {nodeNames[instance.node_id] ?? instance.node_id}
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
