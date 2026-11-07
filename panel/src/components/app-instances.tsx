import type { Node, Vm } from '../api/generated/model';
import { Lamp } from './lamp';

/**
 * The status column of one instance. The API reports an instance `unknown`
 * while its node is not connected; the row says why when the node list does.
 */
export const instanceStatusText = (
  instance: Pick<Vm, 'observed_status' | 'node_id'>,
  nodeStatuses: Record<string, Node['status']>,
): string => {
  const nodeStatus = nodeStatuses[instance.node_id];
  if (instance.observed_status === 'unknown' && nodeStatus && nodeStatus !== 'connected') {
    return `unknown (node ${nodeStatus})`;
  }
  return instance.observed_status;
};

/** The app's instances, one row each, with the node that runs it. */
export function AppInstances({
  instances,
  nodeNames,
  nodeStatuses = {},
  hasSource,
}: {
  instances: readonly Vm[];
  nodeNames: Record<string, string>;
  nodeStatuses?: Record<string, Node['status']>;
  hasSource: boolean;
}) {
  return (
    <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5">
      <h2 className="type-display-light text-[15px] leading-none text-ink">Instances</h2>
      {instances.length === 0 ? (
        <p className="mt-3 text-[13px] text-ink-2">
          {hasSource
            ? 'No instance is running. A successful build deploys the app and boots one.'
            : 'No instance is running. Deploy the app to boot one.'}
        </p>
      ) : (
        <ul className="mt-3 flex flex-col divide-y divide-line border-y border-line">
          {instances.map((instance) => (
            <li
              key={instance.id}
              className="flex flex-wrap items-center gap-x-3 gap-y-2 py-2.5"
            >
              <Lamp vm={instance} size={18} />
              <span className="type-data min-w-0 flex-1 truncate text-[14px] text-ink">
                {instance.name}
              </span>
              <span className="type-data text-[13px] text-ink-2">
                {instanceStatusText(instance, nodeStatuses)}
              </span>
              <span className="type-data text-[13px] text-ink-2">
                {instance.ip ?? 'No IP yet'}
              </span>
              <span className="type-data text-[13px] text-ink-2">
                {instance.restart_count} restarts
              </span>
              <span className="type-data text-[13px] text-ink-2">
                node {nodeNames[instance.node_id] ?? instance.node_id}
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
