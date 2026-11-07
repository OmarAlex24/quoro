import { useListApps } from '../api/generated/apps/apps';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListVms } from '../api/generated/vms/vms';
import { nodeLoad } from '../lib/node-capacity';
import { problemView } from '../lib/problem';
import { SERVICE_LIST_PARAMS } from '../lib/projects';
import { N1Strip } from '../components/n1-strip';
import { NodeCard } from '../components/node-card';
import { ProblemBanner } from '../components/ui/problem-banner';

/**
 * `/nodes`: the N+1 strip, then one card per machine with what its VMs
 * reserve of its CPU, memory and disk, its own live reading, and what it runs.
 */
export function NodesScreen() {
  const nodesQuery = useListNodes(undefined, { query: { refetchInterval: 10_000 } });
  const vmsQuery = useListVms(SERVICE_LIST_PARAMS, { query: { refetchInterval: 10_000 } });
  const appsQuery = useListApps(SERVICE_LIST_PARAMS, { query: { staleTime: 10_000 } });

  const nodes = nodesQuery.data?.items ?? [];
  const vms = vmsQuery.data?.items ?? [];
  const listError =
    nodesQuery.error !== null && nodesQuery.error !== undefined
      ? problemView(nodesQuery.error, 'see nodes')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 pb-8 sm:px-6">
      {listError ? <ProblemBanner view={listError} className="mb-4" /> : null}

      {nodesQuery.isPending ? (
        <div className="nodes" aria-hidden="true">
          <div className="node h-[236px] opacity-60" />
        </div>
      ) : nodes.length === 0 && !listError ? (
        <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
          <p className="text-[15px] text-ink">No node is enrolled in the cluster yet.</p>
        </div>
      ) : (
        <>
          <N1Strip nodes={nodes} apps={appsQuery.data?.items ?? []} />
          <div className="nodes">
            {nodes.map((node) => (
              <NodeCard key={node.id} node={node} load={nodeLoad(node.id, vms)} />
            ))}
          </div>
        </>
      )}
    </main>
  );
}
