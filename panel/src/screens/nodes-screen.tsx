import { useListApps } from '../api/generated/apps/apps';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListVms } from '../api/generated/vms/vms';
import { problemView } from '../lib/problem';
import { SERVICE_LIST_PARAMS } from '../lib/projects';
import { HaVotes } from '../components/ha-votes';
import { NodesBoard, NodesBoardSkeleton } from '../components/nodes-board';
import { ProblemBanner } from '../components/ui/problem-banner';
import { EmptyState } from '../components/empty-state';

/**
 * `/nodes`: the voters and whether HA is ready, then the cluster band, one card per machine with what its
 * VMs reserve of its CPU, memory and disk, its own live charts and what it runs, and the node events.
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
    <main className="nodes-page">
      {listError ? <ProblemBanner view={listError} className="mb-4" /> : null}

      {nodesQuery.isPending ? (
        <NodesBoardSkeleton />
      ) : nodes.length === 0 && !listError ? (
        <EmptyState title="No node is enrolled in the cluster yet" />
      ) : (
        <>
          <HaVotes nodes={nodes} />
          <NodesBoard nodes={nodes} vms={vms} apps={appsQuery.data?.items ?? []} />
        </>
      )}
    </main>
  );
}
