import { useListApprovals } from '../../api/generated/approvals/approvals';
import type { Approval } from '../../api/generated/model';
import { useGetCluster } from '../../api/generated/cluster/cluster';
import { useListNodes } from '../../api/generated/nodes/nodes';
import { clusterSummary, type ClusterSummary } from '../../lib/nodes';
import { useSession } from '../../session';

/*
 * What the shell shows on every page: approvals waiting, the cluster's name
 * and its nodes. Each query waits for a signed-in user, so a visit before
 * sign-in caches no refusal.
 */

/** Approvals waiting for a person; refreshed every 10 s. One query for every view that shows them. */
export function usePendingApprovals(): Approval[] {
  const { user } = useSession();
  const pending = useListApprovals(
    { status: 'pending' },
    { query: { enabled: user !== undefined, refetchInterval: 10_000, retry: false } },
  );
  return pending.data?.items ?? [];
}

/** How many approvals wait for a person. */
export function usePendingApprovalCount(): number {
  return usePendingApprovals().length;
}

/** The cluster's name, or the product's when it cannot be read. */
export function useWorkspaceName(): string {
  const { user } = useSession();
  const cluster = useGetCluster({
    query: { enabled: user !== undefined, staleTime: 60_000, retry: false },
  });
  return cluster.data?.name ?? 'Quoro';
}

export function useClusterSummary(): { summary: ClusterSummary; pending: boolean } {
  const { user } = useSession();
  const nodes = useListNodes(undefined, {
    query: { enabled: user !== undefined, refetchInterval: 10_000 },
  });
  return { summary: clusterSummary(nodes.data?.items ?? []), pending: nodes.isPending };
}

export const roleLabel = (role: string): string => role.charAt(0).toUpperCase() + role.slice(1);
