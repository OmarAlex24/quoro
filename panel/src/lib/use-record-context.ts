// SPDX-License-Identifier: AGPL-3.0-only
import { useMemo } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import { useListApprovals } from '../api/generated/approvals/approvals';
import { useListApps } from '../api/generated/apps/apps';
import { useListGrants } from '../api/generated/grants/grants';
import { useListIncidents } from '../api/generated/incidents/incidents';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { useListUsers } from '../api/generated/users/users';
import { useListVms } from '../api/generated/vms/vms';
import { useListWebhooks } from '../api/generated/webhooks/webhooks';
import type { Approval, Grant } from '../api/generated/model';
import { useSession } from '../session';
import type { RecordContext } from './activity';
import { canListUsers } from './capabilities';
import { PROJECT_LIST_PARAMS, SERVICE_LIST_PARAMS } from './projects';

/** The list size the agency screens read with, so they share one cache entry each. */
export const AGENCY_LIST = { limit: 200 } as const;

type Named = { id: string; name: string };

/**
 * Names for the ids an audit record holds, from the lists the panel reads
 * anyway: agents, people, apps, VMs, projects, webhooks, incidents and nodes,
 * plus the approvals and grants a sentence may need to say what was asked.
 * Each list is one cached query shared with the other screens.
 */
export function useRecordContext(): RecordContext & { readonly loading: boolean } {
  const { me, user } = useSession();
  const enabled = user !== undefined;
  const slow = { query: { enabled, staleTime: 30_000, retry: false } } as const;

  const agents = useListAgents(AGENCY_LIST, slow);
  const users = useListUsers(AGENCY_LIST, {
    query: { enabled: enabled && canListUsers(me), staleTime: 60_000, retry: false },
  });
  const apps = useListApps(SERVICE_LIST_PARAMS, slow);
  const vms = useListVms(SERVICE_LIST_PARAMS, slow);
  const projects = useListProjects(PROJECT_LIST_PARAMS, slow);
  const webhooks = useListWebhooks(undefined, slow);
  const incidents = useListIncidents(AGENCY_LIST, slow);
  const nodes = useListNodes(undefined, slow);
  const approvals = useListApprovals(AGENCY_LIST, { query: { enabled, refetchInterval: 10_000, retry: false } });
  const grants = useListGrants(AGENCY_LIST, { query: { enabled, refetchInterval: 15_000, retry: false } });

  return useMemo(() => {
    const names = new Map<string, string>();
    const put = (items: readonly Named[] | undefined) => {
      for (const item of items ?? []) names.set(item.id, item.name);
    };
    put(agents.data?.items);
    put(users.data?.items);
    put(apps.data?.items);
    put(vms.data?.items);
    put(projects.data?.items);
    put(webhooks.data?.items);
    put(nodes.data?.items);
    for (const incident of incidents.data?.items ?? []) names.set(incident.id, incident.title);
    // The signed-in person is always known, even when the user list is not readable.
    if (user) names.set(user.id, user.name);

    const approvalsById = new Map<string, Approval>((approvals.data?.items ?? []).map((item) => [item.id, item]));
    const grantsById = new Map<string, Grant>((grants.data?.items ?? []).map((item) => [item.id, item]));

    return {
      name: (_type: string, id: string) => names.get(id),
      approval: (id: string) => approvalsById.get(id),
      grant: (id: string) => grantsById.get(id),
      loading: agents.isPending || apps.isPending,
    };
  }, [
    agents.data,
    users.data,
    apps.data,
    vms.data,
    projects.data,
    webhooks.data,
    incidents.data,
    nodes.data,
    approvals.data,
    grants.data,
    user,
    agents.isPending,
    apps.isPending,
  ]);
}
