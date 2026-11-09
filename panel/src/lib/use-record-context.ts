// SPDX-License-Identifier: AGPL-3.0-only
import { useQueries } from '@tanstack/react-query';
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
import { getListAuditQueryOptions } from '../api/generated/audit/audit';
import type { Approval, AuditRecord, Grant } from '../api/generated/model';
import { useSession } from '../session';
import { CREATED_BY, createdName, namesFromRecords, type RecordContext } from './activity';
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
    // A kind is known in full when its list came back on one page; only then
    // does an id missing from it mean the resource is gone.
    const whole = (list: { items: readonly Named[]; next_cursor: string | null } | undefined) =>
      list && list.next_cursor === null ? new Set(list.items.map((item) => item.id)) : null;
    const listed: Readonly<Record<string, Set<string> | null>> = {
      app: whole(apps.data),
      vm: whole(vms.data),
      project: whole(projects.data),
    };

    return {
      name: (_type: string, id: string) => names.get(id),
      approval: (id: string) => approvalsById.get(id),
      grant: (id: string) => grantsById.get(id),
      gone: (type: string, id: string) => {
        const ids = listed[type];
        return ids !== null && ids !== undefined && !ids.has(id);
      },
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

/** At most this many deleted targets are looked up at once. */
const KEPT_NAME_LOOKUPS = 25;
const NO_RECORDS: readonly AuditRecord[] = [];

/**
 * The context with names for targets that are gone from their lists: first
 * from create records the view already holds, then from the oldest audit
 * record about each target, which for an app, a VM or a project is the call
 * that created it and holds its name. A deleted target keeps its name; a
 * lookup that finds none leaves the sentence to say "a deleted app".
 */
export function useKeptNames(
  ctx: RecordContext,
  targets: ReadonlyArray<{ type: string; id: string }>,
  records: ReadonlyArray<AuditRecord> = NO_RECORDS,
): RecordContext {
  const held = useMemo(() => namesFromRecords(records), [records]);
  const missing = targets
    .filter(
      (target) =>
        target.type in CREATED_BY &&
        ctx.name?.(target.type, target.id) === undefined &&
        !held.has(target.id) &&
        ctx.gone?.(target.type, target.id) === true,
    )
    .slice(0, KEPT_NAME_LOOKUPS);
  const found = useQueries({
    queries: missing.map((target) =>
      getListAuditQueryOptions({ target_id: target.id, limit: 1 }, { query: { staleTime: Infinity, retry: false } }),
    ),
    // One string, so the context below is rebuilt only when a name arrives.
    combine: (results) =>
      JSON.stringify(
        results.flatMap((result) => {
          const record = result.data?.items[0];
          const name = record ? createdName(record) : undefined;
          return record?.target_id && name ? [[record.target_id, name]] : [];
        }),
      ),
  });

  return useMemo(() => {
    const looked = JSON.parse(found) as [string, string][];
    if (held.size === 0 && looked.length === 0) return ctx;
    const kept = new Map([...held, ...looked]);
    return { ...ctx, name: (type: string, id: string) => ctx.name?.(type, id) ?? kept.get(id) };
  }, [ctx, held, found]);
}
