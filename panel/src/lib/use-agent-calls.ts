// SPDX-License-Identifier: AGPL-3.0-only
import { useQuery } from '@tanstack/react-query';
import { listAudit } from '../api/generated/audit/audit';
import type { AuditRecord } from '../api/generated/model';
import { useSession } from '../session';

/** Pages of the agent's records the card reads before it gives up counting. */
export const CALL_PAGES = 4;
/** The largest page `listAudit` returns (audit-log-format.md, section 2.4). */
export const CALL_PAGE = 500;

export interface AgentCalls {
  /** The agent's records, oldest first. */
  readonly records: readonly AuditRecord[];
  /**
   * The log holds more of the agent's records than the card reads. The log
   * is read oldest first, so what is missing is the newest: the last 24 h
   * cannot be counted honestly and the card says so instead.
   */
  readonly capped: boolean;
}

/** Every record that matches one filter of `listAudit`, oldest first, up to `CALL_PAGES` pages. */
async function readAudit(filter: { actor_id: string } | { incident_id: string }): Promise<AgentCalls> {
  const records: AuditRecord[] = [];
  let cursor: string | null | undefined;
  for (let page = 0; page < CALL_PAGES; page += 1) {
    const list = await listAudit(cursor ? { ...filter, limit: CALL_PAGE, cursor } : { ...filter, limit: CALL_PAGE });
    records.push(...list.items);
    cursor = list.next_cursor;
    if (!cursor) return { records, capped: false };
  }
  return { records, capped: true };
}

/** Every record of one agent, up to `CALL_PAGES` pages. */
export const readAgentCalls = (agentId: string): Promise<AgentCalls> => readAudit({ actor_id: agentId });

/**
 * The agent's records, read through the audit log filtered by actor. Keyed
 * under the audit list so a refresh of the log refreshes the cards too.
 */
export function useAgentCalls(agentId: string) {
  const { user } = useSession();
  return useQuery({
    queryKey: ['/api/v1/audit', 'agent-calls', agentId],
    queryFn: () => readAgentCalls(agentId),
    enabled: user !== undefined && agentId !== '',
    refetchInterval: 30_000,
    staleTime: 10_000,
    retry: false,
  });
}

/**
 * Every record of one incident, by anyone, oldest first: what the story is
 * told from. `capped` says the log holds more than the story read.
 */
export function useIncidentRecords(incidentId: string) {
  const { user } = useSession();
  return useQuery({
    queryKey: ['/api/v1/audit', 'incident', incidentId],
    queryFn: () => readAudit({ incident_id: incidentId }),
    enabled: user !== undefined && incidentId !== '',
    refetchInterval: 10_000,
    retry: false,
  });
}
