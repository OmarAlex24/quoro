// SPDX-License-Identifier: AGPL-3.0-only
import { useQuery } from '@tanstack/react-query';
import { listAudit } from '../api/generated/audit/audit';
import type { AuditList, AuditRecord, ListAuditParams } from '../api/generated/model';
import { useSession } from '../session';

/** Pages of the log a reader reads before it gives up. */
export const CALL_PAGES = 4;
/** The largest page `listAudit` returns (audit-log-format.md, section 2.4). */
export const CALL_PAGE = 500;
/** How far back the agent cards count calls. */
export const CALL_WINDOW_MS = 24 * 3_600_000;

/** One call of `listAudit`; the generated client unless a test passes its own. */
export type AuditLister = (params: ListAuditParams) => Promise<AuditList>;

export interface AgentCalls {
  /** The agent's records, newest first, reaching back past the last 24 h. */
  readonly records: readonly AuditRecord[];
  /**
   * The last 24 h hold more of the agent's records than the card reads: the
   * pages ran out before they reached a record older than the window, so the
   * day cannot be counted honestly and the card says so instead.
   */
  readonly capped: boolean;
}

export interface IncidentRecords {
  /** The incident's records, oldest first. */
  readonly records: readonly AuditRecord[];
  /** The log holds more of the incident's records than the story read. */
  readonly capped: boolean;
}

/** Every record of one incident, oldest first, up to `CALL_PAGES` pages. */
async function readIncidentRecords(incidentId: string): Promise<IncidentRecords> {
  const filter = { incident_id: incidentId, limit: CALL_PAGE };
  const records: AuditRecord[] = [];
  let cursor: string | null | undefined;
  for (let page = 0; page < CALL_PAGES; page += 1) {
    const result = await listAudit(cursor ? { ...filter, cursor } : filter);
    records.push(...result.items);
    cursor = result.next_cursor;
    if (!cursor) return { records, capped: false };
  }
  return { records, capped: true };
}

/**
 * One agent's records, newest first (ADR 0283), until a page reaches a record
 * older than the last 24 h, the log ends, or `CALL_PAGES` pages are read.
 * Records past the window stay in the result: the agent's page shows its
 * latest calls however old they are, and the card counts only the window.
 */
export async function readAgentCalls(
  agentId: string,
  {
    now = Date.now(),
    list = listAudit,
    pageSize = CALL_PAGE,
  }: { now?: number; list?: AuditLister; pageSize?: number } = {},
): Promise<AgentCalls> {
  const filter: ListAuditParams = { actor_id: agentId, order: 'desc', limit: pageSize };
  const since = now - CALL_WINDOW_MS;
  const records: AuditRecord[] = [];
  let cursor: string | null | undefined;
  for (let page = 0; page < CALL_PAGES; page += 1) {
    const result = await list(cursor ? { ...filter, cursor } : filter);
    records.push(...result.items);
    cursor = result.next_cursor;
    // `seq` and `time` rise together, so the last record of a page is its oldest.
    const oldest = result.items[result.items.length - 1];
    if (!cursor || (oldest && Date.parse(oldest.time) <= since)) return { records, capped: false };
  }
  return { records, capped: true };
}

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
    queryFn: () => readIncidentRecords(incidentId),
    enabled: user !== undefined && incidentId !== '',
    refetchInterval: 10_000,
    retry: false,
  });
}
