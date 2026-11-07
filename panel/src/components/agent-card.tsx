// SPDX-License-Identifier: AGPL-3.0-only
import { Link } from '@tanstack/react-router';
import { useMemo } from 'react';
import { useListAgents } from '../api/generated/agents/agents';
import { useListGrants } from '../api/generated/grants/grants';
import { useListIncidents } from '../api/generated/incidents/incidents';
import type { Agent, Approval, Grant, Incident } from '../api/generated/model';
import { agentStatus, callBuckets } from '../lib/agents';
import { grantKindWords, grantMeter, meterGrant } from '../lib/grants';
import { useAgentCalls } from '../lib/use-agent-calls';
import { useNow } from '../lib/use-now';
import { AGENCY_LIST, useRecordContext } from '../lib/use-record-context';
import { useSession } from '../session';
import { cn } from '../lib/utils';
import { Avatar } from './avatar';
import { CallSpark } from './call-spark';
import { DitherMeter } from './dither-meter';
import { UserName } from './identity-name';
import { LampDot } from './lamp';
import { usePendingApprovals } from './shell/shell-data';

/** What a card knows about its agent, gathered once by the list. */
export interface AgentFacts {
  /** The agent's grants, every status. */
  readonly grants: readonly Grant[];
  /** Its requests that still wait for a person. */
  readonly pending: readonly Approval[];
  /** The name of the first request's target. */
  readonly pendingTargetName: string | null;
  /** The open incident it holds an incident grant for. */
  readonly incident: Incident | null;
}

/** Group the cluster's grants, requests and open incidents by agent. */
export function agentFacts(
  agentId: string,
  lists: {
    grants: readonly Grant[];
    pending: readonly Approval[];
    openIncidents: readonly Incident[];
    name: (type: string, id: string) => string | undefined;
  },
): AgentFacts {
  const grants = lists.grants.filter((grant) => grant.agent_id === agentId);
  const pending = lists.pending.filter((approval) => approval.agent_id === agentId);
  const handled = new Set(
    grants.filter((grant) => grant.kind === 'incident' && grant.incident_id).map((grant) => grant.incident_id),
  );
  const incident =
    [...lists.openIncidents]
      .filter((candidate) => handled.has(candidate.id))
      .sort((left, right) => right.opened_at.localeCompare(left.opened_at))[0] ?? null;
  const first = pending[0];
  return {
    grants,
    pending,
    pendingTargetName: first ? (lists.name(first.target.type, first.target.id) ?? null) : null,
    incident,
  };
}

/**
 * The agents and what the cards say about them, from one read of each list:
 * the grants, the open incidents and the requests that wait. Only the
 * sparkline reads per agent.
 */
export function useAgentDirectory() {
  const { user } = useSession();
  const enabled = user !== undefined;
  const agents = useListAgents(AGENCY_LIST, { query: { enabled, staleTime: 15_000, retry: false } });
  const grants = useListGrants(AGENCY_LIST, { query: { enabled, refetchInterval: 15_000, retry: false } });
  const incidents = useListIncidents(
    { status: 'open', limit: 200 },
    { query: { enabled, refetchInterval: 15_000, retry: false } },
  );
  const pending = usePendingApprovals();
  const ctx = useRecordContext();

  const facts = useMemo(() => {
    const lists = {
      grants: grants.data?.items ?? [],
      pending,
      openIncidents: incidents.data?.items ?? [],
      name: (type: string, id: string) => ctx.name?.(type, id),
    };
    return (agentId: string) => agentFacts(agentId, lists);
  }, [grants.data, pending, incidents.data, ctx]);

  return { agents, facts, ctx };
}

/**
 * One agent as a card: its square dithered avatar, model and owner, a lamp
 * with one phrase for where it stands, a meter for the grant it works under,
 * and its calls over the last day.
 */
export function AgentCard({ agent, facts, selected }: { agent: Agent; facts: AgentFacts; selected: boolean }) {
  const now = useNow(true, 30_000);
  const active = facts.grants.filter((grant) => grant.status === 'active');
  const status = agentStatus({
    agent,
    activeGrants: active.length,
    pending: facts.pending,
    pendingTargetName: facts.pendingTargetName,
    incident: facts.incident,
  });
  const grant = agent.disabled ? undefined : meterGrant(active);
  const meter = grant ? grantMeter(grant, now) : null;
  const calls = useAgentCalls(agent.id);
  const spark = useMemo(
    () => (calls.data && !calls.data.capped ? callBuckets(calls.data.records.map((record) => record.time), now) : null),
    [calls.data, now],
  );

  return (
    <Link
      to="/agents"
      search={{ agent: agent.id }}
      className={cn('acard', selected && 'acard-on', agent.disabled && 'acard-off')}
      aria-current={selected ? 'page' : undefined}
    >
      <span className="acard-h">
        <Avatar kind="agent" seed={agent.id} size="lg" className="acard-av" />
        <span className="acard-t">
          <b>{agent.name}</b>
          <small>
            {agent.model} · owned by <UserName id={agent.owner_user_id} />
          </small>
        </span>
      </span>

      <span className={cn('acard-state', status.attention && 'acard-state-ask')}>
        <LampDot ring={status.tone} core={status.tone} pulse={status.pulse} size={14} />
        <span className="min-w-0 truncate">{status.phrase}</span>
      </span>

      {grant && meter ? (
        <DitherMeter label={grantKindWords[grant.kind]} words={meter.words} share={meter.share} />
      ) : null}

      {spark ? (
        <CallSpark buckets={spark.buckets} total={spark.total} />
      ) : calls.data?.capped ? (
        <span className="acard-note">
          More of its calls are recorded than the card reads; Activity has them all.
        </span>
      ) : calls.isError ? (
        <span className="acard-note">Its calls could not be read.</span>
      ) : (
        <span className="spark spark-wait" aria-hidden="true" />
      )}
    </Link>
  );
}
