// SPDX-License-Identifier: AGPL-3.0-only
import { Link } from '@tanstack/react-router';
import { useListAgents } from '../api/generated/agents/agents';
import type { Approval } from '../api/generated/model';
import type { RecordContext } from '../lib/activity';
import { shortHash, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import {
  approvalMeter,
  askState,
  approvalWants,
  approvalWords,
  callText,
  decidedAt,
  DECIDER_ONLY_SENTENCE,
  isDestructiveScope,
  type Decision,
} from '../lib/approvals';
import type { ProblemView } from '../lib/problem';
import { useNow } from '../lib/use-now';
import { AGENCY_LIST } from '../lib/use-record-context';
import { cn } from '../lib/utils';
import { ApprovalDecision } from './approval-decision';
import { Avatar } from './avatar';
import { CopyButton } from './copy-button';
import { DitherMeter } from './dither-meter';
import { SkelName } from './skeleton';
import { UserName } from './identity-name';
import { Tag, type TagTone } from './ui/tag';

const STATUS_TONE: Record<Approval['status'], TagTone> = {
  pending: 'shift',
  approved: 'run',
  denied: 'fault',
  expired: 'idle',
};

const linkClass =
  'text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

/**
 * One request, open: who asks to do what, why, the only call it allows, how
 * long it waits, and the decision (or who made it and when).
 */
export function ApprovalDetail({
  approval,
  ctx,
  canDecide,
  onDecided,
  onConflict,
  focusAfter,
}: {
  approval: Approval;
  ctx: RecordContext;
  canDecide: boolean;
  onDecided: (kind: Decision, decided: Approval) => void;
  onConflict: (view: ProblemView) => void;
  /** Where focus goes once the decision is sent. */
  focusAfter?: () => HTMLElement | null | undefined;
}) {
  const pending = approval.status === 'pending';
  const now = useNow(pending, 15_000);
  const agents = useListAgents(AGENCY_LIST, { query: { staleTime: 30_000, retry: false } });
  const agent = agents.data?.items.find((candidate) => candidate.id === approval.agent_id);
  // The name, a bar while the agents list loads, and the id once it has loaded without this agent.
  const knownName = agent?.name ?? ctx.name?.('agent', approval.agent_id);
  const naming = knownName === undefined && agents.isPending;
  const agentName = knownName ?? (naming ? 'An agent' : shortId(approval.agent_id));
  const name = (type: string, id: string) => ctx.name?.(type, id);
  const state = askState(approval, ctx);
  const meter = approvalMeter(approval, now);
  const incident = approval.incident_id ? ctx.name?.('incident', approval.incident_id) : undefined;

  return (
    <article className="apr" aria-label={`${agentName} ${approvalWants(approval, name, state)}`}>
      <Link to="/approvals" className={cn('apr-back text-[13px]', linkClass)}>
        All requests
      </Link>

      <header className="apr-h">
        <Avatar kind="agent" seed={approval.agent_id} size="lg" />
        <div className="min-w-0">
          <h2>
            {naming ? <SkelName label="Loading the agent’s name" width="7em" /> : agentName}{' '}
            {approvalWants(approval, name, state)}
          </h2>
          <p className="apr-tags">
            <Tag tone="code">{approval.scope}</Tag>
            {isDestructiveScope(approval.scope) ? <Tag tone="fault">Destructive</Tag> : null}
            {approval.incident_id ? (
              <Link to="/activity" search={{ incident_id: approval.incident_id }} className="apr-tag-link">
                <Tag tone="plain">Incident: {incident ?? 'open the log'}</Tag>
              </Link>
            ) : null}
            {!pending ? <Tag tone={STATUS_TONE[approval.status]}>{approvalWords[approval.status]}</Tag> : null}
          </p>
          <p className="apr-sub">
            {agent ? `${agent.model} · ` : ''}asked <span title={approval.created_at}>{relativeTime(approval.created_at, now)}</span>
            {approval.target.type === 'app' && !state.gone ? (
              <>
                {' · '}
                <Link to="/apps/$appId" params={{ appId: approval.target.id }} title={approval.target.id} className={linkClass}>
                  Open {name('app', approval.target.id) ?? 'the app'}
                </Link>
              </>
            ) : null}
          </p>
        </div>
      </header>

      <section className="apr-sec">
        <h3>Why the agent asks</h3>
        <p className="quote">{approval.justification}</p>
      </section>

      <section className="apr-sec">
        <h3>The only call this approval allows</h3>
        <div className="callbox">
          <code className="call">{callText(approval)}</code>
          <span className="callbox-hash">
            <span className="type-code" title={approval.params_hash}>
              params {shortHash(approval.params_hash)}
            </span>
            <CopyButton value={approval.params_hash} label="Copy" />
          </span>
        </div>
        <p className="text-[12.5px] leading-snug text-ink-2">
          Only a call with exactly these parameters can use the approval, once, within 10 minutes.
        </p>
      </section>

      {pending ? (
        <>
          <DitherMeter
            tone="shift"
            label={meter.words}
            words={meter.then}
            share={meter.share}
          />
          {canDecide ? (
            <ApprovalDecision approval={approval} onDecided={onDecided} onConflict={onConflict} focusAfter={focusAfter} />
          ) : (
            <p className="box px-3.5 py-2.5 text-[13px] leading-snug text-ink-2">{DECIDER_ONLY_SENTENCE}</p>
          )}
        </>
      ) : (
        <section className="apr-sec">
          <h3>Decision</h3>
          <p className="text-[14px] leading-snug text-ink">
            {approval.status === 'expired' ? (
              <>
                Nobody decided; it expired{' '}
                <span title={decidedAt(approval)}>{relativeTime(decidedAt(approval), now)}</span>.
              </>
            ) : (
              <>
                {approvalWords[approval.status]}
                {approval.decided_by ? (
                  <>
                    {' by '}
                    <UserName id={approval.decided_by} />
                  </>
                ) : null}{' '}
                <span title={decidedAt(approval)}>{relativeTime(decidedAt(approval), now)}</span>.
              </>
            )}
          </p>
          {approval.decision_note ? <p className="quote">{approval.decision_note}</p> : null}
          {approval.status === 'approved' && approval.grant_id ? (
            <p className="text-[13px] text-ink-2">
              Approving issued a one-time grant.{' '}
              <Link to="/agents" search={{ agent: approval.agent_id, tab: 'grants' }} className={linkClass}>
                See the grant
              </Link>
            </p>
          ) : null}
        </section>
      )}
    </article>
  );
}
