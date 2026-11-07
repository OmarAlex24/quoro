import { Link } from '@tanstack/react-router';
import { useListAgents } from '../api/generated/agents/agents';
import type { Approval } from '../api/generated/model';
import { expiryWords, shortHash, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { isDestructiveScope, paramRows, type Decision } from '../lib/approvals';
import type { ProblemView } from '../lib/problem';
import { ApprovalDecision } from './approval-decision';
import { CopyButton } from './copy-button';
import { Avatar } from './avatar';
import { AgentName } from './identity-name';
import { ToneLamp } from './tone-lamp';

/**
 * One pending request, as a permit slip clipped to the chassis: an amber left
 * edge, the agent and its model, what it wants to delete, the exact call, the
 * agent's reason, and (for a deciding human) the note and two buttons.
 */
export function ApprovalPermit({
  approval,
  canDecide,
  onDecided,
  onConflict,
}: {
  approval: Approval;
  canDecide: boolean;
  onDecided: (kind: Decision, decided: Approval) => void;
  onConflict: (view: ProblemView) => void;
}) {
  const agents = useListAgents({ limit: 200 }, { query: { staleTime: 60_000 } });
  const agent = agents.data?.items.find((candidate) => candidate.id === approval.agent_id);
  const destructive = isDestructiveScope(approval.scope);
  const rows = paramRows(approval.params);

  return (
    <article className="overflow-hidden rounded-[10px] border border-line border-l-4 border-l-shift bg-surface card-sheen">
      <div className="flex flex-col gap-4 p-4 sm:p-5">
        <div className="flex flex-col gap-2">
          <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
            <p className="flex min-w-0 flex-wrap items-center gap-x-2">
              <Avatar kind="agent" seed={approval.agent_id} />
              <span className="type-bay-title text-[16px] text-ink">
                <AgentName id={approval.agent_id} />
              </span>
              <span className="text-[13px] text-ink-2">
                filed {relativeTime(approval.created_at)}
              </span>
            </p>
            <p className="type-data text-[13px] text-ink">
              Expires {expiryWords(approval.expires_at)}
            </p>
          </div>
          <p className="text-[13px] text-ink-2">
            Model{' '}
            <span className="type-data text-ink">
              {agents.isPending ? 'Loading…' : (agent?.model ?? 'not listed')}
            </span>
          </p>
        </div>

        <dl className="grid grid-cols-1 gap-x-4 gap-y-2 sm:grid-cols-[7rem_minmax(0,1fr)]">
          <dt className="text-[13px] leading-snug text-ink-2">Scope</dt>
          <dd className="flex flex-wrap items-center gap-x-2 gap-y-1">
            <span className="type-code text-[12.5px] text-ink">{approval.scope}</span>
            {destructive ? (
              <span className="inline-flex items-center gap-1.5 text-[13px] text-ink">
                <ToneLamp tone="fault" size={13} label="Destructive" />
                Destructive
              </span>
            ) : null}
          </dd>

          <dt className="text-[13px] leading-snug text-ink-2">Target</dt>
          <dd className="flex flex-wrap items-baseline gap-x-2 gap-y-1 text-[14px]">
            <span className="text-ink-2">{approval.target.type}</span>
            {approval.target.type === 'app' ? (
              <Link
                to="/apps/$appId"
                params={{ appId: approval.target.id }}
                title={approval.target.id}
                className="type-code text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
              >
                {shortId(approval.target.id)}
              </Link>
            ) : (
              <span className="type-code text-ink" title={approval.target.id}>
                {shortId(approval.target.id)}
              </span>
            )}
          </dd>

          <dt className="text-[13px] leading-snug text-ink-2">Operation</dt>
          <dd className="type-code break-words text-[12px] text-ink">{approval.operation}</dd>
        </dl>

        <div>
          <p className="text-[13px] leading-snug text-ink-2">Parameters</p>
          {rows.length === 0 ? (
            <p className="mt-1.5 text-[13px] text-ink-2">None.</p>
          ) : (
            <dl className="mt-1.5 flex flex-col gap-1.5 border-l-2 border-line pl-3">
              {rows.map((row) => (
                <div
                  key={row.key}
                  className="grid grid-cols-1 gap-x-3 sm:grid-cols-[minmax(0,9rem)_minmax(0,1fr)]"
                >
                  <dt className="type-code break-words text-[12px] text-ink-2">
                    {row.key}
                  </dt>
                  <dd className="type-code whitespace-pre-wrap break-words text-[12px] text-ink">
                    {row.text}
                  </dd>
                </div>
              ))}
            </dl>
          )}

          <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1">
            <span className="text-[13px] text-ink-2">Params hash</span>
            <span className="type-code text-[12px] text-ink" title={approval.params_hash}>
              {shortHash(approval.params_hash)}
            </span>
            <CopyButton value={approval.params_hash} label="Copy hash" />
          </div>
          <p className="mt-1 text-[13px] leading-snug text-ink-2">
            Only a call with exactly these parameters can use the approval.
          </p>
        </div>

        <div>
          <p className="text-[13px] leading-snug text-ink-2">Justification</p>
          <blockquote className="mt-1.5 border-l-2 border-line pl-3 text-[14px] leading-snug text-ink">
            {approval.justification}
          </blockquote>
        </div>

        {approval.incident_id ? (
          <p className="text-[13px] text-ink-2">
            Incident{' '}
            <Link
              to="/activity"
              search={{ incident_id: approval.incident_id }}
              title={approval.incident_id}
              className="type-code text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
            >
              {shortId(approval.incident_id)}
            </Link>
          </p>
        ) : null}
      </div>

      {canDecide ? (
        <ApprovalDecision
          approval={approval}
          onDecided={onDecided}
          onConflict={onConflict}
        />
      ) : null}
    </article>
  );
}
