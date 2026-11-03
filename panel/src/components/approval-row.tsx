import { Link } from '@tanstack/react-router';
import type { Approval } from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { approvalLamp, approvalWords, decidedAt } from '../lib/approvals';
import { AgentName, UserName } from './identity-name';
import { ToneLamp } from './tone-lamp';

/** One decided request: a compact row, newest first in the list above it. */
export function ApprovalRow({ approval }: { approval: Approval }) {
  const words = approvalWords[approval.status];
  return (
    <li className="py-3">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <span className="inline-flex shrink-0 items-center gap-1.5 text-[13px] text-ink">
          <ToneLamp tone={approvalLamp(approval.status)} size={13} label={words} />
          {words}
        </span>
        <span className="min-w-0 text-[14px] text-ink">
          <AgentName id={approval.agent_id} />
        </span>
        <span className="type-data text-[13px] text-ink">{approval.scope}</span>
        <span className="text-[13px] text-ink-muted">
          on {approval.target.type}{' '}
          <span className="type-data text-ink" title={approval.target.id}>
            {shortId(approval.target.id)}
          </span>
        </span>
        {approval.decided_by ? (
          <span className="text-[13px] text-ink-muted">
            by <UserName id={approval.decided_by} />{' '}
            <span title={decidedAt(approval)}>{relativeTime(decidedAt(approval))}</span>
          </span>
        ) : (
          <span className="text-[13px] text-ink-muted" title={decidedAt(approval)}>
            {relativeTime(decidedAt(approval))}
          </span>
        )}
        {approval.status === 'approved' && approval.grant_id ? (
          <Link
            to="/grants"
            search={{ agent_id: approval.agent_id, kind: 'approval' }}
            title={approval.grant_id}
            className="type-data text-[13px] text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
          >
            Grant {shortId(approval.grant_id)}
          </Link>
        ) : null}
      </div>
      {approval.decision_note ? (
        <p className="mt-1 border-l-2 border-rail pl-3 text-[13px] leading-snug text-ink-muted">
          {approval.decision_note}
        </p>
      ) : null}
    </li>
  );
}
