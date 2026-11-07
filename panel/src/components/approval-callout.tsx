import { Link } from '@tanstack/react-router';
import type { Approval } from '../api/generated/model';
import { approvalAsk, approvalExpiry } from '../lib/access';
import { Avatar } from './avatar';
import { AgentName } from './identity-name';
import { Button } from './ui/button';

/**
 * A request that waits on a person, at the top of the page it concerns:
 * which agent, what it wants, how long it has, and the way to decide.
 * Only `listApprovals` fields; the target's name comes from the lists the
 * page already holds.
 */
export function ApprovalCallout({
  approval,
  targetName,
}: {
  approval: Approval;
  targetName?: string | null;
}) {
  const ask = approvalAsk(approval, targetName);
  return (
    <div className="callout">
      <Avatar kind="agent" seed={approval.agent_id} />
      <p className="min-w-0 flex-1">
        <b className="font-[620]">
          <AgentName id={approval.agent_id} link={false} />
        </b>{' '}
        {ask.wants} <b className="font-[620]">{ask.target}</b>. {approvalExpiry(approval.expires_at)}
      </p>
      <Button size="sm" asChild>
        <Link to="/approvals">Review</Link>
      </Button>
    </div>
  );
}
