import { Link } from '@tanstack/react-router';
import type { Approval } from '../api/generated/model';
import { approvalAsk } from '../lib/access';
import { Avatar } from './avatar';
import { AgentName } from './identity-name';
import { Button } from './ui/button';

/**
 * A request that waits on a person, inside the project tile it acts in:
 * which agent and what it wants, and the way to it. Its expiry and both
 * decisions are in the board's "Needs you". Only `listApprovals` fields; the
 * target's name comes from the lists the page already holds.
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
      <p className="callout-t">
        <b>
          <AgentName id={approval.agent_id} link={false} />
        </b>{' '}
        {ask.wants} <b>{ask.target}</b>
      </p>
      <Button size="sm" asChild>
        <Link to="/approvals" search={{ approval: approval.id }}>
          Review
        </Link>
      </Button>
    </div>
  );
}
