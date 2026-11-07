// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useState } from 'react';
import { getListGrantsQueryKey, revokeGrant } from '../api/generated/grants/grants';
import type { Grant } from '../api/generated/model';
import { grantSpan } from '../lib/activity';
import { describeSelector, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { expirySentence, grantKindWords, grantMeter, grantStatusWords, usesWords } from '../lib/grants';
import { problemView, type ProblemView } from '../lib/problem';
import { useNow } from '../lib/use-now';
import { cn } from '../lib/utils';
import { Avatar } from './avatar';
import { CopyButton } from './copy-button';
import { DitherMeter } from './dither-meter';
import { AgentName, UserName } from './identity-name';
import { RevokeGrantDialog } from './revoke-grant-dialog';
import { Button } from './ui/button';
import { Tag, type TagTone } from './ui/tag';

const STATUS_TONE: Record<Grant['status'], TagTone> = {
  active: 'run',
  expired: 'idle',
  revoked: 'fault',
  exhausted: 'idle',
};

const linkClass =
  'text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

/**
 * One grant: what it allows (scopes), where (selector), for how long (TTL and
 * a meter of the time left), how often (uses), why, and who lent the
 * authority. An active grant can be revoked from here.
 */
export function GrantCard({
  grant,
  canManage,
  onRevoke,
  showAgent = false,
}: {
  grant: Grant;
  canManage: boolean;
  onRevoke: (grant: Grant) => void;
  /** The cluster-wide list names the agent; an agent's own tab does not. */
  showAgent?: boolean;
}) {
  const now = useNow(grant.status === 'active', 30_000);
  const meter = grant.status === 'active' ? grantMeter(grant, now) : null;
  const span = grantSpan(grant);
  const status = grantStatusWords[grant.status];

  return (
    <article className={cn('box gcard', grant.status !== 'active' && 'gcard-past')}>
      <div className="gcard-h">
        {showAgent ? (
          <span className="gcard-agent">
            <Avatar kind="agent" seed={grant.agent_id} />
            <b>
              <AgentName id={grant.agent_id} />
            </b>
          </span>
        ) : null}
        <span className="gcard-kind">{grantKindWords[grant.kind]}</span>
        <Tag tone={STATUS_TONE[grant.status]}>{status}</Tag>
        {canManage && grant.status === 'active' ? (
          <Button variant="danger" size="sm" className="ml-auto" onClick={() => onRevoke(grant)}>
            Revoke
          </Button>
        ) : null}
      </div>

      {grant.scopes.length > 0 ? (
        <ul aria-label="Scopes" className="gcard-scopes">
          {grant.scopes.map((scope) => (
            <li key={scope}>
              <Tag tone="code">{scope}</Tag>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-[13px] text-ink-2">No scopes</p>
      )}

      <p className="gcard-facts">
        <span>{describeSelector(grant.selector)}</span>
        {span ? <span>For {span}</span> : null}
        {grant.status === 'active' || grant.status === 'expired' ? (
          <span title={grant.expires_at}>{expirySentence(grant.expires_at, now)}</span>
        ) : null}
        <span>{usesWords(grant)}</span>
      </p>

      {meter ? <DitherMeter label="Time left" words={meter.words} share={meter.share} /> : null}

      {grant.reason ? <p className="gcard-reason">{grant.reason}</p> : null}

      <p className="gcard-meta">
        <span>
          On behalf of <UserName id={grant.on_behalf_of} />
        </span>
        <span>
          Issued by{' '}
          {grant.created_by_type === 'system' ? 'Quoro, for an incident' : <UserName id={grant.created_by_id} />}{' '}
          <span title={grant.created_at}>{relativeTime(grant.created_at, now)}</span>
        </span>
        {grant.incident_id ? (
          <span>
            Incident{' '}
            <Link to="/activity" search={{ incident_id: grant.incident_id }} title={grant.incident_id} className={cn('type-code', linkClass)}>
              {shortId(grant.incident_id)}
            </Link>
          </span>
        ) : null}
        {grant.approval_id ? (
          <span>
            Approval{' '}
            <Link to="/approvals" search={{ approval: grant.approval_id }} title={grant.approval_id} className={cn('type-code', linkClass)}>
              {shortId(grant.approval_id)}
            </Link>
          </span>
        ) : null}
        {grant.revoked_at ? (
          <span>
            Revoked <span title={grant.revoked_at}>{relativeTime(grant.revoked_at, now)}</span>
            {grant.revoked_by ? (
              <>
                {' by '}
                <UserName id={grant.revoked_by} />
              </>
            ) : null}
          </span>
        ) : null}
        <span className="inline-flex items-center gap-1">
          <span className="type-code" title={grant.id}>
            {shortId(grant.id)}
          </span>
          <CopyButton value={grant.id} label="Copy grant id" />
        </span>
      </p>
    </article>
  );
}

/**
 * A list of grant cards with the revoke step: the confirm dialog, the call,
 * and a line saying what happened. A conflict (the grant already ended)
 * refreshes the list.
 */
export function GrantCards({
  grants,
  canManage,
  showAgent = false,
  onNotice,
}: {
  grants: readonly Grant[];
  canManage: boolean;
  showAgent?: boolean;
  onNotice: (words: string) => void;
}) {
  const queryClient = useQueryClient();
  const [target, setTarget] = useState<Grant | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const revoke = useMutation({
    mutationFn: (grant: Grant) => revokeGrant(grant.id),
    onSuccess: (grant) => {
      setProblem(null);
      setTarget(null);
      onNotice(`Revoked grant ${shortId(grant.id)}`);
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
    },
    onError: (error) => {
      setProblem(problemView(error, 'revoke grants'));
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
    },
  });

  return (
    <>
      <div className="gcards">
        {grants.map((grant) => (
          <GrantCard
            key={grant.id}
            grant={grant}
            canManage={canManage}
            showAgent={showAgent}
            onRevoke={(chosen) => {
              setProblem(null);
              setTarget(chosen);
            }}
          />
        ))}
      </div>
      <RevokeGrantDialog
        grant={target}
        open={target !== null}
        onOpenChange={(open) => {
          if (!open) setTarget(null);
        }}
        pending={revoke.isPending}
        problem={problem}
        onConfirm={() => {
          if (target) revoke.mutate(target);
        }}
      />
    </>
  );
}
