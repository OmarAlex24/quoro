import { Link } from '@tanstack/react-router';
import type { Grant } from '../api/generated/model';
import { describeSelector, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import {
  expirySentence,
  grantKindWords,
  grantLamp,
  grantStatusWords,
  usesWords,
} from '../lib/grants';
import { Button } from './ui/button';
import { CopyButton } from './copy-button';
import { AgentName, UserName } from './identity-name';
import { ToneLamp } from './tone-lamp';

/**
 * One grant as a rack bay: the lamp burns while the grant is active and settles
 * when it is spent, expired or revoked. The plate says what it allows, where,
 * until when and who lent the authority.
 */
export function GrantRow({
  grant,
  canManage,
  onRevoke,
}: {
  grant: Grant;
  canManage: boolean;
  onRevoke: (grant: Grant) => void;
}) {
  const statusWords = grantStatusWords[grant.status];

  return (
    <article className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[6px] border border-rail bg-panel bay-shadow">
      <div className="flex items-center justify-center border-r border-rail bg-recess">
        <ToneLamp tone={grantLamp(grant.status)} label={statusWords} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="flex min-w-0 flex-1 basis-[min(100%,320px)] flex-col gap-2">
          <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <span className="type-bay-title text-[18px] leading-tight text-ink">
              <AgentName id={grant.agent_id} />
            </span>
            <span className="text-[13px] text-ink-muted">{grantKindWords[grant.kind]}</span>
            <span className="type-data text-[13px] text-ink-muted">{statusWords}</span>
          </div>

          <p className="type-data break-words text-[13px] text-ink">
            {grant.scopes.length > 0 ? grant.scopes.join(', ') : 'No scopes'}
          </p>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[13px] text-ink-muted">
            <span>{describeSelector(grant.selector)}</span>
            {grant.status === 'active' || grant.status === 'expired' ? (
              <span className="type-data">{expirySentence(grant.expires_at)}</span>
            ) : null}
            <span className="type-data">{usesWords(grant)}</span>
            <span className="inline-flex items-center gap-1">
              <span className="type-data" title={grant.id}>
                {shortId(grant.id)}
              </span>
              <CopyButton value={grant.id} label="Copy id" />
            </span>
          </div>

          <p className="text-[14px] leading-snug text-ink">{grant.reason}</p>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[13px] text-ink-muted">
            <span>
              On behalf of <UserName id={grant.on_behalf_of} />
            </span>
            <span>
              Created by{' '}
              {grant.created_by_type === 'system' ? (
                'Quoro, for an incident'
              ) : (
                <UserName id={grant.created_by_id} />
              )}
            </span>
            {grant.incident_id ? (
              <span>
                Incident{' '}
                <Link
                  to="/audit"
                  search={{ incident_id: grant.incident_id }}
                  title={grant.incident_id}
                  className="type-data text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cobalt"
                >
                  {shortId(grant.incident_id)}
                </Link>
              </span>
            ) : null}
            {grant.approval_id ? (
              <span>
                Approval{' '}
                <span className="type-data text-ink" title={grant.approval_id}>
                  {shortId(grant.approval_id)}
                </span>
              </span>
            ) : null}
            {grant.revoked_at ? (
              <span>
                Revoked {relativeTime(grant.revoked_at)}
                {grant.revoked_by ? (
                  <>
                    {' by '}
                    <UserName id={grant.revoked_by} />
                  </>
                ) : null}
              </span>
            ) : null}
          </div>
        </div>

        {canManage && grant.status === 'active' ? (
          <Button variant="danger" className="ml-auto" onClick={() => onRevoke(grant)}>
            Revoke grant
          </Button>
        ) : null}
      </div>
    </article>
  );
}
