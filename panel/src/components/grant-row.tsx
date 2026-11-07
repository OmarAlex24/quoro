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
import { Avatar } from './avatar';
import { ToneLamp } from './tone-lamp';
import { Tag } from './ui/tag';

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
    <article className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[10px] border border-line bg-surface card-sheen">
      <div className="flex items-center justify-center border-r border-line bg-sunk">
        <ToneLamp tone={grantLamp(grant.status)} label={statusWords} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="flex min-w-0 flex-1 basis-[min(100%,320px)] flex-col gap-2">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <Avatar kind="agent" seed={grant.agent_id} />
            <span className="type-bay-title text-[18px] leading-tight text-ink">
              <AgentName id={grant.agent_id} />
            </span>
            <span className="text-[13px] text-ink-2">{grantKindWords[grant.kind]}</span>
            <span className="type-data text-[13px] text-ink-2">{statusWords}</span>
          </div>

          {grant.scopes.length > 0 ? (
            <ul aria-label="Scopes" className="flex flex-wrap gap-1.5">
              {grant.scopes.map((scope) => (
                <li key={scope}>
                  <Tag tone="code">{scope}</Tag>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-[13px] text-ink">No scopes</p>
          )}

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[13px] text-ink-2">
            <span>{describeSelector(grant.selector)}</span>
            {grant.status === 'active' || grant.status === 'expired' ? (
              <span className="type-data">{expirySentence(grant.expires_at)}</span>
            ) : null}
            <span className="type-data">{usesWords(grant)}</span>
            <span className="inline-flex items-center gap-1">
              <span className="type-code" title={grant.id}>
                {shortId(grant.id)}
              </span>
              <CopyButton value={grant.id} label="Copy id" />
            </span>
          </div>

          <p className="text-[14px] leading-snug text-ink">{grant.reason}</p>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[13px] text-ink-2">
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
                  to="/activity"
                  search={{ incident_id: grant.incident_id }}
                  title={grant.incident_id}
                  className="type-code text-ink underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
                >
                  {shortId(grant.incident_id)}
                </Link>
              </span>
            ) : null}
            {grant.approval_id ? (
              <span>
                Approval{' '}
                <span className="type-code text-ink" title={grant.approval_id}>
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
