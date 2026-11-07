import type { ReactNode } from 'react';
import type { Webhook } from '../api/generated/model';
import { describeSelector, formatDuration, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { AgentName, UserName } from './identity-name';
import { CopyButton } from './copy-button';
import { ToneLamp } from './tone-lamp';

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="grid grid-cols-1 gap-1 border-t border-line py-2.5 first:border-t-0 sm:grid-cols-[160px_minmax(0,1fr)] sm:gap-4">
      <dt className="type-body-medium text-[13px] text-ink-2">{label}</dt>
      <dd className="min-w-0 text-[14px] text-ink">{children}</dd>
    </div>
  );
}

/** Every field of one webhook, as the read-only plate above its deliveries. */
export function WebhookPlate({ webhook }: { webhook: Webhook }) {
  const wake = webhook.wake;

  return (
    <dl className="rounded-[10px] border border-line bg-surface px-4 pb-1 pt-0.5 card-sheen">
      <Row label="Name">{webhook.name}</Row>

      <Row label="Receiver URL">
        <span className="type-code block break-all" title={webhook.url}>
          {webhook.url}
        </span>
      </Row>

      <Row label="Deliveries">
        <span className="inline-flex items-center gap-2">
          <ToneLamp
            tone={webhook.enabled ? 'run' : 'idle'}
            size={16}
            label={webhook.enabled ? 'Delivering' : 'Paused'}
          />
          <span>{webhook.enabled ? 'Delivering' : 'Paused'}</span>
        </span>
      </Row>

      <Row label="Events">
        {webhook.event_types.length === 0 ? (
          <span>Every event</span>
        ) : (
          <span className="type-code">{webhook.event_types.join(', ')}</span>
        )}
      </Row>

      <Row label="Resources">{describeSelector(webhook.selector)}</Row>

      <Row label="Wake">
        {wake ? (
          <span className="flex flex-col gap-1">
            <span>
              Wakes <AgentName id={wake.agent_id} />
            </span>
            <span className="type-data text-[13px] text-ink-2">
              Scopes:{' '}
              {(wake.grant_scopes ?? []).length > 0 ? (
                <span className="type-code">{(wake.grant_scopes ?? []).join(', ')}</span>
              ) : (
                'none'
              )}
            </span>
            <span className="text-[13px] text-ink-2">
              Grant lasts {formatDuration(wake.grant_ttl_s ?? 3_600)}
              {wake.grant_max_uses
                ? `, at most ${wake.grant_max_uses} uses`
                : ', no use limit'}
            </span>
          </span>
        ) : (
          <span className="text-ink-2">Notifies only</span>
        )}
      </Row>

      <Row label="Created by">
        <span className="flex flex-col gap-1">
          <UserName id={webhook.created_by} />
          <span className="text-[13px] text-ink-2">
            Incident grants act on this person&rsquo;s behalf.
          </span>
        </span>
      </Row>

      <Row label="Created">
        <span className="type-data" title={webhook.created_at}>
          {relativeTime(webhook.created_at)}
        </span>
      </Row>

      <Row label="Updated">
        <span className="type-data" title={webhook.updated_at}>
          {relativeTime(webhook.updated_at)}
        </span>
      </Row>

      <Row label="Webhook id">
        <span className="flex flex-wrap items-center gap-1">
          <span className="type-code truncate" title={webhook.id}>
            {shortId(webhook.id)}
          </span>
          <CopyButton value={webhook.id} label="Copy id" />
        </span>
      </Row>
    </dl>
  );
}
