import type { ReactNode } from 'react';
import type { Webhook } from '../api/generated/model';
import { describeSelector, formatDuration, shortId } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { Avatar } from './avatar';
import { AgentName, UserName } from './identity-name';
import { CopyButton } from './copy-button';
import { ToneLamp } from './tone-lamp';

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="set-row">
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

/** Every field of one webhook, as the read-only box above its deliveries. */
export function WebhookPlate({ webhook }: { webhook: Webhook }) {
  const wake = webhook.wake;
  const scopes = wake?.grant_scopes ?? [];

  return (
    <dl className="box set-rows">
      <Row label="Deliveries">
        <span className="inline-flex items-center gap-2">
          <ToneLamp
            tone={webhook.enabled ? 'run' : 'idle'}
            size={11}
            label={webhook.enabled ? 'Delivering' : 'Paused'}
          />
          {webhook.enabled
            ? 'Attempted for every matching event'
            : 'Paused: no deliveries are attempted'}
        </span>
      </Row>

      <Row label="Events">
        {webhook.event_types.length === 0 ? (
          'Every event'
        ) : (
          <span className="type-code">{webhook.event_types.join(', ')}</span>
        )}
      </Row>

      <Row label="Resources">{describeSelector(webhook.selector)}</Row>

      <Row label="Wake">
        {wake ? (
          <span className="grid gap-1">
            <span className="inline-flex flex-wrap items-center gap-1.5">
              Wakes
              <Avatar kind="agent" seed={wake.agent_id} className="set-av" />
              <AgentName id={wake.agent_id} />
            </span>
            <span className="text-ink-2">
              {scopes.length > 0 ? (
                <>
                  With <span className="type-code">{scopes.join(', ')}</span>
                </>
              ) : (
                'With no scopes'
              )}
              {', for '}
              {formatDuration(wake.grant_ttl_s ?? 3_600)}
              {wake.grant_max_uses ? `, at most ${wake.grant_max_uses} uses` : ', no use limit'}
            </span>
          </span>
        ) : (
          <span className="text-ink-2">Notifies only</span>
        )}
      </Row>

      <Row label="Created by">
        <span className="grid gap-1">
          <UserName id={webhook.created_by} />
          <span className="text-ink-2">Incident grants act on this person&rsquo;s behalf.</span>
        </span>
      </Row>

      <Row label="Created">
        <span title={webhook.created_at}>{relativeTime(webhook.created_at)}</span>
      </Row>

      <Row label="Updated">
        <span title={webhook.updated_at}>{relativeTime(webhook.updated_at)}</span>
      </Row>

      <Row label="Webhook id">
        <span className="inline-flex flex-wrap items-center gap-1">
          <span className="type-code truncate" title={webhook.id}>
            {shortId(webhook.id)}
          </span>
          <CopyButton value={webhook.id} label="Copy id" />
        </span>
      </Row>
    </dl>
  );
}
