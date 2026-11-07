import { Link } from '@tanstack/react-router';
import { Send } from 'lucide-react';
import type { ReactNode } from 'react';
import type { Webhook } from '../api/generated/model';
import { useListWebhookDeliveries } from '../api/generated/webhooks/webhooks';
import { cn } from '../lib/utils';
import { lastDeliveryLine } from '../lib/webhooks';
import { Avatar } from './avatar';
import { AgentName } from './identity-name';
import { ToneLamp } from './tone-lamp';
import { Tag } from './ui/tag';

/**
 * One webhook in the list: its name (Paused when it is), the receiver URL, and
 * one line with the latest delivery and the agent it wakes. `actions` is the
 * ⋯ menu, for a role that can change webhooks.
 */
export function WebhookRow({ webhook, actions }: { webhook: Webhook; actions?: ReactNode }) {
  const deliveries = useListWebhookDeliveries(webhook.id, { limit: 1 }, { query: { staleTime: 5_000 } });
  const line = lastDeliveryLine(deliveries.data?.items[0]);

  return (
    <li className="srow">
      <span className="glyph glyph-neutral" aria-hidden="true">
        <Send strokeWidth={1.7} />
      </span>
      <div className="srow-t">
        <b>
          <Link
            to="/settings/webhooks/$webhookId"
            params={{ webhookId: webhook.id }}
            className="srow-link truncate"
          >
            {webhook.name}
          </Link>
          {webhook.enabled ? null : <Tag tone="plain">Paused</Tag>}
        </b>
        <span className="srow-code type-code" title={webhook.url}>
          {webhook.url}
        </span>
        <small>
          {deliveries.isPending ? (
            <span>Reading deliveries…</span>
          ) : (
            <span className={cn('inline-flex items-center gap-1.5', line.fault && 'text-fault')}>
              <ToneLamp tone={line.tone} pulse={line.pulse} size={11} label={line.text} />
              <span aria-hidden="true">{line.text}</span>
            </span>
          )}
          {webhook.wake ? (
            <>
              <span className="dot-sep" aria-hidden="true" />
              <span className="inline-flex items-center gap-1.5">
                wakes
                <Avatar kind="agent" seed={webhook.wake.agent_id} />
                <AgentName id={webhook.wake.agent_id} />
              </span>
            </>
          ) : null}
        </small>
      </div>
      {actions}
    </li>
  );
}
