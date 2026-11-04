import { http, type HttpHandler } from 'msw';
import { EventType } from '../api/generated/model';
import type {
  CreateWebhookRequest,
  UpdateWebhookRequest,
  Webhook,
  WebhookCreated,
  WebhookDelivery,
  WebhookDeliveryList,
  WebhookList,
} from '../api/generated/model';
import { isGrantableScope } from '../lib/scopes';
import { isValidWebhookUrl } from '../lib/webhooks';
import { agency, appendAudit, type AgencyState } from './agency';
import { mockId, now, schedule } from './db';
import {
  isIntAtLeast,
  json,
  noContent,
  notFound,
  page,
  readJson,
  refuseUnlessWriter,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

const KNOWN_EVENT_TYPES = new Set<string>(Object.values(EventType));

/** A 43-character base64url string, the shape of a signing secret. */
const signingSecret = (): string => {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

/**
 * Validate a create or patch body against the contract's rules. `requireName`
 * is true on create, where name and URL are mandatory.
 */
const validateWebhookBody = (
  body: CreateWebhookRequest | UpdateWebhookRequest,
  state: AgencyState,
  requireName: boolean,
): string | null => {
  if (body.name !== undefined) {
    const name = body.name.trim();
    if (name.length < 1 || name.length > 200) {
      return 'Name must be 1-200 characters.';
    }
  } else if (requireName) {
    return 'Name is required.';
  }

  if (body.url !== undefined) {
    if (!isValidWebhookUrl(body.url)) {
      return 'Receiver URL must start with https:// (http://localhost is allowed for testing).';
    }
  } else if (requireName) {
    return 'Receiver URL is required.';
  }

  if (body.event_types !== undefined) {
    if (!Array.isArray(body.event_types)) {
      return 'Event types must be a list.';
    }
    for (const type of body.event_types) {
      if (!KNOWN_EVENT_TYPES.has(type)) {
        return `"${type}" is not a known event type.`;
      }
    }
  }

  if (body.selector !== undefined && (typeof body.selector !== 'object' || body.selector === null)) {
    return 'The resource selector could not be read.';
  }

  if (body.wake) {
    const wake = body.wake;
    if (!state.agents.some((agent) => agent.id === wake.agent_id)) {
      return `There is no agent ${wake.agent_id}. Choose an agent that exists.`;
    }
    for (const scope of wake.grant_scopes ?? []) {
      if (!isGrantableScope(scope)) {
        return `"${scope}" cannot be granted to an agent: it is destructive or human-only.`;
      }
    }
    if (wake.grant_ttl_s !== undefined && !isIntAtLeast(wake.grant_ttl_s, 1)) {
      return 'Grant lifetime must be a whole number of at least 1 second.';
    }
    if (
      wake.grant_max_uses !== undefined &&
      wake.grant_max_uses !== null &&
      !isIntAtLeast(wake.grant_max_uses, 1)
    ) {
      return 'Maximum uses must be a whole number of at least 1, or empty for no limit.';
    }
  }

  return null;
};

/** One webhook, newest first: the list the panel paints as bays. */
const newestFirst = (webhooks: Webhook[]): Webhook[] =>
  [...webhooks].sort((left, right) => right.created_at.localeCompare(left.created_at));

const newestDeliveriesFirst = (deliveries: WebhookDelivery[]): WebhookDelivery[] =>
  [...deliveries].sort((left, right) => right.created_at.localeCompare(left.created_at));

/** Stateful mock handlers for the webhooks screens. Mock mode only. */
export const webhooksHandlers: HttpHandler[] = [
  http.get('*/api/v1/webhooks', ({ request }) => {
    if (!signedIn()) return unauthorized();
    const list: WebhookList = page(newestFirst(agency().webhooks), request);
    return json(list);
  }),

  http.post('*/api/v1/webhooks', async ({ request }) => {
    const refusal = refuseUnlessWriter('webhook:write');
    if (refusal) return refusal;
    const state = agency();
    const user = signedIn();
    if (!user) return unauthorized();

    const body = await readJson<CreateWebhookRequest>(request);
    if (!body) {
      return validationFailed('The form could not be read. Try again.');
    }
    const invalid = validateWebhookBody(body, state, true);
    if (invalid) return validationFailed(invalid);

    const createdAt = now();
    const webhook: Webhook = {
      id: mockId('whk'),
      name: body.name.trim(),
      url: body.url.trim(),
      event_types: [...(body.event_types ?? [])],
      selector: body.selector ?? { all: true },
      enabled: body.enabled ?? true,
      ...(body.wake ? { wake: body.wake } : {}),
      created_by: user.id,
      created_at: createdAt,
      updated_at: createdAt,
    };
    // No secret is kept: it is returned once below and never stored again.
    state.webhooks.push(webhook);

    const secret = signingSecret();
    const created: WebhookCreated = { ...webhook, secret };

    appendAudit({
      operation: 'POST /api/v1/webhooks',
      scope: 'webhook:write',
      target_type: 'webhook',
      target_id: webhook.id,
      params: {
        name: webhook.name,
        url: webhook.url,
        event_types: webhook.event_types,
        selector: webhook.selector,
        enabled: webhook.enabled,
        ...(webhook.wake ? { wake: webhook.wake } : {}),
        secret: '[redacted]',
      },
    });

    return json(created, { status: 201 });
  }),

  http.get('*/api/v1/webhooks/:webhookId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const webhook = agency().webhooks.find(
      (candidate) => candidate.id === params.webhookId,
    );
    if (!webhook) return notFound('That webhook');
    return json(webhook);
  }),

  http.patch('*/api/v1/webhooks/:webhookId', async ({ request, params }) => {
    const refusal = refuseUnlessWriter('webhook:write');
    if (refusal) return refusal;
    const state = agency();

    const webhook = state.webhooks.find((candidate) => candidate.id === params.webhookId);
    if (!webhook) return notFound('That webhook');

    const body = await readJson<UpdateWebhookRequest>(request);
    if (!body) {
      return validationFailed('The form could not be read. Try again.');
    }
    const invalid = validateWebhookBody(body, state, false);
    if (invalid) return validationFailed(invalid);

    if (body.name !== undefined) webhook.name = body.name.trim();
    if (body.url !== undefined) webhook.url = body.url.trim();
    if (body.event_types !== undefined) webhook.event_types = [...body.event_types];
    if (body.selector !== undefined) webhook.selector = body.selector;
    if (body.enabled !== undefined) webhook.enabled = body.enabled;
    if (body.wake !== undefined) webhook.wake = body.wake;
    webhook.updated_at = now();

    appendAudit({
      operation: 'PATCH /api/v1/webhooks/{webhook_id}',
      scope: 'webhook:write',
      target_type: 'webhook',
      target_id: webhook.id,
      params: {
        ...(body.name !== undefined ? { name: webhook.name } : {}),
        ...(body.url !== undefined ? { url: webhook.url } : {}),
        ...(body.event_types !== undefined ? { event_types: webhook.event_types } : {}),
        ...(body.selector !== undefined ? { selector: webhook.selector } : {}),
        ...(body.enabled !== undefined ? { enabled: webhook.enabled } : {}),
        ...(body.wake !== undefined ? { wake: webhook.wake } : {}),
      },
    });

    return json(webhook);
  }),

  http.delete('*/api/v1/webhooks/:webhookId', ({ params }) => {
    const refusal = refuseUnlessWriter('webhook:write');
    if (refusal) return refusal;
    const state = agency();

    const index = state.webhooks.findIndex(
      (candidate) => candidate.id === params.webhookId,
    );
    if (index === -1) return notFound('That webhook');
    const [webhook] = state.webhooks.splice(index, 1);
    if (!webhook) return notFound('That webhook');

    // Deliveries already attempted stay in the history; they are kept here.
    appendAudit({
      operation: 'DELETE /api/v1/webhooks/{webhook_id}',
      scope: 'webhook:write',
      target_type: 'webhook',
      target_id: webhook.id,
      params: { name: webhook.name, url: webhook.url },
    });

    return noContent();
  }),

  http.post('*/api/v1/webhooks/:webhookId/test', ({ params }) => {
    const refusal = refuseUnlessWriter('webhook:write');
    if (refusal) return refusal;
    const state = agency();

    const webhook = state.webhooks.find((candidate) => candidate.id === params.webhookId);
    if (!webhook) return notFound('That webhook');

    const newestEvent = state.events[state.events.length - 1];
    const delivery: WebhookDelivery = {
      id: mockId('whd'),
      webhook_id: webhook.id,
      event_id: newestEvent?.id ?? mockId('evt'),
      attempt: 1,
      status: 'pending',
      response_code: null,
      error: null,
      next_attempt_at: null,
      test: true,
      created_at: now(),
    };
    state.deliveries.push(delivery);

    appendAudit({
      operation: 'POST /api/v1/webhooks/{webhook_id}/test',
      scope: 'webhook:write',
      target_type: 'webhook',
      target_id: webhook.id,
      params: { webhook_id: webhook.id, delivery_id: delivery.id, url: webhook.url },
    });

    // The receiver answers a moment later. A URL containing "fail" fails, so
    // the retry path can be seen in the panel.
    schedule(() => {
      const current = state.deliveries.find((candidate) => candidate.id === delivery.id);
      if (!current || current.status !== 'pending') return;
      if (webhook.url.includes('fail')) {
        current.status = 'failed';
        current.response_code = 500;
        current.error = 'receiver answered 500 Internal Server Error';
        current.next_attempt_at = new Date(Date.now() + 10_000).toISOString();
      } else {
        current.status = 'delivered';
        current.response_code = 200;
      }
    }, 1_200);

    return json(delivery, { status: 202 });
  }),

  http.get('*/api/v1/webhooks/:webhookId/deliveries', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const state = agency();
    const webhook = state.webhooks.find((candidate) => candidate.id === params.webhookId);
    if (!webhook) return notFound('That webhook');

    const items = newestDeliveriesFirst(
      state.deliveries.filter((delivery) => delivery.webhook_id === webhook.id),
    );
    const list: WebhookDeliveryList = page(items, request);
    return json(list);
  }),
];
