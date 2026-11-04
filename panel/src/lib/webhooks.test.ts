import { describe, expect, it } from 'vitest';
import type { Webhook } from '../api/generated/model';
import {
  buildCreateWebhookBody,
  buildUpdateWebhookBody,
  deliveryAttemptWords,
  deliveryLamp,
  deliveryPollingInterval,
  describeEventTypes,
  eventTypesBySubject,
  formFromWebhook,
  isValidWebhookUrl,
  updateHasChanges,
  wakeSummary,
  type WebhookForm,
} from './webhooks';

const baseForm: WebhookForm = {
  name: '  Wake on-call  ',
  url: 'https://agents.example.com/hooks/quoro',
  event_types: ['app.down'],
  selector: { all: true },
  enabled: true,
};

const webhook: Webhook = {
  id: 'whk_01JC9Z6Q5W0000000000000000',
  name: 'Wake on-call',
  url: 'https://agents.example.com/hooks/quoro',
  event_types: ['app.down'],
  selector: { all: true },
  enabled: true,
  wake: {
    agent_id: 'agt_01JC9Z6Q5W0000000000000000',
    grant_scopes: ['app:read', 'app:restart'],
    grant_ttl_s: 3_600,
    grant_max_uses: null,
  },
  created_by: 'usr_01JC9Z6Q5W0000000000000000',
  created_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
};

describe('isValidWebhookUrl', () => {
  it('accepts HTTPS anywhere and plain HTTP only to localhost', () => {
    expect(isValidWebhookUrl('https://hooks.example.com/quoro')).toBe(true);
    expect(isValidWebhookUrl('http://localhost:8787/hook')).toBe(true);
    expect(isValidWebhookUrl('http://localhost')).toBe(true);
  });

  it('refuses plain HTTP elsewhere, other schemes and text that is not a URL', () => {
    expect(isValidWebhookUrl('http://hooks.example.com/quoro')).toBe(false);
    expect(isValidWebhookUrl('ftp://hooks.example.com/quoro')).toBe(false);
    expect(isValidWebhookUrl('hooks.example.com')).toBe(false);
    expect(isValidWebhookUrl('https://hooks example.com')).toBe(false);
    expect(isValidWebhookUrl('')).toBe(false);
  });
});

describe('eventTypesBySubject', () => {
  it('groups every event type under its subject, in contract order', () => {
    const groups = eventTypesBySubject();
    expect(groups.map((group) => group.subject)).toEqual([
      'node',
      'vm',
      'app',
      'deployment',
      'certificate',
      'grant',
      'approval',
      'incident',
      'audit',
    ]);
    expect(groups.find((group) => group.subject === 'app')?.types).toEqual([
      'app.created',
      'app.updated',
      'app.deleted',
      'app.down',
      'app.degraded',
      'app.up',
      'app.restarted',
      'app.vulnerable',
    ]);
  });
});

describe('describeEventTypes', () => {
  it('says every event for an empty list', () => {
    expect(describeEventTypes([])).toBe('Every event');
  });

  it('lists a short filter and counts a long one', () => {
    expect(describeEventTypes(['app.down', 'incident.opened'])).toBe(
      'app.down, incident.opened',
    );
    expect(
      describeEventTypes(['app.down', 'app.up', 'app.restarted', 'incident.opened']),
    ).toBe('4 event types');
  });
});

describe('deliveryLamp', () => {
  it('maps each delivery status to its lamp and word', () => {
    expect(deliveryLamp('delivered')).toMatchObject({ tone: 'run', word: 'Delivered' });
    expect(deliveryLamp('pending')).toMatchObject({
      tone: 'shift',
      pulse: true,
      word: 'Pending',
    });
    expect(deliveryLamp('failed')).toMatchObject({ tone: 'fault', word: 'Failed' });
  });

  it('polls every 2 s while anything is pending', () => {
    expect(deliveryPollingInterval([{ status: 'delivered' }])).toBe(15_000);
    expect(deliveryPollingInterval([{ status: 'delivered' }, { status: 'pending' }])).toBe(
      2_000,
    );
  });
});

describe('wakeSummary', () => {
  it('says notifies only when there is no wake', () => {
    expect(wakeSummary(undefined, 'on-call')).toBe('Notifies only');
  });

  it('names the agent, its scopes and the grant lifetime', () => {
    expect(
      wakeSummary(
        {
          agent_id: webhook.wake?.agent_id ?? 'agt_x',
          grant_scopes: ['app:read', 'app:logs'],
          grant_ttl_s: 3_600,
        },
        'on-call-restarter',
      ),
    ).toBe('Wakes on-call-restarter with app:read, app:logs for 1 h');
  });
});

describe('buildCreateWebhookBody', () => {
  it('trims name and URL and keeps an empty event list as every event', () => {
    const body = buildCreateWebhookBody({ ...baseForm, event_types: [] });
    expect(body).toEqual({
      name: 'Wake on-call',
      url: 'https://agents.example.com/hooks/quoro',
      event_types: [],
      selector: { all: true },
      enabled: true,
    });
    expect(body.wake).toBeUndefined();
  });

  it('fills the wake defaults when the form wakes an agent', () => {
    const body = buildCreateWebhookBody({
      ...baseForm,
      wake: { agent_id: webhook.wake?.agent_id ?? 'agt_x', grant_scopes: ['app:read'] },
    });
    expect(body.wake).toEqual({
      agent_id: webhook.wake?.agent_id ?? 'agt_x',
      grant_scopes: ['app:read'],
      grant_ttl_s: 3_600,
      grant_max_uses: null,
    });
  });
});

describe('buildUpdateWebhookBody', () => {
  it('sends only fields that changed', () => {
    const form = formFromWebhook(webhook);
    expect(buildUpdateWebhookBody(webhook, form)).toEqual({});
    expect(updateHasChanges(buildUpdateWebhookBody(webhook, form))).toBe(false);

    const renamed = { ...form, name: 'Wake the on-call agent' };
    expect(buildUpdateWebhookBody(webhook, renamed)).toEqual({
      name: 'Wake the on-call agent',
    });
  });

  it('leaves the wake block out when the form is not waking an agent', () => {
    const form = formFromWebhook(webhook);
    const { wake: _wake, ...withoutWake } = form;
    expect(buildUpdateWebhookBody(webhook, withoutWake).wake).toBeUndefined();
  });

  it('sends a changed wake block', () => {
    const form = {
      ...formFromWebhook(webhook),
      wake: { ...webhook.wake!, grant_ttl_s: 14_400 },
    };
    expect(buildUpdateWebhookBody(webhook, form).wake).toMatchObject({
      grant_ttl_s: 14_400,
    });
  });
});

describe('deliveryAttemptWords', () => {
  it('numbers the attempts of a real delivery', () => {
    expect(deliveryAttemptWords({ attempt: 1, test: false })).toBe('Attempt 1');
    expect(deliveryAttemptWords({ attempt: 3, test: false })).toBe('Attempt 3');
  });

  it('names a test delivery instead of numbering it', () => {
    expect(deliveryAttemptWords({ attempt: 1, test: true })).toBe('Test delivery');
  });
});
