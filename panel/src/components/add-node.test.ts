import { MutationObserver, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import type { NodeJoinTokenCreated } from '../api/generated/model';
import { RecordingClient, serveMockApi } from '../mocks/node-server';
import { AddNodeDialog, JoinCommand } from './add-node';
import { Button } from './ui/button';

const { requests } = serveMockApi();

const render = (trigger: ReactElement, client: QueryClient = new QueryClient()): string =>
  renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(AddNodeDialog, { trigger })));

/** What a reader sees: the markup without its tags. */
const text = (html: string): string =>
  html
    .replace(/<[^>]*>/g, '')
    .replace(/&#x27;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');

const TOKEN: NodeJoinTokenCreated = {
  id: 'njt_01JCA0A1B2C3D4E5F6G7H8J9KM',
  token: 'njt_secret',
  expires_at: new Date(Date.now() + 3_600_000).toISOString(),
  join_command: 'quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c',
};

const INSTALL =
  'curl -fsSL https://raw.githubusercontent.com/OmarAlex24/quoro/main/installer/install.sh | sudo QUORO_ROLE=node';

describe('AddNodeDialog', () => {
  // Radix returns focus on close to the dialog's own trigger; a control that only set `open` lost it.
  it('makes the control that opens it the dialog trigger, so focus returns to it on close', () => {
    const html = render(createElement(Button, null, 'Add a node'));
    const button = html.match(/<button[^>]*>/)?.[0] ?? '';
    expect(button).toContain('aria-haspopup="dialog"');
    expect(button).toContain('aria-expanded="false"');
    expect(button).toContain('data-state="closed"');
    expect(html).toContain('Add a node</button>');
  });

  it('keeps the invitation its own button, wired the same way', () => {
    const html = render(createElement('button', { type: 'button', className: 'pghost ninvite' }, 'Add a node'));
    const button = html.match(/<button[^>]*>/)?.[0] ?? '';
    expect(button).toContain('class="pghost ninvite"');
    expect(button).toContain('aria-haspopup="dialog"');
    expect(html.match(/<button/g)).toHaveLength(1);
  });

  it('gives the installer command to run as root, at the cluster\'s version, with the agent it starts', () => {
    const html = renderToStaticMarkup(
      createElement(JoinCommand, { token: TOKEN, version: '0.1.0', onDone: () => undefined }),
    );
    const shown = text(html);
    expect(shown).toContain('Join command for the new machine');
    // The join command is the server's, unchanged.
    expect(shown).toContain('quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c');
    expect(shown).toContain(
      'On the new machine, run the installer as root, with the URL, token and fingerprint from this command:',
    );
    expect(shown).toContain(
      `${INSTALL} QUORO_VERSION=0.1.0 QUORO_JOIN_URL=<url> QUORO_JOIN_TOKEN=<token> QUORO_JOIN_FINGERPRINT=<fingerprint> sh`,
    );
    expect(shown).toContain(
      'It joins the machine and starts its node agent, quoro-node.service. QUORO_VERSION keeps the node at the cluster’s version, 0.1.0. Without it, the installer installs the latest release.',
    );
    expect(html).toContain('aria-label="Copy installer command"');
    expect(html).toContain('Done</button>');
  });

  it('keeps the token out of the installer command, which is not secret', () => {
    const html = renderToStaticMarkup(
      createElement(JoinCommand, { token: TOKEN, version: '0.1.0', onDone: () => undefined }),
    );
    // In the join command's well only, never in the installer's.
    expect(text(html).match(/njt_secret/g)).toHaveLength(1);
    const wells = html.match(/<code[^>]*>[\s\S]*?<\/code>/g) ?? [];
    const installer = wells.filter((well) => well.includes('install.sh'));
    expect(installer).toHaveLength(1);
    expect(installer[0]).not.toContain('njt_secret');
  });

  it('asks for the cluster\'s version while it is unknown', () => {
    const shown = text(renderToStaticMarkup(createElement(JoinCommand, { token: TOKEN, onDone: () => undefined })));
    expect(shown).toContain(`${INSTALL} QUORO_VERSION=<version> QUORO_JOIN_URL=<url>`);
    expect(shown).toContain(
      'Set QUORO_VERSION to the cluster’s version. Without it, the installer installs the latest release.',
    );
  });

  // The options the dialog gives `useMutation`, from a server render, run against the mock API.
  it('asks for a new join token with a fresh Idempotency-Key on every click', async () => {
    const client = new RecordingClient();
    render(createElement(Button, null, 'Add a node'), client);
    expect(client.mutations).toHaveLength(1);
    const observer = new MutationObserver(client, client.mutations[0]!);
    await observer.mutate(undefined);
    await observer.mutate(undefined);

    const keys = requests
      .filter((request) => request.method === 'POST' && request.path === '/api/v1/nodes/join-tokens')
      .map((request) => request.headers.get('Idempotency-Key'));
    expect(keys).toHaveLength(2);
    for (const key of keys) expect(key).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(keys[0]).not.toBe(keys[1]);
  });
});
