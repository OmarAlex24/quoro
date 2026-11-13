import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import type { NodeJoinTokenCreated } from '../api/generated/model';
import { AddNodeDialog, JoinCommand } from './add-node';
import { Button } from './ui/button';

const render = (trigger: ReactElement): string =>
  renderToStaticMarkup(
    createElement(QueryClientProvider, { client: new QueryClient() }, createElement(AddNodeDialog, { trigger })),
  );

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

  it('says how the installer takes the join command, and that it starts the agent', () => {
    const token: NodeJoinTokenCreated = {
      id: 'njt_01JCA0A1B2C3D4E5F6G7H8J9KM',
      token: 'njt_secret',
      expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      join_command: 'quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c',
    };
    const html = renderToStaticMarkup(createElement(JoinCommand, { token, onDone: () => undefined }));
    const text = html.replace(/<[^>]*>/g, '').replace(/&#x27;/g, "'");
    expect(text).toContain('Join command for the new machine');
    // The command is the server's, unchanged.
    expect(text).toContain('quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c');
    expect(text).toContain(
      'Run the installer on the new machine with QUORO_ROLE=node, and this command’s URL, token and fingerprint in QUORO_JOIN_URL, QUORO_JOIN_TOKEN and QUORO_JOIN_FINGERPRINT. It runs the command for you and starts the node agent, quoro-node.service.',
    );
    expect(html).not.toContain('Run this on the new machine');
    expect(html).toContain('Done</button>');
  });
});
