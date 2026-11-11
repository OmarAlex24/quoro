import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { AddNodeDialog } from './add-node';
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
});
