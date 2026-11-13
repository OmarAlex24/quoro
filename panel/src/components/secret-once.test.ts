// SPDX-License-Identifier: AGPL-3.0-only
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { SecretOnce } from './secret-once';

/** The text of the value's `<code>`, as a browser reads it: tags stripped, entities decoded. */
const shownValue = (secret: string): string => {
  const html = renderToStaticMarkup(createElement(SecretOnce, { label: 'Key', secret, note: 'Shown once.' }));
  const code = /<code[^>]*>([\s\S]*)<\/code>/.exec(html);
  if (!code) throw new Error('no <code> in the markup');
  return code[1]
    .replace(/<[^>]*>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&amp;/g, '&');
};

describe('SecretOnce', () => {
  it.each([
    ['a join command', 'quoro join https://10.0.0.3:8443 --token njt_abc-DEF_123 --fingerprint sha256:4f1c9a2e --role witness'],
    ['leading, trailing and repeated spaces', '  two  spaces then trailing ones  '],
    ['markup characters', 'a<b>&c "d" \'e\''],
    ['one long word', 'x'.repeat(200)],
  ])('shows %s exactly as the API returned it', (_, secret) => {
    expect(shownValue(secret)).toBe(secret);
  });

  it('keeps each word out of the accessibility tree, so the value reads as one text', () => {
    const html = renderToStaticMarkup(
      createElement(SecretOnce, { label: 'Key', secret: 'quoro join --role witness', note: 'Shown once.' }),
    );
    const words = html.match(/<span[^>]*inline-block[^>]*>/g) ?? [];
    expect(words).toHaveLength(4);
    for (const word of words) expect(word).toContain('role="none"');
  });
});
