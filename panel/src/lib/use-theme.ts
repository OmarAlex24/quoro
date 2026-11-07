import { useSyncExternalStore } from 'react';

/**
 * The theme follows `prefers-color-scheme`; `data-theme="dark"` or `"light"`
 * on `<html>` overrides it. The tokens in `styles.css` already switch on both,
 * so CSS needs nothing from here. Code that paints with resolved colours (the
 * dither canvases, the terminal) reads this to know when to repaint.
 */
export type ResolvedTheme = 'dark' | 'light';

const DARK_QUERY = '(prefers-color-scheme: dark)';
const LIGHT_QUERY = '(prefers-color-scheme: light)';

export const resolveTheme = (attribute: string | null, prefersLight: boolean): ResolvedTheme => {
  if (attribute === 'dark' || attribute === 'light') return attribute;
  // Dark is the default: only an explicit light preference turns it light.
  return prefersLight ? 'light' : 'dark';
};

const snapshot = (): ResolvedTheme =>
  resolveTheme(
    document.documentElement.getAttribute('data-theme'),
    window.matchMedia(LIGHT_QUERY).matches,
  );

const subscribe = (onChange: () => void): (() => void) => {
  const observer = new MutationObserver(onChange);
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  const queries = [window.matchMedia(DARK_QUERY), window.matchMedia(LIGHT_QUERY)];
  for (const query of queries) query.addEventListener('change', onChange);
  return () => {
    observer.disconnect();
    for (const query of queries) query.removeEventListener('change', onChange);
  };
};

/** The theme in effect now; re-renders when the attribute or the system preference changes. */
export const useResolvedTheme = (): ResolvedTheme =>
  useSyncExternalStore(subscribe, snapshot, () => 'dark');

/** Reads a token (`--accent`) from the root element as the current theme resolves it. */
export const readToken = (name: string): string =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();
