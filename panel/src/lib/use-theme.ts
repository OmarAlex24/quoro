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

/*
 * The person's choice, stored per browser: System follows the operating
 * system; Dark and Light set `data-theme` on <html>. Storage can be missing
 * or refuse (private windows, blocked site data); the choice then lasts for
 * the page and the panel still works.
 */
export type ThemeChoice = 'system' | 'dark' | 'light';

export const THEME_CHOICES: readonly ThemeChoice[] = ['system', 'dark', 'light'];

export const THEME_STORAGE_KEY = 'quoro.theme';

export const parseThemeChoice = (value: unknown): ThemeChoice =>
  value === 'dark' || value === 'light' ? value : 'system';

/** The `data-theme` value a choice sets; System sets none. */
export const themeAttributeFor = (choice: ThemeChoice): ResolvedTheme | null =>
  choice === 'system' ? null : choice;

type ChoiceStorage = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;
type ThemeRoot = Pick<Element, 'setAttribute' | 'removeAttribute'>;

export interface ThemeStore {
  get(): ThemeChoice;
  set(choice: ThemeChoice): void;
  /** Re-reads storage, for a change made in another tab. */
  reload(): void;
  subscribe(listener: () => void): () => void;
}

/** Applies the stored choice to `root` at once, and keeps both in step afterwards. */
export function createThemeStore(storage: ChoiceStorage | null, root: ThemeRoot | null): ThemeStore {
  const read = (): ThemeChoice => {
    try {
      return parseThemeChoice(storage?.getItem(THEME_STORAGE_KEY));
    } catch {
      return 'system';
    }
  };
  const apply = (choice: ThemeChoice) => {
    const attribute = themeAttributeFor(choice);
    if (attribute) root?.setAttribute('data-theme', attribute);
    else root?.removeAttribute('data-theme');
  };
  const listeners = new Set<() => void>();
  let current = read();
  apply(current);

  const change = (choice: ThemeChoice) => {
    current = choice;
    apply(choice);
    for (const listener of listeners) listener();
  };

  return {
    get: () => current,
    set(choice) {
      try {
        if (choice === 'system') storage?.removeItem(THEME_STORAGE_KEY);
        else storage?.setItem(THEME_STORAGE_KEY, choice);
      } catch {
        // Not stored: the choice still holds until the page closes.
      }
      change(choice);
    },
    reload() {
      const stored = read();
      if (stored !== current) change(stored);
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}

let browserStore: ThemeStore | null = null;

/** The browser's store, created on first use; applies the stored choice to <html>. */
export const themeStore = (): ThemeStore => {
  if (!browserStore) {
    let storage: ChoiceStorage | null = null;
    try {
      storage = window.localStorage;
    } catch {
      storage = null;
    }
    const store = createThemeStore(storage, document.documentElement);
    window.addEventListener('storage', (event) => {
      if (event.key === null || event.key === THEME_STORAGE_KEY) store.reload();
    });
    browserStore = store;
  }
  return browserStore;
};

const subscribeChoice = (listener: () => void) => themeStore().subscribe(listener);
const choiceSnapshot = () => themeStore().get();

/** The stored choice (System, Dark or Light), re-rendering when it changes here or in another tab. */
export const useThemeChoice = (): ThemeChoice =>
  useSyncExternalStore(subscribeChoice, choiceSnapshot, () => 'system');

export const setThemeChoice = (choice: ThemeChoice): void => themeStore().set(choice);
