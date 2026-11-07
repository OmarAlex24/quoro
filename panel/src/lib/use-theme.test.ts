import { describe, expect, it } from 'vitest';
import {
  createThemeStore,
  parseThemeChoice,
  resolveTheme,
  THEME_STORAGE_KEY,
  themeAttributeFor,
} from './use-theme';

describe('resolveTheme', () => {
  it('follows the system when no attribute is set, dark by default', () => {
    expect(resolveTheme(null, false)).toBe('dark');
    expect(resolveTheme(null, true)).toBe('light');
  });

  it('lets data-theme on <html> override the system either way', () => {
    expect(resolveTheme('dark', true)).toBe('dark');
    expect(resolveTheme('light', false)).toBe('light');
  });

  it('ignores a value it does not know', () => {
    expect(resolveTheme('sepia', true)).toBe('light');
    expect(resolveTheme('', false)).toBe('dark');
  });
});

/** A storage double; `broken` throws like a browser with site data blocked. */
const memoryStorage = (initial: Record<string, string> = {}, broken = false) => {
  const values = new Map(Object.entries(initial));
  const guard = () => {
    if (broken) throw new Error('SecurityError');
  };
  return {
    values,
    getItem: (key: string) => (guard(), values.get(key) ?? null),
    setItem: (key: string, value: string) => (guard(), void values.set(key, value)),
    removeItem: (key: string) => (guard(), void values.delete(key)),
  };
};

const fakeRoot = () => {
  const attributes = new Map<string, string>();
  return {
    attributes,
    setAttribute: (name: string, value: string) => void attributes.set(name, value),
    removeAttribute: (name: string) => void attributes.delete(name),
  };
};

describe('theme choice', () => {
  it('reads System for anything but dark or light', () => {
    expect(parseThemeChoice('dark')).toBe('dark');
    expect(parseThemeChoice('light')).toBe('light');
    expect(parseThemeChoice('system')).toBe('system');
    expect(parseThemeChoice(null)).toBe('system');
    expect(parseThemeChoice('sepia')).toBe('system');
  });

  it('sets data-theme for Dark and Light and none for System', () => {
    expect(themeAttributeFor('dark')).toBe('dark');
    expect(themeAttributeFor('light')).toBe('light');
    expect(themeAttributeFor('system')).toBeNull();
  });

  it('applies the stored choice as soon as it is created', () => {
    const root = fakeRoot();
    const store = createThemeStore(memoryStorage({ [THEME_STORAGE_KEY]: 'light' }), root);
    expect(store.get()).toBe('light');
    expect(root.attributes.get('data-theme')).toBe('light');
  });

  it('stores a choice, applies it, tells listeners, and forgets it for System', () => {
    const storage = memoryStorage();
    const root = fakeRoot();
    const store = createThemeStore(storage, root);
    let heard = 0;
    const stop = store.subscribe(() => {
      heard += 1;
    });

    store.set('dark');
    expect(storage.values.get(THEME_STORAGE_KEY)).toBe('dark');
    expect(root.attributes.get('data-theme')).toBe('dark');

    store.set('system');
    expect(storage.values.has(THEME_STORAGE_KEY)).toBe(false);
    expect(root.attributes.has('data-theme')).toBe(false);
    expect(heard).toBe(2);

    stop();
    store.set('light');
    expect(heard).toBe(2);
  });

  it('survives a reload: a new store reads what the last one stored', () => {
    const storage = memoryStorage();
    createThemeStore(storage, fakeRoot()).set('light');
    const root = fakeRoot();
    expect(createThemeStore(storage, root).get()).toBe('light');
    expect(root.attributes.get('data-theme')).toBe('light');
  });

  it('follows a change made in another tab', () => {
    const storage = memoryStorage();
    const root = fakeRoot();
    const store = createThemeStore(storage, root);
    storage.values.set(THEME_STORAGE_KEY, 'dark');
    store.reload();
    expect(store.get()).toBe('dark');
    expect(root.attributes.get('data-theme')).toBe('dark');
  });

  it('still switches for the page when storage refuses', () => {
    const root = fakeRoot();
    const store = createThemeStore(memoryStorage({}, true), root);
    expect(store.get()).toBe('system');
    store.set('light');
    expect(store.get()).toBe('light');
    expect(root.attributes.get('data-theme')).toBe('light');
  });

  it('works with no storage and no root at all', () => {
    const store = createThemeStore(null, null);
    store.set('dark');
    expect(store.get()).toBe('dark');
  });
});
