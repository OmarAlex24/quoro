/** Project slugs: `^[a-z0-9-]{1,40}$`, fixed by the contract. */
export const SLUG_PATTERN = /^[a-z0-9-]{1,40}$/;

/** Derive a slug from a display name: lowercase, digits, single dashes. */
export const deriveSlug = (name: string): string =>
  name
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40)
    .replace(/-+$/g, '');

export const validateSlug = (slug: string): string | undefined => {
  if (slug.length === 0) return 'Enter a slug.';
  if (!SLUG_PATTERN.test(slug)) return 'Use 1–40 lowercase letters, digits or dashes.';
  return undefined;
};
