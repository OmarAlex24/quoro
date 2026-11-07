import type { Selector } from '../api/generated/model';

/** `2 h 5 min`, `45 min`, `3 d`: a duration in the largest sensible units. */
export const formatDuration = (seconds: number): string => {
  const total = Math.max(0, Math.round(seconds));
  if (total < 60) return `${total} s`;
  const minutes = Math.floor(total / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  const restMinutes = minutes % 60;
  if (hours < 24) return restMinutes ? `${hours} h ${restMinutes} min` : `${hours} h`;
  const days = Math.floor(hours / 24);
  const restHours = hours % 24;
  return restHours ? `${days} d ${restHours} h` : `${days} d`;
};

/** "in 45 min" or "12 min ago", relative to `now`. */
export const expiryWords = (iso: string, now: number = Date.now()): string => {
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return 'unknown time';
  const delta = Math.round((at - now) / 1_000);
  if (Math.abs(delta) < 30) return 'now';
  return delta > 0 ? `in ${formatDuration(delta)}` : `${formatDuration(-delta)} ago`;
};

/** A selector said plainly: "every resource", "project blog", "2 apps". */
export const describeSelector = (selector: Selector): string => {
  if (selector.all) return 'Every resource';
  const parts: string[] = [];
  const projects = selector.projects ?? [];
  const apps = selector.apps ?? [];
  const nodes = selector.nodes ?? [];
  if (projects.length === 1) parts.push(`project ${projects[0]}`);
  else if (projects.length > 1) parts.push(`projects ${projects.join(', ')}`);
  if (apps.length > 0) parts.push(apps.length === 1 ? '1 app' : `${apps.length} apps`);
  if (nodes.length > 0) parts.push(nodes.length === 1 ? '1 node' : `${nodes.length} nodes`);
  if (parts.length === 0) return 'Nothing';
  const sentence = parts.join(' and ');
  return sentence.charAt(0).toUpperCase() + sentence.slice(1);
};

/** `agt_01JC9Z6Q5W` + ellipsis: enough of an id to tell rows apart. */
export const shortId = (id: string, keep = 10): string =>
  id.length > keep + 4 ? `${id.slice(0, id.indexOf('_') + 1 + keep - 4)}…` : id;

/** First and last eight hex characters of a 64-hex hash. */
export const shortHash = (hash: string): string =>
  hash.length > 20 ? `${hash.slice(0, 8)}…${hash.slice(-8)}` : hash;

/**
 * Up to two initials for a human avatar: the first letters of the first and
 * last words, or the first two letters of a single word.
 */
export const initials = (name: string): string => {
  const words = name.split('@')[0]!.trim().split(/[\s._-]+/).filter(Boolean);
  if (words.length === 0) return '?';
  const first = words[0]!;
  const last = words.length > 1 ? words[words.length - 1]! : '';
  return (last ? `${first[0]}${last[0]}` : first.slice(0, 2)).toUpperCase();
};
