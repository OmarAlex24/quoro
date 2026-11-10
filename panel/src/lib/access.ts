import type { Approval, Grant, Project, Scope } from '../api/generated/model';
import { shortId } from './agency-format';

/*
 * Who else can act on a project, and what an agent asks for, in plain words.
 * The canvas chip reads `listGrants`; the projects callout reads
 * `listApprovals`. Both say only what those fields support.
 */

/* ---------- the access chip ---------- */

/**
 * Does this grant let its agent act inside the project right now? Only an
 * active, unexpired grant whose selector names the project (by slug) or
 * every resource. A grant limited to some apps or nodes is not "here": it
 * covers those apps, not the project.
 */
export const grantCoversProject = (
  grant: Pick<Grant, 'status' | 'expires_at' | 'selector'>,
  project: Pick<Project, 'slug'>,
  now: number = Date.now(),
): boolean => {
  if (grant.status !== 'active') return false;
  const expires = Date.parse(grant.expires_at);
  if (Number.isNaN(expires) || expires <= now) return false;
  return grant.selector.all === true || (grant.selector.projects ?? []).includes(project.slug);
};

/** The project's resources a scope can act on, and how to name them. */
const NOUNS: Readonly<Record<string, string>> = {
  app: 'apps',
  vm: 'VMs',
  backup: 'backups',
  project: 'the project',
};

/** Verbs in the order a sentence says them. */
const VERBS: ReadonlyArray<readonly [string, string]> = [
  ['read', 'read'],
  ['create', 'create'],
  ['update', 'change'],
  ['write', 'change'],
  ['deploy', 'deploy'],
  ['redeploy', 'redeploy'],
  ['restart', 'restart'],
  ['start', 'start'],
  ['stop', 'stop'],
  ['console', 'open the console of'],
];

const joinAnd = (words: readonly string[]): string =>
  words.length <= 1
    ? (words[0] ?? '')
    : `${words.slice(0, -1).join(', ')} and ${words[words.length - 1]}`;

/**
 * "read and restart apps", from a grant's scopes. Only the scopes that act
 * on a project's apps, VMs, backups or the project itself are said; reading
 * logs folds into reading. Null when none of the scopes acts inside a project.
 */
export const accessPhrase = (scopes: readonly Scope[]): string | null => {
  const phrases: string[] = [];
  for (const [resource, noun] of Object.entries(NOUNS)) {
    const verbs = new Set(
      scopes
        .filter((scope) => scope.startsWith(`${resource}:`))
        .map((scope) => scope.slice(resource.length + 1)),
    );
    if (verbs.size === 0) continue;
    const words = [...new Set(VERBS.filter(([verb]) => verbs.has(verb)).map(([, word]) => word))];
    if (verbs.has('logs') && !verbs.has('read')) words.unshift('read the logs of');
    if (words.length > 0) phrases.push(`${joinAnd(words)} ${noun}`);
  }
  return phrases.length === 0 ? null : joinAnd(phrases);
};

/**
 * True when `accessPhrase` says every one of these scopes: each acts on a
 * project's apps, VMs, backups or the project itself with a verb the phrase
 * knows. A scope it would leave out (`approval:create`, `app:terminal`) makes
 * it false, so a summary names the scopes instead.
 */
export const accessCovers = (scopes: readonly Scope[]): boolean =>
  scopes.every((scope) => {
    const resource = scope.slice(0, scope.indexOf(':'));
    const verb = scope.slice(scope.indexOf(':') + 1);
    return resource in NOUNS && (verb === 'logs' || VERBS.some(([known]) => known === verb));
  });

/** "6 d left", "20 h left", "45 min left": the time a grant has, in its largest unit. */
export const timeLeft = (iso: string, now: number = Date.now()): string => {
  const seconds = Math.max(0, (Date.parse(iso) - now) / 1_000);
  if (seconds >= 86_400) return `${Math.floor(seconds / 86_400)} d left`;
  if (seconds >= 3_600) return `${Math.floor(seconds / 3_600)} h left`;
  return `${Math.max(1, Math.floor(seconds / 60))} min left`;
};

export interface ProjectAccess {
  readonly grant: Grant;
  readonly phrase: string;
}

/**
 * The grants that let an agent act in this project, soonest expiry first,
 * each with what it lets the agent do.
 */
export const projectAccess = (
  grants: readonly Grant[],
  project: Pick<Project, 'slug'>,
  now: number = Date.now(),
): ProjectAccess[] =>
  grants
    .filter((grant) => grantCoversProject(grant, project, now))
    .map((grant) => ({ grant, phrase: accessPhrase(grant.scopes) }))
    .filter((entry): entry is ProjectAccess => entry.phrase !== null)
    .sort((left, right) => Date.parse(left.grant.expires_at) - Date.parse(right.grant.expires_at));

/* ---------- the approval callout ---------- */

const ASK_VERBS: Readonly<Record<string, string>> = {
  delete: 'delete',
  remove: 'remove',
  restore: 'restore',
  failover: 'fail over',
  delete_quarantine: 'delete a quarantined copy of',
};

/**
 * What a pending approval asks, without the agent, in two parts so the
 * target can be set apart: "wants to delete" and "blog". The verb comes from
 * the approval's scope, the target from its name when the caller knows it.
 */
export const approvalAsk = (
  approval: Pick<Approval, 'scope' | 'target'>,
  targetName: string | null | undefined,
): { wants: string; target: string } => {
  const verb = approval.scope.slice(approval.scope.indexOf(':') + 1);
  const words = ASK_VERBS[verb] ?? verb.replace(/_/g, ' ');
  return {
    wants: `wants to ${words}`,
    target: targetName ?? `${approval.target.type} ${shortId(approval.target.id)}`,
  };
};
