import type { Approval, Scope } from '../api/generated/model';
import { shortId } from './agency-format';

/*
 * Who else can act on a project, and what an agent asks for, in plain words.
 * The project rail reads `listGrants`; the projects callout reads
 * `listApprovals`. Both say only what those fields support.
 */

/* ---------- what a grant lets an agent do ---------- */

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
