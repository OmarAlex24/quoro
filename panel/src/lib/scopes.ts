import { Scope } from '../api/generated/model';

/**
 * Scope classes from permission-model.md, sections 4.1 and 4.2. A standard
 * grant and a webhook's incident grant may carry neither class: the server
 * refuses them with `validation_failed`, and the panel never offers them.
 */
export const HUMAN_ONLY_SCOPES: ReadonlySet<Scope> = new Set<Scope>([
  'node:enroll',
  'node:remove',
  'node:fence_ack',
  'node:roles',
  'node:declare_lost',
  'agent:write',
  'grant:write',
  'approval:decide',
  'webhook:write',
  'provider:manage',
  'user:read',
  'user:write',
  'app:delete_quarantine',
]);

export const DESTRUCTIVE_SCOPES: ReadonlySet<Scope> = new Set<Scope>([
  'node:remove',
  'project:delete',
  'vm:delete',
  'app:delete',
  'app:failover',
  'app:delete_quarantine',
]);

export const ALL_SCOPES: readonly Scope[] = Object.values(Scope);

/** Scopes a standard grant or a webhook's incident grant may carry. */
export const grantableScopes = (): Scope[] =>
  ALL_SCOPES.filter((scope) => !HUMAN_ONLY_SCOPES.has(scope) && !DESTRUCTIVE_SCOPES.has(scope));

export const isGrantableScope = (scope: Scope): boolean =>
  !HUMAN_ONLY_SCOPES.has(scope) && !DESTRUCTIVE_SCOPES.has(scope);

/** `app:restart` -> `app`. */
export const scopeResource = (scope: Scope): string => scope.split(':')[0] ?? scope;

/** Grantable scopes grouped by resource, in contract order. */
export const grantableScopesByResource = (): Array<{ resource: string; scopes: Scope[] }> => {
  const groups: Array<{ resource: string; scopes: Scope[] }> = [];
  for (const scope of grantableScopes()) {
    const resource = scopeResource(scope);
    const group = groups.find((candidate) => candidate.resource === resource);
    if (group) group.scopes.push(scope);
    else groups.push({ resource, scopes: [scope] });
  }
  return groups;
};
