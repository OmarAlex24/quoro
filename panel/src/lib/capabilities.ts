import type { MeResponse } from '../api/generated/model';

/**
 * What the signed-in identity may do in the agency screens. The server always
 * decides; the panel only hides controls the session could never use.
 *
 * `agent:write`, `grant:write`, `approval:decide` and `webhook:write` are
 * human-only (permission-model.md, section 4.2): an agent session never sees
 * them, whatever grants it holds. Among humans, `viewer` holds read scopes
 * only, while `owner` and `operator` hold all four.
 */

/** The credential belongs to a human, not to an agent. */
export const isHumanSession = (me: MeResponse | undefined): boolean =>
  me?.type === 'user' && me.user !== undefined && !me.user.disabled;

const humanWriter = (me: MeResponse | undefined): boolean =>
  isHumanSession(me) && (me?.user?.role === 'owner' || me?.user?.role === 'operator');

/**
 * Approve or deny an approval request (`approval:decide`). An agent can never
 * decide, so it cannot approve its own request (permission-model.md, section 9).
 */
export const canDecideApprovals = humanWriter;

/** Create, change, disable or delete agents and their keys (`agent:write`). */
export const canManageAgents = humanWriter;

/** Create and revoke grants (`grant:write`). */
export const canManageGrants = humanWriter;

/** Create, change, delete and test webhooks (`webhook:write`). */
export const canManageWebhooks = humanWriter;

/**
 * Create, change, disable, reset and delete human users (`user:write`). Only an
 * `owner` holds it: an `operator` runs the cluster but cannot touch identities,
 * and an agent session never can (permission-model.md, section 4.2).
 */
export const canManageUsers = (me: MeResponse | undefined): boolean =>
  isHumanSession(me) && me?.user?.role === 'owner';

/**
 * Read the user list to show names instead of ids (`user:read`). Every human
 * role holds it, so owner, operator and viewer all see names; an agent session
 * never lists users.
 */
export const canListUsers = (me: MeResponse | undefined): boolean => isHumanSession(me);

/**
 * Open a web terminal into an app's guest (`app:terminal`). The scope is
 * human-only in M4 (ADR 0114), so an owner or operator may, an agent never.
 */
export const canOpenTerminal = humanWriter;

/**
 * View a VM's serial console (`vm:console`). The scope is grantable to agents,
 * and the server checks the grant, so an enabled agent session also sees the
 * entry point; the console is read-only either way.
 */
export const canViewConsole = (me: MeResponse | undefined): boolean =>
  humanWriter(me) ||
  (me?.type === 'agent' && me.agent !== undefined && !me.agent.disabled);
