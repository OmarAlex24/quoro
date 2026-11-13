// SPDX-License-Identifier: AGPL-3.0-only
import { createNodeJoinToken } from '../api/generated/nodes/nodes';
import type { NodeJoinTokenCreated } from '../api/generated/model';

/** One call of `createNodeJoinToken`; the generated client unless a test passes its own. */
export type JoinTokenCreator = typeof createNodeJoinToken;

/**
 * A join token for one new machine, as Add a node and the HA view's witness
 * offer ask for it: an empty body, so the server's one-hour expiry applies,
 * and a fresh `Idempotency-Key` for every call: a repeated key with the same
 * body returns the original result instead of a new token.
 */
export const createJoinToken = ({
  create = createNodeJoinToken,
  idempotencyKey = crypto.randomUUID(),
}: { create?: JoinTokenCreator; idempotencyKey?: string } = {}): Promise<NodeJoinTokenCreated> =>
  create({}, { headers: { 'Idempotency-Key': idempotencyKey } });
