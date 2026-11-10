// SPDX-License-Identifier: AGPL-3.0-only
import { http } from 'msw';
import type {
  ClusterVotes,
  Node,
  NodeJoinTokenCreated,
  NodeJoinTokenRequest,
  NodeRolesRequest,
} from '../api/generated/model';
import { allocatedFrom } from './capacity';
import { db, mockId } from './db';
import {
  conflict,
  json,
  notFound,
  readJson,
  refuseUnlessWriter,
  validationFailed,
} from './http';

/*
 * The voters and the witness role (ADR 0170): the cluster's `votes`, the
 * roles operation and the join token for a new machine. Mock mode only.
 */

/** Voters HA needs (ADR 0017). */
const REQUIRED_VOTES = 3;

/** The server's words for a voter change made before the last one reached a majority (section 8.10). */
export const VOTERS_CHANGING =
  'the previous voter-set change has not reached a majority of voters yet; try again once they have acknowledged it';

/** The fingerprint the mock's join commands pin: a fixed 64-hex value. */
const MOCK_FINGERPRINT = '4f1c9a2e7b3d5f60a8c2e4b6d8f0a1c3e5b7d9f1a3c5e7b9d1f3a5c7e9b1d3f5';

let failNextRoles = false;

/**
 * Tests only: the next roles call answers `409 conflict` with the server's
 * words, as when the previous voter change has not reached a majority yet.
 */
export const failNextRolesCall = (): void => {
  failNextRoles = true;
};

/** Tests only: forget a pending `failNextRolesCall`. */
export const resetNodesMock = (): void => {
  failNextRoles = false;
};

const machines = (): Node[] => [db.node, ...db.extraNodes];

const findNode = (nodeId: string): Node | undefined =>
  machines().find((node) => node.id === nodeId);

const votes = (node: Pick<Node, 'roles'>): boolean =>
  node.roles.some((role) => role === 'node' || role === 'witness');

/**
 * `Cluster.votes`: every enrolled machine with the `node` or `witness` role
 * that is not removed, connected or not, oldest enrolled first. The mock has
 * no support to track, so `ha_ready` is true once the count reaches `required`,
 * which is what the server reports while a majority supports the primary.
 */
export const clusterVotes = (): ClusterVotes => {
  const voters = machines()
    .filter((node) => node.status !== 'removed' && votes(node))
    .map((node) => ({
      node_id: node.id,
      roles: [...node.roles],
      connected: node.status === 'connected',
    }));
  return {
    voters,
    count: voters.length,
    required: REQUIRED_VOTES,
    ha_ready: voters.length >= REQUIRED_VOTES,
  };
};

const isWitnessList = (value: unknown): value is ['witness'] =>
  Array.isArray(value) && value.length === 1 && value[0] === 'witness';

export const nodesHandlers = [
  // `updateNodeRoles` (ADR 0170): add or remove `witness`, without enrolling again.
  http.post('*/api/v1/nodes/:nodeId/roles', async ({ params, request }) => {
    const refused = refuseUnlessWriter('node:roles');
    if (refused) return refused;
    const node = findNode(String(params.nodeId));
    if (!node) return notFound('That node');

    const body = await readJson<NodeRolesRequest>(request);
    if (!body) return validationFailed('The request could not be read.');
    const adding = body.add !== undefined;
    const removing = body.remove !== undefined;
    if (adding === removing) return validationFailed('give exactly one of add and remove');
    const roles = adding ? body.add : body.remove;
    if (!isWitnessList(roles)) return validationFailed('only the witness role can change');
    if (node.status === 'removed') return conflict('the node is removed');

    const has = node.roles.includes('witness');
    // Adding a role the machine has, or removing one it lacks, changes nothing.
    if (adding === has) return json({ ...node, allocated: allocatedFrom(node.id, db.vms) });

    const next: Node['roles'] = adding
      ? [...node.roles, 'witness']
      : node.roles.filter((role) => role !== 'witness');
    if (next.length === 0) return validationFailed('the machine would be left with no role');
    if (votes(node) !== votes({ roles: next }) && failNextRoles) {
      failNextRoles = false;
      return conflict(VOTERS_CHANGING);
    }

    node.roles = next;
    node.desired_rev += 1;
    return json({ ...node, allocated: allocatedFrom(node.id, db.vms) });
  }),

  // `createNodeJoinToken`: the secret and the join command, once.
  http.post('*/api/v1/nodes/join-tokens', async ({ request }) => {
    const refused = refuseUnlessWriter('node:enroll');
    if (refused) return refused;
    const body = (await readJson<NodeJoinTokenRequest>(request)) ?? {};
    const expiresAt = body.expires_at ?? new Date(Date.now() + 3_600_000).toISOString();
    if (Number.isNaN(Date.parse(expiresAt)) || Date.parse(expiresAt) <= Date.now()) {
      return validationFailed('expires_at must be in the future');
    }
    const bytes = crypto.getRandomValues(new Uint8Array(32));
    const secret = `njt_${btoa(String.fromCharCode(...bytes))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '')}`;
    const created: NodeJoinTokenCreated = {
      id: mockId('njt'),
      token: secret,
      expires_at: expiresAt,
      join_command: `quoro join https://${db.node.address ?? '127.0.0.1'}:8443 --token ${secret} --fingerprint ${MOCK_FINGERPRINT}`,
    };
    return json(created, { status: 201 });
  }),
];
