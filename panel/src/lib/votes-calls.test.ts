import { MutationObserver, QueryClient } from '@tanstack/react-query';
import { afterEach, describe, expect, it } from 'vitest';
import { isApiProblem } from '../api/fetcher';
import { getGetClusterQueryKey } from '../api/generated/cluster/cluster';
import type { ClusterVotes, Node } from '../api/generated/model';
import { getListNodesQueryKey } from '../api/generated/nodes/nodes';
import { getListVmsQueryKey } from '../api/generated/vms/vms';
import { db } from '../mocks/db';
import { clusterVotes, failNextRolesCall, VOTERS_CHANGING } from '../mocks/handlers-nodes';
import { serveMockApi } from '../mocks/node-server';
import { joinTokenMutation } from './join-token';
import { addWitnessMutation, dropTokenUnlessShort } from './votes';

/*
 * The HA view's calls as the container makes them: its mutation options
 * through a real QueryClient and MutationObserver, against the mock API.
 */

const { requests } = serveMockApi();

const edge = (): Node => {
  const found = db.extraNodes.find((node) => node.name === 'lab-edge');
  if (!found) throw new Error('the seed has no lab-edge');
  return found;
};

// Every test starts from the seed: lab-edge is an edge without a vote.
afterEach(() => {
  edge().roles = ['edge'];
});

/** A client that holds the votes, two pages of the node list and the VMs, none invalidated. */
const seeded = (): QueryClient => {
  const client = new QueryClient();
  client.setQueryData(getGetClusterQueryKey(), { votes: clusterVotes() });
  client.setQueryData(getListNodesQueryKey(), { items: [] });
  client.setQueryData(getListNodesQueryKey({ limit: 500 }), { items: [] });
  client.setQueryData(getListVmsQueryKey(), { items: [] });
  return client;
};

const invalidated = (client: QueryClient) =>
  [getGetClusterQueryKey(), getListNodesQueryKey(), getListNodesQueryKey({ limit: 500 }), getListVmsQueryKey()].map(
    (key) => client.getQueryState(key)?.isInvalidated,
  );

describe('addWitnessMutation', () => {
  it('adds the role, names the machine while in flight, then reads the voters again', async () => {
    const client = seeded();
    const adding: (string | null)[] = [];
    const observer = new MutationObserver(client, addWitnessMutation(client, (id) => adding.push(id)));

    const answered = await observer.mutate(edge());

    expect(answered.roles).toEqual(['edge', 'witness']);
    const sent = requests.filter((request) => request.path === `/api/v1/nodes/${edge().id}/roles`);
    expect(sent.map((request) => request.method)).toEqual(['POST']);
    expect(JSON.parse(await sent[0]!.body)).toEqual({ add: ['witness'] });
    expect(adding).toEqual([edge().id, null]);
    // The votes and every page of the node list, and nothing else.
    expect(invalidated(client)).toEqual([true, true, true, false]);
  });

  it('reads the voters again after a 409 too, which the call rejects with', async () => {
    const client = seeded();
    const adding: (string | null)[] = [];
    const observer = new MutationObserver(client, addWitnessMutation(client, (id) => adding.push(id)));
    failNextRolesCall();

    const refused = await observer.mutate(edge()).then(
      () => null,
      (error: unknown) => error,
    );

    expect(isApiProblem(refused) && refused.status).toBe(409);
    expect(isApiProblem(refused) && refused.detail).toBe(VOTERS_CHANGING);
    expect(edge().roles).toEqual(['edge']);
    expect(adding).toEqual([edge().id, null]);
    expect(invalidated(client)).toEqual([true, true, true, false]);
  });
});

describe('dropTokenUnlessShort with a real token', () => {
  const short: ClusterVotes = { voters: [], count: 2, required: 3, ha_ready: false };
  const full: ClusterVotes = { voters: [], count: 3, required: 3, ha_ready: false };

  it('keeps the token while HA is short and drops it from the mutation once HA has its count', async () => {
    const observer = new MutationObserver(new QueryClient(), joinTokenMutation());
    const created = await observer.mutate();
    const drop = () => observer.reset();

    dropTokenUnlessShort(short, drop);
    expect(observer.getCurrentResult().data).toBe(created);

    dropTokenUnlessShort(full, drop);
    expect(observer.getCurrentResult().data).toBeUndefined();
    expect(observer.getCurrentResult().status).toBe('idle');
  });
});
