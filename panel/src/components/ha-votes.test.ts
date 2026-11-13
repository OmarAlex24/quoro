import { MutationObserver, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { afterEach, describe, expect, it } from 'vitest';
import { ApiProblem, isApiProblem } from '../api/fetcher';
import { getGetMeQueryKey } from '../api/generated/auth/auth';
import { getGetClusterQueryKey } from '../api/generated/cluster/cluster';
import type { Cluster, ClusterVotes, MeResponse, Node, NodeJoinTokenCreated } from '../api/generated/model';
import { getListNodesQueryKey } from '../api/generated/nodes/nodes';
import { getListVmsQueryKey } from '../api/generated/vms/vms';
import { problemView } from '../lib/problem';
import { haReadout, witnessCandidates, witnessProblem } from '../lib/votes';
import { db } from '../mocks/db';
import { clusterVotes, failNextRolesCall } from '../mocks/handlers-nodes';
import { RecordingClient, serveMockApi } from '../mocks/node-server';
import { SessionProvider } from '../session';
import { HaVotes, HaVotesView, type HaVotesViewProps } from './ha-votes';

const node = (name: string, roles: Node['roles'], fields: Partial<Node> = {}): Node =>
  ({ id: `nod_${name}`, name, roles, status: 'connected', cp_status: null, ...fields }) as Node;

const lab3 = node('lab-3', ['node'], { cp_status: 'primary' });
const lab4 = node('lab-4', ['node', 'edge']);
const edge = node('lab-edge', ['edge']);
const witness = node('lab-edge', ['edge', 'witness']);

const votesOf = (voters: Node[], ha_ready: boolean): ClusterVotes => ({
  voters: voters.map((voter) => ({ node_id: voter.id, roles: voter.roles, connected: true })),
  count: voters.length,
  required: 3,
  ha_ready,
});

const render = (votes: ClusterVotes, nodes: Node[], overrides: Partial<HaVotesViewProps> = {}): string =>
  renderToStaticMarkup(
    createElement(HaVotesView, {
      readout: haReadout(votes, nodes),
      candidates: witnessCandidates(nodes, votes),
      canChange: true,
      addingId: null,
      busy: false,
      problem: null,
      joinToken: null,
      tokenProblem: null,
      onAddWitness: () => undefined,
      onCreateToken: () => undefined,
      onDropToken: () => undefined,
      ...overrides,
    }),
  );

const decode = (html: string): string => html.replace(/&#x27;/g, "'").replace(/&quot;/g, '"');

/** What a reader sees: the markup without its tags. */
const text = (html: string): string => decode(html.replace(/<[^>]*>/g, ''));

describe('HaVotesView with two voters', () => {
  const votes = votesOf([lab3, lab4], false);
  const nodes = [lab3, lab4, edge];

  it('says HA needs a third vote, with the voters, the count and an open seat', () => {
    const html = render(votes, nodes);
    expect(html).toContain('HA needs a third vote');
    expect(html).toContain('2 of 3 votes');
    expect(html).toContain('lab-3');
    expect(html).toContain('lab-4');
    expect(html).toContain('No voter');
    expect(html.match(/class="ha-seat"/g)).toHaveLength(2);
    expect(html.match(/ha-seat ha-seat-open/g)).toHaveLength(1);
  });

  it('offers the witness role on the edge and a join token for a new machine', () => {
    const html = render(votes, nodes);
    expect(html).toContain('aria-label="Machines without a vote"');
    expect(html).toMatch(/lab-edge<\/b>.*Add the witness role/s);
    expect(html).toContain('Create a join token');
    expect(html).toContain('stores no application data');
  });

  it('shows the join command with --role witness once a token exists', () => {
    const token: NodeJoinTokenCreated = {
      id: 'njt_01JCA0A1B2C3D4E5F6G7H8J9KM',
      token: 'njt_secret',
      expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      join_command: 'quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c',
    };
    const html = render(votes, nodes, { joinToken: token });
    // The command is set word by word, so read its text without the markup.
    expect(html.replace(/<[^>]*>/g, '')).toContain(
      'quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c --role witness',
    );
    expect(html).toContain('Copy command');
    expect(html).not.toContain('Create a join token');
  });

  it('says the new machine needs the quoro binary at the cluster\'s version, before any token', () => {
    expect(text(render(votes, nodes, { version: '0.1.0' }))).toContain(
      'A small machine is enough. It needs the quoro binary at the cluster’s version, 0.1.0. The join token is for one machine and lasts an hour.',
    );
    expect(text(render(votes, nodes))).toContain('It needs the quoro binary at the cluster’s version. The join token');
  });

  it('says to run the join as root, then to start the node agent as root and keep it running', () => {
    const token: NodeJoinTokenCreated = {
      id: 'njt_01JCA0A1B2C3D4E5F6G7H8J9KM',
      token: 'njt_secret',
      expires_at: new Date(Date.now() + 3_600_000).toISOString(),
      join_command: 'quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c',
    };
    const html = render(votes, nodes, { joinToken: token });
    expect(text(html)).toMatch(
      /Run this as root on the new machine.*--role witness.*Then start the node agent there with quoro node, as root too, and keep it running, for example as a systemd service\. Until it connects, the witness cannot vote\./s,
    );
    expect(html).toContain('<code class="type-code">quoro node</code>');
    // No installer: it has no witness role.
    expect(html).not.toContain('QUORO_ROLE');
    expect(render(votes, nodes)).not.toContain('quoro node');
  });

  it('notes beside the button that a candidate which is not connected votes only once its agent connects', () => {
    const away = node('lab-edge', ['edge'], { status: 'disconnected' });
    const html = render(votes, [lab3, lab4, away]);
    const note = 'It gets the role now and can vote once its node agent connects.';
    expect(html).toContain(`<p id="ha-cand-note-${away.id}" class="ha-cand-note">${note}</p>`);
    // The lamp says "Not connected" once; the note does not repeat it.
    expect(html.match(/Not connected/g)).toHaveLength(1);
    // The button stays enabled, as the server takes the role, and points at the note.
    expect(html).toMatch(new RegExp(`<button[^>]*aria-describedby="ha-cand-note-${away.id}"[^>]*>Add the witness role`));
    expect(html).not.toMatch(/<button[^>]*disabled=""[^>]*aria-describedby/);
  });

  it('shows no note to a session without the button', () => {
    const away = node('lab-edge', ['edge'], { status: 'disconnected' });
    const html = render(votes, [lab3, lab4, away], { canChange: false });
    expect(html).not.toContain('ha-cand-note');
    expect(html).not.toContain('It gets the role now');
    expect(html).toContain('lab-edge');
  });

  it('says what a seat that is not connected needs', () => {
    const current = votesOf([lab3, lab4], false);
    current.voters[1] = { ...current.voters[1]!, connected: false };
    const html = render(current, nodes);
    expect(html.match(/class="next-step"/g)).toHaveLength(1);
    expect(text(html)).toMatch(
      /lab-4.*Disconnected.*Make sure its node agent, quoro node, is running on that machine\..*No voter/s,
    );
    expect(render(votes, nodes)).not.toContain('Make sure');
  });

  it('adds no note to a connected candidate', () => {
    const html = render(votes, nodes);
    expect(html).not.toContain('ha-cand-note');
    expect(html).not.toContain('aria-describedby');
  });

  it('keeps the votes when a refetch failed, with a quiet line beside them', () => {
    const html = render(votes, nodes, { stale: problemView(new TypeError('Failed to fetch'), 'see the voters') });
    expect(html).toContain('HA needs a third vote');
    expect(html).toContain('Add the witness role');
    expect(html).toMatch(
      /<p class="ha-stale" role="status">.*These votes may be out of date\. Could not reach the server\. Check the connection and try again\./,
    );
    expect(html).not.toContain('role="alert"');
  });

  it('shows the server words of a 409 and says to wait and try again', () => {
    const detail =
      'the previous voter-set change has not reached a majority of voters yet; try again once they have acknowledged it';
    const html = render(votes, nodes, {
      problem: witnessProblem(new ApiProblem({ status: 409, code: 'conflict', detail })),
    });
    expect(html).toContain('role="alert"');
    expect(html).toContain(detail);
    expect(html).toContain('Wait a few seconds and try again.');
  });

  it('shows another refusal as the panel does elsewhere', () => {
    const html = decode(
      render(votes, nodes, {
        problem: witnessProblem(new ApiProblem({ status: 403, code: 'forbidden', reason: 'human_only' })),
        tokenProblem: problemView(new TypeError('Failed to fetch'), 'create join tokens'),
      }),
    );
    expect(html).toContain("Your role can't change the voters (human_only)");
    expect(html).toContain('Could not reach the server.');
    expect(html).not.toContain('Wait a few seconds');
  });

  it('disables every action while a call is in flight', () => {
    const html = render(votes, nodes, { busy: true, addingId: edge.id });
    expect(html).toContain('Adding the witness role…');
    expect(html.match(/<button[^>]*disabled=""/g)).toHaveLength(2);
  });

  it('hides the actions from a session that cannot change the voters', () => {
    const html = render(votes, nodes, { canChange: false });
    expect(html).not.toContain('<button');
    expect(html).toContain('An owner or an operator can add the witness role or create a join token.');
    expect(html).toContain('HA needs a third vote');
  });
});

describe('HaVotesView with three voters', () => {
  const nodes = [lab3, lab4, witness];

  it('says quietly that HA is ready and offers no witness', () => {
    const html = render(votesOf([lab3, lab4, witness], true), nodes);
    expect(html).toContain('HA is ready.');
    expect(html).toContain('3 of 3 votes, and a majority supports the primary.');
    expect(html).toContain('<b>lab-edge</b><span>edge, witness</span>');
    expect(html).toContain('<b>lab-4</b><span>node, edge</span>');
    expect(html).not.toContain('witness role');
    expect(html).not.toContain('join token');
    expect(html).not.toContain('<button');
  });

  it('says which voter is disconnected', () => {
    const votes = votesOf([lab3, lab4, witness], false);
    votes.voters[1] = { ...votes.voters[1]!, connected: false };
    const html = render(votes, nodes);
    expect(html).toContain('<b>lab-4</b><span>node, edge</span><span class="text-fault">Disconnected</span>');
  });

  it('says what a disconnected voter needs, even while HA is ready', () => {
    // The server counts the voter connected or not: HA has its count, and the readout stays the server's.
    const votes = votesOf([lab3, lab4, witness], true);
    votes.voters[2] = { ...votes.voters[2]!, connected: false };
    const html = render(votes, nodes);
    expect(html).toContain('HA is ready.');
    expect(html).toContain('ha-quiet-rows');
    expect(text(html)).toContain('Make sure the node agent, quoro node, is running on lab-edge.');
    expect(html).toContain('<b>lab-edge</b>.</p>');
  });

  it('names every disconnected voter in one line', () => {
    const votes = votesOf([lab3, lab4, witness], false);
    votes.voters[1] = { ...votes.voters[1]!, connected: false };
    votes.voters[2] = { ...votes.voters[2]!, connected: false };
    expect(text(render(votes, nodes))).toContain('Make sure the node agent, quoro node, is running on lab-4 and lab-edge.');
  });

  it('adds no line while every voter is connected', () => {
    const html = render(votesOf([lab3, lab4, witness], true), nodes);
    expect(html).not.toContain('next-step');
    expect(html).not.toContain('ha-quiet-rows');
  });

  it('keeps the quiet line when a refetch failed, with the line beside it', () => {
    const html = render(votesOf([lab3, lab4, witness], true), nodes, {
      stale: problemView(new TypeError('Failed to fetch'), 'see the voters'),
    });
    expect(html).toContain('HA is ready.');
    expect(html).toContain('These votes may be out of date.');
    expect(render(votesOf([lab3, lab4, witness], true), nodes)).not.toContain('ha-stale');
  });

  it('waits for a majority before ha_ready, still without an offer', () => {
    const html = render(votesOf([lab3, lab4, witness], false), nodes);
    expect(html).toContain('Waiting for a majority.');
    expect(html).toContain('n1-shift');
    expect(html).not.toContain('join token');
  });
});

/*
 * The container itself, rendered on the server: what it draws from the
 * getCluster query, and the options it gives `useMutation`, run against the
 * mock API. Effects do not run on the server, so dropping the token once HA
 * has its votes (an effect that calls `dropTokenUnlessShort`) is pinned in
 * lib/votes-calls.test.ts, not here.
 */
describe('the HaVotes container', () => {
  const { requests } = serveMockApi();

  const machines = (): Node[] => [db.node, ...db.extraNodes];
  const mockEdge = (): Node => {
    const found = db.extraNodes.find((machine) => machine.name === 'lab-edge');
    if (!found) throw new Error('the seed has no lab-edge');
    return found;
  };

  afterEach(() => {
    mockEdge().roles = ['edge'];
  });

  /** A client signed in as the owner, with the votes and the node list when asked. */
  const signedIn = <C extends QueryClient>(client: C, { votes = true } = {}): C => {
    if (!db.owner) throw new Error('no owner');
    const me: MeResponse = { type: 'user', user: db.owner };
    client.setQueryData(getGetMeQueryKey(), me);
    if (votes) {
      client.setQueryData(getGetClusterQueryKey(), { votes: clusterVotes(), version: '0.1.0' } as Cluster);
      client.setQueryData(getListNodesQueryKey(), { items: machines() });
      client.setQueryData(getListNodesQueryKey({ limit: 500 }), { items: machines() });
      client.setQueryData(getListVmsQueryKey(), { items: [] });
    }
    return client;
  };

  const renderContainer = (client: QueryClient): string =>
    renderToStaticMarkup(
      createElement(
        QueryClientProvider,
        { client },
        createElement(SessionProvider, { children: createElement(HaVotes, { nodes: machines() }) }),
      ),
    );

  it('draws the loading strip while the first votes are on their way', () => {
    const html = renderContainer(signedIn(new QueryClient(), { votes: false }));
    expect(html).toContain('ha-skel');
    expect(html).toContain('Loading the voters');
    expect(html).not.toContain('HA needs');
  });

  it('draws nothing for a session that is not signed in', () => {
    expect(renderContainer(new QueryClient())).toBe('');
  });

  it('draws the votes from the cache, with the cluster\'s version in the offer', () => {
    const html = renderContainer(signedIn(new QueryClient()));
    expect(html).not.toContain('ha-skel');
    expect(html).toContain('HA needs a third vote');
    expect(text(html)).toContain('It needs the quoro binary at the cluster’s version, 0.1.0.');
    expect(html).toContain('Add the witness role');
  });

  /** The container's two mutations, in the order it calls `useMutation`. */
  const containerMutations = () => {
    const client = signedIn(new RecordingClient());
    renderContainer(client);
    expect(client.mutations).toHaveLength(2);
    const [roles, joinToken] = [...client.mutations];
    return { client, roles: roles!, joinToken: joinToken! };
  };

  it('asks for a new join token with a fresh Idempotency-Key on every click', async () => {
    const { client, joinToken } = containerMutations();
    const observer = new MutationObserver(client, joinToken);
    await observer.mutate(undefined);
    await observer.mutate(undefined);

    const keys = requests
      .filter((request) => request.method === 'POST' && request.path === '/api/v1/nodes/join-tokens')
      .map((request) => request.headers.get('Idempotency-Key'));
    expect(keys).toHaveLength(2);
    for (const key of keys) expect(key).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(keys[0]).not.toBe(keys[1]);
  });

  const invalidated = (client: QueryClient) =>
    [getGetClusterQueryKey(), getListNodesQueryKey(), getListNodesQueryKey({ limit: 500 }), getListVmsQueryKey()].map(
      (key) => client.getQueryState(key)?.isInvalidated,
    );

  it('reads the votes and the node list again after a roles call that answers 200', async () => {
    const { client, roles } = containerMutations();
    expect(invalidated(client)).toEqual([false, false, false, false]);

    await new MutationObserver(client, roles).mutate(mockEdge());

    expect(mockEdge().roles).toEqual(['edge', 'witness']);
    expect(requests.some((request) => request.path === `/api/v1/nodes/${mockEdge().id}/roles`)).toBe(true);
    expect(invalidated(client)).toEqual([true, true, true, false]);
  });

  it('reads them again after a roles call that answers 409', async () => {
    const { client, roles } = containerMutations();
    failNextRolesCall();

    const refused = await new MutationObserver(client, roles).mutate(mockEdge()).then(
      () => null,
      (error: unknown) => error,
    );

    expect(isApiProblem(refused) && refused.status).toBe(409);
    expect(invalidated(client)).toEqual([true, true, true, false]);
  });
});
