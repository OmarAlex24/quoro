import { QueryClient } from '@tanstack/react-query';
import { describe, expect, it } from 'vitest';
import { ApiProblem } from '../api/fetcher';
import { getGetClusterQueryKey } from '../api/generated/cluster/cluster';
import type { ClusterVoter, ClusterVotes, Node } from '../api/generated/model';
import { getListNodesQueryKey } from '../api/generated/nodes/nodes';
import { getListVmsQueryKey } from '../api/generated/vms/vms';
import {
  candidateNote,
  disconnectedVoters,
  dropTokenUnlessShort,
  haReadout,
  haVotesState,
  missingVoteWords,
  refreshVoters,
  shortOfVotes,
  tallyWords,
  witnessCandidates,
  witnessJoinCommand,
  witnessProblem,
} from './votes';

const node = (name: string, roles: Node['roles'], fields: Partial<Node> = {}): Node =>
  ({ id: `nod_${name}`, name, roles, status: 'connected', cp_status: null, ...fields }) as Node;

const lab3 = node('lab-3', ['node'], { cp_status: 'primary' });
const lab4 = node('lab-4', ['node', 'edge']);
const edge = node('lab-edge', ['edge']);

const voter = (machine: Node, connected = true): ClusterVoter => ({
  node_id: machine.id,
  roles: machine.roles,
  connected,
});

const votes = (voters: ClusterVoter[], ha_ready = false, required = 3): ClusterVotes => ({
  voters,
  count: voters.length,
  required,
  ha_ready,
});

describe('missingVoteWords', () => {
  it('names the missing vote when one is missing', () => {
    expect(missingVoteWords(2, 3)).toBe('HA needs a third vote');
    expect(missingVoteWords(4, 5)).toBe('HA needs a fifth vote');
  });

  it('counts the missing votes when more are missing, as on a single node', () => {
    expect(missingVoteWords(1, 3)).toBe('HA needs two more votes');
    expect(missingVoteWords(0, 3)).toBe('HA needs three more votes');
  });

  it('says nothing when none is missing', () => {
    expect(missingVoteWords(3, 3)).toBeNull();
    expect(missingVoteWords(4, 3)).toBeNull();
  });
});

describe('tallyWords', () => {
  it('counts the votes against required', () => {
    expect(tallyWords(2, 3)).toBe('2 of 3 votes');
    expect(tallyWords(3, 3)).toBe('3 of 3 votes');
    expect(tallyWords(0, 1)).toBe('0 of 1 vote');
    expect(tallyWords(4, 3)).toBe('4 votes, 3 needed');
  });
});

describe('haReadout', () => {
  it('is short of a vote with two voters, and keeps a seat open', () => {
    const readout = haReadout(votes([voter(lab3), voter(lab4)]), [lab3, lab4, edge]);
    expect(readout.state).toBe('short');
    expect(readout.headline).toBe('HA needs a third vote');
    expect(readout.line).toBe('With two voters, losing one leaves no majority.');
    expect(readout.tally).toBe('2 of 3 votes');
    expect(readout.open).toBe(1);
    expect(readout.seats.map((seat) => seat.name)).toEqual(['lab-3', 'lab-4']);
    expect(readout.seats[1]).toMatchObject({ roles: ['node', 'edge'], connected: true, tone: 'run', words: 'Connected' });
  });

  it('is short of two votes on a single node', () => {
    const readout = haReadout(votes([voter(lab3)]), [lab3]);
    expect(readout.state).toBe('short');
    expect(readout.headline).toBe('HA needs two more votes');
    expect(readout.line).toBe('With one voter, losing it leaves no majority.');
    expect(readout.open).toBe(2);
  });

  it('shows a disconnected voter as one', () => {
    const readout = haReadout(votes([voter(lab3), voter(lab4, false)]), [lab3, lab4]);
    expect(readout.seats[1]).toMatchObject({ connected: false, tone: 'fault', words: 'Disconnected' });
  });

  it('names a voter by its short id while the nodes list has none', () => {
    const unknown = 'nod_01JCA0A1B2C3D4E5F6G7H8J9KM';
    const readout = haReadout(votes([{ node_id: unknown, roles: ['witness'], connected: true }]), []);
    expect(readout.seats[0]?.name).toBe('nod_01JCA0…');
  });

  it('waits for a majority once the count reaches required, before ha_ready', () => {
    const readout = haReadout(votes([voter(lab3), voter(lab4), voter(edge)], false), [lab3, lab4, edge]);
    expect(readout.state).toBe('waiting');
    expect(readout.headline).toBe('Waiting for a majority');
    expect(readout.line).toBe('3 of 3 votes. HA is ready once a majority of the voters supports the primary.');
    expect(readout.open).toBe(0);
  });

  it('is ready with ha_ready', () => {
    const witness = node('lab-edge', ['edge', 'witness']);
    const readout = haReadout(votes([voter(lab3), voter(lab4), voter(witness)], true), [lab3, lab4, witness]);
    expect(readout.state).toBe('ready');
    expect(readout.headline).toBe('HA is ready');
    expect(readout.line).toBe('3 of 3 votes, and a majority supports the primary.');
    expect(readout.seats.map((seat) => seat.name)).toEqual(['lab-3', 'lab-4', 'lab-edge']);
  });

  it('never reads ready below required, whatever ha_ready says', () => {
    expect(haReadout(votes([voter(lab3), voter(lab4)], true), [lab3, lab4]).state).toBe('short');
  });
});

describe('witnessCandidates', () => {
  it('offers the enrolled machines that do not vote', () => {
    const current = votes([voter(lab3), voter(lab4)]);
    expect(witnessCandidates([lab3, lab4, edge], current).map((machine) => machine.name)).toEqual(['lab-edge']);
  });

  it('leaves out removed machines and those that vote already', () => {
    const removed = node('old-edge', ['edge'], { status: 'removed' });
    const witness = node('lab-w', ['witness']);
    const disconnected = node('far-edge', ['edge'], { status: 'disconnected' });
    const current = votes([voter(lab3), voter(witness)]);
    expect(
      witnessCandidates([lab3, removed, witness, disconnected], current).map((machine) => machine.name),
    ).toEqual(['far-edge']);
  });

  it('leaves out a machine whose roles vote even before the votes list it', () => {
    const fresh = node('lab-5', ['node']);
    expect(witnessCandidates([fresh], votes([]))).toEqual([]);
  });
});

describe('witnessJoinCommand', () => {
  const command = 'quoro join https://10.0.0.3:8443 --token njt_abc --fingerprint 4f1c';

  it('adds --role witness, since a join token carries no role', () => {
    expect(witnessJoinCommand(command)).toBe(`${command} --role witness`);
  });

  it('replaces a role the command already names', () => {
    expect(witnessJoinCommand(`${command} --role node`)).toBe(`${command} --role witness`);
  });
});

describe('witnessProblem', () => {
  it('shows the server words of a conflict and says to wait and try again', () => {
    const detail =
      'the previous voter-set change has not reached a majority of voters yet; try again once they have acknowledged it';
    const problem = witnessProblem(new ApiProblem({ status: 409, code: 'conflict', detail }));
    expect(problem.view.message).toBe(detail);
    expect(problem.view.code).toBe('conflict');
    expect(problem.hint).toBe('Wait a few seconds and try again.');
  });

  it('reads other refusals as the panel does elsewhere', () => {
    const forbidden = witnessProblem(
      new ApiProblem({ status: 403, code: 'forbidden', reason: 'human_only', scope: 'node:roles' }),
    );
    expect(forbidden.view.message).toBe("Your role can't change the voters (human_only: node:roles)");
    expect(forbidden.hint).toBeNull();

    const offline = witnessProblem(new TypeError('Failed to fetch'));
    expect(offline.view.message).toBe('Could not reach the server. Check the connection and try again.');
    expect(offline.hint).toBeNull();
  });
});

describe('shortOfVotes', () => {
  // The HA view keeps a join token only while this holds (`HaVotes`).
  it('holds while HA lacks a vote, so a join token stays', () => {
    expect(shortOfVotes(votes([voter(lab3), voter(lab4)]))).toBe(true);
    expect(shortOfVotes(votes([voter(lab3)]))).toBe(true);
    expect(shortOfVotes(votes([]))).toBe(true);
  });

  it('ends once the count reaches required, before ha_ready, so the token goes', () => {
    expect(shortOfVotes(votes([voter(lab3), voter(lab4), voter(edge)], false))).toBe(false);
    expect(shortOfVotes(votes([voter(lab3), voter(lab4), voter(edge)], true))).toBe(false);
    expect(shortOfVotes(votes([voter(lab3), voter(lab4), voter(edge)], true, 2))).toBe(false);
  });

  it('counts a disconnected voter, as the server does', () => {
    expect(shortOfVotes(votes([voter(lab3), voter(lab4), voter(edge, false)]))).toBe(false);
  });

  it('does not hold with no votes yet', () => {
    expect(shortOfVotes(undefined)).toBe(false);
  });

  it('agrees with the readout', () => {
    for (const list of [[], [voter(lab3)], [voter(lab3), voter(lab4)], [voter(lab3), voter(lab4), voter(edge)]]) {
      const current = votes(list);
      expect(shortOfVotes(current)).toBe(haReadout(current, []).state === 'short');
    }
  });
});

describe('dropTokenUnlessShort', () => {
  const calls = (current: ClusterVotes | undefined): number => {
    let dropped = 0;
    dropTokenUnlessShort(current, () => {
      dropped += 1;
    });
    return dropped;
  };

  it('keeps the token while HA is short of a vote', () => {
    expect(calls(votes([voter(lab3), voter(lab4)]))).toBe(0);
    expect(calls(votes([]))).toBe(0);
  });

  it('drops it once the count reaches required, a disconnected voter included, before ha_ready', () => {
    expect(calls(votes([voter(lab3), voter(lab4), voter(edge, false)], false))).toBe(1);
    expect(calls(votes([voter(lab3), voter(lab4), voter(edge)], true))).toBe(1);
  });

  it('drops it with no votes, so no token outlives the offer', () => {
    expect(calls(undefined)).toBe(1);
  });
});

describe('haVotesState', () => {
  const current = votes([voter(lab3), voter(lab4)]);
  const offline = new TypeError('Failed to fetch');

  it('shows the votes when they arrived', () => {
    expect(haVotesState({ data: { votes: current }, error: null, isLoading: false })).toEqual({
      kind: 'votes',
      votes: current,
      stale: null,
    });
  });

  it('keeps the last votes when a refetch fails, and says why beside them', () => {
    const state = haVotesState({ data: { votes: current }, error: offline, isLoading: false });
    expect(state.kind).toBe('votes');
    if (state.kind !== 'votes') return;
    expect(state.votes).toBe(current);
    expect(state.stale?.message).toBe('Could not reach the server. Check the connection and try again.');
  });

  it('says why when the call fails before any votes arrived', () => {
    const forbidden = new ApiProblem({ status: 403, code: 'forbidden', reason: 'scope_missing' });
    expect(haVotesState({ data: undefined, error: forbidden, isLoading: false })).toEqual({
      kind: 'failed',
      problem: { message: "Your role can't see the voters (scope_missing)", code: 'forbidden', reason: 'scope_missing' },
    });
  });

  it('is loading until the first answer, and hidden while the query is off', () => {
    expect(haVotesState({ data: undefined, error: null, isLoading: true })).toEqual({ kind: 'loading' });
    expect(haVotesState({ data: undefined, error: null, isLoading: false })).toEqual({ kind: 'hidden' });
  });
});

describe('candidateNote', () => {
  it('says nothing about a connected machine', () => {
    expect(candidateNote(edge)).toBeNull();
  });

  it('says what adding the role does to a machine that is not connected, without repeating its lamp', () => {
    const note = 'It gets the role now and can vote once its node agent connects.';
    expect(candidateNote(node('lab-edge', ['edge'], { status: 'disconnected' }))).toBe(note);
    expect(candidateNote(node('lab-edge', ['edge'], { status: 'pending' }))).toBe(note);
    expect(candidateNote(node('lab-edge', ['edge'], { status: 'fenced' }))).toBe(note);
  });
});

describe('disconnectedVoters', () => {
  it('names the voters that are not connected, in their order', () => {
    const current = votes([voter(lab3), voter(lab4, false), voter(edge, false)]);
    expect(disconnectedVoters(haReadout(current, [lab3, lab4, edge]).seats)).toEqual(['lab-4', 'lab-edge']);
  });

  it('is empty when every voter is connected', () => {
    expect(disconnectedVoters(haReadout(votes([voter(lab3), voter(lab4)]), [lab3, lab4]).seats)).toEqual([]);
  });
});

describe('refreshVoters', () => {
  const invalidated = (client: QueryClient, key: readonly unknown[]): boolean | undefined =>
    client.getQueryState(key)?.isInvalidated;

  it('reads the votes and every page of the node list again, and leaves other lists alone', async () => {
    const client = new QueryClient();
    const nodesPage = getListNodesQueryKey({ limit: 500 });
    client.setQueryData(getGetClusterQueryKey(), { votes: votes([voter(lab3)]) });
    client.setQueryData(getListNodesQueryKey(), { items: [lab3] });
    client.setQueryData(nodesPage, { items: [lab3] });
    client.setQueryData(getListVmsQueryKey(), { items: [] });

    await refreshVoters(client);

    expect(invalidated(client, getGetClusterQueryKey())).toBe(true);
    expect(invalidated(client, getListNodesQueryKey())).toBe(true);
    expect(invalidated(client, nodesPage)).toBe(true);
    expect(invalidated(client, getListVmsQueryKey())).toBe(false);
  });
});
