import { describe, expect, it } from 'vitest';
import { ApiProblem } from '../api/fetcher';
import type { ClusterVoter, ClusterVotes, Node } from '../api/generated/model';
import {
  haReadout,
  missingVoteWords,
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
