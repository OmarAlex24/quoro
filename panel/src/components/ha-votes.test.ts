import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { ApiProblem } from '../api/fetcher';
import type { ClusterVotes, Node, NodeJoinTokenCreated } from '../api/generated/model';
import { problemView } from '../lib/problem';
import { haReadout, witnessCandidates, witnessProblem } from '../lib/votes';
import { HaVotesView, type HaVotesViewProps } from './ha-votes';

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
    expect(html).toContain('quoro join https://10.0.0.3:8443 --token njt_secret --fingerprint 4f1c --role witness');
    expect(html).toContain('Copy command');
    expect(html).not.toContain('Create a join token');
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

  it('waits for a majority before ha_ready, still without an offer', () => {
    const html = render(votesOf([lab3, lab4, witness], false), nodes);
    expect(html).toContain('Waiting for a majority.');
    expect(html).toContain('n1-shift');
    expect(html).not.toContain('join token');
  });
});
