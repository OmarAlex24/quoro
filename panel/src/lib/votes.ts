// SPDX-License-Identifier: AGPL-3.0-only
import type { QueryClient, UseMutationOptions } from '@tanstack/react-query';
import { isApiProblem } from '../api/fetcher';
import { getGetClusterQueryKey } from '../api/generated/cluster/cluster';
import type { Cluster, ClusterVotes, Node } from '../api/generated/model';
import { getListNodesQueryKey, updateNodeRoles } from '../api/generated/nodes/nodes';
import { shortId } from './agency-format';
import type { LampTone } from './lamp';
import { problemView, type ProblemView } from './problem';

/*
 * The HA view (ADR 0017, ADR 0170): the voters of the cluster against the
 * votes HA needs, and, while some are missing, the offer of a witness.
 */

/**
 * - `short`: fewer voters than `required`; the view offers a witness.
 * - `waiting`: enough voters, but the primary does not hold a majority's
 *   support yet, as for a few seconds after a voter joins.
 * - `ready`: `ha_ready`.
 */
export type HaState = 'short' | 'waiting' | 'ready';

/** One voter as the view shows it: its name, its roles and its session. */
export interface VoterSeat {
  nodeId: string;
  /** Its name in the node list, or its short id while the list has none. */
  name: string;
  /**
   * Whether the node list names it. A machine that has just joined is in the
   * votes (read every 5 s) a few seconds before it is in the node list (every
   * 10 s).
   */
  named: boolean;
  roles: string[];
  connected: boolean;
  tone: LampTone;
  words: string;
}

export interface HaReadout {
  state: HaState;
  headline: string;
  line: string;
  /** "2 of 3 votes". */
  tally: string;
  seats: VoterSeat[];
  /** Seats still empty: `required - count`, never below zero. */
  open: number;
}

const ORDINALS = ['first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth', 'ninth'];
const NUMBERS = ['no', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];

const ordinal = (n: number): string => ORDINALS[n - 1] ?? `${n}th`;
const number = (n: number): string => NUMBERS[n] ?? String(n);
const plural = (n: number, one: string, many: string): string => (n === 1 ? one : many);

/**
 * What HA lacks, said plainly: `HA needs a third vote` when one is missing,
 * `HA needs two more votes` when more are. Null when none is missing.
 */
export const missingVoteWords = (count: number, required: number): string | null => {
  const missing = required - count;
  if (missing <= 0) return null;
  if (missing === 1) return `HA needs a ${ordinal(required)} vote`;
  return `HA needs ${number(missing)} more votes`;
};

/** The count against `required`: `2 of 3 votes`, or `4 votes, 3 needed` above it. */
export const tallyWords = (count: number, required: number): string =>
  count > required
    ? `${count} votes, ${required} needed`
    : `${count} of ${required} ${plural(required, 'vote', 'votes')}`;

/** Each voter with its name from the nodes list, or its short id while the list has none. */
export const voterSeats = (votes: ClusterVotes, nodes: readonly Node[]): VoterSeat[] =>
  votes.voters.map((voter) => {
    const node = nodes.find((candidate) => candidate.id === voter.node_id);
    return {
      nodeId: voter.node_id,
      name: node?.name ?? shortId(voter.node_id),
      named: node !== undefined,
      roles: [...voter.roles],
      connected: voter.connected,
      tone: voter.connected ? 'run' : 'fault',
      words: voter.connected ? 'Connected' : 'Disconnected',
    };
  });

/**
 * Whether HA is short of a vote, so the view offers a witness. A join token
 * lives only as long as that offer: false with no votes yet, false once
 * `count` reaches `required`.
 */
export const shortOfVotes = (votes: ClusterVotes | undefined): boolean =>
  votes !== undefined && missingVoteWords(votes.count, votes.required) !== null;

/**
 * The HA view calls this whenever the votes change: the join token goes with
 * the offer, so once HA is no longer short of a vote `drop` (the token
 * mutation's `reset`) runs and nothing shows that token again, even if HA
 * falls short later.
 */
export const dropTokenUnlessShort = (votes: ClusterVotes | undefined, drop: () => void): void => {
  if (!shortOfVotes(votes)) drop();
};

/** What the HA view says about `Cluster.votes`. */
export const haReadout = (votes: ClusterVotes, nodes: readonly Node[]): HaReadout => {
  const { count, required } = votes;
  const seats = voterSeats(votes, nodes);
  const open = Math.max(0, required - count);
  const tally = tallyWords(count, required);
  const missing = missingVoteWords(count, required);

  if (missing !== null) {
    return {
      state: 'short',
      headline: missing,
      line:
        count === 0
          ? 'No machine votes yet.'
          : count === 1
            ? 'With one voter, losing it leaves no majority.'
            : `With ${number(count)} voters, losing one leaves no majority.`,
      tally,
      seats,
      open,
    };
  }
  if (!votes.ha_ready) {
    return {
      state: 'waiting',
      headline: 'Waiting for a majority',
      line: `${tally}. HA is ready once a majority of the voters supports the primary.`,
      tally,
      seats,
      open,
    };
  }
  return {
    state: 'ready',
    headline: 'HA is ready',
    line: `${tally}, and a majority supports the primary.`,
    tally,
    seats,
    open,
  };
};

/**
 * The enrolled machines that can gain a vote with the `witness` role: not
 * removed and not voting already. An edge-only machine is the typical one
 * (ADR 0018).
 */
export const witnessCandidates = (nodes: readonly Node[], votes: ClusterVotes): Node[] => {
  const voting = new Set(votes.voters.map((voter) => voter.node_id));
  return nodes.filter(
    (node) =>
      node.status !== 'removed' &&
      !voting.has(node.id) &&
      !node.roles.some((role) => role === 'node' || role === 'witness'),
  );
};

/**
 * What to know before giving the witness role to a candidate that is not
 * connected, or null for a connected one. The server takes the role either
 * way; the machine acts on it from its next desired state, so it votes only
 * once its node agent connects. The view shows it beside the button only, and
 * its lamp already says "Not connected".
 */
export const candidateNote = (node: Pick<Node, 'status'>): string | null =>
  node.status === 'connected' ? null : 'It gets the role now and can vote once its node agent connects.';

/** One machine, or the machines not named yet, in the line about disconnected voters. */
export interface AwayVoter {
  /** A machine's name, or "the new machine". */
  words: string;
  /** Whether `words` is a machine's name, which the line sets in bold. */
  named: boolean;
}

/**
 * The voters that are not connected, for the line that says what to do about
 * them. The server counts a voter connected or not, so HA can reach its
 * count, and even be ready, while one of them still waits for its agent.
 *
 * Each named voter by its name, in order. A voter the node list does not name
 * yet is a machine that has just joined: the line calls it "the new machine"
 * (two or more: "the new machines", once, at the end), never by a cut-off id.
 */
export const disconnectedVoters = (seats: readonly VoterSeat[]): AwayVoter[] => {
  const away = seats.filter((seat) => !seat.connected);
  const named: AwayVoter[] = away.filter((seat) => seat.named).map((seat) => ({ words: seat.name, named: true }));
  const unnamed = away.length - named.length;
  if (unnamed === 0) return named;
  return [...named, { words: unnamed === 1 ? 'the new machine' : 'the new machines', named: false }];
};

/**
 * What the HA view draws for `getCluster`:
 * - `votes`: the last votes the server sent. `stale` says why the newest
 *   refetch failed, beside them, or is null; one failed refetch does not
 *   hide votes that are likely still true.
 * - `failed`: the call failed and no votes ever arrived.
 * - `loading`: the first answer is on its way.
 * - `hidden`: nothing to show, as while the query is disabled.
 */
export type HaVotesState =
  | { kind: 'votes'; votes: ClusterVotes; stale: ProblemView | null }
  | { kind: 'failed'; problem: ProblemView }
  | { kind: 'loading' }
  | { kind: 'hidden' };

/** From the `getCluster` query as `useGetCluster` returns it, passed whole. */
export const haVotesState = (cluster: {
  data?: Pick<Cluster, 'votes'>;
  error: unknown;
  isLoading: boolean;
}): HaVotesState => {
  const votes = cluster.data?.votes;
  const { error } = cluster;
  const problem = error === null || error === undefined ? null : problemView(error, 'see the voters');
  if (votes) return { kind: 'votes', votes, stale: problem };
  if (problem) return { kind: 'failed', problem };
  return cluster.isLoading ? { kind: 'loading' } : { kind: 'hidden' };
};

/**
 * After a roles call, answered or refused: read the cluster's votes and the
 * node list again, since both carry a machine's roles. Every page of the
 * node list goes, whatever its parameters; nothing else does.
 */
export const refreshVoters = (queryClient: Pick<QueryClient, 'invalidateQueries'>): Promise<unknown> =>
  Promise.all([
    queryClient.invalidateQueries({ queryKey: getGetClusterQueryKey() }),
    queryClient.invalidateQueries({ queryKey: getListNodesQueryKey() }),
  ]);

/**
 * The HA view's roles mutation, passed to `useMutation` as is: add `witness`
 * to an enrolled machine. `adding` names the machine while the call is in
 * flight, then null. Answered or refused, the voters are read again.
 */
export const addWitnessMutation = (
  queryClient: Pick<QueryClient, 'invalidateQueries'>,
  adding: (nodeId: string | null) => void,
): UseMutationOptions<Node, Error, Node> => ({
  mutationFn: (node) => updateNodeRoles(node.id, { add: ['witness'] }),
  onMutate: (node) => adding(node.id),
  onSettled: () => {
    adding(null);
    void refreshVoters(queryClient);
  },
});

/**
 * The join command for a new witness. A join token carries no role
 * (`NodeJoinTokenRequest` has only `expires_at`); the role is the command's
 * `--role` flag, so the panel sets it to `witness`.
 */
export const witnessJoinCommand = (joinCommand: string): string => {
  const command = joinCommand.trim();
  if (/\s--role\s+\S+/.test(command)) return command.replace(/(\s--role\s+)\S+/, '$1witness');
  return `${command} --role witness`;
};

/** A refused roles call: the panel's usual words, and what to do about a conflict. */
export interface WitnessProblem {
  view: ProblemView;
  /** Direction under the message, or null. */
  hint: string | null;
}

/**
 * `409 conflict` on the roles call means the previous voter change has not
 * reached a majority of voters yet (section 8.10): the server's own words,
 * then wait and try again. Anything else reads as it does everywhere else.
 */
export const witnessProblem = (error: unknown): WitnessProblem => {
  const view = problemView(error, 'change the voters');
  if (isApiProblem(error) && error.code === 'conflict') {
    return { view, hint: 'Wait a few seconds and try again.' };
  }
  return { view, hint: null };
};
