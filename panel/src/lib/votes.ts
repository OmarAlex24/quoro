// SPDX-License-Identifier: AGPL-3.0-only
import { isApiProblem } from '../api/fetcher';
import type { ClusterVotes, Node } from '../api/generated/model';
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
  name: string;
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
  votes.voters.map((voter) => ({
    nodeId: voter.node_id,
    name: nodes.find((node) => node.id === voter.node_id)?.name ?? shortId(voter.node_id),
    roles: [...voter.roles],
    connected: voter.connected,
    tone: voter.connected ? 'run' : 'fault',
    words: voter.connected ? 'Connected' : 'Disconnected',
  }));

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
