// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ShieldAlert, ShieldCheck, ShieldEllipsis } from 'lucide-react';
import { Fragment, useEffect, useState } from 'react';
import { useGetCluster } from '../api/generated/cluster/cluster';
import type { Node, NodeJoinTokenCreated } from '../api/generated/model';
import { expiryWords } from '../lib/agency-format';
import { canChangeVoters } from '../lib/capabilities';
import { joinTokenMutation, releasePage } from '../lib/join-token';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import {
  addWitnessMutation,
  candidateNote,
  disconnectedVoters,
  dropTokenUnlessShort,
  haReadout,
  haVotesState,
  witnessCandidates,
  witnessJoinCommand,
  witnessProblem,
  type HaReadout,
  type VoterSeat,
  type WitnessProblem,
} from '../lib/votes';
import { useSession } from '../session';
import { LampDot } from './lamp';
import { SecretOnce } from './secret-once';
import { SkelBar, Skeleton } from './skeleton';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';
import { ProblemBanner } from './ui/problem-banner';
import { Tag } from './ui/tag';

const WITNESS_NOTE =
  'A witness only votes. It stores no application data, runs no VMs and needs no KVM. Keep it out of the failure domain of the nodes: a machine that shares their power or network goes down with them.';

/**
 * The HA view (ADR 0017, ADR 0170): the voters of the cluster against the
 * votes HA needs. Below `required` it says that HA needs another vote and
 * offers a witness, on an enrolled machine through the roles operation or on
 * a new one with a join token; once HA is ready it says so in one line.
 */
export function HaVotes({ nodes }: { nodes: readonly Node[] }) {
  const { user, me } = useSession();
  const queryClient = useQueryClient();
  // Its own interval: `ha_ready` follows a majority's support within seconds.
  const cluster = useGetCluster({
    query: { enabled: user !== undefined, refetchInterval: 5_000, retry: false },
  });
  const [addingId, setAddingId] = useState<string | null>(null);

  const addWitness = useMutation(addWitnessMutation(queryClient, setAddingId));
  // The secret lives in this mutation's result only; `reset()` drops it.
  const joinToken = useMutation(joinTokenMutation());

  const votes = cluster.data?.votes;
  const dropToken = joinToken.reset;
  // The secret goes with the offer: once HA has its votes, nothing shows it again.
  useEffect(() => dropTokenUnlessShort(votes, dropToken), [votes, dropToken]);

  const state = haVotesState(cluster);
  if (state.kind === 'loading') return <HaVotesSkeleton />;
  if (state.kind === 'failed') return <ProblemBanner view={state.problem} className="mb-4" />;
  if (state.kind === 'hidden') return null;

  return (
    <HaVotesView
      readout={haReadout(state.votes, nodes)}
      candidates={witnessCandidates(nodes, state.votes)}
      version={cluster.data?.version}
      canChange={canChangeVoters(me)}
      addingId={addingId}
      busy={addWitness.isPending || joinToken.isPending}
      problem={addWitness.error ? witnessProblem(addWitness.error) : null}
      joinToken={joinToken.data ?? null}
      tokenProblem={joinToken.error ? problemView(joinToken.error, 'create join tokens') : null}
      stale={state.stale}
      onAddWitness={(node) => {
        joinToken.reset();
        addWitness.mutate(node);
      }}
      onCreateToken={() => {
        addWitness.reset();
        joinToken.mutate();
      }}
      onDropToken={() => joinToken.reset()}
    />
  );
}

/** The view while the first votes load: the one-line strip HA settles into, as dither. */
function HaVotesSkeleton() {
  return (
    <div className="n1 ha-skel">
      <span className="n1-gl" aria-hidden="true" />
      <Skeleton label="Loading the voters" className="ha-skel-f">
        <SkelBar width="min(320px, 70%)" />
        <SkelBar width="min(200px, 45%)" height={10} />
      </Skeleton>
    </div>
  );
}

export interface HaVotesViewProps {
  readout: HaReadout;
  candidates: readonly Node[];
  /** The cluster's version, which a new witness's `quoro` binary should match. */
  version?: string;
  canChange: boolean;
  /** The machine whose roles call is in flight. */
  addingId: string | null;
  /** A voter change or a token is on its way: one voter change at a time. */
  busy: boolean;
  problem: WitnessProblem | null;
  joinToken: NodeJoinTokenCreated | null;
  tokenProblem: ProblemView | null;
  /** The newest refetch failed: the view shows the last votes and says so. */
  stale?: ProblemView | null;
  onAddWitness: (node: Node) => void;
  onCreateToken: () => void;
  onDropToken: () => void;
}

/** What the HA view draws, from props only. */
export function HaVotesView(props: HaVotesViewProps) {
  const { readout, stale = null } = props;
  if (readout.state !== 'short') return <HaSettled readout={readout} stale={stale} />;

  return (
    <section className="ha" aria-labelledby="ha-title">
      <div className="ha-h">
        <span className="n1-gl ha-gl" aria-hidden="true">
          <ShieldAlert strokeWidth={1.7} />
        </span>
        <div className="ha-ht">
          <h2 id="ha-title" className="ha-title">
            {readout.headline}
          </h2>
          <p className="ha-line">{readout.line}</p>
          <StaleNote problem={stale} />
        </div>
        <span className="ha-tally">{readout.tally}</span>
      </div>

      <ul className="ha-seats" aria-label="Voters">
        {readout.seats.map((seat) => (
          <Seat key={seat.nodeId} seat={seat} />
        ))}
        {Array.from({ length: readout.open }, (_, index) => (
          <li key={`open-${index}`} className="ha-seat ha-seat-open">
            <b>No voter</b>
            <span>A witness can take this vote</span>
          </li>
        ))}
      </ul>

      <WitnessOffer {...props} />
    </section>
  );
}

function Seat({ seat }: { seat: VoterSeat }) {
  return (
    <li className="ha-seat">
      <span className="ha-seat-n">
        <ToneLamp tone={seat.tone} size={13} label={seat.words} />
        <b>{seat.name}</b>
      </span>
      <span className="ha-seat-r">
        {seat.roles.map((role) => (
          <Tag key={role} tone={role === 'witness' ? 'accent' : 'plain'}>
            {role}
          </Tag>
        ))}
        <span className={cn('ha-seat-w', !seat.connected && 'text-fault')}>{seat.words}</span>
      </span>
      {seat.connected ? null : (
        <p className="next-step">
          Make sure its node agent, <code className="type-code">quoro node</code>, is running on that machine.
        </p>
      )}
    </li>
  );
}

/** The two ways to a third vote: an enrolled machine gains the role, or a new one joins as a witness. */
function WitnessOffer({
  candidates,
  version,
  canChange,
  addingId,
  busy,
  problem,
  joinToken,
  tokenProblem,
  onAddWitness,
  onCreateToken,
  onDropToken,
}: HaVotesViewProps) {
  return (
    <div className="ha-offer">
      <div className="ha-way">
        <h3 className="ha-way-t">Add a witness on an enrolled machine</h3>
        <p className="ha-way-p">The machine keeps its roles and is not enrolled again. An edge is the usual choice.</p>
        {candidates.length === 0 ? (
          <p className="ha-way-p">Every enrolled machine votes already. Enroll a new one as the witness.</p>
        ) : (
          <ul className="ha-cands" aria-label="Machines without a vote">
            {candidates.map((node) => (
              <Candidate
                key={node.id}
                node={node}
                canChange={canChange}
                adding={addingId === node.id}
                busy={busy}
                onAdd={() => onAddWitness(node)}
              />
            ))}
          </ul>
        )}
        {problem ? (
          <div className="grid gap-1.5">
            <ProblemBanner view={problem.view} />
            {problem.hint ? <p className="ha-hint">{problem.hint}</p> : null}
          </div>
        ) : null}
      </div>

      <div className="ha-way">
        <h3 className="ha-way-t">Enroll a new machine as the witness</h3>
        <p className="ha-way-p">
          A small machine is enough, with iproute2 and nftables installed. From the archive of{' '}
          <a href={releasePage(version)} target="_blank" rel="noreferrer" className="set-inline-link">
            {version ? `release ${version}` : 'the release'}
          </a>
          {version ? ', the cluster’s version,' : ' at the cluster’s version,'} put{' '}
          <code className="type-code">quoro</code> in <code className="type-code">/usr/local/bin</code> and{' '}
          <code className="type-code">quoro-node.service</code> in{' '}
          <code className="type-code">/etc/systemd/system</code>. The join token is for one machine and lasts an hour.
        </p>
        {joinToken ? (
          <div className="grid gap-2">
            <SecretOnce
              label="Run this as root on the new machine"
              secret={witnessJoinCommand(joinToken.join_command)}
              copyLabel="Copy command"
              note={`The token is shown only this once and expires ${expiryWords(joinToken.expires_at)}.`}
            />
            <p className="next-step">
              Then start its node agent, as root too:{' '}
              <code className="type-code">systemctl enable --now quoro-node</code>. Until the agent connects, the
              witness cannot vote.
            </p>
            <Button size="sm" className="justify-self-start" onClick={onDropToken}>
              Done
            </Button>
          </div>
        ) : canChange ? (
          <Button size="sm" className="justify-self-start" disabled={busy} onClick={onCreateToken}>
            Create a join token
          </Button>
        ) : null}
        {tokenProblem ? <ProblemBanner view={tokenProblem} /> : null}
      </div>

      {canChange ? null : (
        <p className="ha-way-p ha-span">An owner or an operator can add the witness role or create a join token.</p>
      )}
      <p className="ha-note ha-span">{WITNESS_NOTE}</p>
    </div>
  );
}

/**
 * A machine that can gain the vote. One that is not connected can too (the
 * server takes the role either way); beside the button, a note says that it
 * votes only once its agent connects. Without the button there is no note.
 */
function Candidate({
  node,
  canChange,
  adding,
  busy,
  onAdd,
}: {
  node: Node;
  canChange: boolean;
  adding: boolean;
  busy: boolean;
  onAdd: () => void;
}) {
  const connected = node.status === 'connected';
  const note = canChange ? candidateNote(node) : null;
  const noteId = `ha-cand-note-${node.id}`;
  return (
    <li className="ha-cand">
      <ToneLamp tone={connected ? 'run' : 'fault'} size={11} label={connected ? 'Connected' : 'Not connected'} />
      <b>{node.name}</b>
      {node.roles.map((role) => (
        <Tag key={role} tone="plain">
          {role}
        </Tag>
      ))}
      {canChange ? (
        <Button
          size="sm"
          className="ml-auto"
          disabled={busy}
          aria-describedby={note ? noteId : undefined}
          onClick={onAdd}
        >
          {adding ? 'Adding the witness role…' : 'Add the witness role'}
        </Button>
      ) : null}
      {note ? (
        <p id={noteId} className="ha-cand-note">
          {note}
        </p>
      ) : null}
    </li>
  );
}

/**
 * The newest refetch failed: the last votes stay, with this beside them. The
 * status element stays mounted, empty and out of sight while the votes are
 * current, so a screen reader announces the note when it fills.
 */
function StaleNote({ problem }: { problem: ProblemView | null }) {
  return (
    <p className={cn('ha-stale', !problem && 'sr-only')} role="status">
      {problem ? (
        <>
          <LampDot ring="fault" core="fault" size={9} />
          <span>These votes may be out of date. {problem.message}</span>
        </>
      ) : null}
    </p>
  );
}

/**
 * Enough voters: one quiet line, and the voters by name. A voter that is not
 * connected still counts, so under them a line says what it needs.
 */
function HaSettled({ readout, stale }: { readout: HaReadout; stale: ProblemView | null }) {
  const ready = readout.state === 'ready';
  const Icon = ready ? ShieldCheck : ShieldEllipsis;
  const away = disconnectedVoters(readout.seats);
  return (
    <section
      className={cn('n1 ha-quiet', !ready && 'n1-shift', (stale || away.length > 0) && 'ha-quiet-rows')}
      aria-labelledby="ha-title"
    >
      <span className="n1-gl" aria-hidden="true">
        <Icon strokeWidth={1.7} />
      </span>
      <p className="n1-t" aria-live="polite">
        <b id="ha-title">{readout.headline}.</b> {readout.line}
      </p>
      <ul className="ha-quiet-v" aria-label="Voters">
        {readout.seats.map((seat) => (
          <li key={seat.nodeId}>
            <ToneLamp tone={seat.tone} size={11} label={seat.words} />
            <b>{seat.name}</b>
            <span>{seat.roles.join(', ')}</span>
            {seat.connected ? null : <span className="text-fault">{seat.words}</span>}
          </li>
        ))}
      </ul>
      {away.length > 0 ? (
        <p className="next-step">
          Make sure the node agent, <code className="type-code">quoro node</code>, is running on{' '}
          {away.map((voter, index) => (
            <Fragment key={index}>
              {index === 0 ? null : index === away.length - 1 ? ' and ' : ', '}
              {voter.named ? <b>{voter.words}</b> : voter.words}
            </Fragment>
          ))}
          .
        </p>
      ) : null}
      <StaleNote problem={stale} />
    </section>
  );
}
