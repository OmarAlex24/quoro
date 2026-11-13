// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ShieldAlert, ShieldCheck, ShieldEllipsis } from 'lucide-react';
import { useEffect, useState } from 'react';
import { getGetClusterQueryKey, useGetCluster } from '../api/generated/cluster/cluster';
import type { Node, NodeJoinTokenCreated } from '../api/generated/model';
import { createNodeJoinToken, getListNodesQueryKey, updateNodeRoles } from '../api/generated/nodes/nodes';
import { expiryWords } from '../lib/agency-format';
import { canChangeVoters } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { cn } from '../lib/utils';
import {
  haReadout,
  witnessCandidates,
  witnessJoinCommand,
  witnessProblem,
  type HaReadout,
  type VoterSeat,
  type WitnessProblem,
} from '../lib/votes';
import { useSession } from '../session';
import { SecretOnce } from './secret-once';
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

  const addWitness = useMutation({
    mutationFn: (node: Node) => updateNodeRoles(node.id, { add: ['witness'] }),
    onMutate: (node) => setAddingId(node.id),
    onSettled: () => {
      setAddingId(null);
      void queryClient.invalidateQueries({ queryKey: getGetClusterQueryKey() });
      void queryClient.invalidateQueries({ queryKey: getListNodesQueryKey() });
    },
  });
  // The secret lives in this mutation's result only; `reset()` drops it.
  const joinToken = useMutation({
    mutationFn: () => createNodeJoinToken({}, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
  });

  const votes = cluster.data?.votes;
  const short = votes !== undefined && haReadout(votes, nodes).state === 'short';
  const dropToken = joinToken.reset;
  // The secret goes with the offer: once HA has its votes, nothing shows it again.
  useEffect(() => {
    if (!short) dropToken();
  }, [short, dropToken]);

  if (cluster.error) {
    return <ProblemBanner view={problemView(cluster.error, 'see the voters')} className="mb-4" />;
  }
  if (!votes) return null;

  return (
    <HaVotesView
      readout={haReadout(votes, nodes)}
      candidates={witnessCandidates(nodes, votes)}
      canChange={canChangeVoters(me)}
      addingId={addingId}
      busy={addWitness.isPending || joinToken.isPending}
      problem={addWitness.error ? witnessProblem(addWitness.error) : null}
      joinToken={joinToken.data ?? null}
      tokenProblem={joinToken.error ? problemView(joinToken.error, 'create join tokens') : null}
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

export interface HaVotesViewProps {
  readout: HaReadout;
  candidates: readonly Node[];
  canChange: boolean;
  /** The machine whose roles call is in flight. */
  addingId: string | null;
  /** A voter change or a token is on its way: one voter change at a time. */
  busy: boolean;
  problem: WitnessProblem | null;
  joinToken: NodeJoinTokenCreated | null;
  tokenProblem: ProblemView | null;
  onAddWitness: (node: Node) => void;
  onCreateToken: () => void;
  onDropToken: () => void;
}

/** What the HA view draws, from props only. */
export function HaVotesView(props: HaVotesViewProps) {
  const { readout } = props;
  if (readout.state !== 'short') return <HaSettled readout={readout} />;

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
    </li>
  );
}

/** The two ways to a third vote: an enrolled machine gains the role, or a new one joins as a witness. */
function WitnessOffer({
  candidates,
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
              <li key={node.id} className="ha-cand">
                <ToneLamp
                  tone={node.status === 'connected' ? 'run' : 'fault'}
                  size={11}
                  label={node.status === 'connected' ? 'Connected' : 'Not connected'}
                />
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
                    onClick={() => onAddWitness(node)}
                  >
                    {addingId === node.id ? 'Adding the witness role…' : 'Add the witness role'}
                  </Button>
                ) : null}
              </li>
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
        <p className="ha-way-p">A small machine is enough. The join token is for one machine and lasts an hour.</p>
        {joinToken ? (
          <div className="grid gap-2">
            <SecretOnce
              label="Run this on the new machine"
              secret={witnessJoinCommand(joinToken.join_command)}
              copyLabel="Copy command"
              note={`The token is shown only this once and expires ${expiryWords(joinToken.expires_at)}.`}
            />
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

/** Enough voters: one quiet line, and the voters by name. */
function HaSettled({ readout }: { readout: HaReadout }) {
  const ready = readout.state === 'ready';
  const Icon = ready ? ShieldCheck : ShieldEllipsis;
  return (
    <section className={cn('n1 ha-quiet', !ready && 'n1-shift')} aria-labelledby="ha-title">
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
    </section>
  );
}
