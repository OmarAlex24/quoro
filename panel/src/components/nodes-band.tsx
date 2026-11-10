// SPDX-License-Identifier: AGPL-3.0-only
import { ShieldAlert, ShieldCheck } from 'lucide-react';
import { useGetCluster } from '../api/generated/cluster/cluster';
import type { App, ClusterCapacity, Node } from '../api/generated/model';
import { n1Readout, type Segment } from '../lib/node-capacity';
import { clusterFacts, simulationSentence } from '../lib/nodes-page';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { SkelBar, Skeleton } from './skeleton';
import { Button } from './ui/button';
import { Select, SelectContent, SelectItem, SelectTrigger } from './ui/select';
import type { useLossSimulation } from './n1-strip';

type LossSimulation = ReturnType<typeof useLossSimulation>;

/**
 * The cluster band over the node cards. On the left, the N+1 sentence from
 * the primary's own simulation of a loss (`getClusterCapacity`, ADR 0178) and
 * "Simulate losing <node>", which draws that answer on the cards until it is
 * stopped; the chevron beside it picks which node the question is about. On
 * the right, the primary, the term, the voters and the version (`getCluster`).
 */
export function NodesBand({
  nodes,
  apps,
  loss,
  simulated,
  onSimulate,
}: {
  nodes: readonly Node[];
  apps: readonly App[];
  loss: LossSimulation;
  /** The answer drawn on the cards now, or null when no loss is simulated. */
  simulated: ClusterCapacity | null;
  onSimulate: (on: boolean) => void;
}) {
  const { user } = useSession();
  // The same request as the HA view's, so both read one cache entry.
  const cluster = useGetCluster({ query: { enabled: user !== undefined, refetchInterval: 5_000, retry: false } });
  const { candidates, chosen, setPicked, answer, lost, loading } = loss;
  const readout = lost ? n1Readout({ lost, nodes, answer, apps }) : null;
  const simulatedLost = simulated ? nodes.find((node) => node.id === simulated.lost_node_id) : undefined;

  const segments: Segment[] | null =
    simulated && simulatedLost
      ? simulationSentence({ lost: simulatedLost, answer: simulated, nodes, apps })
      : (readout?.segments ?? null);
  // A loss that leaves an app without room stays red while it is drawn; one that fits is amber, a what-if.
  const tone = simulated ? (simulated.fits ? 'shift' : 'fault') : (readout?.tone ?? 'run');
  const Icon = tone === 'run' ? ShieldCheck : ShieldAlert;
  // The button acts on the answer for the node it names, never on one still loading.
  const ready = chosen !== undefined && answer?.lost_node_id === chosen.id && !loading;
  const facts = cluster.data ? clusterFacts(cluster.data, nodes) : null;

  return (
    <section className={cn('card nband', `nband-${tone}`)} data-sim={simulated ? '' : undefined} aria-label="Cluster">
      <div className="nband-ha" aria-busy={loading || undefined}>
        <span className="nband-sh" aria-hidden="true">
          <Icon strokeWidth={1.7} />
        </span>
        <div className="nband-t" aria-live="polite">
          {segments ? (
            segments.map((segment, index) =>
              typeof segment === 'string' ? segment : <b key={index}>{segment.strong}</b>,
            )
          ) : candidates.length > 1 ? (
            // The band is busy while the answer comes; the facts beside it say loading for both.
            <SkelBar width="min(420px, 100%)" />
          ) : (
            'Enroll a second node with the node role, and the cluster can lose one.'
          )}
        </div>
        {candidates.length > 1 && chosen ? (
          <div className="nband-act">
            <Button
              size="sm"
              aria-pressed={simulated !== null}
              disabled={simulated === null && !ready}
              onClick={() => onSimulate(simulated === null)}
            >
              {simulated ? 'Stop the simulation' : `Simulate losing ${chosen.name}`}
            </Button>
            <Select value={chosen.id} onValueChange={setPicked}>
              <SelectTrigger className="nband-pick" aria-label={`Node to simulate losing: ${chosen.name}`} />
              <SelectContent align="end">
                {candidates.map((node) => (
                  <SelectItem key={node.id} value={node.id}>
                    Simulate losing {node.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        ) : null}
      </div>
      {cluster.error ? null : !facts ? (
        <Skeleton label="Loading the cluster" className="nband-cl">
          {['Primary', 'Term', 'Voters', 'Version'].map((label) => (
            <div key={label}>
              <span className="nband-skel-k">{label}</span>
              <SkelBar width="40px" />
            </div>
          ))}
        </Skeleton>
      ) : (
        <dl className="nband-cl">
          {facts.map((fact) => (
            <div key={fact.label}>
              <dt>{fact.label}</dt>
              <dd>
                {fact.spoken ? (
                  <>
                    <span aria-hidden="true">{fact.value}</span>
                    <span className="sr-only">{fact.spoken}</span>
                  </>
                ) : (
                  fact.value
                )}
              </dd>
            </div>
          ))}
        </dl>
      )}
    </section>
  );
}

/** The band's shape while the nodes load: the shield, a sentence bar and the four facts. Inside a `Skeleton`. */
export function NodesBandSkeleton() {
  return (
    <div className="card nband">
      <div className="contents">
        <div className="nband-ha">
          <SkelBar className="nband-skel-sh" />
          <span className="nband-t">
            <SkelBar width="min(420px, 100%)" />
          </span>
        </div>
        <div className="nband-cl">
          {['Primary', 'Term', 'Voters', 'Version'].map((label) => (
            <div key={label}>
              <span className="nband-skel-k">{label}</span>
              <SkelBar width="40px" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
