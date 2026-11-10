// SPDX-License-Identifier: AGPL-3.0-only
import { useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { getListNodesQueryKey } from '../api/generated/nodes/nodes';
import type { App, Node, Vm } from '../api/generated/model';
import { nodeLoad } from '../lib/node-capacity';
import { simulateOnCards, vmCells } from '../lib/nodes-page';
import { AddNodeInvite } from './add-node';
import { LiveNodesProvider } from './live-nodes';
import { useLossSimulation } from './n1-strip';
import { NodeCard, NodeCardSkeleton } from './node-card';
import { NodeEvents } from './node-events';
import { NodesBand, NodesBandSkeleton } from './nodes-band';
import { Skeleton } from './skeleton';

/**
 * The Nodes page under the HA view: the cluster band, one card per machine
 * (two across on a wide screen), then the node events and the invitation to
 * add a node. "Simulate losing <node>" draws the primary's answer on the
 * cards until it is stopped. Each connected node's own metrics stream is
 * opened once, here, and read by its card.
 */
export function NodesBoard({
  nodes,
  vms,
  apps,
}: {
  nodes: readonly Node[];
  vms: readonly Vm[];
  apps: readonly App[];
}) {
  const loss = useLossSimulation(nodes);
  const [simulating, setSimulating] = useState(false);
  // Drawn only while the answer is the one for the node the button names.
  const simulated =
    simulating && loss.answer && loss.chosen && loss.answer.lost_node_id === loss.chosen.id ? loss.answer : null;
  const cards = useMemo(() => simulateOnCards(nodes, simulated, apps), [nodes, simulated, apps]);
  // "Seen N s ago" as the server said it when the list was read, not a clock that runs on between reads.
  const readAt = useQueryClient().getQueryState(getListNodesQueryKey())?.dataUpdatedAt || Date.now();

  return (
    <LiveNodesProvider nodes={nodes}>
      <NodesBand nodes={nodes} apps={apps} loss={loss} simulated={simulated} onSimulate={setSimulating} />
      <div className="ncards">
        {nodes.map((node) => (
          <NodeCard
            key={node.id}
            node={node}
            load={nodeLoad(node.id, vms)}
            readAt={readAt}
            cells={vmCells(node.id, vms, apps)}
            lost={cards[node.id]?.lost}
            ghosts={cards[node.id]?.ghosts}
            reserved={cards[node.id]?.reserved}
          />
        ))}
      </div>
      <div className="nunder">
        <NodeEvents nodes={nodes} />
        <AddNodeInvite />
      </div>
    </LiveNodesProvider>
  );
}

/** The band and two cards while the node list loads, as one status region. */
export function NodesBoardSkeleton() {
  return (
    <Skeleton label="Loading nodes" className="nodes-skel">
      <NodesBandSkeleton />
      <div className="ncards">
        <NodeCardSkeleton />
        <NodeCardSkeleton />
      </div>
    </Skeleton>
  );
}
