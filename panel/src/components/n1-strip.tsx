// SPDX-License-Identifier: AGPL-3.0-only
import { keepPreviousData } from '@tanstack/react-query';
import { ShieldAlert, ShieldCheck } from 'lucide-react';
import { useState } from 'react';
import { useGetClusterCapacity } from '../api/generated/cluster/cluster';
import type { App, Node } from '../api/generated/model';
import { biggestNode, lossCandidates, n1Readout } from '../lib/node-capacity';
import { cn } from '../lib/utils';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';

/**
 * The N+1 strip: one sentence on whether the cluster can lose its biggest
 * node, from the primary's own simulation of that loss (`getClusterCapacity`,
 * ADR 0178). "Simulate losing" asks the same question about another node; it
 * is a read, and nothing is reserved. With no answer, the strip is not shown.
 */
export function N1Strip({ nodes, apps }: { nodes: readonly Node[]; apps: readonly App[] }) {
  const candidates = lossCandidates(nodes);
  const biggest = biggestNode(nodes);
  const [picked, setPicked] = useState<string | null>(null);
  const chosen = candidates.find((node) => node.id === picked) ?? biggest;
  const ask = chosen !== undefined && candidates.length > 1;

  const capacity = useGetClusterCapacity(
    { lost_node_id: chosen?.id ?? '' },
    { query: { enabled: ask, retry: false, refetchInterval: 30_000, placeholderData: keepPreviousData } },
  );
  const answer = capacity.error ? undefined : capacity.data;
  // While another node's answer loads, the sentence about the last one stays, dimmed.
  const lost = (answer && nodes.find((node) => node.id === answer.lost_node_id)) || chosen;
  if (!lost) return null;
  const readout = n1Readout({ lost, nodes, answer, apps });
  if (!readout) return null;

  const Icon = readout.tone === 'run' ? ShieldCheck : ShieldAlert;
  return (
    <section
      className={cn('n1', `n1-${readout.tone}`)}
      aria-label="If a node goes down"
      aria-busy={capacity.isPlaceholderData || undefined}
    >
      <span className="n1-gl" aria-hidden="true">
        <Icon strokeWidth={1.7} />
      </span>
      <p className="n1-t" aria-live="polite">
        {readout.segments.map((segment, index) =>
          typeof segment === 'string' ? segment : <b key={index}>{segment.strong}</b>,
        )}
      </p>
      {candidates.length > 1 && chosen ? (
        <Select value={chosen.id} onValueChange={setPicked}>
          <SelectTrigger className="n1-pick" aria-label="Node to simulate losing">
            <SelectValue />
          </SelectTrigger>
          <SelectContent align="end">
            {candidates.map((node) => (
              <SelectItem key={node.id} value={node.id}>
                Simulate losing {node.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      ) : null}
    </section>
  );
}
