// SPDX-License-Identifier: AGPL-3.0-only
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import type { Node, NodeMetricSample } from '../api/generated/model';
import { useNodeMetricsStream, type MetricsStreamStatus } from '../lib/use-metrics-stream';

/** One node's own reading of its host: the last ten minutes and how its stream stands. */
export interface NodeFeedState {
  samples: NodeMetricSample[];
  status: MetricsStreamStatus;
}

const LiveNodesContext = createContext<Readonly<Record<string, NodeFeedState>>>({});

/**
 * Opens one metrics stream per connected node (`GET /nodes/{id}/metrics` and
 * its stream) and hands the samples to whatever reads `useLiveNodes`. The
 * page passes its content as `children`, so a new sample re-renders only the
 * parts that read the samples, never the project tiles.
 */
export function LiveNodesProvider({ nodes, children }: { nodes: readonly Node[]; children: ReactNode }) {
  const [feeds, setFeeds] = useState<Record<string, NodeFeedState>>({});
  const report = useCallback((id: string, state: NodeFeedState | null) => {
    setFeeds((current) => {
      if (state === null) {
        if (!(id in current)) return current;
        const { [id]: _gone, ...rest } = current;
        return rest;
      }
      return { ...current, [id]: state };
    });
  }, []);
  const live = useMemo(() => nodes.filter((node) => node.status === 'connected'), [nodes]);
  return (
    <LiveNodesContext.Provider value={feeds}>
      {live.map((node) => (
        <NodeFeed key={node.id} nodeId={node.id} report={report} />
      ))}
      {children}
    </LiveNodesContext.Provider>
  );
}

function NodeFeed({ nodeId, report }: { nodeId: string; report: (id: string, state: NodeFeedState | null) => void }) {
  const { samples, status } = useNodeMetricsStream(nodeId);
  useEffect(() => report(nodeId, { samples, status }), [nodeId, samples, status, report]);
  useEffect(() => () => report(nodeId, null), [nodeId, report]);
  return null;
}

/** Each connected node's samples and stream state, by node id. */
export const useLiveNodes = (): Readonly<Record<string, NodeFeedState>> => useContext(LiveNodesContext);
