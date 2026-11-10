import type { Node, NodeStatus } from '../api/generated/model';
import type { LampTone } from './lamp';

export interface NodeReadout {
  tone: LampTone;
  words: string;
  pulse: boolean;
}

/** A node's session state as a lamp and a word. */
export const nodeReadout = (status: NodeStatus): NodeReadout => {
  switch (status) {
    case 'connected':
      return { tone: 'run', words: 'Connected', pulse: false };
    case 'pending':
      return { tone: 'shift', words: 'Connecting', pulse: true };
    case 'disconnected':
      return { tone: 'fault', words: 'Disconnected', pulse: false };
    case 'fenced':
      return { tone: 'fault', words: 'Fenced', pulse: false };
    case 'removed':
      return { tone: 'idle', words: 'Removed', pulse: false };
  }
};

export interface ClusterSummary {
  /** The node holding the primary status, or the first one when none says so. */
  primary?: Node;
  connected: number;
  /** Enrolled machines that are not removed. */
  total: number;
}

/** What the sidebar foot says about the cluster: its primary and how many nodes answer. */
export const clusterSummary = (nodes: readonly Node[]): ClusterSummary => {
  const enrolled = nodes.filter((node) => node.status !== 'removed');
  return {
    primary: enrolled.find((node) => node.cp_status === 'primary') ?? enrolled[0],
    connected: enrolled.filter((node) => node.status === 'connected').length,
    total: enrolled.length,
  };
};

export const connectedLine = ({ connected, total }: ClusterSummary): string =>
  total === 0
    ? 'No node enrolled'
    : `${connected} of ${total} ${total === 1 ? 'node' : 'nodes'} connected`;

export const primaryLine = (primary: Node): string =>
  `${primary.name} · ${primary.cp_status ?? 'node'}`;
