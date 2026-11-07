import { useListNodes } from '../api/generated/nodes/nodes';
import type { NodeStatus } from '../api/generated/model';
import type { LampTone } from '../lib/lamp';
import { formatBytes } from '../lib/metrics';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { ClusterVulnerabilities } from '../components/cluster-vulnerabilities';
import { NodeMetricsBay } from '../components/metrics-bay';
import { LampDot } from '../components/lamp';
import { ToneLamp } from '../components/tone-lamp';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const MIB = 1024 * 1024;

interface NodeReadout {
  tone: LampTone;
  words: string;
  pulse: boolean;
}

const nodeReadout = (status: NodeStatus): NodeReadout => {
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

/** The machines of the cluster: their record, and their live metrics. */
export function NodesScreen() {
  const { user, loading, unauthenticated } = useSession();
  const nodesQuery = useListNodes(undefined, {
    query: { refetchInterval: 10_000 },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const nodes = nodesQuery.data?.items ?? [];
  const nodeNames = Object.fromEntries(nodes.map((node) => [node.id, node.name]));
  const connected = nodes.filter((node) => node.status === 'connected').length;
  const listError =
    nodesQuery.error !== null && nodesQuery.error !== undefined
      ? problemView(nodesQuery.error, 'see nodes')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="type-display text-[24px] leading-none">Nodes</h1>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px] text-ink-2">
        <span className="inline-flex items-center gap-1.5">
          <LampDot ring="run" core="run" size={12} />
          <span className="type-data">
            {connected} of {nodes.length} connected
          </span>
        </span>
        <span aria-hidden="true">·</span>
        <span>Metrics are read from each node on demand and kept for ten minutes.</span>
      </div>

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {nodesQuery.isPending ? (
          <div className="h-[150px] rounded-[10px] border border-line bg-surface/60" />
        ) : nodes.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">No node is enrolled in the cluster yet.</p>
          </div>
        ) : (
          nodes.map((node) => {
            const readout = nodeReadout(node.status);
            const isConnected = node.status === 'connected';
            return (
              <article
                key={node.id}
                className="rounded-[10px] border border-line bg-surface px-4 py-4 sm:px-5"
              >
                <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
                  <ToneLamp
                    tone={readout.tone}
                    pulse={readout.pulse}
                    size={18}
                    label={readout.words}
                  />
                  <h2 className="type-display min-w-0 truncate text-[20px] leading-none">
                    {node.name}
                  </h2>
                  <span className="type-data text-[13px] text-ink-2">
                    {node.roles.join(' · ')}
                  </span>
                  <span className="type-data text-[13px] text-ink">
                    {readout.words}
                  </span>
                  <span className="type-data text-[13px] text-ink-2">
                    {node.cp_status === 'primary' ? 'primary' : 'replica'}
                  </span>
                </div>

                <dl className="type-data mt-3 grid grid-cols-2 gap-x-6 gap-y-1 border-t border-line pt-2.5 text-[13px] sm:grid-cols-4">
                  <div className="flex gap-2">
                    <dt className="text-ink-2">CPUs</dt>
                    <dd className="text-ink">
                      {node.allocated.cpus} of {node.capacity.cpus}
                    </dd>
                  </div>
                  <div className="flex gap-2">
                    <dt className="text-ink-2">Memory</dt>
                    <dd className="text-ink">
                      {formatBytes(node.allocated.memory_mb * MIB)} of{' '}
                      {formatBytes(node.capacity.memory_mb * MIB)}
                    </dd>
                  </div>
                  <div className="flex gap-2">
                    <dt className="text-ink-2">Address</dt>
                    <dd className="min-w-0 truncate text-ink">
                      {node.address ?? (node.behind_nat ? 'behind NAT' : 'none')}
                    </dd>
                  </div>
                  <div className="flex gap-2">
                    <dt className="text-ink-2">KVM</dt>
                    <dd className="text-ink">
                      {node.kvm.available ? 'available' : 'not available'}
                    </dd>
                  </div>
                </dl>

                {isConnected ? (
                  <NodeMetricsBay nodeId={node.id} className="mt-4" />
                ) : (
                  <p className="mt-3 text-[13px] leading-snug text-ink-2">
                    Metrics are read from the node live; they stay hidden until it
                    connects.
                  </p>
                )}
              </article>
            );
          })
        )}
      </div>

      <ClusterVulnerabilities nodeNames={nodeNames} className="mt-5" />
    </main>
  );
}
