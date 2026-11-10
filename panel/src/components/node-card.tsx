// SPDX-License-Identifier: AGPL-3.0-only
import { Box, Cpu, Globe } from 'lucide-react';
import type { CSSProperties } from 'react';
import type { Node, NodeCapacity, NodeMetricSample } from '../api/generated/model';
import { gibOne, gibTotal } from '../lib/board';
import { formatPercent } from '../lib/metrics';
import { capacityShare, loadWords, meterTone, nodeMeters, nodeTags, type NodeLoad } from '../lib/node-capacity';
import { nodeReadout } from '../lib/nodes';
import { changeLogWords, groupedCount, seenWords, type VmCell } from '../lib/nodes-page';
import { useNow } from '../lib/use-now';
import { cn } from '../lib/utils';
import { DitherArea } from './dither-area';
import { DitherMeter } from './dither-meter';
import { LampDot } from './lamp';
import { useLiveNodes } from './live-nodes';
import { SkelBar, Skeleton } from './skeleton';
import { ToneLamp } from './tone-lamp';
import { Tag } from './ui/tag';

const MIB = 1024 * 1024;
const COLLECTING = 'Collecting samples — the first one arrives within 5 s';
const CPU_AXIS = { max: 100, ticks: [] };

/**
 * One machine: its lamp, name and tags, when it was last heard from, its
 * address, KVM and versions; what its VMs reserve of its CPU, memory and disk,
 * with a mark at what is in use now; its own CPU and memory over the last ten
 * minutes; the VMs it runs, each as wide as its vCPUs; and where it stands on
 * the change log. While a loss is simulated, the lost node dims and a node
 * that would take apps shows them as ghost cells and the reservation they
 * would reach.
 */
export function NodeCard({
  node,
  load,
  cells,
  lost = false,
  ghosts = [],
  reserved = null,
}: {
  node: Node;
  load: NodeLoad;
  cells: readonly VmCell[];
  /** The simulation's lost node. */
  lost?: boolean;
  /** Apps that would arrive here in the simulation. */
  ghosts?: readonly VmCell[];
  /** What would be reserved here in the simulation; null outside it, or when it cannot be said. */
  reserved?: NodeCapacity | null;
}) {
  const feed = useLiveNodes()[node.id];
  const samples = feed?.samples ?? [];
  const last = samples[samples.length - 1];
  const readout = nodeReadout(node.status);
  const connected = node.status === 'connected';
  const runsVms = node.roles.includes('node');
  const now = useNow(connected);
  const seen = seenWords(node.last_seen_at, now);
  const state = lost ? 'Simulated: lost' : seen ? `${readout.words} · ${seen}` : readout.words;
  const log = changeLogWords(node.log_lag);
  const address = node.address ?? (node.behind_nat ? 'Behind NAT' : null);

  return (
    <article
      className="card ncard"
      data-lost={lost ? '' : undefined}
      aria-labelledby={`node-${node.id}`}
      aria-describedby={lost ? `node-${node.id}-lost` : undefined}
    >
      <div className="ncard-h">
        <ToneLamp tone={lost ? 'idle' : readout.tone} pulse={!lost && readout.pulse} size={13} label={lost ? 'Simulated: lost' : readout.words} />
        <h2 id={`node-${node.id}`} className="ncard-name">
          {node.name}
        </h2>
        {nodeTags(node).map((tag) => (
          <Tag key={tag} tone={tag === 'primary' ? 'accent' : 'plain'}>
            {tag}
          </Tag>
        ))}
        <span id={`node-${node.id}-lost`} className="ncard-conn">
          {state}
        </span>
      </div>

      <div className="ncard-meta">
        {address ? (
          <span>
            <Globe strokeWidth={1.7} aria-hidden="true" />
            {node.address ? <code className="type-code">{address}</code> : address}
          </span>
        ) : null}
        <span className={node.kvm.available ? undefined : 'text-fault'}>
          <Cpu strokeWidth={1.7} aria-hidden="true" />
          {node.kvm.available ? 'KVM' : 'No KVM'}
        </span>
        <Versions node={node} />
      </div>

      <div className="ncard-b">
        {runsVms ? (
          <Meters node={node} reserved={reserved} last={last} />
        ) : (
          <p className="ncard-note">
            Runs no VMs: this machine is {node.roles.join(' and ')} only.
          </p>
        )}
        <div className="ncard-ch">
          {connected ? (
            <>
              <DitherArea
                compact
                title="CPU"
                subtitle="busy"
                reading={last ? formatPercent(last.cpu_usage) : '—'}
                samples={samples}
                value={cpuValue}
                axis={CPU_AXIS}
                tickLabel={String}
                formatValue={formatCpu}
                intervalS={5}
                emptyText={COLLECTING}
              />
              <DitherArea
                compact
                title="Memory"
                subtitle="in use"
                tone="run"
                reading={last ? `${gibOne(last.memory_used_bytes)} of ${gibTotal(last.memory_total_bytes)} GiB` : '—'}
                samples={samples}
                value={memoryValue}
                axis={{ max: Math.max(1, (last?.memory_total_bytes ?? 0) / MIB), ticks: [] }}
                tickLabel={String}
                formatValue={formatMemory}
                intervalS={5}
                emptyText={COLLECTING}
              />
            </>
          ) : (
            <p className="ncard-note">The node’s own reading of its host returns when it connects again.</p>
          )}
        </div>
      </div>

      {runsVms ? (
        <div className="ncard-r">
          <div className="ncard-r-h">
            <b>{loadWords(load)}</b>
            <span>sized by vCPU</span>
          </div>
          {cells.length + ghosts.length > 0 ? (
            <ul className="vcells" aria-label={`VMs on ${node.name}`}>
              {[...cells, ...ghosts].map((cell) => (
                <Cell key={cell.key} cell={cell} />
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}

      <div className="ncard-f">
        {log ? (
          <span>
            <LampDot ring={log.tone} core={log.tone} size={10} />
            {log.words}
          </span>
        ) : null}
        {log ? (
          <>
            <span>
              seq <b>{groupedCount(node.log_seq)}</b>
            </span>
            <span>
              lag <b>{groupedCount(node.log_lag ?? 0)}</b>
            </span>
          </>
        ) : null}
        <span>
          revision <b>{groupedCount(node.applied_rev)}</b> applied
          {node.desired_rev > node.applied_rev ? <>, {groupedCount(node.desired_rev)} wanted</> : null}
        </span>
      </div>
    </article>
  );
}

/** `engine 0.3.0 · agent 0.3.0`, each version as code; what the node does not report is left out. */
function Versions({ node }: { node: Pick<Node, 'engine_version' | 'agent_version'> }) {
  const parts = [
    node.engine_version ? (['engine', node.engine_version] as const) : null,
    node.agent_version ? (['agent', node.agent_version] as const) : null,
  ].filter((part) => part !== null);
  if (parts.length === 0) return null;
  return (
    <span>
      <Box strokeWidth={1.7} aria-hidden="true" />
      {parts.map(([what, version], index) => (
        <span key={what} className="ncard-ver">
          {index > 0 ? '· ' : null}
          {what} <code className="type-code">{version}</code>
        </span>
      ))}
    </span>
  );
}

/** CPU, memory and disk reserved, CPU and memory with a mark at what is in use now. */
function Meters({
  node,
  reserved,
  last,
}: {
  node: Node;
  reserved: NodeCapacity | null;
  last: NodeMetricSample | undefined;
}) {
  const [cpu, memory, disk] = nodeMeters({ capacity: node.capacity, allocated: reserved ?? node.allocated });
  const marks = {
    cpu: last ? Math.min(1, Math.max(0, last.cpu_usage)) : null,
    memory: last ? capacityShare(last.memory_used_bytes, last.memory_total_bytes) : null,
    disk: null,
  };
  const marked = marks.cpu !== null || marks.memory !== null;
  return (
    <div className="ncard-mt">
      {[cpu, memory, disk].map((meter) =>
        meter ? (
          meter.share === null ? (
            <div key={meter.key} className="meter">
              <div className="meter-h">
                <b>{meter.label}</b>
                <span>{meter.words}</span>
              </div>
              <p className="meter-unknown">{meter.unknown}</p>
            </div>
          ) : (
            <DitherMeter
              key={meter.key}
              label={meter.label}
              words={meter.words}
              share={Math.min(1, meter.share)}
              tone={meterTone(meter.share)}
              mark={marks[meter.key]}
              spoken={`${meter.spoken}${
                marks[meter.key] !== null ? `, ${Math.round(marks[meter.key]! * 100)}% in use now` : ''
              }`}
            />
          )
        ) : null,
      )}
      {marked ? (
        <p className="ncard-u">
          <i aria-hidden="true" />
          The mark is what is in use right now
        </p>
      ) : null}
    </div>
  );
}

/** One VM, or one app that would arrive, in the strip. */
function Cell({ cell }: { cell: VmCell }) {
  return (
    <li
      className={cn('vcell', cell.ghost && 'vcell-ghost')}
      data-k={cell.kind}
      style={{ '--u': cell.units } as CSSProperties}
      title={cell.title}
    >
      {cell.tone ? <LampDot ring={cell.tone} core={cell.tone} size={8} /> : null}
      <span aria-hidden="true">{cell.label}</span>
      <span className="sr-only">{cell.title}</span>
    </li>
  );
}

/** A card while the nodes load: the header, the meters and charts, the strip and the foot, as bars. */
export function NodeCardSkeleton() {
  return (
    <div className="card ncard">
      <div className="ncard-h">
        <SkelBar className="ncard-skel-lamp" />
        <SkelBar width="88px" height={16} />
        <SkelBar width="52px" />
      </div>
      <div className="ncard-meta">
        <SkelBar width="min(320px, 80%)" />
      </div>
      <div className="ncard-b">
        <div className="ncard-mt">
          {[0, 1, 2].map((index) => (
            <div key={index} className="meter">
              <SkelBar width="100%" />
              <SkelBar height={8} />
            </div>
          ))}
        </div>
        <div className="ncard-ch">
          <SkelBar height={96} className="ncard-skel-chart" />
          <SkelBar height={96} className="ncard-skel-chart" />
        </div>
      </div>
      <div className="ncard-r">
        <SkelBar width="30%" />
        <SkelBar height={26} />
      </div>
      <div className="ncard-f">
        <SkelBar width="60%" />
      </div>
    </div>
  );
}

/** The cards while the nodes load, in the page's grid. */
export function NodeCardsSkeleton({ count = 2 }: { count?: number }) {
  return (
    <Skeleton label="Loading nodes" className="ncards">
      {Array.from({ length: count }, (_, index) => (
        <NodeCardSkeleton key={index} />
      ))}
    </Skeleton>
  );
}

const cpuValue = (sample: NodeMetricSample): number => sample.cpu_usage * 100;
const formatCpu = (percent: number): string => formatPercent(percent / 100);
const memoryValue = (sample: NodeMetricSample): number => sample.memory_used_bytes / MIB;
const formatMemory = (mib: number): string => `${gibOne(mib * MIB)} GiB`;
