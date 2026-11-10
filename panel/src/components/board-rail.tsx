// SPDX-License-Identifier: AGPL-3.0-only
import { Link } from '@tanstack/react-router';
import { useCallback, useMemo, useRef, useState, type RefObject } from 'react';
import { useListAudit } from '../api/generated/audit/audit';
import type { App, Approval, AuditRecord, Incident, Node } from '../api/generated/model';
import { approvalAsk } from '../lib/access';
import { clockTime, groupReads, kindWord, readsSentence, segmentsText } from '../lib/activity';
import { expiryWords } from '../lib/agency-format';
import { relativeTime } from '../lib/app-status';
import { approvalMeter } from '../lib/approvals';
import { railSentence } from '../lib/board';
import { drawMeter, type DitherSurface, type MeterTone, type TokenReader } from '../lib/dither-draw';
import { capacityShare, gibWords, meterTone, nodeMeters, nodeTags } from '../lib/node-capacity';
import { nodeReadout } from '../lib/nodes';
import { useNow } from '../lib/use-now';
import { useRecordContext } from '../lib/use-record-context';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { ActorAvatar, Sentence } from './activity-row';
import { DenyButton } from './approval-decision';
import { Aura } from './aura';
import { Avatar } from './avatar';
import { DitherCanvas } from './dither-canvas';
import { AgentName } from './identity-name';
import { LampDot } from './lamp';
import { useLiveNodes } from './live-nodes';
import { N1Strip } from './n1-strip';
import { SkelBar, Skeleton } from './skeleton';
import { Button } from './ui/button';
import { Tag } from './ui/tag';

/* ---------- Needs you ---------- */

/**
 * What waits on a person, at the top of the rail: the request that expires
 * first, with the agent, what it wants, its justification, how long it has,
 * and the way to review or deny it; then the other requests and the open
 * incidents. Deny is the inbox's own, behind the same confirm step, and only
 * for someone who may decide.
 */
export function NeedsYou({
  approvals,
  incidents,
  targetName,
  canDecide,
  loading = false,
}: {
  /** Pending requests, the one that expires first first. */
  approvals: readonly Approval[];
  incidents: readonly Incident[];
  targetName: (approval: Approval) => string | undefined;
  canDecide: boolean;
  /** The requests or the incidents have not come back yet. */
  loading?: boolean;
}) {
  const now = useNow(approvals.length > 0 || incidents.length > 0, 30_000);
  const [notice, setNotice] = useState<string | null>(null);
  // After a Deny the request and its buttons leave, so focus goes to this section's heading.
  const heading = useRef<HTMLHeadingElement>(null);
  const [first, ...more] = approvals;
  if (loading && !first && incidents.length === 0) return <NeedsYouSkeleton />;
  if (!first && incidents.length === 0 && notice === null) return null;

  return (
    <section className="card pneed" aria-label="Needs you">
      {first ? <Aura tone="shift" margin={10} /> : null}
      <div className="psec-h">
        <LampDot ring="shift" core="shift" size={11} />
        <h2 ref={heading} tabIndex={-1}>
          Needs you
        </h2>
        {approvals.length > 0 ? (
          <span className="pneed-n">
            {approvals.length}
            <span className="sr-only"> waiting</span>
          </span>
        ) : null}
        <Link to="/approvals" className="psec-link">
          All approvals
        </Link>
      </div>
      {notice ? (
        <p role="status" className="pneed-note">
          {notice}
        </p>
      ) : null}
      {first ? (
        <WaitingRequest
          approval={first}
          targetName={targetName(first)}
          now={now}
          canDecide={canDecide}
          onNotice={setNotice}
          heading={heading}
        />
      ) : null}
      {more.map((approval) => {
        const ask = approvalAsk(approval, targetName(approval));
        return (
          <Link key={approval.id} to="/approvals" search={{ approval: approval.id }} className="pinc">
            <Avatar kind="agent" seed={approval.agent_id} />
            <p>
              <b>
                <AgentName id={approval.agent_id} link={false} />
              </b>{' '}
              {ask.wants} <b>{ask.target}</b>
            </p>
          </Link>
        );
      })}
      {incidents.map((incident) => (
        <Link key={incident.id} to="/activity" search={{ incident_id: incident.id }} className="pinc">
          <LampDot ring="fault" core="fault" size={11} />
          <p>
            <b>{incident.title}</b> · incident opened {relativeTime(incident.opened_at, now)}
          </p>
        </Link>
      ))}
    </section>
  );
}

function WaitingRequest({
  approval,
  targetName,
  now,
  canDecide,
  onNotice,
  heading,
}: {
  approval: Approval;
  targetName?: string;
  now: number;
  canDecide: boolean;
  onNotice: (notice: string | null) => void;
  heading: RefObject<HTMLHeadingElement | null>;
}) {
  const ask = approvalAsk(approval, targetName);
  const meter = approvalMeter(approval, now);
  const until = expiryWords(approval.expires_at, now);
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, meter.share, read, 'shift'),
    [meter.share],
  );
  return (
    <div className="pneed-b">
      <div className="pneed-who">
        <Avatar kind="agent" seed={approval.agent_id} />
        <p>
          <b>
            <AgentName id={approval.agent_id} link={false} />
          </b>{' '}
          {ask.wants} {targetName ? `the ${kindWord(approval.target.type)} ` : ''}
          <b>{ask.target}</b>
        </p>
      </div>
      {approval.justification ? <p className="pneed-why">{approval.justification}</p> : null}
      <div className="pneed-exp">
        <div>
          <span>
            {meter.over ? (
              'Expired'
            ) : until.startsWith('in ') ? (
              <>
                Expires in <b>{until.slice(3)}</b>
              </>
            ) : (
              meter.words
            )}
          </span>
          <span>asked {relativeTime(approval.created_at, now)}</span>
        </div>
        <DitherCanvas paint={paint} label={`${meter.words}, ${Math.round(meter.share * 100)}% of its time left`} />
      </div>
      <div className="pneed-acts">
        <Button variant="primary" size="sm" asChild>
          <Link to="/approvals" search={{ approval: approval.id }}>
            Review the request
          </Link>
        </Button>
        {canDecide ? (
          <DenyButton
            approval={approval}
            onDecided={() => onNotice('Denied. The agent’s retry will be refused.')}
            onConflict={(view) => onNotice(view.message)}
            focusAfter={() => heading.current}
          />
        ) : null}
      </div>
    </div>
  );
}

/* ---------- Nodes ---------- */

function RailMeter({ share, mark, spoken }: { share: number | null; mark: number | null; spoken: string }) {
  const tone: MeterTone = meterTone(share);
  const value = share ?? 0;
  const paint = useCallback(
    (surface: DitherSurface, read: TokenReader) => drawMeter(surface, value, read, tone, mark),
    [value, tone, mark],
  );
  return <DitherCanvas paint={paint} label={spoken} />;
}

/**
 * The nodes, each with its role, how busy its host is right now, and its CPU
 * and memory reserved against capacity with a mark at what is in use; then
 * the N+1 sentence.
 */
export function RailNodes({
  nodes,
  apps,
  loading = false,
}: {
  nodes: readonly Node[];
  apps: readonly App[];
  /** The node list has not come back yet. */
  loading?: boolean;
}) {
  const feeds = useLiveNodes();
  const shown = nodes.filter((node) => node.status !== 'removed');
  if (loading) return <RailNodesSkeleton />;
  if (shown.length === 0) return null;
  return (
    <section className="card pnodes" aria-label="Nodes">
      <div className="psec-h">
        <h2>Nodes</h2>
        <Link to="/nodes" className="psec-link">
          Open nodes
        </Link>
      </div>
      {shown.map((node) => {
        const readout = nodeReadout(node.status);
        const samples = feeds[node.id]?.samples ?? [];
        const last = samples[samples.length - 1];
        const runsVms = node.roles.includes('node');
        const [cpu, memory] = nodeMeters(node);
        const cpuMark = last ? Math.min(1, last.cpu_usage) : null;
        const memoryMark = last ? capacityShare(last.memory_used_bytes, last.memory_total_bytes) : null;
        const tag = nodeTags(node)[0];
        return (
          <Link
            key={node.id}
            to="/nodes"
            search={runsVms ? { tab: 'vms', node: node.id } : {}}
            className="pnd"
            aria-label={`${node.name}, ${readout.words}${last ? `, ${Math.round(last.cpu_usage * 100)}% busy` : ''}${runsVms ? '. Its VMs' : ''}`}
          >
            <LampDot ring={readout.tone} core={readout.tone} pulse={readout.pulse} size={11} label={readout.words} />
            <div className="pnd-n">
              <b>{node.name}</b>
              {tag ? <Tag tone="plain">{tag}</Tag> : null}
              <span className="pnd-x">{last ? `${Math.round(last.cpu_usage * 100)}% busy` : readout.words}</span>
            </div>
            {runsVms && cpu && memory ? (
              <div className="pnd-ms">
                <div>
                  <RailMeter
                    share={cpu.share}
                    mark={cpuMark}
                    spoken={`${cpu.spoken}${cpuMark !== null ? `, ${Math.round(cpuMark * 100)}% busy now` : ''}`}
                  />
                  <span>
                    {node.allocated.cpus} of {node.capacity.cpus} {node.capacity.cpus === 1 ? 'CPU' : 'CPUs'}
                  </span>
                </div>
                <div>
                  <RailMeter
                    share={memory.share}
                    mark={memoryMark}
                    spoken={`${memory.spoken}${memoryMark !== null ? `, ${Math.round(memoryMark * 100)}% in use now` : ''}`}
                  />
                  <span>
                    {gibWords(node.allocated.memory_mb)} of {gibWords(node.capacity.memory_mb)} GiB
                  </span>
                </div>
              </div>
            ) : null}
          </Link>
        );
      })}
      <N1Strip nodes={nodes} apps={apps} className="pfit" />
    </section>
  );
}

/* ---------- Recent activity ---------- */

const RECENT = { order: 'desc', limit: 20 } as const;
const RECENT_ROWS = 5;

/**
 * The newest records of the audit log, newest first, each with who acted. A
 * record that arrives while the page is open slides in at the top.
 */
export function RecentActivity() {
  const { user } = useSession();
  const audit = useListAudit(RECENT, { query: { enabled: user !== undefined, refetchInterval: 10_000, retry: false } });
  const ctx = useRecordContext();
  const items = useMemo(() => groupReads(audit.data?.items ?? []).slice(0, RECENT_ROWS), [audit.data]);
  // The records on screen when the feed first loaded; any other row is new and slides in.
  const first = useRef<Set<number> | null>(null);
  if (first.current === null && audit.data) first.current = new Set(audit.data.items.map((record) => record.seq));
  if (audit.isError || (audit.data && items.length === 0)) return null;
  // Until the records and the names they mention are in, the rows are bars: never ids.
  if (!audit.data || ctx.loading) return <RecentActivitySkeleton />;

  return (
    <section className="card pacts" aria-label="Recent activity">
      <div className="psec-h">
        <h2>Recent activity</h2>
        <Link to="/activity" className="psec-link">
          Open activity
        </Link>
      </div>
      <ol className="pfeed">
        {items.map((item) => {
          const records = item.kind === 'one' ? [item.record] : item.records;
          const newest = records[0]!;
          const fresh = first.current !== null && !first.current.has(newest.seq);
          return (
            <FeedRow
              key={newest.seq}
              record={newest}
              segments={item.kind === 'one' ? railSentence(newest, ctx) : readsSentence([...records], ctx).filter((segment) => !segment.faint)}
              fresh={fresh}
            />
          );
        })}
      </ol>
    </section>
  );
}

function FeedRow({
  record,
  segments,
  fresh,
}: {
  record: AuditRecord;
  segments: ReturnType<typeof railSentence>;
  fresh: boolean;
}) {
  return (
    <li className={cn('pev', fresh && 'pev-new')}>
      <Link to="/activity" search={{ seq: record.seq }} className="pev-a" aria-label={`${segmentsText(segments)}, ${clockTime(record.time)}`}>
        <ActorAvatar record={record} />
        <span className="pev-s">
          <Sentence segments={segments} />
        </span>
        <time dateTime={record.time}>{clockTime(record.time)}</time>
      </Link>
    </li>
  );
}

/* ---------- skeletons ---------- */

/** A section head: its title as words, the link as a bar. */
function SkeletonHead({ title }: { title: string }) {
  return (
    <div className="psec-h">
      <h2>{title}</h2>
      <SkelBar width="72px" className="psec-skel-link" />
    </div>
  );
}

/** Needs you while the requests load: who, the reason and the two buttons as bars. */
function NeedsYouSkeleton() {
  return (
    <section className="card pneed" aria-label="Needs you">
      <Skeleton label="Loading what needs you">
        <SkeletonHead title="Needs you" />
        <div className="pneed-b">
          <div className="pneed-who">
            <SkelBar className="rail-skel-avatar" />
            <SkelBar width="70%" />
          </div>
          <SkelBar height={52} className="rail-skel-box" />
          <div className="pneed-acts">
            <SkelBar height={28} className="rail-skel-button" />
            <SkelBar height={28} className="rail-skel-button" />
          </div>
        </div>
      </Skeleton>
    </section>
  );
}

const NODE_SKELETON = ['38%', '30%', '44%'] as const;

/** Nodes while the list loads: each row its name, tag and busy share, and the two meters as bars. */
function RailNodesSkeleton() {
  return (
    <section className="card pnodes" aria-label="Nodes">
      <Skeleton label="Loading nodes">
        <SkeletonHead title="Nodes" />
        {NODE_SKELETON.map((width, index) => (
          <div key={index} className="pnd">
            <SkelBar className="rail-skel-dot" />
            <div className="pnd-n">
              <SkelBar width={width} />
              <SkelBar width="44px" className="ml-auto" />
            </div>
            <div className="pnd-ms">
              <SkelBar height={6} />
              <SkelBar height={6} />
            </div>
          </div>
        ))}
      </Skeleton>
    </section>
  );
}

const ACTIVITY_SKELETON = ['82%', '64%', '90%', '58%', '74%'] as const;

/** Recent activity while it loads: an avatar, a sentence and its time per row. */
function RecentActivitySkeleton() {
  return (
    <section className="card pacts" aria-label="Recent activity">
      <Skeleton label="Loading recent activity">
        <SkeletonHead title="Recent activity" />
        <div className="pfeed">
          {ACTIVITY_SKELETON.map((width, index) => (
            <div key={index} className="pev-a">
              <SkelBar className="rail-skel-avatar" />
              <SkelBar width={width} />
              <SkelBar width="34px" />
            </div>
          ))}
        </div>
      </Skeleton>
    </section>
  );
}
