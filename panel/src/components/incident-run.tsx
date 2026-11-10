// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useMemo, useState, type ReactNode } from 'react';
import { useListApprovals } from '../api/generated/approvals/approvals';
import { getListGrantsQueryKey, useListGrants } from '../api/generated/grants/grants';
import {
  getGetIncidentQueryKey,
  getListIncidentsQueryKey,
  resolveIncident,
  useGetIncident,
} from '../api/generated/incidents/incidents';
import type { Agent, Approval, Grant, Incident } from '../api/generated/model';
import { clockTime, type RecordContext } from '../lib/activity';
import { relativeTime } from '../lib/app-status';
import { approvalMeter, callText, DECIDER_ONLY_SENTENCE, type Decision } from '../lib/approvals';
import { problemView, type ProblemView } from '../lib/problem';
import { ribbonLayout, type RibbonLayout } from '../lib/ribbon';
import { buildStory, type StoryStep } from '../lib/story';
import { useIncidentRecords } from '../lib/use-agent-calls';
import { useNow } from '../lib/use-now';
import { AGENCY_LIST } from '../lib/use-record-context';
import { cn } from '../lib/utils';
import { Sentence } from './activity-row';
import { ApprovalDecision } from './approval-decision';
import { Aura } from './aura';
import { DitherMeter } from './dither-meter';
import { LampDot } from './lamp';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { Tag } from './ui/tag';
import { Textarea } from './ui/textarea';
import { SkeletonRows } from './skeleton';

const linkClass =
  'text-ink-2 underline-offset-4 hover:text-ink hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent';

/** Where the incident's subject opens: an app on its project, a VM on Nodes. */
function OpenSubject({ incident, name }: { incident: Incident; name: string | undefined }) {
  const { type, id } = incident.subject;
  if (type === 'app') {
    return (
      <Button variant="default" asChild>
        <Link to="/apps/$appId" params={{ appId: id }}>
          Open {name ?? 'app'}
        </Link>
      </Button>
    );
  }
  if (type === 'vm') {
    return (
      <Button variant="default" asChild>
        <Link to="/nodes" search={{ tab: 'vms' }}>
          Open {name ?? 'VM'}
        </Link>
      </Button>
    );
  }
  return null;
}

/** The hour ribbon: the grant window dotted, each call a point, the wait for a person amber, and now. */
function Ribbon({ layout }: { layout: RibbonLayout }) {
  return (
    <div className="ribbon" role="img" aria-label={ribbonWords(layout)}>
      <div className="lane">
        {layout.windows.map((window) => (
          <span
            key={window.id}
            className={cn('gw', window.open && 'gw-open')}
            style={{ left: `${window.left}%`, width: `${window.width}%` }}
          />
        ))}
        {layout.waits.map((wait) => (
          <span key={wait.id} className="ww" style={{ left: `${wait.left}%`, width: `${wait.width}%` }} />
        ))}
        {layout.points.map((point) => (
          <span
            key={point.seq}
            className={cn('ev', point.tone === 'refused' && 'ev-x', point.tone === 'other' && 'ev-other')}
            style={{ left: `${point.left}%` }}
          />
        ))}
        {layout.ticks.map((tick, index) =>
          tick.hidden ? null : (
            <span key={tick.at} className={cn('tk', index === 0 && 'tk-first')} style={{ left: `${tick.left}%` }}>
              {clockTime(tick.at)}
            </span>
          ),
        )}
        {layout.now !== null ? <span className="now" style={{ left: `${layout.now}%` }} /> : null}
      </div>
    </div>
  );
}

/** What the ribbon shows, for a screen reader. */
const ribbonWords = (layout: RibbonLayout): string => {
  const calls = layout.points.filter((point) => point.tone !== 'other').length;
  const refused = layout.points.filter((point) => point.tone === 'refused').length;
  const waiting = layout.waits.some((wait) => wait.live);
  return [
    `From ${clockTime(layout.start)} to ${clockTime(layout.end)}`,
    `${calls} ${calls === 1 ? 'call' : 'calls'} by the agent`,
    refused > 0 ? `${refused} refused` : null,
    waiting ? 'waiting on a person now' : null,
  ]
    .filter(Boolean)
    .join(', ');
};

/** The request that waits, at the end of the story: why, the exact call, its clock, and the decision. */
export function DecideBox({
  approval,
  canDecide,
  now,
  onDecided,
  onConflict,
}: {
  approval: Approval;
  canDecide: boolean;
  now: number;
  onDecided: (kind: Decision, decided: Approval) => void;
  onConflict: (view: ProblemView) => void;
}) {
  const meter = approvalMeter(approval, now);
  const inbox = (
    <Link to="/approvals" search={{ approval: approval.id }} className={cn('text-[13px]', linkClass)}>
      Open in Approvals
    </Link>
  );
  return (
    <div className="decide-wrap">
      <Aura tone="shift" breathe margin={22} />
      <div className="decide">
        <p className="why">
          <b>Why</b>
          {approval.justification}
        </p>
        <p className="call" aria-label="The exact call">
          {callText(approval)}
        </p>
        <DitherMeter
          tone="shift"
          label={meter.words}
          words={meter.then}
          share={meter.share}
        />
        {canDecide ? (
          <ApprovalDecision
            approval={approval}
            withNote={false}
            aside={inbox}
            onDecided={onDecided}
            onConflict={onConflict}
          />
        ) : (
          <div className="flex flex-wrap items-center gap-3">
            <p className="min-w-0 flex-1 text-[13px] leading-snug text-ink-2">{DECIDER_ONLY_SENTENCE}</p>
            {inbox}
          </div>
        )}
      </div>
    </div>
  );
}

function StoryRow({ step, children }: { step: StoryStep; children?: ReactNode }) {
  const meta = step.lead || (step.tags && step.tags.length > 0) || step.note;
  return (
    <li className="st">
      <time dateTime={step.time} title={step.time}>
        {clockTime(step.time)}
      </time>
      <span className="spine" aria-hidden="true">
        <i className={cn('mk2', `mk2-${step.mark}`)} />
      </span>
      <div className="st-c">
        <p className="st-s">
          <Sentence segments={step.sentence} />
          {step.tag ? (
            <>
              {' '}
              <Tag tone={step.tag.tone}>{step.tag.words}</Tag>
            </>
          ) : null}
        </p>
        {meta ? (
          <p className="st-meta">
            {step.lead ? <span>{step.lead}</span> : null}
            {step.tags?.map((tag) => (
              <Tag key={tag} tone="code">
                {tag}
              </Tag>
            ))}
            {step.more || step.note ? (
              <span>{[step.more ? `+${step.more}` : null, step.note].filter(Boolean).join(' · ')}</span>
            ) : null}
          </p>
        ) : null}
        {step.why ? (
          <p className="why">
            <b>Why</b>
            {step.why}
          </p>
        ) : null}
        {children}
      </div>
    </li>
  );
}

/**
 * An agent's incident, told as a run: the header with the incident and what
 * can be done about it, the hour ribbon, and the story, one step per audit
 * record of the incident, ending on the request that waits for a person.
 */
export function IncidentRun({
  agent,
  incident,
  ctx,
  canDecide,
  canResolve,
}: {
  agent: Agent;
  incident: Incident;
  ctx: RecordContext;
  canDecide: boolean;
  canResolve: boolean;
}) {
  const queryClient = useQueryClient();
  const now = useNow(true, 15_000);
  const [notice, setNotice] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [resolveOpen, setResolveOpen] = useState(false);
  const [resolveNote, setResolveNote] = useState('');

  const detail = useGetIncident(incident.id, { query: { refetchInterval: 15_000, retry: false } });
  const records = useIncidentRecords(incident.id);
  const approvals = useListApprovals(AGENCY_LIST, { query: { refetchInterval: 10_000, retry: false } });
  const grantList = useListGrants(AGENCY_LIST, { query: { refetchInterval: 15_000, retry: false } });

  const current = detail.data ?? incident;
  const ours = useMemo(
    () =>
      (approvals.data?.items ?? []).filter(
        (approval) => approval.incident_id === incident.id && approval.agent_id === agent.id,
      ),
    [approvals.data, incident.id, agent.id],
  );
  const grants = useMemo(() => {
    const byId = new Map<string, Grant>();
    for (const grant of detail.data?.grants ?? []) byId.set(grant.id, grant);
    for (const grant of grantList.data?.items ?? []) {
      if (grant.incident_id === incident.id) byId.set(grant.id, grant);
    }
    return [...byId.values()].filter((grant) => grant.agent_id === agent.id);
  }, [detail.data, grantList.data, incident.id, agent.id]);

  const items = records.data?.records;
  const steps = useMemo(
    () =>
      buildStory({
        agentId: agent.id,
        incident: { ...current, trigger_event: detail.data?.trigger_event ?? null },
        records: items ?? [],
        approvals: ours,
        grants,
        ctx,
        now,
      }),
    [agent.id, current, detail.data, items, ours, grants, ctx, now],
  );
  const layout = useMemo(
    () =>
      ribbonLayout({
        openedAt: current.opened_at,
        resolvedAt: current.resolved_at,
        now,
        grants: grants.filter((grant) => grant.kind === 'incident'),
        calls: (items ?? []).map((record) => ({
          seq: record.seq,
          time: record.time,
          refused: record.result !== 'ok',
          other: !(record.actor_type === 'agent' && record.actor_id === agent.id),
        })),
        waits: ours.map((approval) => ({
          id: approval.id,
          from: approval.created_at,
          to:
            approval.status === 'pending'
              ? null
              : (approval.decided_at ?? (approval.status === 'expired' ? approval.expires_at : null)),
        })),
      }),
    [current, now, grants, items, ours, agent.id],
  );

  const resolve = useMutation({
    mutationFn: () => resolveIncident(incident.id, resolveNote.trim() === '' ? {} : { resolution_note: resolveNote.trim() }),
    onSuccess: (resolved) => {
      setResolveOpen(false);
      setResolveNote('');
      setProblem(null);
      setNotice(`Resolved “${resolved.title}”. Its incident grants stop working now.`);
      void queryClient.invalidateQueries({ queryKey: getListIncidentsQueryKey() });
      void queryClient.invalidateQueries({ queryKey: getGetIncidentQueryKey(incident.id) });
      void queryClient.invalidateQueries({ queryKey: getListGrantsQueryKey() });
      void queryClient.invalidateQueries({ queryKey: ['/api/v1/audit'] });
    },
    onError: (error) => setProblem(problemView(error, 'resolve incidents')),
  });

  const subjectName = ctx.name?.(current.subject.type, current.subject.id);
  const projectName = current.project_id ? ctx.name?.('project', current.project_id) : undefined;
  const resolved = current.status === 'resolved';

  return (
    <section aria-label={`Incident: ${current.title}`} className="run">
      <header className="run-h">
        <LampDot
          ring={resolved ? 'run' : 'fault'}
          core={resolved ? 'run' : 'fault'}
          size={20}
          className="run-lamp"
          label={resolved ? 'Resolved' : 'Open'}
        />
        <div className="min-w-0 flex-1">
          <h2>{current.title}</h2>
          <p>
            Incident · {resolved ? `resolved ${relativeTime(current.resolved_at ?? '', now)}` : `opened ${relativeTime(current.opened_at, now)}`}
            {projectName ? ` · project ${projectName}` : ''} · handled by {agent.name}
          </p>
        </div>
        <div className="run-actions">
          <OpenSubject incident={current} name={subjectName} />
          {canResolve && !resolved ? (
            <Button variant="default" onClick={() => setResolveOpen(true)}>
              Resolve
            </Button>
          ) : null}
        </div>
      </header>

      {notice ? (
        <div className="mt-3">
          <NoticeLine>{notice}</NoticeLine>
        </div>
      ) : null}
      {problem ? <ProblemBanner view={problem} className="mt-3" /> : null}

      <Ribbon layout={layout} />

      {records.isPending ? (
        <SkeletonRows label="Loading the incident’s records" rows={3} className="mt-5" />
      ) : records.isError ? (
        <ProblemBanner view={problemView(records.error, 'read the audit log')} className="mt-5" />
      ) : (
        <ol className="story">
          {steps.map((step) => {
            const waiting = step.pendingApprovalId
              ? ours.find((approval) => approval.id === step.pendingApprovalId && approval.status === 'pending')
              : undefined;
            return (
              <StoryRow key={step.key} step={step}>
                {waiting ? (
                  <DecideBox
                    approval={waiting}
                    canDecide={canDecide}
                    now={now}
                    onConflict={(view) => {
                      setNotice(null);
                      setProblem(view);
                    }}
                    onDecided={(kind) => {
                      setProblem(null);
                      setNotice(
                        kind === 'approve'
                          ? `Approved. ${agent.name} has 10 minutes to repeat the exact call.`
                          : `Denied. ${agent.name}’s retry will be refused.`,
                      );
                    }}
                  />
                ) : null}
              </StoryRow>
            );
          })}
        </ol>
      )}
      {records.data?.capped ? (
        <p className="mt-2 text-[12.5px] text-ink-2">
          The story stops at the first {records.data.records.length} records of this incident.{' '}
          <Link to="/activity" search={{ incident_id: incident.id }} className={linkClass}>
            Activity has them all
          </Link>
          .
        </p>
      ) : null}

      <Dialog open={resolveOpen} onOpenChange={setResolveOpen}>
        <DialogContent
          title="Resolve this incident?"
          description={`Closes “${current.title}” and records who resolved it. The incident grants stop working at once.`}
        >
          <form
            className="flex flex-col gap-3"
            onSubmit={(event) => {
              event.preventDefault();
              resolve.mutate();
            }}
          >
            {problem && resolve.isError ? <ProblemBanner view={problem} /> : null}
            <Field label="What was done (optional)" htmlFor="resolve-note" hint="Recorded in the audit log.">
              <Textarea
                id="resolve-note"
                value={resolveNote}
                maxLength={2000}
                placeholder="Redeployed blog from a clean image"
                onChange={(event) => setResolveNote(event.target.value)}
              />
            </Field>
            <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="default" type="button" onClick={() => setResolveOpen(false)}>
                Keep it open
              </Button>
              <Button busy={resolve.isPending} variant="primary" type="submit" disabled={resolve.isPending}>
                {resolve.isPending ? 'Resolving…' : 'Resolve incident'}
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
    </section>
  );
}
