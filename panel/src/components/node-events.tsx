// SPDX-License-Identifier: AGPL-3.0-only
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useRef } from 'react';
import { listEvents } from '../api/generated/events/events';
import type { Node } from '../api/generated/model';
import { relativeTime } from '../lib/app-status';
import { NODE_EVENT_TYPES, nodeEventRow, readNodeEvents, type NodeEventsRead } from '../lib/nodes-page';
import { problemView } from '../lib/problem';
import { useNow } from '../lib/use-now';
import { cn } from '../lib/utils';
import { useSession } from '../session';
import { EmptyState } from './empty-state';
import { LampDot } from './lamp';
import { SkelBar, Skeleton } from './skeleton';
import { ProblemBanner } from './ui/problem-banner';

const QUERY_KEY = ['nodes-page', 'node-events'] as const;

/**
 * The node events, newest first. The log is served in ascending `seq` only,
 * so the first read of the session walks it to its end once; every later read,
 * and every later visit to the page, asks only for what came after the last
 * `seq` read (`readNodeEvents`). The read is kept for the session.
 */
function useNodeEvents() {
  const { user } = useSession();
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: QUERY_KEY,
    enabled: user !== undefined,
    refetchInterval: 10_000,
    gcTime: Infinity,
    retry: false,
    queryFn: ({ signal }): Promise<NodeEventsRead> =>
      readNodeEvents(queryClient.getQueryData<NodeEventsRead>(QUERY_KEY), (after) =>
        listEvents({ type: NODE_EVENT_TYPES, after_seq: after, limit: 500 }, { signal }),
      ),
  });
}

/** Node events under the cards: each a lamp, a sentence and when; a new one slides in at the top. */
export function NodeEvents({ nodes }: { nodes: readonly Node[] }) {
  const query = useNodeEvents();
  const events = { data: query.data?.events, error: query.error };
  const now = useNow(true, 30_000);
  // Only a row that arrives after the card first showed its events slides in: the newest seq on
  // screen at that first render is the line, so nothing already in the log animates.
  const shownUpTo = useRef<number | null>(null);
  if (shownUpTo.current === null && query.data) shownUpTo.current = query.data.after;

  if (!events.data && !events.error) return <NodeEventsSkeleton />;

  return (
    <section className="card nevents" aria-labelledby="node-events-title">
      <div className="psec-h">
        <h2 id="node-events-title">Node events</h2>
        <Link to="/activity" className="psec-link">
          Open activity
        </Link>
      </div>
      {events.error ? (
        <div className="px-3.5 pb-3.5">
          <ProblemBanner view={problemView(events.error, 'see node events')} />
        </div>
      ) : events.data && events.data.length === 0 ? (
        <EmptyState title="No node events yet" glyph={false} className="nevents-empty">
          A machine joining, connecting or losing its session shows here.
        </EmptyState>
      ) : (
        <ol className="pfeed">
          {(events.data ?? []).map((event) => {
            const row = nodeEventRow(event, nodes);
            const fresh = shownUpTo.current !== null && event.seq > shownUpTo.current;
            return (
              <li key={event.seq} className={cn('pev', fresh && 'pev-new')}>
                <div className="pev-a nev">
                  <span className="nev-lamp">
                    <LampDot ring={row.tone} core={row.tone} size={10} />
                  </span>
                  <span className="pev-s">
                    {row.segments.map((segment, index) =>
                      typeof segment === 'string' ? segment : <b key={index}>{segment.strong}</b>,
                    )}
                  </span>
                  <time dateTime={event.time}>{relativeTime(event.time, now)}</time>
                </div>
              </li>
            );
          })}
        </ol>
      )}
    </section>
  );
}

const SKELETON_ROWS = ['62%', '48%', '70%', '40%'] as const;

/** Node events while they load: the heading, then a lamp, a sentence and a time per row. */
function NodeEventsSkeleton() {
  return (
    <section className="card nevents" aria-label="Node events">
      <Skeleton label="Loading node events">
        <div className="psec-h">
          <h2>Node events</h2>
          <SkelBar width="72px" className="psec-skel-link" />
        </div>
        <div className="pfeed">
          {SKELETON_ROWS.map((width, index) => (
            <div key={index} className="pev-a nev">
              <SkelBar className="nev-skel-lamp" />
              <SkelBar width={width} />
              <SkelBar width="40px" />
            </div>
          ))}
        </div>
      </Skeleton>
    </section>
  );
}
