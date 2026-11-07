// SPDX-License-Identifier: AGPL-3.0-only
import { useEffect, useState } from 'react';
import { getBuildLogs } from '../api/generated/builds/builds';
import type { BuildLogLine } from '../api/generated/model';
import { buildLogStreamUrl, orderBuildLog, parseBuildStreamEvent } from './build-log';

const PAGE_LIMIT = 1_000;
const MAX_PAGES = 5;

/**
 * A build's log lines, for timing its BuildKit steps. While the build runs
 * they come over `streamBuildLogs` (SSE, as the build log does); a finished
 * build, or a stream that cannot open, is read from `getBuildLogs` pages.
 * The log stays on the node: when it cannot be read the list stays empty and
 * the steps are simply not shown.
 */
export function useBuildLines(buildId: string | null, live: boolean): BuildLogLine[] {
  const [lines, setLines] = useState<BuildLogLine[]>([]);

  useEffect(() => {
    setLines([]);
  }, [buildId]);

  useEffect(() => {
    if (buildId === null) return;
    let disposed = false;
    let source: EventSource | null = null;

    const readPages = async () => {
      try {
        let cursor: string | undefined;
        for (let page = 0; page < MAX_PAGES; page += 1) {
          const answer = await getBuildLogs(buildId, { limit: PAGE_LIMIT, cursor });
          if (disposed) return;
          setLines((current) => orderBuildLog(current, answer.items));
          if (answer.next_cursor === null) break;
          cursor = answer.next_cursor;
        }
      } catch {
        // The node keeps the log; without it the steps stay out of view.
      }
    };

    if (!live) {
      void readPages();
      return () => {
        disposed = true;
      };
    }

    let opened = false;
    const close = () => {
      if (!source) return;
      source.removeEventListener('open', onOpen);
      source.removeEventListener('log', onLog);
      source.removeEventListener('end', onEnd);
      source.removeEventListener('error', onError);
      source.close();
      source = null;
    };
    function onOpen() {
      opened = true;
    }
    function onLog(event: Event) {
      const message = event as MessageEvent<string>;
      const parsed = parseBuildStreamEvent({ id: message.lastEventId, event: 'log', data: message.data });
      if (parsed.kind === 'log') setLines((current) => orderBuildLog(current, [parsed.line]));
    }
    function onEnd() {
      close();
    }
    function onError() {
      // A stream that never opened, or one the node closed: read the pages once.
      if (!opened || source?.readyState === EventSource.CLOSED) {
        close();
        void readPages();
      }
    }

    source = new EventSource(buildLogStreamUrl(buildId), { withCredentials: true });
    source.addEventListener('open', onOpen);
    source.addEventListener('log', onLog);
    source.addEventListener('end', onEnd);
    source.addEventListener('error', onError);

    return () => {
      disposed = true;
      close();
    };
  }, [buildId, live]);

  return lines;
}
