// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The builds API in mock mode: list, start, read, cancel and both log reads.
 * The stream replays the build's lines with `seq` ids, sends `status` whenever
 * the build moves, and closes with `end` once it is terminal. Mock mode only.
 */
import { HttpResponse, http } from 'msw';
import type {
  Build,
  BuildList,
  BuildLogList,
  StartBuildRequest,
} from '../api/generated/model';
import { formatSseFrame } from '../lib/log-buffer';
import { findApp } from './db';
import {
  appBuilds,
  buildLogLines,
  cancelMockBuild,
  findBuild,
  hasLiveBuild,
  isMockBuildTerminal,
  startMockBuild,
} from './builds';
import {
  conflict,
  json,
  notFound,
  page,
  readJson,
  refuseUnlessWriter,
  signedIn,
  unauthorized,
  validationFailed,
} from './http';

export const buildsHandlers = [
  http.get('*/api/v1/apps/:appId/builds', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) return notFound('That app');
    const list: BuildList = page(appBuilds(app.id), request);
    return json(list);
  }),

  http.post('*/api/v1/apps/:appId/builds', async ({ request, params }) => {
    const refusal = refuseUnlessWriter('app:deploy');
    if (refusal) return refusal;

    const app = findApp(String(params.appId));
    if (!app) return notFound('That app');
    if (!app.source) {
      return conflict('This app deploys an image given by reference; it has no Git source.');
    }
    if (hasLiveBuild(app.id)) {
      return conflict('This app already has a build that is not finished.');
    }

    const body = await readJson<StartBuildRequest>(request);
    const ref = typeof body?.ref === 'string' ? body.ref.trim() : undefined;
    if (ref !== undefined && (ref === '' || ref.length > 255)) {
      return validationFailed('A ref must be 1-255 characters.');
    }

    const build = startMockBuild(app, ref);
    return json(build, { status: 202 });
  }),

  http.get('*/api/v1/builds/:buildId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const build = findBuild(String(params.buildId));
    if (!build) return notFound('That build');
    return json(build);
  }),

  http.post('*/api/v1/builds/:buildId/cancel', ({ params }) => {
    const refusal = refuseUnlessWriter('app:deploy');
    if (refusal) return refusal;

    const build = findBuild(String(params.buildId));
    if (!build) return notFound('That build');
    if (isMockBuildTerminal(build.status)) {
      return conflict('This build has already finished.');
    }
    cancelMockBuild(build);
    return json(build, { status: 202 });
  }),

  http.get('*/api/v1/builds/:buildId/logs', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const build = findBuild(String(params.buildId));
    if (!build) return notFound('That build');

    const url = new URL(request.url);
    const limit = Number(url.searchParams.get('limit') ?? '500');
    const size = Number.isInteger(limit) && limit > 0 ? Math.min(limit, 1_000) : 500;
    const cursor = Number(url.searchParams.get('cursor') ?? '0');
    const start = Number.isInteger(cursor) && cursor > 0 ? cursor : 0;

    const all = buildLogLines(build.id);
    const items = all.slice(start, start + size);
    const next = start + items.length;
    const list: BuildLogList = {
      items,
      next_cursor: next < all.length ? String(next) : null,
    };
    return json(list);
  }),

  http.get('*/api/v1/builds/:buildId/logs/stream', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const build = findBuild(String(params.buildId));
    if (!build) return notFound('That build');

    const url = new URL(request.url);
    const lastEventId = Number(request.headers.get('Last-Event-ID') ?? '');
    const tail = Number(url.searchParams.get('tail') ?? '');
    const all = buildLogLines(build.id);

    let cursor = 0;
    if (Number.isInteger(lastEventId) && lastEventId > 0) {
      cursor = all.filter((line) => line.seq <= lastEventId).length;
    } else if (Number.isInteger(tail) && tail > 0) {
      cursor = Math.max(0, all.length - tail);
    }

    const encoder = new TextEncoder();
    let timer: ReturnType<typeof setInterval> | undefined;
    let closed = false;
    let lastStatus: Build['status'] | null = null;

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        const stop = () => {
          closed = true;
          if (timer !== undefined) clearInterval(timer);
          try {
            controller.close();
          } catch {
            // The consumer went away first; nothing to close.
          }
        };
        const send = (frame: string) => {
          if (closed) return;
          try {
            controller.enqueue(encoder.encode(frame));
          } catch {
            closed = true;
            if (timer !== undefined) clearInterval(timer);
          }
        };

        const tick = () => {
          const current = findBuild(build.id);
          if (!current) {
            stop();
            return;
          }
          const lines = buildLogLines(build.id);
          while (cursor < lines.length) {
            const line = lines[cursor];
            cursor += 1;
            if (!line) break;
            send(
              formatSseFrame({
                id: line.seq,
                event: 'log',
                data: JSON.stringify(line),
              }),
            );
          }
          if (current.status !== lastStatus) {
            lastStatus = current.status;
            send(
              formatSseFrame({
                event: 'status',
                data: JSON.stringify(current),
              }),
            );
          }
          if (isMockBuildTerminal(current.status) && cursor >= lines.length) {
            send(formatSseFrame({ event: 'end', data: '' }));
            stop();
          }
        };

        tick();
        timer = setInterval(tick, 150);
      },
      cancel() {
        closed = true;
        if (timer !== undefined) clearInterval(timer);
      },
    });

    return new HttpResponse(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
      },
    });
  }),
];
