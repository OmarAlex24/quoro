// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import type { BuildLogLine } from '../api/generated/model';
import {
  buildLogStreamUrl,
  failingLineIndex,
  orderBuildLog,
  parseBuildStreamEvent,
} from './build-log';

const line = (seq: number, text = `line ${seq}`): BuildLogLine => ({
  seq,
  time: '2026-11-03T10:00:00.000Z',
  stream: 'stdout',
  vertex: null,
  line: text,
});

describe('orderBuildLog', () => {
  it('orders a resumed tail by seq without repeating a line', () => {
    const ordered = orderBuildLog(
      [line(1), line(2), line(3)],
      [line(3, 'replayed'), line(4)],
    );
    expect(ordered.map((entry) => entry.seq)).toEqual([1, 2, 3, 4]);
    expect(ordered[2]?.line).toBe('replayed');
  });

  it('ignores malformed sequences instead of letting them in', () => {
    const ordered = orderBuildLog([line(1)], [
      { ...line(0), seq: 0 },
      { ...line(2), seq: 2 },
    ]);
    expect(ordered.map((entry) => entry.seq)).toEqual([1, 2]);
  });

  it('keeps only the newest lines once the cap is reached', () => {
    const ordered = orderBuildLog([line(1), line(2)], [line(3)], 2);
    expect(ordered.map((entry) => entry.seq)).toEqual([2, 3]);
  });
});

describe('parseBuildStreamEvent', () => {
  it('reads a log frame with its vertex', () => {
    const event = parseBuildStreamEvent({
      id: '7',
      event: 'log',
      data: JSON.stringify({
        seq: 7,
        time: '2026-11-03T10:00:00.000Z',
        stream: 'stderr',
        vertex: 5,
        line: '#5 DONE 1.2s',
      }),
    });
    expect(event).toEqual({
      kind: 'log',
      line: {
        seq: 7,
        time: '2026-11-03T10:00:00.000Z',
        stream: 'stderr',
        vertex: 5,
        line: '#5 DONE 1.2s',
      },
    });
  });

  it('drops a malformed log line', () => {
    expect(
      parseBuildStreamEvent({ event: 'log', data: '{"seq":0,"line":"x"}' }).kind,
    ).toBe('unknown');
    expect(parseBuildStreamEvent({ event: 'log', data: 'not json' }).kind).toBe(
      'unknown',
    );
  });

  it('reads a status frame and requires a known status', () => {
    const event = parseBuildStreamEvent({
      event: 'status',
      data: JSON.stringify({ id: 'bld_1', status: 'building' }),
    });
    expect(event.kind).toBe('status');
    expect(
      parseBuildStreamEvent({
        event: 'status',
        data: JSON.stringify({ id: 'bld_1', status: 'exploded' }),
      }).kind,
    ).toBe('unknown');
  });

  it('turns an error frame into plain words and recognises end', () => {
    const error = parseBuildStreamEvent({
      event: 'error',
      data: JSON.stringify({
        code: 'node_unreachable',
        status: 504,
        detail: 'The node is not reachable.',
      }),
    });
    expect(error.kind).toBe('error');
    if (error.kind === 'error') {
      expect(error.problem.code).toBe('node_unreachable');
      expect(error.problem.message).toBe('The node is not reachable.');
    }
    expect(parseBuildStreamEvent({ event: 'end', data: '' }).kind).toBe('end');
    expect(parseBuildStreamEvent({ event: 'other', data: '' }).kind).toBe(
      'unknown',
    );
  });
});

describe('buildLogStreamUrl', () => {
  it('addresses the stream with a tail', () => {
    expect(buildLogStreamUrl('bld_ABC')).toBe(
      '/api/v1/builds/bld_ABC/logs/stream?tail=2000',
    );
  });
});

describe('failingLineIndex', () => {
  const excerpt = [
    'Dockerfile:3',
    '--------------------',
    '   1 | FROM node:22-alpine',
    '   2 |     RUN echo "step one ok"',
    '   3 | >>> RUN ls /does-not-exist',
    '--------------------',
    'ERROR: failed to solve: exit code: 2',
  ];

  it('finds the line BuildKit marks with >>>', () => {
    expect(failingLineIndex(excerpt, 3)).toBe(4);
  });

  it('falls back to the Dockerfile line number', () => {
    const unmarked = ['   1 | FROM alpine', '   2 | RUN false'];
    expect(failingLineIndex(unmarked, 2)).toBe(1);
  });

  it('returns -1 when the excerpt points at no single line', () => {
    expect(failingLineIndex(excerpt, null)).toBe(4);
    expect(failingLineIndex(['ERROR: timeout'], null)).toBe(-1);
    expect(failingLineIndex(['   1 | FROM alpine'], 9)).toBe(-1);
  });
});
