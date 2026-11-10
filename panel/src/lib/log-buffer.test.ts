import { describe, expect, it } from 'vitest';
import {
  appendLogLines,
  formatLogTime,
  formatSseFrame,
  isTransientProblem,
  LOG_LINE_CAP,
  parseSseChunk,
  parseSseFrame,
  problemViewFromEvent,
} from './log-buffer';

describe('appendLogLines', () => {
  it('appends in order', () => {
    expect(appendLogLines(['a'], ['b', 'c'])).toEqual(['a', 'b', 'c']);
  });

  it('keeps at most 2000 lines and drops the oldest', () => {
    const current = Array.from({ length: LOG_LINE_CAP }, (_, index) => index);
    const next = appendLogLines(current, [LOG_LINE_CAP, LOG_LINE_CAP + 1]);
    expect(next).toHaveLength(LOG_LINE_CAP);
    expect(next[0]).toBe(2);
    expect(next[next.length - 1]).toBe(LOG_LINE_CAP + 1);
  });

  it('honours a smaller cap', () => {
    expect(appendLogLines([1, 2], [3, 4], 3)).toEqual([2, 3, 4]);
  });

  it('returns the same list when nothing arrives', () => {
    const current = ['a'];
    expect(appendLogLines(current, [])).toBe(current);
  });
});

describe('SSE frames', () => {
  it('writes id, event and data with a blank line', () => {
    expect(
      formatSseFrame({ id: 7, event: 'log', data: '{"line":"hi"}' }),
    ).toBe('id: 7\nevent: log\ndata: {"line":"hi"}\n\n');
  });

  it('prefixes every line of a multi-line payload', () => {
    expect(formatSseFrame({ event: 'log', data: 'a\nb' })).toBe(
      'event: log\ndata: a\ndata: b\n\n',
    );
  });

  it('parses a single frame', () => {
    expect(parseSseFrame('id: 3\nevent: log\ndata: {"line":"ok"}')).toEqual({
      id: '3',
      event: 'log',
      data: '{"line":"ok"}',
    });
  });

  it('joins multi-line data and handles CRLF', () => {
    expect(parseSseFrame('id: 4\r\nevent: log\r\ndata: a\r\ndata: b')).toEqual({
      id: '4',
      event: 'log',
      data: 'a\nb',
    });
  });

  it('ignores comments and empty blocks', () => {
    expect(parseSseFrame(': keep-alive')).toBeNull();
    expect(parseSseFrame('')).toBeNull();
  });
});

describe('parseSseChunk', () => {
  it('returns complete frames and keeps the unfinished tail', () => {
    const first = parseSseChunk('', 'id: 1\nevent: log\ndata: one\n\nid: 2\nevent: log\nda');
    expect(first.frames).toEqual([{ id: '1', event: 'log', data: 'one' }]);
    expect(first.rest).toBe('id: 2\nevent: log\nda');

    const second = parseSseChunk(first.rest, 'ta: two\n\n');
    expect(second.frames).toEqual([{ id: '2', event: 'log', data: 'two' }]);
    expect(second.rest).toBe('');
  });
});

describe('formatLogTime', () => {
  it('renders local HH:MM:SS.mmm', () => {
    expect(formatLogTime('2026-11-02T12:34:56.789Z')).toMatch(
      /^\d{2}:\d{2}:\d{2}\.\d{3}$/,
    );
  });

  it('keeps a placeholder for an invalid timestamp', () => {
    expect(formatLogTime('not a time')).toBe('--:--:--.---');
  });
});

describe('SSE problems', () => {
  it('says a node problem in plain words and marks it transient', () => {
    const view = problemViewFromEvent(
      JSON.stringify({
        status: 504,
        code: 'node_unreachable',
        detail: 'The node did not answer.',
      }),
    );
    expect(view.message).toBe('The node did not answer.');
    expect(view.code).toBe('node_unreachable');
    expect(isTransientProblem(view)).toBe(true);
  });

  it('closes on a problem that is not transient', () => {
    const view = problemViewFromEvent(
      JSON.stringify({ status: 403, code: 'forbidden', reason: 'scope_missing' }),
    );
    expect(view.code).toBe('forbidden');
    expect(isTransientProblem(view)).toBe(false);
  });

  it('knows a body above the limit', () => {
    const view = problemViewFromEvent(
      JSON.stringify({ status: 413, code: 'payload_too_large' }),
    );
    expect(view.code).toBe('payload_too_large');
    expect(view.message).toBe('That request is too large to send. Send less at once.');
  });

  it('falls back to plain words when the data is not JSON', () => {
    expect(problemViewFromEvent('boom').message).toContain('log stream stopped');
  });
});
