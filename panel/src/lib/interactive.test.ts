import { describe, expect, it } from 'vitest';
import {
  buildWebSocketUrl,
  clampSize,
  closeReasonText,
  CLOSE_REASONS,
  encodeResize,
  isSessionLive,
  parseServerControl,
  sessionStateText,
} from './interactive';

describe('parseServerControl', () => {
  it('reads an exit frame with its code', () => {
    expect(parseServerControl('{"type":"exit","code":0}')).toEqual({
      type: 'exit',
      code: 0,
    });
    expect(parseServerControl('{"type":"exit","code":127}')).toEqual({
      type: 'exit',
      code: 127,
    });
  });

  it('reads a closed frame with a known reason', () => {
    expect(parseServerControl('{"type":"closed","reason":"idle_timeout"}')).toEqual({
      type: 'closed',
      reason: 'idle_timeout',
    });
  });

  it('falls back to error for an unknown close reason', () => {
    expect(parseServerControl('{"type":"closed","reason":"meteor"}')).toEqual({
      type: 'closed',
      reason: 'error',
    });
  });

  it('reads an error frame with a Problem code as a string', () => {
    expect(
      parseServerControl('{"type":"error","code":"conflict","message":"The console is read-only."}'),
    ).toEqual({
      type: 'error',
      code: 'conflict',
      message: 'The console is read-only.',
    });
  });

  it('keeps a numeric error code as text', () => {
    expect(parseServerControl('{"type":"error","code":1011,"message":"boom"}')).toEqual({
      type: 'error',
      code: '1011',
      message: 'boom',
    });
  });

  it('fills a missing error message', () => {
    expect(parseServerControl('{"type":"error","code":"internal"}')).toEqual({
      type: 'error',
      code: 'internal',
      message: 'The session reported an error.',
    });
  });

  it('ignores malformed input and unknown controls', () => {
    expect(parseServerControl('not json')).toBeNull();
    expect(parseServerControl('null')).toBeNull();
    expect(parseServerControl('[]')).toBeNull();
    expect(parseServerControl('{"type":"ping"}')).toBeNull();
    expect(parseServerControl('{"type":"exit"}')).toBeNull();
  });
});

describe('encodeResize', () => {
  it('writes whole rows and columns', () => {
    expect(encodeResize(24, 80)).toBe('{"type":"resize","rows":24,"cols":80}');
  });

  it('rounds and clamps to the contract range', () => {
    expect(encodeResize(0, 5000)).toBe('{"type":"resize","rows":1,"cols":1000}');
    expect(encodeResize(23.6, 79.2)).toBe('{"type":"resize","rows":24,"cols":79}');
    expect(encodeResize(Number.NaN, -3)).toBe('{"type":"resize","rows":1,"cols":1}');
    expect(clampSize(12)).toBe(12);
  });
});

describe('closeReasonText', () => {
  it('has a sentence for every reason in the contract', () => {
    for (const reason of CLOSE_REASONS) {
      expect(closeReasonText(reason).length).toBeGreaterThan(0);
    }
  });

  it('says the idle timeout in minutes', () => {
    expect(closeReasonText('idle_timeout')).toBe('Closed after 15 minutes without input.');
  });
});

describe('sessionStateText', () => {
  it('names each phase', () => {
    expect(sessionStateText(null)).toBe('Not connected');
    expect(sessionStateText({ phase: 'connecting' })).toBe('Connecting');
    expect(sessionStateText({ phase: 'live' })).toBe('Live');
    expect(sessionStateText({ phase: 'exited', code: 2 })).toBe('Exited with code 2');
    expect(sessionStateText({ phase: 'closed', reason: 'max_duration' })).toBe(
      'Closed after the 8-hour session limit.',
    );
    expect(sessionStateText({ phase: 'error', message: 'The node is unreachable.' })).toBe(
      'The node is unreachable.',
    );
  });

  it('treats connecting and live as live for the controls', () => {
    expect(isSessionLive({ phase: 'connecting' })).toBe(true);
    expect(isSessionLive({ phase: 'live' })).toBe(true);
    expect(isSessionLive({ phase: 'exited', code: 0 })).toBe(false);
    expect(isSessionLive(null)).toBe(false);
  });
});

describe('buildWebSocketUrl', () => {
  const path = '/api/v1/interactive-sessions/trm_01JC9Z6Q5W8N3K4T7R2M1XHB0E/ws';

  it('uses wss on an https page', () => {
    expect(
      buildWebSocketUrl(path, 'abc-_123', { protocol: 'https:', host: 'panel.example.com' }),
    ).toBe(`wss://panel.example.com${path}?ticket=abc-_123`);
  });

  it('uses ws on an http page', () => {
    expect(
      buildWebSocketUrl(path, 'abc', { protocol: 'http:', host: 'localhost:5173' }),
    ).toBe(`ws://localhost:5173${path}?ticket=abc`);
  });

  it('adds a leading slash and encodes the ticket', () => {
    expect(
      buildWebSocketUrl('api/v1/interactive-sessions/a/ws', 'a/b+c=', {
        protocol: 'https:',
        host: 'h',
      }),
    ).toBe('wss://h/api/v1/interactive-sessions/a/ws?ticket=a%2Fb%2Bc%3D');
  });
});
