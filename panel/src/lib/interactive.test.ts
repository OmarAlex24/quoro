import { describe, expect, it } from 'vitest';
import { ApiProblem } from '../api/fetcher';
import {
  buildWebSocketUrl,
  clampSize,
  clearRedeployHint,
  closeReasonText,
  CLOSE_REASONS,
  encodeResize,
  isSessionLive,
  needsRedeploy,
  parseServerControl,
  readRedeployHint,
  rememberRedeployHint,
  sessionStateText,
} from './interactive';

const conflict = (detail: string) =>
  new ApiProblem({ status: 409, code: 'conflict', detail });

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

describe('needsRedeploy', () => {
  it('recognises a VM with no vsock device for a terminal', () => {
    expect(
      needsRedeploy(conflict('conflict: VM vm_01M6FH5D8PJ21YJKMRNC2PQ6W1 has no vsock device'), 'terminal'),
    ).toBe(true);
  });

  it('recognises a serial port that is not a socket for the console', () => {
    expect(
      needsRedeploy(
        conflict("conflict: VM vm_01M6FH5D8PJ21YJKMRNC2PQ6W1's serial port is not a socket"),
        'console',
      ),
    ).toBe(true);
  });

  it('recognises the two texts only for their own kind', () => {
    expect(
      needsRedeploy(conflict('conflict: VM vm_1 has no vsock device'), 'console'),
    ).toBe(false);
    expect(
      needsRedeploy(conflict("conflict: VM vm_1's serial port is not a socket"), 'terminal'),
    ).toBe(false);
  });

  it('leaves the other 409 conflicts alone', () => {
    const others = [
      'The app has no running instance to open a terminal in.',
      'conflict: VM vm_1 is not running',
      'conflict: the node session holds too many open streams',
    ];
    for (const detail of others) {
      expect(needsRedeploy(conflict(detail), 'terminal')).toBe(false);
      expect(needsRedeploy(conflict(detail), 'console')).toBe(false);
    }
  });

  it('is false for a 504 and for things that are not problem documents', () => {
    expect(needsRedeploy(new ApiProblem({ status: 504, code: 'internal' }), 'terminal')).toBe(
      false,
    );
    expect(needsRedeploy(new Error('network down'), 'terminal')).toBe(false);
    expect(needsRedeploy(null, 'console')).toBe(false);
    expect(needsRedeploy({ status: 409, code: 'conflict', detail: 'has no vsock device' }, 'terminal')).toBe(
      false,
    );
  });

  it('is false when the problem carries no detail', () => {
    expect(needsRedeploy(new ApiProblem({ status: 409, code: 'conflict' }), 'console')).toBe(false);
  });
});

describe('redeploy hint storage', () => {
  // The unit environment has no window, so storage is refused rather than
  // throwing; the hint then lives only in the component state for one view.
  it('works without sessionStorage', () => {
    expect(readRedeployHint('terminal', 'vm_1')).toBe(false);
    expect(() => rememberRedeployHint('terminal', 'vm_1')).not.toThrow();
    expect(() => clearRedeployHint('terminal', 'vm_1')).not.toThrow();
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
