/**
 * The wire protocol of a terminal or console session (docs/contracts/openapi.yaml,
 * `connectInteractiveSession`), kept pure so it can be tested without a browser.
 *
 * Binary frames are raw terminal bytes; text frames are one JSON control object
 * each. The ticket is single-use and valid for 30 seconds, so the caller opens
 * the WebSocket immediately after the POST and never shows or stores it.
 */

export const CLOSE_REASONS = [
  'exited',
  'ended_by_client',
  'ended_by_request',
  'ticket_expired',
  'idle_timeout',
  'max_duration',
  'access_revoked',
  'credential_expired',
  'vm_stopped',
  'node_disconnected',
  'transcript_limit',
  'primary_restarted',
  'error',
] as const;

export type CloseReason = (typeof CLOSE_REASONS)[number];

export const isCloseReason = (value: unknown): value is CloseReason =>
  typeof value === 'string' && (CLOSE_REASONS as readonly string[]).includes(value);

/** What the server can say in a text frame. */
export type ServerControl =
  | { type: 'exit'; code: number }
  | { type: 'closed'; reason: CloseReason }
  | { type: 'error'; code: string; message: string };

/** A terminal size the server accepts: whole columns and rows, 1 to 1000. */
export const clampSize = (value: number): number => {
  if (!Number.isFinite(value)) return 1;
  return Math.min(1000, Math.max(1, Math.round(value)));
};

/**
 * Parse one text frame. Anything that is not a control object the panel knows
 * is ignored (the contract says unknown controls are ignored), and a malformed
 * close reason falls back to the generic `error` reason instead of guessing.
 */
export const parseServerControl = (data: string): ServerControl | null => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null;

  const value = parsed as Record<string, unknown>;
  if (value.type === 'exit') {
    return typeof value.code === 'number' && Number.isInteger(value.code)
      ? { type: 'exit', code: value.code }
      : null;
  }
  if (value.type === 'closed') {
    return {
      type: 'closed',
      reason: isCloseReason(value.reason) ? value.reason : 'error',
    };
  }
  if (value.type === 'error') {
    const code =
      typeof value.code === 'string'
        ? value.code
        : typeof value.code === 'number'
          ? String(value.code)
          : 'internal';
    return {
      type: 'error',
      code,
      message:
        typeof value.message === 'string' && value.message !== ''
          ? value.message
          : 'The session reported an error.',
    };
  }
  return null;
};

/** The `resize` control a terminal sends when its fitted size changes. */
export const encodeResize = (rows: number, cols: number): string =>
  JSON.stringify({ type: 'resize', rows: clampSize(rows), cols: clampSize(cols) });

/** One sentence per close reason, in the panel's plain words. */
export const closeReasonText = (reason: CloseReason): string => {
  switch (reason) {
    case 'exited':
      return 'The program exited.';
    case 'ended_by_client':
      return 'The session closed from this window.';
    case 'ended_by_request':
      return 'The session was ended on request.';
    case 'ticket_expired':
      return 'The ticket expired before the terminal attached. Connect again.';
    case 'idle_timeout':
      return 'Closed after 15 minutes without input.';
    case 'max_duration':
      return 'Closed after the 8-hour session limit.';
    case 'access_revoked':
      return 'Access to this session was revoked.';
    case 'credential_expired':
      return 'The credential that opened this session expired. Log in again.';
    case 'vm_stopped':
      return 'The VM stopped, so the session ended.';
    case 'node_disconnected':
      return 'The node disconnected. Sessions do not resume.';
    case 'transcript_limit':
      return 'The transcript reached its 16 MiB limit.';
    case 'primary_restarted':
      return 'The primary restarted while the session was open.';
    case 'error':
      return 'The session closed on an error.';
  }
};

/** The panel's view of one session, from connect to close. */
export type SessionViewState =
  | { phase: 'connecting' }
  | { phase: 'live'; notice?: string }
  | { phase: 'exited'; code: number }
  | { phase: 'closed'; reason: CloseReason }
  | { phase: 'error'; message: string };

/** The status line under the canvas: short, and never the ticket. */
export const sessionStateText = (state: SessionViewState | null): string => {
  if (state === null) return 'Not connected';
  switch (state.phase) {
    case 'connecting':
      return 'Connecting';
    case 'live':
      return state.notice ? `Live. ${state.notice}` : 'Live';
    case 'exited':
      return `Exited with code ${state.code}`;
    case 'closed':
      return closeReasonText(state.reason);
    case 'error':
      return state.message;
  }
};

export const isSessionLive = (state: SessionViewState | null): boolean =>
  state !== null && (state.phase === 'connecting' || state.phase === 'live');

export interface LocationLike {
  protocol: string;
  host: string;
}

/**
 * `wss://host/api/v1/interactive-sessions/<id>/ws?ticket=<ticket>` on an HTTPS
 * page, `ws://` on plain HTTP. The ticket is encoded, never logged.
 */
export const buildWebSocketUrl = (
  websocketPath: string,
  ticket: string,
  location: LocationLike,
): string => {
  const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const path = websocketPath.startsWith('/') ? websocketPath : `/${websocketPath}`;
  return `${scheme}//${location.host}${path}?ticket=${encodeURIComponent(ticket)}`;
};
