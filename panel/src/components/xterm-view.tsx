import { useEffect, useRef } from 'react';
import { Terminal, type ITheme } from '@xterm/xterm';
import { FitAddon } from '@xterm/addon-fit';
import '@xterm/xterm/css/xterm.css';
import type { InteractiveSessionTicket } from '../api/generated/model';
import {
  buildWebSocketUrl,
  encodeResize,
  parseServerControl,
  type SessionViewState,
} from '../lib/interactive';
import { readToken, useResolvedTheme } from '../lib/use-theme';

/**
 * The terminal wears the log body of the panel: the sunk tone with ink text,
 * and the guest's ANSI colours mapped onto the state and kind tokens. xterm.js
 * needs resolved colours, so they are read from the tokens when the terminal
 * opens and again whenever the theme changes. The font stack is the system
 * monospace on purpose: xterm.js measures its cell before a web font loads.
 */
const withAlpha = (hex: string, alpha: string): string =>
  /^#[0-9a-f]{6}$/i.test(hex) ? `${hex}${alpha}` : hex;

export const terminalTheme = (read: (name: string) => string = readToken): ITheme => ({
  background: read('--sunk'),
  foreground: read('--ink'),
  cursor: read('--shift'),
  cursorAccent: read('--sunk'),
  selectionBackground: withAlpha(read('--accent'), '59'),
  black: read('--line-2'),
  red: read('--fault'),
  green: read('--run'),
  yellow: read('--shift'),
  blue: read('--accent'),
  magenta: read('--violet'),
  cyan: read('--ion'),
  white: read('--ink-2'),
  brightBlack: read('--ink-3'),
  brightRed: read('--fault'),
  brightGreen: read('--run'),
  brightYellow: read('--shift'),
  brightBlue: read('--accent'),
  brightMagenta: read('--k-vm'),
  brightCyan: read('--k-image'),
  brightWhite: read('--ink'),
});

const FONT_STACK =
  'ui-monospace, "SFMono-Regular", "SF Mono", Menlo, Consolas, "Liberation Mono", monospace';

const RESIZE_DEBOUNCE_MS = 150;

export interface XtermViewProps {
  /** The one-shot ticket from the open call. Never shown, stored or logged. */
  ticket: InteractiveSessionTicket;
  kind: InteractiveSessionTicket['kind'];
  /** Every phase change, so the screen can drive its status line and buttons. */
  onState: (state: SessionViewState) => void;
}

/**
 * One xterm.js terminal bound to one WebSocket. Only xterm.js renders guest
 * bytes; the clipboard and web-links addons are deliberately not loaded, so an
 * untrusted guest cannot write the clipboard (OSC 52) or open links on its own.
 *
 * `kind === 'console'` disables stdin: the serial console is read-only, so
 * keystrokes never leave the page and only resize is refused by the server.
 */
export function XtermView({ ticket, kind, onState }: XtermViewProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const onStateRef = useRef(onState);
  onStateRef.current = onState;
  const terminalRef = useRef<Terminal | null>(null);
  const theme = useResolvedTheme();

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const terminal = new Terminal({
      // A PTY already sends CR LF; the serial console may send bare LF.
      convertEol: kind === 'console',
      cursorBlink: kind === 'terminal',
      disableStdin: kind === 'console',
      fontFamily: FONT_STACK,
      fontSize: 13,
      lineHeight: 1.25,
      scrollback: 5_000,
      theme: terminalTheme(),
      allowProposedApi: false,
    });
    const fit = new FitAddon();
    terminal.loadAddon(fit);
    terminal.open(container);
    terminalRef.current = terminal;

    const report = (state: SessionViewState) => onStateRef.current(state);

    let socket: WebSocket | null = null;
    let closedByControl = false;
    let exited = false;
    let disposed = false;
    let fitFrame = 0;
    let openTimer: ReturnType<typeof setTimeout> | undefined;
    let resizeTimer: ReturnType<typeof setTimeout> | undefined;

    const fitNow = () => {
      try {
        fit.fit();
      } catch {
        // A hidden container has no geometry to fit; the observer retries.
      }
    };

    const sendResize = () => {
      if (kind !== 'terminal') return;
      if (!socket || socket.readyState !== WebSocket.OPEN) return;
      socket.send(encodeResize(terminal.rows, terminal.cols));
    };

    const scheduleResize = () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(sendResize, RESIZE_DEBOUNCE_MS);
    };

    // A console refuses client binary frames, so it never writes to the socket.
    const dataDisposable =
      kind === 'terminal'
        ? terminal.onData((data) => {
            if (!socket || socket.readyState !== WebSocket.OPEN) return;
            socket.send(new TextEncoder().encode(data));
          })
        : undefined;

    const observer =
      typeof ResizeObserver === 'undefined'
        ? null
        : new ResizeObserver(() => {
            fitNow();
            scheduleResize();
          });
    observer?.observe(container);

    fitFrame = requestAnimationFrame(() => {
      fitNow();
      // The ticket lives 30 seconds and once, so attach on the next task: a
      // React StrictMode remount cancels this timer before it spends the
      // ticket, and the second mount opens the only socket.
      openTimer = setTimeout(() => {
        if (disposed) return;
        socket = new WebSocket(buildWebSocketUrl(ticket.websocket_path, ticket.ticket, location));
        socket.binaryType = 'arraybuffer';

        socket.onopen = () => {
          if (disposed) return;
          fitNow();
          report({ phase: 'live' });
          sendResize();
          if (kind === 'terminal') terminal.focus();
        };

        socket.onmessage = (event: MessageEvent<unknown>) => {
          if (typeof event.data === 'string') {
            const control = parseServerControl(event.data);
            if (!control) return;
            if (control.type === 'exit' || control.type === 'closed') {
              // Nothing more reaches the guest: freeze the canvas.
              terminal.options.disableStdin = true;
              terminal.options.cursorBlink = false;
            }
            if (control.type === 'exit') {
              closedByControl = true;
              exited = true;
              report({ phase: 'exited', code: control.code });
              return;
            }
            if (control.type === 'closed') {
              closedByControl = true;
              // Keep the exit code on screen: `closed exited` follows `exit`.
              if (!(exited && control.reason === 'exited')) {
                report({ phase: 'closed', reason: control.reason });
              }
              return;
            }
            // An `error` control frame is not fatal (a refused frame, an
            // unknown control): the session stays open until `closed`.
            report({ phase: 'live', notice: control.message });
            return;
          }
          const data: unknown = event.data;
          if (data instanceof ArrayBuffer) {
            terminal.write(new Uint8Array(data));
          } else if (ArrayBuffer.isView(data)) {
            terminal.write(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
          }
        };

        socket.onerror = () => {
          if (closedByControl || disposed) return;
          closedByControl = true;
          report({
            phase: 'error',
            message: 'The connection dropped before the session settled. Connect again.',
          });
        };

        socket.onclose = (event) => {
          if (closedByControl || disposed) return;
          closedByControl = true;
          report({
            phase: 'closed',
            reason: event.code === 1000 ? 'ended_by_client' : 'node_disconnected',
          });
        };
      }, 0);
    });

    return () => {
      disposed = true;
      cancelAnimationFrame(fitFrame);
      clearTimeout(openTimer);
      clearTimeout(resizeTimer);
      observer?.disconnect();
      dataDisposable?.dispose();
      if (socket) {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close(1000, 'panel unmounted');
        socket = null;
      }
      terminalRef.current = null;
      terminal.dispose();
    };
    // The ticket is one-shot: any new ticket mounts a fresh terminal and socket.
  }, [ticket, kind]);

  // A theme change repaints the open terminal without touching its session.
  useEffect(() => {
    if (terminalRef.current) terminalRef.current.options.theme = terminalTheme();
  }, [theme]);

  return (
    <div
      ref={containerRef}
      className="h-[420px] w-full px-2 py-2 sm:h-[520px]"
      aria-label={kind === 'terminal' ? 'Web terminal' : 'VM serial console'}
    />
  );
}
