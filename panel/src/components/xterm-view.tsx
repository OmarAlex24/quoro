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

/**
 * The panel's dark face for a guest terminal: the room lights are off, the
 * guest's own colours and cursor are the only light. The font stack is the one
 * place the panel allows monospace.
 */
const THEME: ITheme = {
  background: '#0f1a26',
  foreground: '#dce1e5',
  cursor: '#e3a008',
  cursorAccent: '#0f1a26',
  selectionBackground: 'rgb(38 67 181 / 0.45)',
  black: '#142030',
  red: '#d0412f',
  green: '#1f9d5a',
  yellow: '#e3a008',
  blue: '#5b83e6',
  magenta: '#8a7cc0',
  cyan: '#4aa3b8',
  white: '#dce1e5',
  brightBlack: '#5b6878',
  brightRed: '#e0604d',
  brightGreen: '#35b975',
  brightYellow: '#f0b52e',
  brightBlue: '#7c9cef',
  brightMagenta: '#a396d8',
  brightCyan: '#6cc2d4',
  brightWhite: '#ffffff',
};

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
      theme: THEME,
      allowProposedApi: false,
    });
    const fit = new FitAddon();
    terminal.loadAddon(fit);
    terminal.open(container);

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
      terminal.dispose();
    };
    // The ticket is one-shot: any new ticket mounts a fresh terminal and socket.
  }, [ticket, kind]);

  return (
    <div
      ref={containerRef}
      className="h-[420px] w-full px-2 py-2 sm:h-[520px]"
      aria-label={kind === 'terminal' ? 'Web terminal' : 'VM serial console'}
    />
  );
}
