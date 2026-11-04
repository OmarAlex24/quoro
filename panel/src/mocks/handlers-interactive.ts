import { http, HttpResponse, ws } from 'msw';
import type {
  CreateConsoleSessionRequest,
  CreateTerminalSessionRequest,
  InteractiveSessionTicket,
  InteractiveSessionTicketKind,
  Vm,
} from '../api/generated/model';
import { db, findApp, mockId } from './db';
import {
  isIntAtLeast,
  json,
  problem,
  readJson,
  signedIn,
  unauthorized,
  writeRefusal,
} from './http';

/** Mock mode only: the terminal and console sessions of ADR 0113 / 0116. */

interface MockSession {
  id: string;
  kind: InteractiveSessionTicketKind;
  vmId: string;
  ticket: string;
  expiresAt: number;
  attached: boolean;
  /** Let the DELETE close a WebSocket that is attached right now. */
  close?: () => void;
}

const sessions = new Map<string, MockSession>();

const randomTicket = (): string => {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
};

const openSession = (
  kind: InteractiveSessionTicketKind,
  vm: Vm,
): InteractiveSessionTicket => {
  const id = mockId('trm');
  const session: MockSession = {
    id,
    kind,
    vmId: vm.id,
    ticket: randomTicket(),
    expiresAt: Date.now() + 30_000,
    attached: false,
  };
  sessions.set(id, session);
  return {
    id,
    kind,
    vm_id: vm.id,
    ticket: session.ticket,
    expires_at: new Date(session.expiresAt).toISOString(),
    websocket_path: `/api/v1/interactive-sessions/${id}/ws`,
  };
};

const encoder = new TextEncoder();

/** A small, typed view of the MSW WebSocket handle, so the mocks stay readable. */
interface MockSocket {
  send(data: string | Uint8Array<ArrayBuffer>): void;
  close(code: number, reason?: string): void;
  onMessage(listener: (data: unknown) => void): void;
  onClose(listener: () => void): void;
}

const sendText = (socket: MockSocket, text: string) => {
  socket.send(encoder.encode(text));
};

const sendControl = (socket: MockSocket, control: Record<string, unknown>) => {
  socket.send(JSON.stringify(control));
};

const bytesOf = async (data: unknown): Promise<Uint8Array | null> => {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (data instanceof Blob) return new Uint8Array(await data.arrayBuffer());
  return null;
};

const terminalBanner = (vm: Vm): string =>
  [
    '',
    'Quoro web terminal · mock guest',
    `instance ${vm.name} · ${vm.vcpus} vCPU · ${vm.memory_mb} MiB · ${vm.image}`,
    'Type a command. `exit` ends the session.',
    '',
    '/ # ',
  ].join('\r\n');

const runTerminal = (socket: MockSocket, vm: Vm) => {
  let line = '';
  let ended = false;

  sendText(socket, terminalBanner(vm));

  const reply = (text: string) => {
    sendText(socket, `\r\n${text}\r\n/ # `);
  };

  const finish = () => {
    if (ended) return;
    ended = true;
    sendText(socket, '\r\n');
    sendControl(socket, { type: 'exit', code: 0 });
    sendControl(socket, { type: 'closed', reason: 'exited' });
    // The in-page mock may deliver a close before queued messages; a real
    // WebSocket keeps the order, so give the frames a moment first.
    setTimeout(() => socket.close(1000, 'exited'), 50);
  };

  const onCommand = () => {
    const command = line.trim();
    line = '';
    if (command === '') {
      sendText(socket, '/ # ');
      return;
    }
    if (command === 'exit') {
      sendText(socket, '\r\n');
      finish();
      return;
    }
    if (command === 'uname -a') {
      reply(
        'Linux blog-1 6.12.0-quoro #1 SMP PREEMPT_DYNAMIC Fri Oct 30 09:14:02 UTC 2026 x86_64 GNU/Linux',
      );
      return;
    }
    if (command === 'help') {
      reply('Built-ins: uname -a, help, exit.');
      return;
    }
    reply(`sh: ${command}: not found`);
  };

  socket.onMessage((data) => {
    void (async () => {
      if (ended) return;
      if (typeof data === 'string') {
        // A resize control; the mock has nothing to reflow.
        return;
      }
      const bytes = await bytesOf(data);
      if (!bytes) return;
      for (const byte of bytes) {
        if (byte === 13 || byte === 10) {
          onCommand();
        } else if (byte === 127 || byte === 8) {
          if (line.length > 0) {
            line = line.slice(0, -1);
            sendText(socket, '\b \b');
          }
        } else if (byte === 3) {
          line = '';
          sendText(socket, '^C\r\n/ # ');
        } else if (byte >= 32) {
          line += String.fromCharCode(byte);
          sendText(socket, String.fromCharCode(byte));
        }
      }
    })();
  });
};

const BOOT_LOG = [
  '[    0.000000] Linux version 6.12.0-quoro (build@quoro) (gcc 14.2.0) #1 SMP PREEMPT_DYNAMIC',
  '[    0.000000] Command line: console=ttyS0 loglevel=4 panic=-1 rdinit=/init',
  '[    0.000000] BIOS-provided physical RAM map:',
  '[    0.000000] BIOS-e820: [mem 0x0000000000000000-0x000000000009fbff] usable',
  '[    0.000000] BIOS-e820: [mem 0x0000000000100000-0x000000003fffffff] usable',
  '[    0.000000] NX (Execute Disable) protection: active',
  '[    0.000000] DMI: Cloud Hypervisor cloud-hypervisor, BIOS 0.3.0',
  '[    0.000000] smpboot: Allowing 2 CPUs, 0 hotplug CPUs',
  '[    0.000000] ACPI: Early table checksum verification disabled',
  '[    0.000000] clocksource: refined-jiffies: mask: 0xffffffff max_cycles: 0xffffffff',
  '[    0.000000] Mount-cache hash table entries: 4096 (order: 3, 32768 bytes)',
  '[    0.031251] virtio_blk virtio1: [vda] 4194304 512-byte logical blocks (2.15 GB/2.00 GiB)',
  '[    0.042882] virtio_net virtio2 eth0: renamed from eth0',
  '[    0.057164] EXT4-fs (vda): mounted filesystem 8f2c1a9e-1d3b-4f0a-9a7e-2b6c5d4e3f21 ro',
  '[    0.068915] Freeing unused kernel image (initmem) memory: 1024K',
  '[    0.074226] Run /init as init process',
  'quoro-init: booting',
  'quoro-init: diagnostics console ready',
  'quoro-init: mounting /proc /sys /dev /run',
  'quoro-init: root: mounting the read-only image',
  'quoro-init: root: switched to the overlay',
  'quoro-init: net: eth0 up, address 10.0.0.7/24',
  'quoro-init: exec: listening on vsock port 1024',
  'quoro-init: env: 2 variables from the app',
  'quoro-init: app: starting /usr/local/bin/blog on port 8080',
  'quoro-init: app: pid 41 is running',
  'quoro-init: ready',
];

const CONSOLE_TICK = [
  '[  31.427110] IPv6: ADDRCONF(NETDEV_CHANGE): eth0: link becomes ready',
  '[  45.008913] audit: type=1400 apparmor="STATUS" operation="profile_load"',
  'quoro-init: health: / still answers 200 after 1.2 ms',
  '[  60.112004] TCP: request_sock_TCP: Possible SYN flooding on port 8080',
  '[  75.663271] virtio_net virtio2 eth0: transmit queue 0 timed out',
  'quoro-init: health: / still answers 200 after 1.1 ms',
];

const runConsole = (socket: MockSocket) => {
  let tick = 0;

  sendText(socket, `${BOOT_LOG.join('\r\n')}\r\n`);

  const timer = setInterval(() => {
    sendText(socket, `${CONSOLE_TICK[tick % CONSOLE_TICK.length]}\r\n`);
    tick += 1;
  }, 3_000);

  socket.onClose(() => clearInterval(timer));
  socket.onMessage((data) => {
    if (typeof data === 'string') return;
    sendControl(socket, {
      type: 'error',
      code: 'forbidden',
      message: 'This console is read-only; input is refused.',
    });
  });
};

/** Intercepts the browser's WebSocket for an interactive session upgrade. */
const interactiveSocket = ws.link('*/api/v1/interactive-sessions/:sessionId/ws');

const interactiveSocketHandler = interactiveSocket.addEventListener(
  'connection',
  ({ client, params }) => {
  const sessionId = String(params.sessionId);
  const session = sessions.get(sessionId);
  const ticket = client.url.searchParams.get('ticket');

  if (
    !session ||
    session.attached ||
    ticket !== session.ticket ||
    Date.now() > session.expiresAt
  ) {
    client.close(1008, 'ticket refused');
    return;
  }
  session.attached = true;

  const socket: MockSocket = {
    send: (data) => client.send(data),
    close: (code, reason) => client.close(code, reason),
    onMessage: (listener) =>
      client.addEventListener('message', (event) => listener(event.data)),
    onClose: (listener) => client.addEventListener('close', () => listener()),
  };

  session.close = () => {
    sendControl(socket, { type: 'closed', reason: 'ended_by_request' });
    client.close(1000, 'ended_by_request');
  };

  client.addEventListener('close', () => {
    session.attached = false;
    session.close = undefined;
    sessions.delete(sessionId);
  });

  const vm = db.vms.find((candidate) => candidate.id === session.vmId);
  if (!vm) {
    client.close(1000, 'vm gone');
    return;
  }
  if (session.kind === 'terminal') {
    runTerminal(socket, vm);
  } else {
    runConsole(socket);
  }
  },
);

export const interactiveHandlers = [
  interactiveSocketHandler,

  http.post('*/api/v1/apps/:appId/terminal-sessions', async ({ request, params }) => {
    const user = signedIn();
    if (!user) return unauthorized();
    if (user.role === 'viewer') return writeRefusal('app:terminal');

    const app = findApp(String(params.appId));
    if (!app) {
      return problem(404, 'not_found', { detail: 'That app no longer exists.' });
    }

    const body = await readJson<CreateTerminalSessionRequest>(request);
    if (!body || !isIntAtLeast(body.rows, 1) || !isIntAtLeast(body.cols, 1)) {
      return problem(422, 'validation_failed', {
        detail: 'A terminal size needs whole rows and columns.',
      });
    }
    if (body.rows > 1000 || body.cols > 1000) {
      return problem(422, 'validation_failed', {
        detail: 'Rows and columns must be 1000 or fewer.',
      });
    }

    const vm = db.vms.find(
      (candidate) =>
        candidate.app_id === app.id &&
        candidate.observed_status === 'running' &&
        (body.vm_id === undefined || candidate.id === body.vm_id),
    );
    if (!vm) {
      return problem(409, 'conflict', {
        detail: 'The app has no running instance to open a terminal in.',
      });
    }

    return json(openSession('terminal', vm), { status: 201 });
  }),

  // The console screen reads one VM; the stateful mock answers from its own
  // database instead of the generated faker record.
  http.get('*/api/v1/vms/:vmId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const vm = db.vms.find((candidate) => candidate.id === String(params.vmId));
    if (!vm) return problem(404, 'not_found', { detail: 'That VM no longer exists.' });
    return json(vm);
  }),

  http.post('*/api/v1/vms/:vmId/console-sessions', async ({ request, params }) => {
    if (!signedIn()) return unauthorized();

    const vm = db.vms.find((candidate) => candidate.id === String(params.vmId));
    if (!vm) {
      return problem(404, 'not_found', { detail: 'That VM no longer exists.' });
    }

    const body = (await readJson<CreateConsoleSessionRequest>(request)) ?? {};
    if (
      body.tail_bytes !== undefined &&
      (!isIntAtLeast(body.tail_bytes, 0) || body.tail_bytes > 1_048_576)
    ) {
      return problem(422, 'validation_failed', {
        detail: 'tail_bytes must be a whole number between 0 and 1048576.',
      });
    }

    return json(openSession('console', vm), { status: 201 });
  }),

  http.delete('*/api/v1/interactive-sessions/:sessionId', ({ params }) => {
    if (!signedIn()) return unauthorized();
    const sessionId = String(params.sessionId);
    const session = sessions.get(sessionId);
    if (!session) {
      return problem(404, 'not_found', {
        detail: 'That session already ended.',
      });
    }
    session.close?.();
    sessions.delete(sessionId);
    return new HttpResponse(null, { status: 204 });
  }),
];

/** A reset can forget every open mock session. */
export const resetInteractiveSessions = (): void => {
  for (const session of sessions.values()) session.close?.();
  sessions.clear();
};
