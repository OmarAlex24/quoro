// SPDX-License-Identifier: AGPL-3.0-only
import * as DialogPrimitive from '@radix-ui/react-dialog';
import { Link, useNavigate } from '@tanstack/react-router';
import { SquareTerminal } from 'lucide-react';
import { useRef, useState } from 'react';
import { useGetApp } from '../api/generated/apps/apps';
import type { Vm } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { useGetVm } from '../api/generated/vms/vms';
import { canViewConsole } from '../lib/capabilities';
import { isConverging, lampState } from '../lib/lamp';
import { nodeReadout } from '../lib/nodes';
import { problemView, type ProblemView } from '../lib/problem';
import { PROJECT_LIST_PARAMS } from '../lib/projects';
import { VM_TABS, type VmTab } from '../lib/routes';
import { memoryWords, vmSize } from '../lib/services';
import { usePaneEnter } from '../lib/use-tab-motion';
import { useSession } from '../session';
import { GlyphTile } from './glyph-tile';
import { ImageRef } from './image-ref';
import { LampDot } from './lamp';
import { CloseButton, SheetTabs } from './service-sheet';
import { Group, Rows } from './sheet-settings';
import { Button } from './ui/button';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { DeleteVmDialog, useVmAction, VmActionsMenu, vmActionNotice } from './vm-actions';

const TAB_WORDS: Record<VmTab, string> = {
  overview: 'Overview',
  settings: 'Settings',
};

const code = (value: string) => <span className="type-code text-[11.5px]">{value}</span>;

/**
 * A VM opened from its project's canvas or the VM table: the same sheet as an
 * app's, with the VM's lamp and two tabs. Overview says what the VM is and
 * where it runs; Settings holds the console and delete. Start and stop sit in
 * the ⋯ menu and the console in the header, as the VM bay had them. Quoro
 * keeps no metrics for a single VM, so there is no Metrics tab.
 */
export function VmSheet({
  projectId,
  vmId,
  tab,
  onClose,
}: {
  projectId: string;
  vmId: string;
  tab: VmTab;
  onClose: () => void;
}) {
  const contentRef = useRef<HTMLDivElement>(null);
  const vmQuery = useGetVm(vmId, {
    query: {
      retry: false,
      refetchInterval: (query) => {
        const vm = query.state.data;
        return vm && isConverging(vm.desired_status, vm.observed_status) ? 1_000 : 10_000;
      },
    },
  });
  const vm = vmQuery.data;

  return (
    <DialogPrimitive.Root open modal={false} onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPrimitive.Content
        ref={contentRef}
        className="sheet svc-sheet"
        aria-describedby={undefined}
        // The canvas stays usable behind the sheet, as with an app.
        onInteractOutside={(event) => event.preventDefault()}
        onOpenAutoFocus={(event) => {
          event.preventDefault();
          contentRef.current?.focus();
        }}
        onCloseAutoFocus={(event) => event.preventDefault()}
        onEscapeKeyDown={(event) => {
          if ((event.target as Element | null)?.closest?.('[role="menu"]')) event.preventDefault();
        }}
      >
        {vm ? (
          <VmSheetBody key={vm.id} vm={vm} projectId={projectId} tab={tab} />
        ) : (
          <>
            <div className="sh-head">
              <div className="sh-title">
                <DialogPrimitive.Title className="sh-name">
                  {vmQuery.isPending ? 'Opening the VM…' : 'VM not available'}
                </DialogPrimitive.Title>
                <span className="flex-1" />
                <CloseButton />
              </div>
            </div>
            <div className="sh-body">
              {vmQuery.isPending ? (
                <div className="box h-[220px] opacity-60" aria-hidden="true" />
              ) : (
                <ProblemBanner view={problemView(vmQuery.error, 'see this VM')} />
              )}
            </div>
          </>
        )}
      </DialogPrimitive.Content>
    </DialogPrimitive.Root>
  );
}

function VmSheetBody({ vm, projectId, tab }: { vm: Vm; projectId: string; tab: VmTab }) {
  const { me, canWrite } = useSession();
  const navigate = useNavigate();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const bodyRef = usePaneEnter<HTMLDivElement>(tab);

  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const projectsQuery = useListProjects(PROJECT_LIST_PARAMS);
  const appQuery = useGetApp(vm.app_id ?? '', { query: { enabled: vm.app_id !== null, retry: false } });
  const node = nodesQuery.data?.items.find((item) => item.id === vm.node_id);
  const project = projectsQuery.data?.items.find((item) => item.id === vm.project_id);

  const action = useVmAction({
    onDone: (kind, result) => {
      setProblem(null);
      setConfirming(false);
      if (kind === 'delete') {
        void navigate({ to: '/projects/$projectId', params: { projectId }, search: {} });
        return;
      }
      setNotice(vmActionNotice(kind, result));
    },
    onProblem: (view) => {
      setConfirming(false);
      setNotice(null);
      setProblem(view);
    },
  });
  const run = (kind: 'start' | 'stop' | 'delete') => {
    setNotice(null);
    action.mutate({ kind, vm });
  };

  const lamp = lampState(vm);
  const showConsole = canViewConsole(me);
  const nodeWords = node ? `${node.name} · ${nodeReadout(node.status).words.toLowerCase()}` : 'An unknown node';

  return (
    <>
      <div className="sh-head">
        <div className="sh-title">
          <GlyphTile kind="vm" size="lg" label />
          <DialogPrimitive.Title className="sh-name">{vm.name}</DialogPrimitive.Title>
          <span className="flex-1" />
          {showConsole ? (
            <Button size="sm" asChild className="max-[479.98px]:hidden">
              <Link to="/vms/$vmId/console" params={{ vmId: vm.id }}>
                <SquareTerminal className="size-3.5" strokeWidth={1.7} aria-hidden="true" />
                Console
              </Link>
            </Button>
          ) : null}
          {canWrite ? (
            <VmActionsMenu
              vm={vm}
              busy={action.isPending}
              onStart={() => run('start')}
              onStop={() => run('stop')}
              onDelete={() => setConfirming(true)}
            />
          ) : null}
          <CloseButton />
        </div>
        <div className="sh-meta">
          <span className="sh-state">
            <LampDot ring={lamp.ringTone} core={lamp.coreTone} pulse={lamp.pulse} size={10} />
            {lamp.statusText}
          </span>
          <span>{vmSize(vm)}</span>
          {node ? <span>{node.name}</span> : null}
        </div>
      </div>

      <SheetTabs label={`${vm.name} sections`} tab={tab}>
        {VM_TABS.map((key) => (
          <Link
            key={key}
            to="/projects/$projectId"
            params={{ projectId }}
            search={{ vm: vm.id, tab: key }}
            className={key === tab ? 'on' : undefined}
            aria-current={key === tab ? 'page' : undefined}
          >
            {TAB_WORDS[key]}
          </Link>
        ))}
      </SheetTabs>

      <div ref={bodyRef} className="sh-body">
        {notice ? <NoticeLine>{notice}</NoticeLine> : null}
        {problem ? <ProblemBanner view={problem} /> : null}

        {tab === 'overview' ? (
          <>
            {lamp.text !== lamp.statusText ? (
              <p className={lamp.coreTone === 'fault' ? 'vm-caption vm-caption-fault' : 'vm-caption'}>
                <LampDot ring={lamp.ringTone} core={lamp.coreTone} pulse={lamp.pulse} size={12} />
                {lamp.text}
              </p>
            ) : null}
            <Group title="General">
              <Rows
                rows={[
                  ['Image', <ImageRef image={vm.image} />],
                  ['Size', `${vm.vcpus} vCPU, ${memoryWords(vm.memory_mb)}, ${vm.disk_gb} GiB disk`],
                  ['Address', vm.ip ? code(vm.ip) : 'None yet'],
                  ['Node', nodeWords],
                  [
                    'Project',
                    <Link to="/projects/$projectId" params={{ projectId: vm.project_id }} className="vmt-link">
                      {project?.name ?? 'This project'}
                    </Link>,
                  ],
                  vm.app_id
                    ? [
                        'App',
                        <Link
                          to="/projects/$projectId"
                          params={{ projectId: vm.project_id }}
                          search={{ app: vm.app_id, tab: 'deployments' }}
                          className="vmt-link"
                        >
                          {appQuery.data ? `Instance of ${appQuery.data.name}` : 'Instance of an app'}
                        </Link>,
                      ]
                    : ['App', 'None: a standalone VM'],
                  ['Restarts', `${vm.restart_count}`],
                  vm.last_exit_code !== null && vm.last_exit_code !== undefined
                    ? ['Last exit code', code(String(vm.last_exit_code))]
                    : null,
                  ['Created', new Date(vm.created_at).toLocaleString()],
                  ['Id', <span className="break-all">{code(vm.id)}</span>],
                ]}
              />
            </Group>
          </>
        ) : (
          <>
            <Group
              title="Console"
              lede="The VM's serial console, read-only: recent boot output and then live kernel and init lines."
            >
              {showConsole ? (
                <div>
                  <Button asChild>
                    <Link to="/vms/$vmId/console" params={{ vmId: vm.id }}>
                      <SquareTerminal className="size-4" strokeWidth={1.7} aria-hidden="true" />
                      Open the console
                    </Link>
                  </Button>
                </div>
              ) : (
                <p className="text-[13px] text-ink-2">
                  This session can't view a serial console. A viewer needs a grant that covers vm:console.
                </p>
              )}
            </Group>
            {canWrite ? (
              <Group title="Danger zone">
                <div className="box box-danger flex flex-wrap items-center gap-x-4 gap-y-3 px-4 py-4">
                  <div className="min-w-0 flex-1">
                    <p className="text-[13.5px] font-medium text-ink">Delete this VM</p>
                    <p className="mt-0.5 text-[12.5px] leading-snug text-ink-2">
                      Its disk is removed with it. This cannot be undone.
                    </p>
                  </div>
                  <Button variant="danger" disabled={action.isPending} onClick={() => setConfirming(true)}>
                    Delete
                  </Button>
                </div>
              </Group>
            ) : null}
          </>
        )}
      </div>

      <DeleteVmDialog
        vm={confirming ? vm : null}
        busy={action.isPending}
        onKeep={() => setConfirming(false)}
        onDelete={() => run('delete')}
      />
    </>
  );
}
