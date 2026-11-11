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
import { memoryWords, vmSize } from '../lib/services';
import { useSheetMotion, type SheetMotion } from '../lib/use-sheet-motion';
import { useSession } from '../session';
import { GlyphTile } from './glyph-tile';
import { ImageRef } from './image-ref';
import { LampDot } from './lamp';
import { CloseButton } from './service-sheet';
import { Rows } from './sheet-settings';
import { Button } from './ui/button';
import { NoticeLine, ProblemBanner } from './ui/problem-banner';
import { DeleteVmDialog, useVmAction, VmActionsMenu, vmActionNotice } from './vm-actions';
import { SkeletonCards } from './skeleton';

const OBSERVED_WORDS: Record<Vm['observed_status'], string> = {
  creating: 'Creating',
  stopped: 'Stopped',
  starting: 'Starting',
  running: 'Running',
  stopping: 'Stopping',
  crashed: 'Crashed',
  deleted: 'Deleted',
  unknown: 'Unknown',
};

const code = (value: string) => <span className="type-code text-[11.5px]">{value}</span>;

/**
 * A VM opened from its project's canvas or the VM table: the same sheet as an
 * app's, as one pane. The console and start or stop come first, then the
 * VM's size and node as figures and what else it is in rows. Start, stop and
 * delete also sit in the ⋯ menu. Quoro keeps no metrics for a single VM, so
 * there are no charts.
 */
export function VmSheet({
  projectId,
  vmId,
  onClose,
  motion,
}: {
  projectId: string;
  vmId: string;
  onClose: () => void;
  /** How it slides in and out, as the app sheet does. */
  motion?: SheetMotion;
}) {
  const contentRef = useRef<HTMLDivElement>(null);
  useSheetMotion(contentRef, motion);
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
        inert={motion?.leaving || undefined}
        data-leaving={motion?.leaving ? '' : undefined}
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
          <VmSheetBody key={vm.id} vm={vm} projectId={projectId} />
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
                <SkeletonCards label="Loading the VM" height={220} lines={4} />
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

function VmSheetBody({ vm, projectId }: { vm: Vm; projectId: string }) {
  const { me, canWrite } = useSession();
  const navigate = useNavigate();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);

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
  const wantsRunning = vm.desired_status === 'running';
  const nodeState = node ? nodeReadout(node.status).words.toLowerCase() : null;
  const [memory, memoryUnit] = memoryWords(vm.memory_mb).split(' ');

  return (
    <>
      <div className="sh-head">
        <div className="sh-title">
          <GlyphTile kind="vm" size="lg" label />
          <DialogPrimitive.Title className="sh-name">{vm.name}</DialogPrimitive.Title>
          <span className="flex-1" />
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
        </div>
      </div>
      <div className="sh-rule" aria-hidden="true" />

      <div className="sh-body">
        {notice ? <NoticeLine>{notice}</NoticeLine> : null}
        {problem ? <ProblemBanner view={problem} /> : null}
        {lamp.text !== lamp.statusText ? (
          <p className={lamp.coreTone === 'fault' ? 'vm-caption vm-caption-fault' : 'vm-caption'}>
            <LampDot ring={lamp.ringTone} core={lamp.coreTone} pulse={lamp.pulse} size={12} />
            {lamp.text}
          </p>
        ) : null}

        {showConsole || canWrite ? (
          <div className="flex flex-wrap items-center gap-2">
            {showConsole ? (
              <Button size="sm" asChild>
                <Link to="/vms/$vmId/console" params={{ vmId: vm.id }}>
                  <SquareTerminal className="size-3.5" strokeWidth={1.7} aria-hidden="true" />
                  Open console
                </Link>
              </Button>
            ) : null}
            {canWrite ? (
              <Button size="sm" variant="ghost" disabled={action.isPending} onClick={() => run(wantsRunning ? 'stop' : 'start')}>
                {wantsRunning ? 'Stop' : 'Start'}
              </Button>
            ) : null}
          </div>
        ) : null}

        <dl className="stats">
          <div>
            <dt>vCPU</dt>
            <dd>{vm.vcpus}</dd>
          </div>
          <div>
            <dt>Memory</dt>
            <dd>
              {memory}
              <em>{memoryUnit}</em>
            </dd>
          </div>
          <div>
            <dt>Disk</dt>
            <dd>
              {vm.disk_gb}
              <em>GiB</em>
            </dd>
          </div>
          <div>
            <dt>Node</dt>
            <dd title={node?.name}>{node?.name ?? 'Unknown'}</dd>
          </div>
        </dl>

        <Rows
          rows={[
            ['Address', vm.ip ? code(vm.ip) : 'None yet'],
            ['Observed status', OBSERVED_WORDS[vm.observed_status]],
            ['Restarts', `${vm.restart_count}`],
            [
              'Console',
              showConsole
                ? 'Read-only serial output'
                : "This session can't view a serial console. A viewer needs a grant that covers vm:console.",
            ],
            ['Image', <ImageRef image={vm.image} />],
            nodeState ? ['Node state', nodeState.charAt(0).toUpperCase() + nodeState.slice(1)] : null,
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
            vm.last_exit_code !== null && vm.last_exit_code !== undefined
              ? ['Last exit code', code(String(vm.last_exit_code))]
              : null,
            ['Created', new Date(vm.created_at).toLocaleString()],
            ['Id', <span className="break-all">{code(vm.id)}</span>],
          ]}
        />
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
