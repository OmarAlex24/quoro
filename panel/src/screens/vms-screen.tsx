import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import type { Vm } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import {
  deleteVm,
  getListVmsQueryKey,
  startVm,
  stopVm,
  useListVms,
} from '../api/generated/vms/vms';
import { countVms, listIsConverging, type LampTone } from '../lib/lamp';
import { canViewConsole } from '../lib/capabilities';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { CreateVmDialog } from '../components/create-vm-dialog';
import { LampDot } from '../components/lamp';
import { Button } from '../components/ui/button';
import { ProblemBanner, NoticeLine } from '../components/ui/problem-banner';
import { VmBay } from '../components/vm-bay';
import { LoginScreen } from './login-screen';

type ActionKind = 'start' | 'stop' | 'delete';

interface VmAction {
  kind: ActionKind;
  vm: Vm;
}

/** Say what actually happened: past tense only once the VM has settled. */
const actionNotice = (kind: ActionKind, vm: Vm): string => {
  if (kind === 'delete') return `Deleted ${vm.name}`;
  if (kind === 'start') {
    return vm.observed_status === 'running' ? `Started ${vm.name}` : `Starting ${vm.name}`;
  }
  return vm.observed_status === 'stopped' ? `Stopped ${vm.name}` : `Stopping ${vm.name}`;
};

const actionPhrase: Record<ActionKind, string> = {
  start: 'start VMs',
  stop: 'stop VMs',
  delete: 'delete VMs',
};

function CountLamp({
  tone,
  value,
  label,
}: {
  tone: LampTone;
  value: number;
  label: string;
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <LampDot ring={tone} core={tone} size={12} />
      <span className="type-data">
        {value} {label}
      </span>
    </span>
  );
}

export function VmsScreen() {
  const { me, user, loading, unauthenticated, canWrite } = useSession();
  const queryClient = useQueryClient();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const vmsQuery = useListVms(undefined, {
    query: {
      refetchInterval: (query) => {
        const data = query.state.data;
        if (!data) return 10_000;
        return listIsConverging(data.items) ? 1_000 : 10_000;
      },
    },
  });

  const projectsQuery = useListProjects();
  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });

  const action = useMutation({
    mutationFn: ({ kind, vm }: VmAction) => {
      if (kind === 'start') return startVm(vm.id);
      if (kind === 'stop') return stopVm(vm.id);
      return deleteVm(vm.id);
    },
    onSuccess: (vm, variables) => {
      setProblem(null);
      setNotice(actionNotice(variables.kind, vm));
      void queryClient.invalidateQueries({ queryKey: getListVmsQueryKey() });
    },
    onError: (error, variables) => {
      setProblem(problemView(error, actionPhrase[variables.kind]));
      void queryClient.invalidateQueries({ queryKey: getListVmsQueryKey() });
    },
  });

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (unauthenticated || !user) return <LoginScreen />;

  const vms = vmsQuery.data?.items ?? [];
  const projects = projectsQuery.data?.items ?? [];
  const nodes = nodesQuery.data?.items ?? [];
  const counts = countVms(vms);

  const projectName = (id: string) =>
    projects.find((project) => project.id === id)?.name ?? `${id.slice(0, 10)}…`;
  const nodeName = (id: string) =>
    nodes.find((node) => node.id === id)?.name ?? `${id.slice(0, 10)}…`;

  const primary = nodes.find((node) => node.cp_status === 'primary') ?? nodes[0];
  const emptyNodeName = primary?.name ?? 'the node';

  const busyId = action.isPending ? action.variables?.vm.id : undefined;
  const listError =
    vmsQuery.error !== null && vmsQuery.error !== undefined
      ? problemView(vmsQuery.error, 'see VMs')
      : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="type-display text-[24px] leading-none">Virtual machines</h1>
        {canWrite ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            New VM
          </Button>
        ) : null}
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-x-3 gap-y-1 text-[13px] text-ink-2">
        <CountLamp tone="run" value={counts.running} label="running" />
        <span aria-hidden="true">·</span>
        <CountLamp tone="idle" value={counts.stopped} label="stopped" />
        <span aria-hidden="true">·</span>
        <CountLamp tone="fault" value={counts.crashed} label="crashed" />
      </div>

      {notice ? <div className="mt-2">{<NoticeLine>{notice}</NoticeLine>}</div> : null}

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {vmsQuery.isPending ? (
          <div className="h-[104px] rounded-[10px] border border-line bg-surface/60" />
        ) : vms.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">
              No VMs yet. Create one to boot a microVM on {emptyNodeName}.
            </p>
            {canWrite ? (
              <Button
                variant="primary"
                className="mt-4"
                onClick={() => setDialogOpen(true)}
              >
                New VM
              </Button>
            ) : null}
          </div>
        ) : (
          vms.map((vm) => (
            <VmBay
              key={vm.id}
              vm={vm}
              projectName={projectName(vm.project_id)}
              nodeName={nodeName(vm.node_id)}
              canWrite={canWrite}
              canViewConsole={canViewConsole(me)}
              busy={busyId === vm.id}
              onStart={() => {
                setNotice(null);
                action.mutate({ kind: 'start', vm });
              }}
              onStop={() => {
                setNotice(null);
                action.mutate({ kind: 'stop', vm });
              }}
              onDelete={() => {
                setNotice(null);
                action.mutate({ kind: 'delete', vm });
              }}
            />
          ))
        )}
      </div>

      <CreateVmDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        projects={projects}
        nodes={nodes}
        onCreated={(name) => {
          setProblem(null);
          setNotice(`Created ${name}`);
        }}
      />
    </main>
  );
}
