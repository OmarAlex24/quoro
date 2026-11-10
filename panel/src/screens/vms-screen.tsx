import { Link } from '@tanstack/react-router';
import { Plus, SquareTerminal } from 'lucide-react';
import { useState } from 'react';
import { useListApps } from '../api/generated/apps/apps';
import type { Vm } from '../api/generated/model';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { useListVms } from '../api/generated/vms/vms';
import { canViewConsole } from '../lib/capabilities';
import { countVms, lampState, listIsConverging, type LampTone } from '../lib/lamp';
import { problemView, type ProblemView } from '../lib/problem';
import { PROJECT_LIST_PARAMS, SERVICE_LIST_PARAMS } from '../lib/projects';
import { memoryWords } from '../lib/services';
import { useSession } from '../session';
import { CreateVmDialog } from '../components/create-vm-dialog';
import { GlyphTile } from '../components/glyph-tile';
import { LampDot } from '../components/lamp';
import { DeleteVmDialog, useVmAction, VmActionsMenu, vmActionNotice } from '../components/vm-actions';
import { Button } from '../components/ui/button';
import { NoticeLine, ProblemBanner } from '../components/ui/problem-banner';
import { EmptyState } from '../components/empty-state';
import { SkeletonRows } from '../components/skeleton';

function CountLamp({ tone, value, label }: { tone: LampTone; value: number; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <LampDot ring={tone} core={tone} size={12} />
      <span>
        {value} {label}
      </span>
    </span>
  );
}

const shortId = (id: string) => `${id.slice(0, 10)}…`;

/**
 * `/nodes?tab=vms`: every VM of the cluster as a table, or only those on one
 * node (`&node=`). Each name opens the VM's sheet on its project's canvas;
 * the row keeps the console, start or stop, and delete with its question.
 */
export function VmsScreen({ nodeId }: { nodeId?: string } = {}) {
  const { me, canWrite } = useSession();
  const [dialogOpen, setDialogOpen] = useState(false);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [confirming, setConfirming] = useState<Vm | null>(null);

  const vmsQuery = useListVms(
    { ...SERVICE_LIST_PARAMS, ...(nodeId ? { node_id: nodeId } : {}) },
    {
      query: {
        refetchInterval: (query) => (listIsConverging(query.state.data?.items ?? []) ? 1_000 : 10_000),
      },
    },
  );
  const projectsQuery = useListProjects(PROJECT_LIST_PARAMS);
  const nodesQuery = useListNodes(undefined, { query: { staleTime: 10_000 } });
  const appsQuery = useListApps(SERVICE_LIST_PARAMS, { query: { staleTime: 10_000 } });

  const action = useVmAction({
    onDone: (kind, vm) => {
      setProblem(null);
      setConfirming(null);
      setNotice(vmActionNotice(kind, vm));
    },
    onProblem: (view) => {
      setConfirming(null);
      setNotice(null);
      setProblem(view);
    },
  });
  const run = (kind: 'start' | 'stop' | 'delete', vm: Vm) => {
    setNotice(null);
    action.mutate({ kind, vm });
  };

  const vms = vmsQuery.data?.items ?? [];
  const projects = projectsQuery.data?.items ?? [];
  const nodes = nodesQuery.data?.items ?? [];
  const apps = appsQuery.data?.items ?? [];
  const counts = countVms(vms);
  const showConsole = canViewConsole(me);
  const filterNode = nodeId ? nodes.find((node) => node.id === nodeId) : undefined;

  const projectName = (id: string) => projects.find((project) => project.id === id)?.name ?? shortId(id);
  const nodeName = (id: string) => nodes.find((node) => node.id === id)?.name ?? shortId(id);
  const appName = (id: string) => apps.find((app) => app.id === id)?.name;
  const primary = nodes.find((node) => node.cp_status === 'primary') ?? nodes[0];

  const busyId = action.isPending ? action.variables?.vm.id : undefined;
  const listError =
    vmsQuery.error !== null && vmsQuery.error !== undefined ? problemView(vmsQuery.error, 'see VMs') : null;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 pb-8 sm:px-6">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-[13px] text-ink-2">
        <CountLamp tone="run" value={counts.running} label="running" />
        <span aria-hidden="true">·</span>
        <CountLamp tone="idle" value={counts.stopped} label="stopped" />
        <span aria-hidden="true">·</span>
        <CountLamp tone="fault" value={counts.crashed} label="crashed" />
        {nodeId ? (
          <>
            <span aria-hidden="true">·</span>
            <span>On {filterNode?.name ?? 'one node'}</span>
            <Link
              to="/nodes"
              search={{ tab: 'vms' }}
              className="text-accent underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
            >
              Show every VM
            </Link>
          </>
        ) : null}
        <span className="flex-1" />
        {canWrite ? (
          <Button variant="primary" onClick={() => setDialogOpen(true)}>
            <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
            New VM
          </Button>
        ) : null}
      </div>

      {notice ? (
        <div className="mt-2">
          <NoticeLine>{notice}</NoticeLine>
        </div>
      ) : null}
      {problem ? <ProblemBanner view={problem} className="mt-4" /> : null}
      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-4">
        {vmsQuery.isPending ? (
          <SkeletonRows label="Loading virtual machines" rows={3} />
        ) : vms.length === 0 && !listError ? (
          <EmptyState
            title={nodeId ? `No VM is placed on ${filterNode?.name ?? 'this node'}` : 'No VMs yet'}
            action={
              canWrite && !nodeId ? (
                <Button variant="primary" onClick={() => setDialogOpen(true)}>
                  New VM
                </Button>
              ) : undefined
            }
          >
            {nodeId
              ? 'The scheduler places VMs on the nodes with room for them. Clear the node to see every VM.'
              : `A VM is a microVM of its own, outside any app. Create one to boot it on ${primary?.name ?? 'a node'}.`}
          </EmptyState>
        ) : (
          <div className="box vmt-box">
            <table className="vmt">
              <caption className="sr-only">
                Virtual machines{filterNode ? ` on ${filterNode.name}` : ''}
              </caption>
              <thead>
                <tr>
                  <th scope="col">Name</th>
                  <th scope="col">Project</th>
                  <th scope="col">Node</th>
                  <th scope="col">vCPU · Memory · Disk</th>
                  <th scope="col">State</th>
                  <th scope="col">
                    <span className="sr-only">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {vms.map((vm) => {
                  const lamp = lampState(vm);
                  const app = vm.app_id ? appName(vm.app_id) : undefined;
                  const busy = busyId === vm.id;
                  return (
                    <tr key={vm.id} aria-busy={busy || undefined}>
                      <td className="vmt-name">
                        <span className="vmt-name-in">
                          <GlyphTile kind="vm" />
                          <span className="min-w-0">
                            <Link
                              to="/projects/$projectId"
                              params={{ projectId: vm.project_id }}
                              search={{ vm: vm.id }}
                              className="vmt-open"
                            >
                              {vm.name}
                            </Link>
                            <small>{vm.app_id ? `Instance of ${app ?? 'an app'}` : 'Standalone VM'}</small>
                          </span>
                        </span>
                      </td>
                      <td className="vmt-project">
                        <Link to="/projects/$projectId" params={{ projectId: vm.project_id }} className="vmt-link">
                          {projectName(vm.project_id)}
                        </Link>
                      </td>
                      <td className="vmt-node">{nodeName(vm.node_id)}</td>
                      <td className="vmt-size">
                        {vm.vcpus} vCPU · {memoryWords(vm.memory_mb)} · {vm.disk_gb} GiB
                      </td>
                      <td className="vmt-state">
                        <span className="vmt-lamp">
                          <LampDot ring={lamp.ringTone} core={lamp.coreTone} pulse={lamp.pulse} size={13} />
                          {lamp.statusText}
                        </span>
                        {lamp.text !== lamp.statusText ? <small>{lamp.text}</small> : null}
                      </td>
                      <td className="vmt-act">
                        <span className="vmt-act-in">
                          {showConsole ? (
                            <Button size="sm" asChild>
                              <Link to="/vms/$vmId/console" params={{ vmId: vm.id }} aria-label={`Console of ${vm.name}`}>
                                <SquareTerminal className="size-3.5" strokeWidth={1.7} aria-hidden="true" />
                                Console
                              </Link>
                            </Button>
                          ) : null}
                          {canWrite ? (
                            <VmActionsMenu
                              vm={vm}
                              busy={busy}
                              size="sm"
                              onStart={() => run('start', vm)}
                              onStop={() => run('stop', vm)}
                              onDelete={() => setConfirming(vm)}
                            />
                          ) : null}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <DeleteVmDialog
        vm={confirming}
        busy={action.isPending}
        onKeep={() => setConfirming(null)}
        onDelete={(vm) => run('delete', vm)}
      />

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
