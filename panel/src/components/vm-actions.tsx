// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { MoreHorizontal, Play, Square, Trash2 } from 'lucide-react';
import type { Vm } from '../api/generated/model';
import { deleteVm, getGetVmQueryKey, getListVmsQueryKey, startVm, stopVm } from '../api/generated/vms/vms';
import { getListNodesQueryKey } from '../api/generated/nodes/nodes';
import { problemView, type ProblemView } from '../lib/problem';
import { MenuPanel, useMenu } from './shell/menu';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';

/*
 * What can be done to a VM, the same from the VM table and from its sheet:
 * start or stop it (whichever it is not set to), and delete it after a
 * confirmation. Each says what actually happened.
 */

export type VmActionKind = 'start' | 'stop' | 'delete';

/** Say what actually happened: past tense only once the VM has settled. */
export const vmActionNotice = (kind: VmActionKind, vm: Vm): string => {
  if (kind === 'delete') return `Deleted ${vm.name}`;
  if (kind === 'start') {
    return vm.observed_status === 'running' ? `Started ${vm.name}` : `Starting ${vm.name}`;
  }
  return vm.observed_status === 'stopped' ? `Stopped ${vm.name}` : `Stopping ${vm.name}`;
};

const actionPhrase: Record<VmActionKind, string> = {
  start: 'start VMs',
  stop: 'stop VMs',
  delete: 'delete VMs',
};

/** Start, stop and delete, refreshing the VM lists and the nodes' allocation afterwards. */
export function useVmAction({
  onDone,
  onProblem,
}: {
  onDone: (kind: VmActionKind, vm: Vm) => void;
  onProblem: (problem: ProblemView) => void;
}) {
  const queryClient = useQueryClient();
  const refresh = (vm: Vm) => {
    void queryClient.invalidateQueries({ queryKey: getListVmsQueryKey() });
    void queryClient.invalidateQueries({ queryKey: getGetVmQueryKey(vm.id) });
    void queryClient.invalidateQueries({ queryKey: getListNodesQueryKey() });
  };
  return useMutation({
    mutationFn: ({ kind, vm }: { kind: VmActionKind; vm: Vm }) => {
      if (kind === 'start') return startVm(vm.id);
      if (kind === 'stop') return stopVm(vm.id);
      return deleteVm(vm.id);
    },
    onSuccess: (result, { kind, vm }) => {
      refresh(vm);
      onDone(kind, result);
    },
    onError: (error, { kind, vm }) => {
      refresh(vm);
      onProblem(problemView(error, actionPhrase[kind]));
    },
  });
}

/** The ⋯ menu of a VM: Start or Stop, then Delete, which asks first. */
export function VmActionsMenu({
  vm,
  busy,
  onStart,
  onStop,
  onDelete,
  size = 'default',
}: {
  vm: Vm;
  busy: boolean;
  onStart: () => void;
  onStop: () => void;
  onDelete: () => void;
  size?: 'default' | 'sm';
}) {
  const menu = useMenu();
  const wantsRunning = vm.desired_status === 'running';
  return (
    <div ref={menu.rootRef} className="relative">
      <button
        ref={menu.triggerRef}
        type="button"
        className={size === 'sm' ? 'iconbtn iconbtn-sm' : 'iconbtn'}
        aria-label={`Actions for ${vm.name}`}
        disabled={busy}
        {...menu.triggerProps}
      >
        <MoreHorizontal strokeWidth={1.7} aria-hidden="true" />
      </button>
      <MenuPanel menu={menu} label={`Actions for ${vm.name}`} align="end" className="min-w-[180px]">
        {wantsRunning ? (
          <button type="button" role="menuitem" className="menu-item" onClick={onStop}>
            <Square strokeWidth={1.7} aria-hidden="true" />
            Stop
          </button>
        ) : (
          <button type="button" role="menuitem" className="menu-item" onClick={onStart}>
            <Play strokeWidth={1.7} aria-hidden="true" />
            Start
          </button>
        )}
        <button type="button" role="menuitem" className="menu-item menu-item-danger" onClick={onDelete}>
          <Trash2 strokeWidth={1.7} aria-hidden="true" />
          Delete…
        </button>
      </MenuPanel>
    </div>
  );
}

/** The question before a VM is deleted: its disk goes with it. */
export function DeleteVmDialog({
  vm,
  busy,
  onKeep,
  onDelete,
}: {
  vm: Vm | null;
  busy: boolean;
  onKeep: () => void;
  onDelete: (vm: Vm) => void;
}) {
  return (
    <Dialog open={vm !== null} onOpenChange={(open) => (open ? undefined : onKeep())}>
      {vm ? (
        <DialogContent title="Delete VM" description={`Delete ${vm.name} and its disk? This cannot be undone.`}>
          <div className="flex flex-wrap justify-end gap-2">
            <Button variant="default" onClick={onKeep}>
              Keep the VM
            </Button>
            <Button busy={busy} variant="danger-solid" disabled={busy} onClick={() => onDelete(vm)}>
              {busy ? 'Deleting…' : 'Delete'}
            </Button>
          </div>
        </DialogContent>
      ) : null}
    </Dialog>
  );
}
