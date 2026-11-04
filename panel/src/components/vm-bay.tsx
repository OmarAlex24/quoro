import { useState } from 'react';
import { Link } from '@tanstack/react-router';
import type { Vm } from '../api/generated/model';
import { builtImageLabel } from '../lib/build-status';
import { lampState } from '../lib/lamp';
import { cn } from '../lib/utils';
import { Lamp } from './lamp';
import { Button } from './ui/button';

const shortId = (id: string) => `${id.slice(0, 10)}…`;

export interface VmBayProps {
  vm: Vm;
  projectName: string;
  nodeName: string;
  canWrite: boolean;
  canViewConsole: boolean;
  busy: boolean;
  onStart: () => void;
  onStop: () => void;
  onDelete: () => void;
}

/** One VM as a rack bay: lamp well on the left, equipment plate on the right. */
export function VmBay({
  vm,
  projectName,
  nodeName,
  canWrite,
  canViewConsole,
  busy,
  onStart,
  onStop,
  onDelete,
}: VmBayProps) {
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const lamp = lampState(vm);
  const wantsRunning = vm.desired_status === 'running';

  return (
    <article
      aria-busy={busy}
      className="grid grid-cols-[56px_minmax(0,1fr)] overflow-hidden rounded-[6px] border border-rail bg-panel bay-shadow"
    >
      <div className="flex items-center justify-center border-r border-rail bg-recess">
        <Lamp vm={vm} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="min-w-0 flex-1 basis-[min(100%,280px)]">
          <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <h3 className="type-bay-title truncate text-[18px] leading-tight text-ink">
              {vm.name}
            </h3>
            <span className="type-data text-[13px] text-ink-muted">{lamp.statusText}</span>
          </div>
          {/* The caption only adds something while the VM is on its way or
              needs action; in steady state it would repeat the status word. */}
          {lamp.text !== lamp.statusText ? (
            <p
              className={cn(
                'mt-0.5 text-[13px] leading-snug',
                lamp.converging ? 'text-ink' : 'text-ink-muted',
              )}
            >
              {lamp.text}
            </p>
          ) : null}
          <p className="type-data mt-2 truncate text-[13px] text-ink">
            {vm.vcpus} vCPU · {vm.memory_mb} MiB · {vm.disk_gb} GiB ·{' '}
            {builtImageLabel(vm.image) ?? vm.image}
          </p>
          <p className="type-data mt-0.5 truncate text-[13px] text-ink-muted">
            <span title={vm.id}>{shortId(vm.id)}</span>
            {vm.ip ? ` · ${vm.ip}` : null} · project {projectName} · node {nodeName}
          </p>
        </div>

        {canWrite || canViewConsole ? (
          <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
            {canViewConsole ? (
              <Button variant="secondary" asChild>
                <Link to="/vms/$vmId/console" params={{ vmId: vm.id }}>
                  Console
                </Link>
              </Button>
            ) : null}
            {canWrite ? (
              confirmingDelete ? (
                <>
                  <p className="mr-1 text-[13px] text-ink">
                    Delete {vm.name} and its disk?
                  </p>
                  <Button
                    variant="danger"
                    disabled={busy}
                    onClick={() => {
                      setConfirmingDelete(false);
                      onDelete();
                    }}
                  >
                    Delete
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() => setConfirmingDelete(false)}
                    disabled={busy}
                  >
                    Keep
                  </Button>
                </>
              ) : (
                <>
                  {wantsRunning ? (
                    <Button variant="secondary" onClick={onStop} disabled={busy}>
                      Stop
                    </Button>
                  ) : (
                    <Button variant="primary" onClick={onStart} disabled={busy}>
                      Start
                    </Button>
                  )}
                  <Button
                    variant="danger"
                    onClick={() => setConfirmingDelete(true)}
                    disabled={busy}
                  >
                    Delete
                  </Button>
                </>
              )
            ) : null}
          </div>
        ) : null}
      </div>
    </article>
  );
}
