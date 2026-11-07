import * as DialogPrimitive from '@radix-ui/react-dialog';
import { X } from 'lucide-react';
import { useListNodes } from '../api/generated/nodes/nodes';
import { useListProjects } from '../api/generated/projects/projects';
import { PROJECT_LIST_PARAMS } from '../lib/projects';
import type { AddKind } from '../lib/routes';
import { DeployScreen } from '../screens/deploy-screen';
import { CreateVmDialog } from './create-vm-dialog';
import { GlyphTile } from './glyph-tile';

const TITLES: Record<AddKind, string> = {
  git: 'New app from Git',
  image: 'New app from an image',
  vm: 'New virtual machine',
};

/**
 * A side sheet over the canvas that hosts an existing create flow: the
 * deploy work order for a Git or an image app, the VM form for a VM. The
 * flows are unchanged; the sheet only frames them and starts them in this
 * project. A deploy that succeeds moves to the new app, which closes the
 * sheet; a VM that is created closes it and appears on the canvas.
 */
export function CreateSheet({
  kind,
  projectId,
  onClose,
}: {
  kind: AddKind;
  projectId: string;
  onClose: () => void;
}) {
  return (
    <DialogPrimitive.Root open onOpenChange={(open) => (open ? undefined : onClose())}>
      <DialogPrimitive.Portal>
        <DialogPrimitive.Overlay className="dialog-overlay fixed inset-0 z-40 bg-scrim" />
        <DialogPrimitive.Content className="sheet" aria-describedby={undefined}>
          <div className="sheet-head">
            <GlyphTile kind={kind} size="lg" />
            <DialogPrimitive.Title className="sheet-title">{TITLES[kind]}</DialogPrimitive.Title>
            <span className="flex-1" />
            <DialogPrimitive.Close className="iconbtn" aria-label="Close">
              <X strokeWidth={1.7} aria-hidden="true" />
            </DialogPrimitive.Close>
          </div>
          <div className="sheet-body">
            {kind === 'vm' ? (
              <VmSheetBody projectId={projectId} onClose={onClose} />
            ) : (
              <DeployScreen sheet={{ source: kind, projectId }} />
            )}
          </div>
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  );
}

function VmSheetBody({ projectId, onClose }: { projectId: string; onClose: () => void }) {
  const projects = useListProjects(PROJECT_LIST_PARAMS);
  const nodes = useListNodes(undefined, { query: { staleTime: 10_000 } });
  return (
    <div className="px-4 pb-5 sm:px-5">
      <CreateVmDialog
        open
        onOpenChange={(open) => {
          if (!open) onClose();
        }}
        projects={projects.data?.items ?? []}
        nodes={nodes.data?.items ?? []}
        onCreated={() => undefined}
        sheet={{ projectId }}
      />
    </div>
  );
}
