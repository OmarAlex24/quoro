// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import { useState } from 'react';
import { createNodeJoinToken } from '../api/generated/nodes/nodes';
import { expiryWords } from '../lib/agency-format';
import { canEnrollNodes } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { SecretOnce } from './secret-once';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { ProblemBanner } from './ui/problem-banner';

const INVITE = 'Create a join token and run the installer on the new machine.';

/**
 * Adding a node: a join token (`createNodeJoinToken`) and the one-line
 * command that enrolls the new machine, shown once in `SecretOnce`, as the HA
 * view's witness offer shows it. The secret lives in the mutation's result
 * only; closing the dialog drops it. Only an owner or an operator can create
 * a token; for anyone else the invitation is not shown.
 */
function AddNodeDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const joinToken = useMutation({
    mutationFn: () => createNodeJoinToken({}, { headers: { 'Idempotency-Key': crypto.randomUUID() } }),
  });
  const close = (next: boolean) => {
    if (!next) joinToken.reset();
    onOpenChange(next);
  };

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent title="Add a node" description={INVITE}>
        <div className="grid gap-3">
          {joinToken.data ? (
            <>
              <SecretOnce
                label="Run this on the new machine"
                secret={joinToken.data.join_command}
                copyLabel="Copy command"
                note={`The token is shown only this once and expires ${expiryWords(joinToken.data.expires_at)}.`}
              />
              <Button className="justify-self-end" onClick={() => close(false)}>
                Done
              </Button>
            </>
          ) : (
            <>
              <p className="text-[13px] leading-snug text-ink-2">
                The join token is for one machine and lasts an hour. The new machine shows here once it connects.
              </p>
              {joinToken.error ? <ProblemBanner view={problemView(joinToken.error, 'create join tokens')} /> : null}
              <Button
                variant="primary"
                className="justify-self-end"
                disabled={joinToken.isPending}
                busy={joinToken.isPending}
                onClick={() => joinToken.mutate()}
              >
                Create a join token
              </Button>
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

/** "Add a node" at the top right of the Nodes section. */
export function AddNodeButton() {
  const { me } = useSession();
  const [open, setOpen] = useState(false);
  if (!canEnrollNodes(me)) return null;
  return (
    <>
      <Button onClick={() => setOpen(true)}>
        <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
        Add a node
      </Button>
      <AddNodeDialog open={open} onOpenChange={setOpen} />
    </>
  );
}

/** "Add a node" as a dashed invitation under the node cards. */
export function AddNodeInvite() {
  const { me } = useSession();
  const [open, setOpen] = useState(false);
  if (!canEnrollNodes(me)) return null;
  return (
    <>
      <button type="button" className="pghost ninvite" onClick={() => setOpen(true)}>
        <span className="pghost-plus">
          <Plus strokeWidth={1.7} aria-hidden="true" />
        </span>
        <span>
          <b>Add a node</b>
          {INVITE}
        </span>
      </button>
      <AddNodeDialog open={open} onOpenChange={setOpen} />
    </>
  );
}
