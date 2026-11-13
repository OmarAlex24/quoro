// SPDX-License-Identifier: AGPL-3.0-only
import { useMutation } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import { useState, type ReactElement } from 'react';
import { useGetCluster } from '../api/generated/cluster/cluster';
import type { NodeJoinTokenCreated } from '../api/generated/model';
import { expiryWords } from '../lib/agency-format';
import { canEnrollNodes } from '../lib/capabilities';
import { joinTokenMutation, nodeInstallerCommand } from '../lib/join-token';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import { CopyWell, SecretOnce } from './secret-once';
import { Button } from './ui/button';
import { Dialog, DialogContent, DialogTrigger } from './ui/dialog';
import { ProblemBanner } from './ui/problem-banner';

const INVITE = 'Create a join token and run the installer on the new machine.';

/**
 * Adding a node: a join token (`createNodeJoinToken`) and the one-line
 * command that enrolls the new machine, shown once in `SecretOnce`, as the HA
 * view's witness offer shows it, with the installer command that takes it.
 * The secret lives in the mutation's result only; closing the dialog drops
 * it. Only an owner or an operator can create a token; for anyone else the
 * invitation is not shown. `trigger` is the control that opens it; focus goes
 * back to it when the dialog closes.
 */
export function AddNodeDialog({ trigger }: { trigger: ReactElement }) {
  const [open, setOpen] = useState(false);
  const joinToken = useMutation(joinTokenMutation());
  // The cluster's version, for the installer command: the Nodes page reads it already.
  const cluster = useGetCluster({ query: { enabled: open, retry: false } });
  const close = (next: boolean) => {
    if (!next) joinToken.reset();
    setOpen(next);
  };

  // The control that opens it is the dialog's trigger, so closing it puts focus back there.
  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogTrigger asChild>{trigger}</DialogTrigger>
      <DialogContent title="Add a node" description={INVITE}>
        <div className="grid gap-3">
          {joinToken.data ? (
            <JoinCommand token={joinToken.data} version={cluster.data?.version} onDone={() => close(false)} />
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

/**
 * The token, once: the join command the server built, and the installer
 * command that uses it. The join command only enrolls; the installer installs
 * the node, runs the join with `QUORO_ROLE=node` and starts the node agent,
 * so the words point there. The installer command is not secret: its URL,
 * token and fingerprint are placeholders, filled from the join command.
 */
export function JoinCommand({
  token,
  version,
  onDone,
}: {
  token: NodeJoinTokenCreated;
  /** The cluster's version, for `QUORO_VERSION`; a placeholder while unknown. */
  version?: string;
  onDone: () => void;
}) {
  return (
    <>
      <SecretOnce
        label="Join command for the new machine"
        secret={token.join_command}
        copyLabel="Copy command"
        note={`The token is shown only this once and expires ${expiryWords(token.expires_at)}.`}
      />
      <div className="next-step grid gap-2">
        <p>
          On the new machine, run the installer as root, with the URL, token and fingerprint from this command:
        </p>
        <CopyWell value={nodeInstallerCommand(version)} copyLabel="Copy installer command" />
        <p>
          It joins the machine and starts its node agent, <code className="type-code">quoro-node.service</code>.{' '}
          {version ? (
            <>
              <code className="type-code">QUORO_VERSION</code> keeps the node at the cluster’s version, {version}.
            </>
          ) : (
            <>
              Set <code className="type-code">QUORO_VERSION</code> to the cluster’s version.
            </>
          )}{' '}
          Without it, the installer installs the latest release.
        </p>
      </div>
      <Button className="justify-self-end" onClick={onDone}>
        Done
      </Button>
    </>
  );
}

/** "Add a node" at the top right of the Nodes section. */
export function AddNodeButton() {
  const { me } = useSession();
  if (!canEnrollNodes(me)) return null;
  return (
    <AddNodeDialog
      trigger={
        <Button>
          <Plus strokeWidth={1.7} aria-hidden="true" className="size-4" />
          Add a node
        </Button>
      }
    />
  );
}

/** "Add a node" as a dashed invitation under the node cards. */
export function AddNodeInvite() {
  const { me } = useSession();
  if (!canEnrollNodes(me)) return null;
  return (
    <AddNodeDialog
      trigger={
        <button type="button" className="pghost ninvite">
          <span className="pghost-plus">
            <Plus strokeWidth={1.7} aria-hidden="true" />
          </span>
          <span>
            <b>Add a node</b>
            {INVITE}
          </span>
        </button>
      }
    />
  );
}
