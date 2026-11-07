import type { Grant } from '../api/generated/model';
import { shortId } from '../lib/agency-format';
import type { ProblemView } from '../lib/problem';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { ProblemBanner } from './ui/problem-banner';

/** Confirm revoking one grant, with the consequence said plainly. */
export function RevokeGrantDialog({
  grant,
  open,
  onOpenChange,
  pending,
  problem,
  onConfirm,
}: {
  grant: Grant | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  pending: boolean;
  problem: ProblemView | null;
  onConfirm: () => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        title="Revoke grant"
        description={grant ? `Grant ${shortId(grant.id)} for this agent ends now.` : undefined}
      >
        <div className="flex flex-col gap-4">
          {problem ? <ProblemBanner view={problem} /> : null}

          <p className="text-[14px] leading-snug text-ink">
            The agent's next call under this grant is refused with grant_revoked. Tokens it
            already holds authorize nothing without an active grant.
          </p>

          <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={() => onOpenChange(false)}>
              Keep grant
            </Button>
            <Button variant="danger-solid" disabled={pending} onClick={onConfirm}>
              {pending ? 'Revoking…' : 'Revoke grant'}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
