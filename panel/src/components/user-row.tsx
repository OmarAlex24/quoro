import type { User, UserRole } from '../api/generated/model';
import { roleLabel, userLamp } from '../lib/users';
import { ToneLamp } from './tone-lamp';
import { Button } from './ui/button';

const ROLES: UserRole[] = ['owner', 'operator', 'viewer'];

const createdDate = (iso: string): string => {
  const parsed = new Date(iso);
  return Number.isNaN(parsed.getTime())
    ? 'unknown'
    : parsed.toLocaleDateString(undefined, {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
};

/**
 * One human identity as a rack bay: the status lamp, the name and email, the
 * fixed role bundle and, for an owner, the controls that change them. The
 * signed-in owner's own row shows no role, disable or delete controls, so the
 * panel cannot lock them out of their own cluster.
 */
export function UserRow({
  user,
  isSelf,
  canManage,
  onRoleChange,
  onToggleDisabled,
  onResetPassword,
  onDelete,
}: {
  user: User;
  isSelf: boolean;
  canManage: boolean;
  onRoleChange: (user: User, role: UserRole) => void;
  onToggleDisabled: (user: User, disabled: boolean) => void;
  onResetPassword: (user: User) => void;
  onDelete: (user: User) => void;
}) {
  const lamp = userLamp(user);
  const canEditRow = canManage && !isSelf;

  return (
    <article className="grid grid-cols-[52px_minmax(0,1fr)] overflow-hidden rounded-[6px] border border-rail bg-panel bay-shadow">
      <div className="flex items-center justify-center border-r border-rail bg-recess">
        <ToneLamp tone={lamp.tone} label={lamp.statusText} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="min-w-0 flex-1 basis-[min(100%,300px)]">
          <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
            <span className="type-bay-title truncate text-[18px] leading-tight text-ink">
              {user.name}
            </span>
            {isSelf ? (
              <span className="type-data text-[12px] text-ink-muted">you</span>
            ) : null}
            <span className="type-data text-[13px] text-ink-muted">{lamp.statusText}</span>
          </div>

          <p className="type-data mt-1 truncate text-[13px] text-ink" title={user.email}>
            {user.email}
          </p>

          <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-2">
            {canEditRow ? (
              <label className="flex items-center gap-2 text-[13px] text-ink-muted">
                <span>Role</span>
                <select
                  aria-label={`Role for ${user.name}`}
                  className="field w-32 px-2 text-[14px]"
                  value={user.role}
                  onChange={(event) => onRoleChange(user, event.target.value as UserRole)}
                >
                  {ROLES.map((role) => (
                    <option key={role} value={role}>
                      {roleLabel(role)}
                    </option>
                  ))}
                </select>
              </label>
            ) : (
              <span className="inline-flex items-center gap-2 text-[13px] text-ink-muted">
                <span>Role</span>
                <span className="type-data rounded-[3px] border border-rail px-1.5 py-0.5 text-[12px] text-ink">
                  {roleLabel(user.role)}
                </span>
              </span>
            )}

            <span className="type-data text-[13px] text-ink-muted" title={user.created_at}>
              created {createdDate(user.created_at)}
            </span>
          </div>
        </div>

        {canManage ? (
          <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
            {canEditRow ? (
              <Button
                variant="secondary"
                size="compact"
                onClick={() => onToggleDisabled(user, !user.disabled)}
              >
                {user.disabled ? 'Enable' : 'Disable'}
              </Button>
            ) : null}
            <Button
              variant="quiet"
              size="compact"
              onClick={() => onResetPassword(user)}
            >
              Reset password
            </Button>
            {canEditRow ? (
              <Button
                variant="danger"
                size="compact"
                onClick={() => onDelete(user)}
              >
                Delete
              </Button>
            ) : null}
          </div>
        ) : null}
      </div>
    </article>
  );
}
