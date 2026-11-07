import type { User, UserRole } from '../api/generated/model';
import { roleLabel, userLamp } from '../lib/users';
import { Avatar } from './avatar';
import { ToneLamp } from './tone-lamp';
import { Tag } from './ui/tag';
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
    <article className="grid grid-cols-[52px_minmax(0,1fr)] overflow-hidden rounded-[10px] border border-line bg-surface card-sheen">
      <div className="flex items-center justify-center border-r border-line bg-sunk">
        <ToneLamp tone={lamp.tone} label={lamp.statusText} />
      </div>

      <div className="flex flex-wrap items-start gap-x-4 gap-y-2 px-4 py-3">
        <div className="min-w-0 flex-1 basis-[min(100%,300px)]">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <Avatar kind="human" name={user.name} />
            <span className="type-bay-title truncate text-[18px] leading-tight text-ink">
              {user.name}
            </span>
            {isSelf ? (
              <Tag tone="accent">you</Tag>
            ) : null}
            <span className="type-data text-[13px] text-ink-2">{lamp.statusText}</span>
          </div>

          <p className="type-data mt-1 truncate text-[13px] text-ink" title={user.email}>
            {user.email}
          </p>

          <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-2">
            {canEditRow ? (
              <label className="flex items-center gap-2 text-[13px] text-ink-2">
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
              <span className="inline-flex items-center gap-2 text-[13px] text-ink-2">
                <span>Role</span>
                <Tag tone="plain">{roleLabel(user.role)}</Tag>
              </span>
            )}

            <span className="type-data text-[13px] text-ink-2" title={user.created_at}>
              created {createdDate(user.created_at)}
            </span>
          </div>
        </div>

        {canManage ? (
          <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
            {canEditRow ? (
              <Button
                variant="default"
                size="sm"
                onClick={() => onToggleDisabled(user, !user.disabled)}
              >
                {user.disabled ? 'Enable' : 'Disable'}
              </Button>
            ) : null}
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onResetPassword(user)}
            >
              Reset password
            </Button>
            {canEditRow ? (
              <Button
                variant="danger"
                size="sm"
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
