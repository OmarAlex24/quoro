import { Ban, Check, KeyRound, Play, Trash2, UserCog } from 'lucide-react';
import type { User, UserRole } from '../api/generated/model';
import { roleLabel, userLamp } from '../lib/users';
import { Avatar } from './avatar';
import { RowMenu, RowMenuItem } from './settings-parts';
import { ToneLamp } from './tone-lamp';
import { Tag } from './ui/tag';

const ROLES: UserRole[] = ['owner', 'operator', 'viewer'];

const joinedDate = (iso: string): string => {
  const parsed = new Date(iso);
  return Number.isNaN(parsed.getTime())
    ? 'unknown'
    : parsed.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
};

/**
 * One member: the round avatar, the name with the role, the email and whether
 * they can sign in, and for an owner a ⋯ menu that changes the role, disables,
 * resets the password or deletes, each through its confirmation. The
 * signed-in owner's own row offers only a password reset, so the panel cannot
 * lock them out of their own cluster.
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
    <li className="srow">
      <Avatar kind="human" name={user.name} className="srow-av" />
      <div className="srow-t">
        <b>
          <span className="truncate">{user.name}</span>
          {isSelf ? <Tag tone="accent">You</Tag> : null}
          <Tag tone="plain">{roleLabel(user.role)}</Tag>
        </b>
        <span className="srow-sub truncate" title={user.email}>
          {user.email}
        </span>
        <small>
          <ToneLamp tone={lamp.tone} size={11} label={lamp.statusText} />
          {user.disabled ? 'Disabled: cannot sign in' : 'Can sign in'}
          <span className="dot-sep" aria-hidden="true" />
          <span className="type-data" title={user.created_at}>
            Joined {joinedDate(user.created_at)}
          </span>
        </small>
      </div>
      {canManage ? (
        <RowMenu label={`Actions for ${user.name}`}>
          {canEditRow ? (
            <>
              <p className="menu-label">Role</p>
              {ROLES.map((role) => (
                <RowMenuItem
                  key={role}
                  icon={role === user.role ? Check : UserCog}
                  checked={role === user.role}
                  onSelect={() => onRoleChange(user, role)}
                >
                  {roleLabel(role)}
                </RowMenuItem>
              ))}
              <div className="menu-sep" role="separator" />
              <RowMenuItem
                icon={user.disabled ? Play : Ban}
                onSelect={() => onToggleDisabled(user, !user.disabled)}
              >
                {user.disabled ? 'Enable…' : 'Disable…'}
              </RowMenuItem>
            </>
          ) : null}
          <RowMenuItem icon={KeyRound} onSelect={() => onResetPassword(user)}>
            Reset password…
          </RowMenuItem>
          {canEditRow ? (
            <RowMenuItem icon={Trash2} danger onSelect={() => onDelete(user)}>
              Remove…
            </RowMenuItem>
          ) : null}
        </RowMenu>
      ) : null}
    </li>
  );
}
