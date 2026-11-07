import { useInfiniteQuery } from '@tanstack/react-query';
import { useState } from 'react';
import type { User, UserRole } from '../api/generated/model';
import { getListUsersQueryKey, listUsers } from '../api/generated/users/users';
import { canListUsers, canManageUsers } from '../lib/capabilities';
import { problemView } from '../lib/problem';
import { useSession } from '../session';
import {
  InviteUserDialog,
  ResetPasswordDialog,
  UserActionDialog,
  type UserAction,
} from '../components/user-dialog';
import { UserRow } from '../components/user-row';
import {
  SettingsEmpty,
  SettingsHead,
  SettingsLoading,
  SettingsNote,
  SettingsPage,
} from '../components/settings-parts';
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { Plus } from 'lucide-react';
import { LoginScreen } from './login-screen';

const PAGE_SIZE = 50;

/** Settings › Members: the people who can sign in, their fixed roles and their status. */
export function UsersScreen() {
  const { me, user, loading } = useSession();
  const [inviteOpen, setInviteOpen] = useState(false);
  const [resetTarget, setResetTarget] = useState<User | null>(null);
  const [action, setAction] = useState<UserAction | null>(null);

  const canList = canListUsers(me);
  const canManage = canManageUsers(me);

  const usersQuery = useInfiniteQuery({
    queryKey: getListUsersQueryKey({ limit: PAGE_SIZE }),
    queryFn: ({ pageParam }) =>
      listUsers({
        limit: PAGE_SIZE,
        cursor: pageParam === null ? undefined : pageParam,
      }),
    initialPageParam: null as string | null,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: canList,
  });

  if (loading) {
    return (
      <SettingsPage label="Members">
        <SettingsHead title="Members" />
        <SettingsLoading label="Loading members" />
      </SettingsPage>
    );
  }

  if (me && !canList) {
    return (
      <SettingsPage label="Members">
        <SettingsHead title="Members" />
        <SettingsEmpty title="Members is not available to an agent session.">
          Human identities and their roles are read and changed by a signed-in person. Ask your
          owner to sign in to this screen.
        </SettingsEmpty>
      </SettingsPage>
    );
  }

  if (!user) return <LoginScreen />;

  const users = usersQuery.data?.pages.flatMap((page) => page.items) ?? [];
  const listError =
    usersQuery.error !== null && usersQuery.error !== undefined
      ? problemView(usersQuery.error, 'see users')
      : null;

  const changeRole = (target: User, role: UserRole) => {
    if (role === target.role) return;
    setAction({ kind: 'role', user: target, role });
  };

  const invite = canManage ? (
    <Button size="sm" onClick={() => setInviteOpen(true)}>
      <Plus size={14} strokeWidth={1.8} aria-hidden="true" />
      Invite user
    </Button>
  ) : null;

  return (
    <SettingsPage label="Members">
      <SettingsHead title="Members" action={invite} />

      {!canManage ? <SettingsNote>Your role can read the members but not change them.</SettingsNote> : null}

      {listError ? <ProblemBanner view={listError} /> : null}

      {usersQuery.isPending ? (
        <SettingsLoading label="Loading members" />
      ) : users.length === 0 ? (
        <SettingsEmpty title="No members yet." action={invite}>
          Invite the first person who should sign in.
        </SettingsEmpty>
      ) : (
        <ul className="box">
          {users.map((row) => (
            <UserRow
              key={row.id}
              user={row}
              isSelf={user.id === row.id}
              canManage={canManage}
              onRoleChange={changeRole}
              onToggleDisabled={(target, disabled) => setAction({ kind: 'disabled', user: target, disabled })}
              onResetPassword={setResetTarget}
              onDelete={(target) => setAction({ kind: 'delete', user: target })}
            />
          ))}
        </ul>
      )}

      {usersQuery.hasNextPage ? (
        <div className="flex justify-center">
          <Button
            variant="default"
            onClick={() => void usersQuery.fetchNextPage()}
            disabled={usersQuery.isFetchingNextPage}
          >
            {usersQuery.isFetchingNextPage ? 'Loading…' : 'Load more'}
          </Button>
        </div>
      ) : null}

      {inviteOpen ? <InviteUserDialog onClose={() => setInviteOpen(false)} /> : null}

      {resetTarget ? (
        <ResetPasswordDialog user={resetTarget} onClose={() => setResetTarget(null)} />
      ) : null}

      {action ? <UserActionDialog action={action} onClose={() => setAction(null)} /> : null}
    </SettingsPage>
  );
}
