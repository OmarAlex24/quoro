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
import { Button } from '../components/ui/button';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const PAGE_SIZE = 50;

/** The `/users` screen: human identities, their fixed roles and their status. */
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
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (me && !canList) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
        <h1 className="type-display text-[24px] leading-none">Users</h1>
        <div className="mt-5 rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
          <p className="text-[15px] text-ink">
            Users is not available to an agent session.
          </p>
          <p className="mx-auto mt-2 max-w-lg text-[13px] leading-snug text-ink-2">
            Human identities and their roles are read and changed by a signed-in
            person. Ask your owner to sign in to this screen.
          </p>
        </div>
      </main>
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

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="type-display text-[24px] leading-none">Users</h1>
          <p className="mt-2 max-w-2xl text-[13px] leading-snug text-ink-2">
            Humans who can sign in. Roles are fixed scope bundles: owner, operator,
            viewer.
          </p>
        </div>
        {canManage ? (
          <Button variant="primary" onClick={() => setInviteOpen(true)}>
            Invite user
          </Button>
        ) : null}
      </div>

      {!canManage ? (
        <p className="mt-3 text-[13px] text-ink-2">
          Your role can read the users but not change them.
        </p>
      ) : null}

      {listError ? <ProblemBanner view={listError} className="mt-4" /> : null}

      <div className="mt-5 flex flex-col gap-2">
        {usersQuery.isPending ? (
          <div className="h-[92px] rounded-[10px] border border-line bg-surface/60" />
        ) : users.length === 0 ? (
          <div className="rounded-[10px] border border-dashed border-line bg-surface/50 px-4 py-10 text-center">
            <p className="text-[15px] text-ink">
              No users yet. Invite the first person who should sign in.
            </p>
            {canManage ? (
              <Button
                variant="primary"
                className="mt-4"
                onClick={() => setInviteOpen(true)}
              >
                Invite user
              </Button>
            ) : null}
          </div>
        ) : (
          users.map((row) => (
            <UserRow
              key={row.id}
              user={row}
              isSelf={user.id === row.id}
              canManage={canManage}
              onRoleChange={changeRole}
              onToggleDisabled={(target, disabled) =>
                setAction({ kind: 'disabled', user: target, disabled })
              }
              onResetPassword={setResetTarget}
              onDelete={(target) => setAction({ kind: 'delete', user: target })}
            />
          ))
        )}
      </div>

      {usersQuery.hasNextPage ? (
        <div className="mt-4 flex justify-center">
          <Button
            variant="default"
            onClick={() => void usersQuery.fetchNextPage()}
            disabled={usersQuery.isFetchingNextPage}
          >
            {usersQuery.isFetchingNextPage ? 'Loading…' : 'Load more'}
          </Button>
        </div>
      ) : null}

      {inviteOpen ? (
        <InviteUserDialog onClose={() => setInviteOpen(false)} />
      ) : null}

      {resetTarget ? (
        <ResetPasswordDialog
          user={resetTarget}
          onClose={() => setResetTarget(null)}
        />
      ) : null}

      {action ? (
        <UserActionDialog action={action} onClose={() => setAction(null)} />
      ) : null}
    </main>
  );
}
