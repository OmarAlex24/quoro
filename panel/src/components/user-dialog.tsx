import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import {
  createUser,
  deleteUser,
  getListUsersQueryKey,
  updateUser,
} from '../api/generated/users/users';
import type { User, UserRole } from '../api/generated/model';
import { problemView, type ProblemView } from '../lib/problem';
import {
  emailProblem,
  GENERATED_PASSWORD_NOTE,
  generatePassword,
  inviteText,
  passwordProblem,
  roleLabel,
  ROLE_DESCRIPTIONS,
  userNameProblem,
} from '../lib/users';
import { CopyButton } from './copy-button';
import { SecretOnce } from './secret-once';
import { Button } from './ui/button';
import { Dialog, DialogContent } from './ui/dialog';
import { Field } from './ui/field';
import { Input } from './ui/input';
import { ProblemBanner } from './ui/problem-banner';

const ROLES: UserRole[] = ['viewer', 'operator', 'owner'];

interface InviteDraft {
  email: string;
  name: string;
  role: UserRole;
}

const emptyDraft = (): InviteDraft => ({ email: '', name: '', role: 'viewer' });

const invalidateUsers = (queryClient: ReturnType<typeof useQueryClient>) => {
  void queryClient.invalidateQueries({ queryKey: getListUsersQueryKey() });
};

/**
 * Invite a human. The initial password is generated in the browser with
 * `crypto.getRandomValues`; it lives only in this component's state and the
 * dialog shows it once on the invite card. It is never put in the query cache,
 * the URL, storage or the console.
 */
export function InviteUserDialog({ onClose }: { onClose: () => void }) {
  const queryClient = useQueryClient();
  const [draft, setDraft] = useState<InviteDraft>(emptyDraft);
  const [password, setPassword] = useState(() => generatePassword());
  const [ownPassword, setOwnPassword] = useState(false);
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [invited, setInvited] = useState<User | null>(null);

  const create = useMutation({
    mutationFn: (input: InviteDraft & { password: string }) =>
      createUser(
        {
          email: input.email.trim(),
          name: input.name.trim(),
          role: input.role,
          password: input.password,
        },
        { headers: { 'Idempotency-Key': crypto.randomUUID() } },
      ),
    onSuccess: (user) => {
      setProblem(null);
      setInvited(user);
      invalidateUsers(queryClient);
    },
    onError: (error) => setProblem(problemView(error, 'invite users')),
  });

  const close = () => {
    // Drop the one-time password with the plate.
    create.reset();
    setPassword('');
    setInvited(null);
    onClose();
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const invalid =
      emailProblem(draft.email) ??
      userNameProblem(draft.name) ??
      passwordProblem(password);
    if (invalid) {
      setProblem({ message: invalid });
      return;
    }
    setProblem(null);
    create.mutate({ ...draft, password });
  };

  const update = (patch: Partial<InviteDraft>) =>
    setDraft((current) => ({ ...current, ...patch }));

  if (invited) {
    const text = inviteText({
      panelUrl: window.location.origin,
      email: invited.email,
      role: invited.role,
    });
    return (
      <Dialog open onOpenChange={(next) => (!next ? close() : undefined)}>
        <DialogContent
          title="User invited"
          description="Copy the password and the invite text, then share them over separate channels."
        >
          <div className="flex flex-col gap-4">
            <SecretOnce
              label={`Initial password for ${invited.name}`}
              secret={password}
              note={GENERATED_PASSWORD_NOTE}
            />

            <div className="rounded-[10px] border border-line bg-sunk/40 px-3.5 py-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <p className="type-body-medium text-[13px] text-ink">Invite text</p>
                <CopyButton value={text} label="Copy invite" />
              </div>
              <p className="mt-2 whitespace-pre-wrap break-words text-[13px] leading-snug text-ink">
                {text}
              </p>
            </div>

            <div className="flex justify-end border-t border-line pt-4">
              <Button variant="primary" onClick={close}>
                Done
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    );
  }

  return (
    <Dialog open onOpenChange={(next) => (!next ? close() : undefined)}>
      <DialogContent
        title="Invite user"
        description="They sign in with this email and the initial password you share separately."
      >
        <form onSubmit={submit} className="flex flex-col gap-4" noValidate>
          {problem ? <ProblemBanner view={problem} /> : null}

          <Field label="Email" htmlFor="user-new-email">
            <Input
              id="user-new-email"
              type="email"
              value={draft.email}
              onChange={(event) => update({ email: event.target.value })}
              placeholder="ada@example.com"
              autoFocus
              spellCheck={false}
              autoComplete="off"
            />
          </Field>

          <Field label="Name" htmlFor="user-new-name">
            <Input
              id="user-new-name"
              value={draft.name}
              onChange={(event) => update({ name: event.target.value })}
              placeholder="Ada Lovelace"
              maxLength={200}
            />
          </Field>

          <Field
            label="Role"
            htmlFor="user-new-role"
            hint={ROLE_DESCRIPTIONS[draft.role]}
          >
            <select
              id="user-new-role"
              className="field"
              value={draft.role}
              onChange={(event) => update({ role: event.target.value as UserRole })}
            >
              {ROLES.map((role) => (
                <option key={role} value={role}>
                  {roleLabel(role)}
                </option>
              ))}
            </select>
          </Field>

          <Field
            label="Initial password"
            htmlFor="user-new-password"
            hint={
              ownPassword
                ? 'At least 12 characters. Share it over a separate channel.'
                : '24 characters, generated here in the browser.'
            }
          >
            <Input
              id="user-new-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              readOnly={!ownPassword}
              minLength={12}
              spellCheck={false}
              autoComplete="new-password"
              className={!ownPassword ? 'select-all' : undefined}
            />
            <div className="mt-1 flex flex-wrap gap-2">
              {ownPassword ? (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setOwnPassword(false);
                    setPassword(generatePassword());
                  }}
                >
                  Use a generated password
                </Button>
              ) : (
                <>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setPassword(generatePassword())}
                  >
                    Generate another
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setOwnPassword(true);
                      setPassword('');
                    }}
                  >
                    Type my own
                  </Button>
                </>
              )}
            </div>
          </Field>

          <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={close}>
              Cancel
            </Button>
            <Button busy={create.isPending} variant="primary" type="submit" disabled={create.isPending}>
              {create.isPending ? 'Inviting…' : 'Invite user'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}

/** One step an owner takes from a user row, waiting for its confirmation. */
export type UserAction =
  | { kind: 'role'; user: User; role: UserRole }
  | { kind: 'disabled'; user: User; disabled: boolean }
  | { kind: 'delete'; user: User };

const actionCopy = (
  action: UserAction,
): { title: string; description: string; confirm: string; danger: boolean } => {
  switch (action.kind) {
    case 'role':
      return {
        title: `Change ${action.user.name} to ${roleLabel(action.role)}?`,
        description: ROLE_DESCRIPTIONS[action.role],
        confirm: 'Change role',
        danger: false,
      };
    case 'disabled':
      return action.disabled
        ? {
            title: `Disable ${action.user.name}?`,
            description: 'They cannot sign in or use tokens until you enable them again.',
            confirm: 'Disable user',
            danger: true,
          }
        : {
            title: `Enable ${action.user.name}?`,
            description: 'They can sign in and use tokens again.',
            confirm: 'Enable user',
            danger: false,
          };
    case 'delete':
      return {
        title: `Delete ${action.user.name}?`,
        description:
          'That deletes the user and revokes their sessions and tokens. A user referenced by agents or grants cannot be deleted; disable it instead.',
        confirm: 'Delete user',
        danger: true,
      };
  }
};

/** Confirm and send a role change, a disable/enable or a delete. */
export function UserActionDialog({
  action,
  onClose,
}: {
  action: UserAction;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const copy = actionCopy(action);

  const save = useMutation<User | void, Error, UserAction>({
    mutationFn: (current: UserAction) => {
      switch (current.kind) {
        case 'role':
          return updateUser(current.user.id, { role: current.role });
        case 'disabled':
          return updateUser(current.user.id, { disabled: current.disabled });
        case 'delete':
          return deleteUser(current.user.id);
      }
    },
    onSuccess: () => {
      setProblem(null);
      invalidateUsers(queryClient);
      onClose();
    },
    onError: (error) => setProblem(problemView(error, 'change users')),
  });

  return (
    <Dialog open onOpenChange={(next) => (!next ? onClose() : undefined)}>
      <DialogContent title={copy.title} description={copy.description}>
        <div className="flex flex-col gap-4">
          {problem ? <ProblemBanner view={problem} /> : null}

          <div className="flex flex-wrap justify-end gap-2 border-t border-line pt-4">
            <Button variant="default" onClick={onClose}>
              Cancel
            </Button>
            <Button
              busy={save.isPending}
              variant={copy.danger ? 'danger' : 'primary'}
              onClick={() => {
                setProblem(null);
                save.mutate(action);
              }}
              disabled={save.isPending}
            >
              {save.isPending ? 'Saving…' : copy.confirm}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

/**
 * Reset a user's password. The new password is generated here, editable, and
 * shown once after the PATCH; it never enters the query cache or storage.
 */
export function ResetPasswordDialog({
  user,
  onClose,
}: {
  user: User;
  onClose: () => void;
}) {
  const queryClient = useQueryClient();
  const [password, setPassword] = useState(() => generatePassword());
  const [shown, setShown] = useState<string | null>(null);
  const [problem, setProblem] = useState<ProblemView | null>(null);

  const save = useMutation({
    mutationFn: (value: string) => updateUser(user.id, { password: value }),
    onSuccess: (_updated, value) => {
      setProblem(null);
      setShown(value);
      invalidateUsers(queryClient);
    },
    onError: (error) => setProblem(problemView(error, 'reset passwords')),
  });

  const close = () => {
    save.reset();
    setPassword('');
    setShown(null);
    onClose();
  };

  return (
    <Dialog open onOpenChange={(next) => (!next ? close() : undefined)}>
      <DialogContent
        title={shown ? 'Password reset' : `Reset password for ${user.name}`}
        description={
          shown
            ? 'Copy it now; it is not shown again.'
            : 'The current password stops working as soon as the new one is saved.'
        }
      >
        {shown ? (
          <div className="flex flex-col gap-4">
            <SecretOnce
              label={`New password for ${user.name}`}
              secret={shown}
              note={GENERATED_PASSWORD_NOTE}
            />
            <div className="flex justify-end border-t border-line pt-4">
              <Button variant="primary" onClick={close}>
                Done
              </Button>
            </div>
          </div>
        ) : (
          <form
            className="flex flex-col gap-4"
            onSubmit={(event) => {
              event.preventDefault();
              const invalid = passwordProblem(password);
              if (invalid) {
                setProblem({ message: invalid });
                return;
              }
              setProblem(null);
              save.mutate(password);
            }}
            noValidate
          >
            {problem ? <ProblemBanner view={problem} /> : null}

            <Field
              label="New password"
              htmlFor="user-reset-password"
              hint="At least 12 characters. Share it over a separate channel."
            >
              <Input
                id="user-reset-password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                minLength={12}
                autoFocus
                spellCheck={false}
                autoComplete="new-password"
              />
              <div className="mt-1 flex flex-wrap gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setPassword(generatePassword())}
                >
                  Generate another
                </Button>
              </div>
            </Field>

            <div className="mt-1 flex flex-wrap justify-end gap-2 border-t border-line pt-4">
              <Button variant="default" onClick={close}>
                Cancel
              </Button>
              <Button busy={save.isPending} variant="primary" type="submit" disabled={save.isPending}>
                {save.isPending ? 'Saving…' : 'Reset password'}
              </Button>
            </div>
          </form>
        )}
      </DialogContent>
    </Dialog>
  );
}
