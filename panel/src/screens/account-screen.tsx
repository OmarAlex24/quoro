import { useMutation } from '@tanstack/react-query';
import { useState, type FormEvent } from 'react';
import { changePassword } from '../api/generated/auth/auth';
import { isApiProblem } from '../api/fetcher';
import {
  passwordDetailField,
  passwordDraftProblem,
  type PasswordChangeDraft,
  type PasswordField,
} from '../lib/password';
import { problemView, type ProblemView } from '../lib/problem';
import { useSession } from '../session';
import { ApiTokens } from '../components/api-tokens';
import { Button } from '../components/ui/button';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { ProblemBanner } from '../components/ui/problem-banner';
import { LoginScreen } from './login-screen';

const emptyDraft = (): PasswordChangeDraft => ({
  current: '',
  next: '',
  repeat: '',
});

type FieldErrors = Partial<Record<PasswordField, string>>;

/**
 * The `/account` screen: who is signed in, their personal API tokens, and the
 * form where they change their own password. The three passwords live in this component's state for
 * as long as the form is open and nowhere else: not in storage, not in the
 * query cache, not in a log.
 */
export function AccountScreen() {
  const { user, loading } = useSession();
  const [draft, setDraft] = useState<PasswordChangeDraft>(emptyDraft);
  const [fieldErrors, setFieldErrors] = useState<FieldErrors>({});
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [changed, setChanged] = useState(false);

  const change = useMutation({
    mutationFn: (value: PasswordChangeDraft) =>
      changePassword({
        current_password: value.current,
        new_password: value.next,
      }),
    onSuccess: () => {
      setDraft(emptyDraft());
      setFieldErrors({});
      setProblem(null);
      setChanged(true);
    },
    onError: (error) => {
      setChanged(false);
      if (isApiProblem(error) && error.code === 'validation_failed') {
        const detail = error.detail ?? 'The form needs a fix before it can be sent.';
        // The server's detail is lower-case prose; show it as a sentence.
        const sentence = `${detail.charAt(0).toUpperCase()}${detail.slice(1)}${/[.!?]$/.test(detail) ? '' : '.'}`;
        setFieldErrors({ [passwordDetailField(detail)]: sentence });
        return;
      }
      if (isApiProblem(error) && error.code === 'rate_limited') {
        setProblem({
          message: 'Too many attempts. Wait a few minutes and try again.',
        });
        return;
      }
      setProblem(problemView(error, 'change your password'));
    },
  });

  const update = (field: PasswordField, value: string) => {
    setDraft((current) => ({ ...current, [field]: value }));
    setFieldErrors((current) => {
      if (current[field] === undefined) return current;
      const next = { ...current };
      delete next[field];
      return next;
    });
  };

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);
    setChanged(false);
    const invalid = passwordDraftProblem(draft);
    if (invalid) {
      setFieldErrors({ [invalid.field]: invalid.message });
      return;
    }
    setFieldErrors({});
    change.mutate(draft);
  };

  if (loading) {
    return (
      <main className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6">
        <p className="text-[15px] text-ink-2">Loading…</p>
      </main>
    );
  }

  if (!user) return <LoginScreen />;

  return (
    <main className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <div>
        <h1 className="type-display text-[24px] leading-none">Your account</h1>
        <p className="mt-2 max-w-2xl text-[13px] leading-snug text-ink-2">
          The identity this panel is signed in as, the tokens that sign the CLI
          and the API in as you, and your password.
        </p>
      </div>

      <section className="mt-5 rounded-[10px] border border-line bg-surface px-4 py-4 card-sheen sm:px-5">
        <h2 className="type-body-medium text-[13px] text-ink-2">Signed in as</h2>
        <dl className="mt-3 grid gap-x-6 gap-y-3 sm:grid-cols-[8rem_1fr]">
          <dt className="text-[13px] text-ink-2">Name</dt>
          <dd className="text-[15px] text-ink">{user.name}</dd>
          <dt className="text-[13px] text-ink-2">Email</dt>
          <dd className="break-all text-[15px] text-ink">{user.email}</dd>
          <dt className="text-[13px] text-ink-2">Role</dt>
          <dd className="text-[15px] text-ink">
            {user.role.charAt(0).toUpperCase() + user.role.slice(1)}
          </dd>
        </dl>
      </section>

      <ApiTokens />

      <section className="mt-4 rounded-[10px] border border-line bg-surface px-4 py-4 card-sheen sm:px-5">
        <h2 className="type-display text-[18px] leading-tight">Change password</h2>
        <p className="mt-1 max-w-2xl text-[13px] leading-snug text-ink-2">
          Your other panel sessions are signed out; this one stays signed in.
        </p>

        {changed ? (
          <p
            role="status"
            aria-live="polite"
            className="mt-4 rounded-[10px] border border-line border-l-4 border-l-run bg-surface px-3.5 py-2.5 text-[15px] leading-snug text-ink"
          >
            Password changed. Your other sessions were signed out.
          </p>
        ) : null}

        {problem ? <ProblemBanner view={problem} className="mt-4" /> : null}

        <form onSubmit={submit} className="mt-4 flex max-w-md flex-col gap-4" noValidate>
          <Field
            label="Current password"
            htmlFor="account-current-password"
            hint={
              fieldErrors.current ? (
                <span className="text-fault">{fieldErrors.current}</span>
              ) : undefined
            }
          >
            <Input
              id="account-current-password"
              type="password"
              value={draft.current}
              onChange={(event) => update('current', event.target.value)}
              autoComplete="current-password"
              spellCheck={false}
              aria-invalid={fieldErrors.current ? true : undefined}
            />
          </Field>

          <Field
            label="New password"
            htmlFor="account-new-password"
            hint={
              fieldErrors.next ? (
                <span className="text-fault">{fieldErrors.next}</span>
              ) : (
                'At least 12 characters.'
              )
            }
          >
            <Input
              id="account-new-password"
              type="password"
              value={draft.next}
              onChange={(event) => update('next', event.target.value)}
              autoComplete="new-password"
              spellCheck={false}
              minLength={12}
              aria-invalid={fieldErrors.next ? true : undefined}
            />
          </Field>

          <Field
            label="Repeat new password"
            htmlFor="account-repeat-password"
            hint={
              fieldErrors.repeat ? (
                <span className="text-fault">{fieldErrors.repeat}</span>
              ) : undefined
            }
          >
            <Input
              id="account-repeat-password"
              type="password"
              value={draft.repeat}
              onChange={(event) => update('repeat', event.target.value)}
              autoComplete="new-password"
              spellCheck={false}
              aria-invalid={fieldErrors.repeat ? true : undefined}
            />
          </Field>

          <div className="mt-1 border-t border-line pt-4">
            <Button variant="primary" type="submit" disabled={change.isPending}>
              {change.isPending ? 'Changing…' : 'Change password'}
            </Button>
          </div>
        </form>
      </section>
    </main>
  );
}
