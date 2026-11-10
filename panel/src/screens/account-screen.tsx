import { useMutation } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useId, useState, type FormEvent } from 'react';
import { changePassword } from '../api/generated/auth/auth';
import { isApiProblem } from '../api/fetcher';
import {
  passwordDetailField,
  passwordDraftProblem,
  type PasswordChangeDraft,
  type PasswordField,
} from '../lib/password';
import { problemView, type ProblemView } from '../lib/problem';
import { setThemeChoice, THEME_CHOICES, useThemeChoice } from '../lib/use-theme';
import { roleLabel } from '../lib/users';
import { useSession } from '../session';
import { Avatar } from '../components/avatar';
import { SettingsHead, SettingsLoading, SettingsPage } from '../components/settings-parts';
import { THEME_WORDS } from '../components/shell/avatar-menu';
import { Button } from '../components/ui/button';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { ProblemBanner } from '../components/ui/problem-banner';
import { Tag } from '../components/ui/tag';
import { LoginScreen } from './login-screen';

const emptyDraft = (): PasswordChangeDraft => ({
  current: '',
  next: '',
  repeat: '',
});

type FieldErrors = Partial<Record<PasswordField, string>>;

const joinedDate = (iso: string): string => {
  const parsed = new Date(iso);
  return Number.isNaN(parsed.getTime())
    ? 'unknown'
    : parsed.toLocaleDateString(undefined, { year: 'numeric', month: 'long', day: 'numeric' });
};

/** How the panel looks in this browser: follow the system, or always dark or light. */
function ThemeChoiceBox() {
  const choice = useThemeChoice();
  const labelId = useId();
  return (
    <section className="box" aria-labelledby={labelId}>
      <div className="box-h box-h-wrap">
        <div className="box-t">
          <b id={labelId}>Theme</b>
          <small>Kept in this browser only. System follows your device.</small>
        </div>
        <div role="radiogroup" aria-labelledby={labelId} className="seg seg-fit">
          {THEME_CHOICES.map((value) => {
            const { label, Icon } = THEME_WORDS[value];
            return (
              <button
                key={value}
                type="button"
                role="radio"
                aria-checked={choice === value}
                onClick={() => setThemeChoice(value)}
              >
                <Icon strokeWidth={1.7} aria-hidden="true" />
                {label}
              </button>
            );
          })}
        </div>
      </div>
    </section>
  );
}

/**
 * Settings › Account: who is signed in, the form where they change their own
 * password (D-0135), and the theme. Their API tokens have their own page. The
 * three passwords live in this component's state for as long as the form is
 * open and nowhere else: not in storage, not in the query cache, not in a log.
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
      <SettingsPage label="Account">
        <SettingsHead title="Account" />
        <SettingsLoading label="Loading your account" />
      </SettingsPage>
    );
  }

  if (!user) return <LoginScreen />;

  return (
    <SettingsPage label="Account">
      <SettingsHead
        title="Account"
        lede={
          <>
            Who this panel is signed in as, your password and how the panel looks. The tokens that
            sign the CLI in as you are under{' '}
            <Link to="/settings/tokens" className="set-inline-link">
              API tokens
            </Link>
            .
          </>
        }
      />

      <section className="box" aria-label="Signed in as">
        <div className="box-h">
          <Avatar kind="human" name={user.name} size="lg" />
          <div className="box-t">
            <b className="inline-flex items-center gap-1.5">
              <span className="truncate">{user.name}</span>
              <Tag tone="plain">{roleLabel(user.role)}</Tag>
            </b>
            <small className="truncate" title={user.email}>
              {user.email}
            </small>
          </div>
        </div>
        <dl className="set-rows border-t border-line">
          <div className="set-row">
            <dt>Member since</dt>
            <dd title={user.created_at}>{joinedDate(user.created_at)}</dd>
          </div>
        </dl>
      </section>

      <section className="box" aria-labelledby="account-password">
        <div className="box-h">
          <div className="box-t">
            <b id="account-password">Change password</b>
            <small>Your other panel sessions are signed out; this one stays signed in.</small>
          </div>
        </div>
        <div className="box-b grid gap-4">
        {changed ? (
          <p role="status" aria-live="polite" className="set-ok">
            Password changed. Your other sessions were signed out.
          </p>
        ) : null}

        {problem ? <ProblemBanner view={problem} /> : null}

        <form onSubmit={submit} className="flex max-w-md flex-col gap-4" noValidate>
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

          <div>
            <Button busy={change.isPending} variant="primary" type="submit" disabled={change.isPending}>
              {change.isPending ? 'Changing…' : 'Change password'}
            </Button>
          </div>
        </form>
        </div>
      </section>

      <ThemeChoiceBox />
    </SettingsPage>
  );
}
