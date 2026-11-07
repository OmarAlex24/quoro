import { useMutation } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useState, type FormEvent } from 'react';
import { setup } from '../api/generated/health/health';
import { isApiProblem } from '../api/fetcher';
import { problemView, type ProblemView } from '../lib/problem';
import { setSetupNotice } from '../lib/setup-notice';
import { AuthPanel } from '../components/auth-panel';
import { Button } from '../components/ui/button';
import { Field } from '../components/ui/field';
import { Input } from '../components/ui/input';
import { ProblemBanner } from '../components/ui/problem-banner';

export function SetupScreen() {
  const navigate = useNavigate();
  const [token, setToken] = useState('');
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [problem, setProblem] = useState<ProblemView | null>(null);
  const [alreadyDone, setAlreadyDone] = useState(false);
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});

  const isMock = import.meta.env.MODE === 'mock';

  const setupMutation = useMutation({
    mutationFn: () =>
      setup(
        { email: email.trim(), name: name.trim(), password },
        { headers: { 'Quoro-Setup-Token': token.trim() } },
      ),
    onSuccess: () => {
      setSetupNotice('Owner created. Log in.');
      void navigate({ to: '/login' });
    },
    onError: (error) => {
      if (isApiProblem(error) && error.status === 409) {
        setAlreadyDone(true);
        setProblem(null);
        return;
      }
      setAlreadyDone(false);
      setProblem(problemView(error, 'set up the owner'));
    },
  });

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setProblem(null);

    const errors: Record<string, string> = {};
    if (token.trim() === '') errors.token = 'Paste the setup token.';
    if (email.trim() === '') errors.email = 'Enter the owner email.';
    if (name.trim() === '') errors.name = 'Enter the owner name.';
    if (password.length < 12) errors.password = 'Use 12 characters or more.';
    setFieldErrors(errors);
    if (Object.keys(errors).length > 0) return;

    setupMutation.mutate();
  };

  return (
    <AuthPanel
      footer={
        <p className="text-[13px] text-ink-2">
          Setup already done?{' '}
          <Link to="/login" className="text-accent underline-offset-4 hover:underline">
            Log in
          </Link>
        </p>
      }
    >
      <h2 className="type-display text-[18px] leading-tight">Set up owner</h2>

      <form onSubmit={submit} className="mt-4 flex flex-col gap-4" noValidate>
        {alreadyDone ? (
          <div className="rounded-[10px] border border-line border-l-4 border-l-fault bg-surface px-3.5 py-2.5">
            <p className="text-[15px] leading-snug text-ink">
              Setup is already done.{' '}
              <Link to="/login" className="text-accent underline-offset-4 hover:underline">
                Log in instead
              </Link>
              .
            </p>
            <p className="type-code mt-1 text-[12px] text-ink-2">conflict</p>
          </div>
        ) : null}

        {problem ? <ProblemBanner view={problem} /> : null}

        <Field
          label="Setup token"
          htmlFor="setup-token"
          hint={
            isMock
              ? 'Mock mode: any token works.'
              : 'Printed by quoro setup-token on the server.'
          }
        >
          <Input
            id="setup-token"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            spellCheck={false}
            aria-invalid={fieldErrors.token !== undefined}
            autoFocus
          />
          {fieldErrors.token ? (
            <p className="text-[13px] text-fault">{fieldErrors.token}</p>
          ) : null}
        </Field>

        <Field label="Email" htmlFor="setup-email">
          <Input
            id="setup-email"
            type="email"
            autoComplete="username"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            aria-invalid={fieldErrors.email !== undefined}
          />
          {fieldErrors.email ? (
            <p className="text-[13px] text-fault">{fieldErrors.email}</p>
          ) : null}
        </Field>

        <Field label="Name" htmlFor="setup-name">
          <Input
            id="setup-name"
            autoComplete="name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            aria-invalid={fieldErrors.name !== undefined}
          />
          {fieldErrors.name ? (
            <p className="text-[13px] text-fault">{fieldErrors.name}</p>
          ) : null}
        </Field>

        <Field
          label="Password"
          htmlFor="setup-password"
          hint="12 characters or more."
        >
          <Input
            id="setup-password"
            type="password"
            autoComplete="new-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            aria-invalid={fieldErrors.password !== undefined}
          />
          {fieldErrors.password ? (
            <p className="text-[13px] text-fault">{fieldErrors.password}</p>
          ) : null}
        </Field>

        <Button
          type="submit"
          variant="primary"
          className="mt-1 w-full"
          disabled={setupMutation.isPending}
        >
          {setupMutation.isPending ? 'Setting up…' : 'Set up owner'}
        </Button>
      </form>
    </AuthPanel>
  );
}
