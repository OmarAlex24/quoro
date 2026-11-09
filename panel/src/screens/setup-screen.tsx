import { useMutation } from '@tanstack/react-query';
import { Link, useNavigate } from '@tanstack/react-router';
import { useState, type FormEvent } from 'react';
import { setup } from '../api/generated/health/health';
import { problemView, type ProblemView } from '../lib/problem';
import { SETUP_DONE, setSetupNotice, setupAlreadyDone } from '../lib/setup-notice';
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
      setSetupNotice('Owner created. Sign in with the email and password you chose.');
      void navigate({ to: '/login' });
    },
    onError: (error) => {
      if (setupAlreadyDone(error)) {
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

  // Nothing typed here can succeed any more: say so and lead back to sign-in.
  if (alreadyDone) {
    return (
      <AuthPanel title={SETUP_DONE.title} lede={SETUP_DONE.lede}>
        <div className="grid gap-4">
          <p role="status" className="auth-note">
            {SETUP_DONE.next}
          </p>
          <Button variant="primary" className="h-9 w-full" asChild>
            <Link to="/login">Sign in</Link>
          </Button>
        </div>
      </AuthPanel>
    );
  }

  return (
    <AuthPanel
      title="Set up the owner"
      lede="The first account of this cluster. It holds every scope and invites everyone else."
      footer={
        <p>
          Setup already done?{' '}
          <Link to="/login" className="auth-link">
            Sign in
          </Link>
        </p>
      }
    >
      <form onSubmit={submit} className="grid gap-4" noValidate>
        {problem ? <ProblemBanner view={problem} /> : null}

        <Field
          label="Setup token"
          htmlFor="setup-token"
          hint={
            fieldErrors.token ? (
              <span className="text-fault">{fieldErrors.token}</span>
            ) : isMock ? (
              'Mock mode: any token works.'
            ) : (
              'Printed by quoro setup-token on the server.'
            )
          }
        >
          <Input
            id="setup-token"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            spellCheck={false}
            autoComplete="off"
            className="type-code"
            aria-invalid={fieldErrors.token !== undefined}
            autoFocus
          />
        </Field>

        <Field
          label="Email"
          htmlFor="setup-email"
          hint={fieldErrors.email ? <span className="text-fault">{fieldErrors.email}</span> : undefined}
        >
          <Input
            id="setup-email"
            type="email"
            autoComplete="username"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            aria-invalid={fieldErrors.email !== undefined}
          />
        </Field>

        <Field
          label="Name"
          htmlFor="setup-name"
          hint={fieldErrors.name ? <span className="text-fault">{fieldErrors.name}</span> : undefined}
        >
          <Input
            id="setup-name"
            autoComplete="name"
            value={name}
            onChange={(event) => setName(event.target.value)}
            aria-invalid={fieldErrors.name !== undefined}
          />
        </Field>

        <Field
          label="Password"
          htmlFor="setup-password"
          hint={
            fieldErrors.password ? (
              <span className="text-fault">{fieldErrors.password}</span>
            ) : (
              '12 characters or more.'
            )
          }
        >
          <Input
            id="setup-password"
            type="password"
            autoComplete="new-password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            aria-invalid={fieldErrors.password !== undefined}
          />
        </Field>

        <Button
          type="submit"
          variant="primary"
          className="h-9 w-full"
          disabled={setupMutation.isPending}
        >
          {setupMutation.isPending ? 'Setting up…' : 'Create the owner'}
        </Button>
      </form>
    </AuthPanel>
  );
}
