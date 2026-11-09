import { isApiProblem } from '../api/fetcher';
import type { SetupState } from '../api/generated/model';

/**
 * The one line the login screen says after a successful setup. Setup does not
 * start a session (the contract returns the user, not a cookie), so the panel
 * carries the news across the navigation in memory.
 */
let notice: string | undefined;

export const setSetupNotice = (value: string) => {
  notice = value;
};

export const takeSetupNotice = (): string | undefined => {
  const value = notice;
  notice = undefined;
  return value;
};

/*
 * Setup runs once, while the cluster has no user. `GET /api/v1/setup`
 * (`getSetup`, ADR 0293) answers that one bit publicly, so the panel asks
 * before it offers anything: the sign-in page links to setup only while setup
 * is open, and `/setup` shows the owner form only then. Two first runs can
 * still race, so the cluster's answer to the submit keeps the last word: setup
 * answers 409 `conflict` as soon as a user exists, before it reads the token,
 * so that answer means the cluster is already set up whatever was typed.
 */

/** Whether the sign-in page offers setup: only on a cluster that said it has no owner yet. */
export const offerSetup = (state: SetupState | undefined): boolean => state?.required === true;

/** What `/setup` shows while it waits for `getSetup`, then once it has the answer. */
export type SetupView = 'checking' | 'form' | 'done';

/**
 * The cluster's answer decides. A failed read leaves the form in place: the
 * submit still meets `conflict` if an owner exists, and a passing network
 * fault must not lock a new cluster out of its first run.
 */
export const setupView = ({ pending, state }: { pending: boolean; state: SetupState | undefined }): SetupView => {
  if (state?.required === false) return 'done';
  if (pending) return 'checking';
  return 'form';
};

/** Whether a failed setup means the cluster already has its owner. */
export const setupAlreadyDone = (error: unknown): boolean => isApiProblem(error) && error.status === 409;

/** What the setup page says while it asks the cluster whether setup is open. */
export const SETUP_CHECKING = {
  title: 'Checking this cluster',
  lede: 'Setup runs once, while the cluster has no owner.',
  note: 'Asking the cluster whether setup is still open.',
} as const;

/** What the setup page says once the cluster answered that it is already set up. */
export const SETUP_DONE = {
  title: 'This cluster is already set up',
  lede: 'Its owner account exists, so setup cannot run again.',
  next: 'Sign in with your account. If you have none yet, ask an owner of this cluster to invite you.',
} as const;
