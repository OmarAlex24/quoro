// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import { ApiProblem } from '../api/fetcher';
import {
  SETUP_CHECKING,
  SETUP_DONE,
  offerSetup,
  setSetupNotice,
  setupAlreadyDone,
  setupView,
  takeSetupNotice,
} from './setup-notice';

// ADR 0293: `getSetup` answers `{ required }`, whether the cluster still has no owner.
describe('offerSetup', () => {
  it('offers setup on sign-in only while the cluster says it has no owner', () => {
    expect(offerSetup({ required: true })).toBe(true);
    expect(offerSetup({ required: false })).toBe(false);
  });

  it('offers nothing before the cluster answers or when the read fails', () => {
    expect(offerSetup(undefined)).toBe(false);
  });
});

describe('setupView', () => {
  it('shows the owner form while setup is open', () => {
    expect(setupView({ pending: false, state: { required: true } })).toBe('form');
  });

  it('shows that the cluster is set up once it has an owner, without a form', () => {
    expect(setupView({ pending: false, state: { required: false } })).toBe('done');
  });

  it('waits for the answer instead of showing a form that may not apply', () => {
    expect(setupView({ pending: true, state: undefined })).toBe('checking');
  });

  it('keeps an answer it already has while a refetch runs', () => {
    expect(setupView({ pending: true, state: { required: false } })).toBe('done');
  });

  it('falls back to the form when the read fails; the submit still meets conflict', () => {
    expect(setupView({ pending: false, state: undefined })).toBe('form');
  });

  it('says what it is waiting for in words', () => {
    expect(SETUP_CHECKING.title).toBe('Checking this cluster');
    expect(Object.values(SETUP_CHECKING).join(' ')).not.toMatch(/getSetup|required|GET /);
  });
});

describe('setupAlreadyDone', () => {
  // Issue 640: once the owner exists, setup answers 409 `conflict` before it reads the token.
  it('reads a conflict as a cluster that is already set up', () => {
    expect(setupAlreadyDone(new ApiProblem({ status: 409, code: 'conflict', detail: 'Setup is already done.' }))).toBe(
      true,
    );
  });

  it('leaves every other failure to the problem banner', () => {
    expect(setupAlreadyDone(new ApiProblem({ status: 401, code: 'unauthenticated' }))).toBe(false);
    expect(setupAlreadyDone(new ApiProblem({ status: 422, code: 'validation_failed' }))).toBe(false);
    expect(setupAlreadyDone(new Error('network down'))).toBe(false);
    expect(setupAlreadyDone(undefined)).toBe(false);
  });

  it('says the cluster is set up in words, with no raw problem code', () => {
    const words = Object.values(SETUP_DONE).join(' ');
    expect(SETUP_DONE.title).toBe('This cluster is already set up');
    expect(words).not.toMatch(/conflict|409/);
  });
});

describe('the setup notice', () => {
  it('is said once, on the next sign-in screen', () => {
    setSetupNotice('Owner created.');
    expect(takeSetupNotice()).toBe('Owner created.');
    expect(takeSetupNotice()).toBeUndefined();
  });
});
