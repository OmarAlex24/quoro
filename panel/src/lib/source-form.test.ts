// SPDX-License-Identifier: AGPL-3.0-only
import { describe, expect, it } from 'vitest';
import {
  DEFAULT_CONTEXT_DIR,
  DEFAULT_DOCKERFILE_PATH,
  DEFAULT_SOURCE_REF,
  deriveAppNameFromRepo,
  repoUrlError,
  validateSourceForm,
  type SourceFormInput,
} from './source-form';

const form = (patch: Partial<SourceFormInput> = {}): SourceFormInput => ({
  repoUrl: 'https://github.com/example/docs-site.git',
  ref: DEFAULT_SOURCE_REF,
  dockerfilePath: DEFAULT_DOCKERFILE_PATH,
  contextDir: DEFAULT_CONTEXT_DIR,
  target: '',
  buildArgs: [],
  secrets: [],
  envNames: [],
  ...patch,
});

describe('validateSourceForm', () => {
  it('accepts the defaults for a public repository', () => {
    expect(validateSourceForm(form())).toEqual({});
  });

  it('needs an https URL and refuses credentials', () => {
    expect(validateSourceForm(form({ repoUrl: '' })).repo_url).toBeDefined();
    expect(validateSourceForm(form({ repoUrl: 'http://github.com/example/docs.git' })).repo_url).toBe(
      'Only public https:// repositories are supported.',
    );
    expect(validateSourceForm(form({ repoUrl: 'git@github.com:example/docs.git' })).repo_url).toBe(
      'Use a public https:// repository URL.',
    );
    expect(
      validateSourceForm(form({ repoUrl: 'https://user:token@github.com/example/docs.git' }))
        .repo_url,
    ).toBe('Public repositories only: remove the credentials from the URL.');
    expect(validateSourceForm(form({ repoUrl: 'https://github.com/a b' })).repo_url).toBe(
      'A repository URL cannot contain spaces.',
    );
    expect(repoUrlError('https://github.com/example/docs.git')).toBeNull();
  });

  it('needs a ref without spaces', () => {
    expect(validateSourceForm(form({ ref: '' })).ref).toBe(
      'Enter a branch, tag or commit.',
    );
    expect(validateSourceForm(form({ ref: 'release 1' })).ref).toBe(
      'A ref cannot contain spaces.',
    );
    expect(validateSourceForm(form({ ref: 'release/1.0' })).ref).toBeUndefined();
  });

  it('keeps the Dockerfile and the context inside the repository', () => {
    expect(validateSourceForm(form({ dockerfilePath: '/etc/Dockerfile' })).dockerfile).toBe(
      'Use a path relative to the build context, not an absolute one.',
    );
    expect(validateSourceForm(form({ dockerfilePath: 'docker/Dockerfile' })).dockerfile).toBeUndefined();
    expect(validateSourceForm(form({ dockerfilePath: 'docker/' })).dockerfile).toBe(
      'Name the Dockerfile, not just a folder.',
    );
    expect(validateSourceForm(form({ contextDir: '../outside' })).context).toBe(
      'The path cannot leave the build context.',
    );
    expect(validateSourceForm(form({ contextDir: 'services/web' })).context).toBeUndefined();
    expect(validateSourceForm(form({ contextDir: '' })).context).toBeDefined();
  });

  it('accepts a target stage but not one with spaces', () => {
    expect(validateSourceForm(form({ target: 'runtime' })).target).toBeUndefined();
    expect(validateSourceForm(form({ target: 'build stage' })).target).toBe(
      'A stage name cannot contain spaces.',
    );
  });

  it('checks build argument names and duplicates', () => {
    expect(
      validateSourceForm(form({ buildArgs: [{ name: 'NODE_ENV', value: 'production' }] })),
    ).toEqual({});
    expect(
      validateSourceForm(form({ buildArgs: [{ name: 'NODE_ENV', value: 'a' }, { name: 'NODE_ENV', value: 'b' }] }))
        .build_args,
    ).toBe('The argument NODE_ENV is listed twice.');
    expect(
      validateSourceForm(form({ buildArgs: [{ name: '9BAD', value: 'x' }] })).build_args,
    ).toBeDefined();
  });

  it('only allows secrets that are environment variables of the app', () => {
    expect(
      validateSourceForm(form({ secrets: ['NPM_TOKEN'], envNames: ['NPM_TOKEN'] })).secrets,
    ).toBeUndefined();
    expect(validateSourceForm(form({ secrets: ['NPM_TOKEN'], envNames: [] })).secrets).toBe(
      'NPM_TOKEN is not an environment variable of this app yet.',
    );
    expect(validateSourceForm(form({ secrets: ['bad name'], envNames: ['bad name'] })).secrets).toBe(
      '"bad name" is not a valid variable name.',
    );
  });
});

describe('deriveAppNameFromRepo', () => {
  it('takes the repository name without .git', () => {
    expect(deriveAppNameFromRepo('https://github.com/example/docs-site.git')).toBe(
      'docs-site',
    );
    expect(deriveAppNameFromRepo('https://github.com/example/Docs_Site')).toBe(
      'docs-site',
    );
  });

  it('falls back while the URL is still being typed', () => {
    expect(deriveAppNameFromRepo('')).toBe('app');
  });
});
