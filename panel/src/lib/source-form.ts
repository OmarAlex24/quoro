// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The Git source form: the defaults, the checks and the app name that come
 * with a public repository. Pure, so the rules are tested without the screen.
 *
 * The contract accepts only public `https://` repositories in M4. Build
 * arguments are not secrets; secrets are names of the app's environment
 * variables.
 */
import { deriveAppName } from './deploy-flow';

export const DEFAULT_SOURCE_REF = 'main';
export const DEFAULT_DOCKERFILE_PATH = 'Dockerfile';
export const DEFAULT_CONTEXT_DIR = '.';

const REPO_URL_MAX = 2_000;
const PATH_MAX = 500;
const REF_MAX = 255;
const TARGET_MAX = 128;
const BUILD_ARG_VALUE_MAX = 4_096;
const ENV_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

export interface BuildArgInput {
  readonly name: string;
  readonly value: string;
}

export interface SourceFormInput {
  readonly repoUrl: string;
  readonly ref: string;
  readonly dockerfilePath: string;
  readonly contextDir: string;
  readonly target: string;
  readonly buildArgs: readonly BuildArgInput[];
  readonly secrets: readonly string[];
  /** Names of the environment variables entered on the same form. */
  readonly envNames: readonly string[];
}

export type SourceField =
  | 'repo_url'
  | 'ref'
  | 'dockerfile'
  | 'context'
  | 'target'
  | 'build_args'
  | 'secrets';

export type SourceErrors = Partial<Record<SourceField, string>>;

const hasControl = (value: string): boolean => /[\u0000-\u001f\u007f]/.test(value);

/** A path relative to the build context; `.` is the root itself. */
const relativePathError = (value: string, wantDirectory: boolean): string | null => {
  if (value === '') return 'Enter a path.';
  if (value.length > PATH_MAX) return `Keep the path under ${PATH_MAX} characters.`;
  if (value.startsWith('/') || /^[A-Za-z]:[\\/]/.test(value)) {
    return 'Use a path relative to the build context, not an absolute one.';
  }
  if (value.startsWith('~')) return 'Use a path inside the repository.';
  if (hasControl(value)) return 'The path contains a control character.';
  if (value.includes('\\')) return 'Use forward slashes in the path.';

  const segments = value.split('/').filter((segment) => segment !== '');
  if (segments.length === 0) {
    return wantDirectory ? null : 'Name the Dockerfile.';
  }
  let depth = 0;
  for (const segment of segments) {
    if (segment === '.') continue;
    if (segment === '..') {
      depth -= 1;
      if (depth < 0) return 'The path cannot leave the build context.';
      continue;
    }
    depth += 1;
  }
  return null;
};

/** The repository URL, or the one thing wrong with it. */
export const repoUrlError = (value: string): string | null => {
  const url = value.trim();
  if (url === '') return 'Enter the repository URL.';
  if (url.length > REPO_URL_MAX) {
    return `Keep the repository URL under ${REPO_URL_MAX} characters.`;
  }
  if (/\s/.test(url)) return 'A repository URL cannot contain spaces.';
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return 'Use a public https:// repository URL.';
  }
  if (parsed.protocol !== 'https:') {
    return 'Only public https:// repositories are supported.';
  }
  if (parsed.hostname === '') return 'Use a public https:// repository URL.';
  if (parsed.username !== '' || parsed.password !== '') {
    return 'Public repositories only: remove the credentials from the URL.';
  }
  return null;
};

/** A branch, tag or full commit SHA, or the one thing wrong with it. */
export const refError = (value: string): string | null => {
  const ref = value.trim();
  if (ref === '') return 'Enter a branch, tag or commit.';
  if (ref.length > REF_MAX) return `Keep the ref under ${REF_MAX} characters.`;
  if (/\s/.test(ref)) return 'A ref cannot contain spaces.';
  if (hasControl(ref)) return 'The ref contains a control character.';
  if (ref.startsWith('-')) return 'A ref cannot start with a dash.';
  return null;
};

/** The stage of a multi-stage Dockerfile, when one is given. */
export const targetError = (value: string): string | null => {
  const target = value.trim();
  if (target === '') return null;
  if (target.length > TARGET_MAX) {
    return `Keep the stage name under ${TARGET_MAX} characters.`;
  }
  if (/\s/.test(target)) return 'A stage name cannot contain spaces.';
  if (target.includes('/')) return 'A stage name is not a path.';
  return null;
};

/**
 * Every field of the Git form, keyed by the control it belongs to. An empty
 * object means the form can be sent.
 */
export const validateSourceForm = (input: SourceFormInput): SourceErrors => {
  const errors: SourceErrors = {};

  const repo = repoUrlError(input.repoUrl);
  if (repo !== null) errors.repo_url = repo;

  const ref = refError(input.ref);
  if (ref !== null) errors.ref = ref;

  const dockerfile =
    relativePathError(input.dockerfilePath.trim(), false) ??
    (input.dockerfilePath.trim().endsWith('/')
      ? 'Name the Dockerfile, not just a folder.'
      : null);
  if (dockerfile !== null) errors.dockerfile = dockerfile;

  if (input.contextDir.trim() === '') {
    errors.context = 'Enter the build context, or use . for the repository root.';
  } else {
    const context = relativePathError(input.contextDir.trim(), true);
    if (context !== null) errors.context = context;
  }

  const target = targetError(input.target);
  if (target !== null) errors.target = target;

  const seen = new Set<string>();
  for (const row of input.buildArgs) {
    if (row.name.trim() === '' && row.value === '') continue;
    const name = row.name.trim();
    if (!ENV_NAME_PATTERN.test(name)) {
      errors.build_args =
        'Argument names use letters, digits and underscores, and cannot start with a digit.';
      break;
    }
    if (seen.has(name)) {
      errors.build_args = `The argument ${name} is listed twice.`;
      break;
    }
    if (row.value.length > BUILD_ARG_VALUE_MAX) {
      errors.build_args = `Keep the value of ${name} under ${BUILD_ARG_VALUE_MAX} characters.`;
      break;
    }
    seen.add(name);
  }

  const envNames = new Set(input.envNames.map((name) => name.trim()));
  const secretSet = new Set<string>();
  for (const secret of input.secrets) {
    if (secretSet.has(secret)) {
      errors.secrets = `The secret ${secret} is listed twice.`;
      break;
    }
    if (!ENV_NAME_PATTERN.test(secret)) {
      errors.secrets = `"${secret}" is not a valid variable name.`;
      break;
    }
    if (!envNames.has(secret)) {
      errors.secrets = `${secret} is not an environment variable of this app yet.`;
      break;
    }
    secretSet.add(secret);
  }

  return errors;
};

/**
 * An app name from a repository URL: the last path segment without `.git`,
 * normalised the same way an image name is.
 */
export const deriveAppNameFromRepo = (repoUrl: string): string => {
  let segment = '';
  try {
    const path = new URL(repoUrl.trim()).pathname;
    segment = path.split('/').filter(Boolean).pop() ?? '';
  } catch {
    segment = repoUrl.trim().split('/').filter(Boolean).pop() ?? '';
  }
  return deriveAppName(segment.replace(/\.git$/i, ''));
};
