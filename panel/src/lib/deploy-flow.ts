/** The four calls of a deploy, in the order the screen awaits them. */
export type DeployStepKey = 'create' | 'env' | 'domain' | 'deploy';

export interface DeployStepDefinition {
  readonly key: DeployStepKey;
  readonly label: string;
}

export type DeployStepState = 'waiting' | 'active' | 'done' | 'error';

/**
 * The checklist shown on the deploy plate. `env` only appears when variables
 * were entered and `domain` only when a hostname was given, so the list is the
 * exact sequence of calls about to be made.
 */
export const deploySteps = ({
  envCount,
  hostname,
}: {
  envCount: number;
  hostname: string;
}): DeployStepDefinition[] => {
  const steps: DeployStepDefinition[] = [{ key: 'create', label: 'Creating the app' }];
  if (envCount > 0) {
    steps.push({
      key: 'env',
      label: `Saving ${envCount} variable${envCount === 1 ? '' : 's'}`,
    });
  }
  if (hostname.trim() !== '') {
    steps.push({ key: 'domain', label: `Adding ${hostname.trim()}` });
  }
  steps.push({ key: 'deploy', label: 'Starting the deployment' });
  return steps;
};

/**
 * An app name from an image reference: last path segment, no tag or digest,
 * lowercased, invalid runs collapsed to a dash, at most 40 characters.
 */
export const deriveAppName = (image: string): string => {
  const withoutDigest = image.split('@')[0] ?? '';
  const segment = withoutDigest.split('/').filter(Boolean).pop() ?? '';
  const withoutTag = segment.replace(/:[^:]*$/, '');
  const name = withoutTag
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 40)
    .replace(/-+$/g, '');
  return name === '' ? 'app' : name;
};
