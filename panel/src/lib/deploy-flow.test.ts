import { describe, expect, it } from 'vitest';
import { deploySteps, deriveAppName } from './deploy-flow';

describe('deploySteps', () => {
  it('keeps create, environment, domain and deployment in that order', () => {
    const steps = deploySteps({ envCount: 2, hostname: 'blog.example.com' });
    expect(steps.map((step) => step.key)).toEqual(['create', 'env', 'domain', 'deploy']);
    expect(steps.map((step) => step.label)).toEqual([
      'Creating the app',
      'Saving 2 variables',
      'Adding blog.example.com',
      'Starting the deployment',
    ]);
  });

  it('skips the environment step when no variables were entered', () => {
    const steps = deploySteps({ envCount: 0, hostname: '' });
    expect(steps.map((step) => step.key)).toEqual(['create', 'deploy']);
  });

  it('says one variable in the singular', () => {
    expect(deploySteps({ envCount: 1, hostname: '' })[1]?.label).toBe('Saving 1 variable');
  });

  it('skips the domain step when the hostname is blank', () => {
    expect(deploySteps({ envCount: 0, hostname: '   ' }).map((step) => step.key)).toEqual([
      'create',
      'deploy',
    ]);
  });

  it('ends a Git app with its first build instead of a deployment', () => {
    const steps = deploySteps({ envCount: 1, hostname: '', build: true });
    expect(steps.map((step) => step.key)).toEqual(['create', 'env', 'build']);
    expect(steps[2]?.label).toBe('Starting the first build');
  });
});

describe('deriveAppName', () => {
  it('takes the image repository and drops the tag', () => {
    expect(deriveAppName('docker.io/library/nginx:alpine')).toBe('nginx');
    expect(deriveAppName('ghcr.io/acme/blog@sha256:abcd')).toBe('blog');
  });

  it('normalises to the contract pattern', () => {
    expect(deriveAppName('docker.io/My_App:1.0')).toBe('my-app');
    expect(deriveAppName('')).toBe('app');
    expect(deriveAppName('a'.repeat(50))).toHaveLength(40);
  });
});
