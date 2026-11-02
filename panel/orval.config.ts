import { defineConfig } from 'orval';

/**
 * The panel is generated from the frozen contract in docs/contracts/openapi.yaml.
 * Only the tags the panel talks to are generated; the spec itself is never edited.
 */
export default defineConfig({
  quoro: {
    input: {
      target: '../docs/contracts/openapi.yaml',
      filters: {
        tags: ['health', 'auth', 'nodes', 'projects', 'vms'],
      },
    },
    output: {
      mode: 'tags-split',
      target: 'src/api/generated',
      schemas: 'src/api/generated/model',
      client: 'react-query',
      httpClient: 'fetch',
      mock: true,
      override: {
        mutator: {
          path: './src/api/fetcher.ts',
          name: 'apiFetch',
        },
        fetch: {
          includeHttpResponseReturnType: false,
        },
        query: {
          useQuery: true,
          signal: true,
        },
      },
    },
  },
});
