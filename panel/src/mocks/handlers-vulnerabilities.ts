// SPDX-License-Identifier: AGPL-3.0-only
/**
 * The vulnerability API in mock mode: an app's report with its filters and
 * cursor, and the cluster summary with cursor paging over the apps. Mock mode
 * only.
 */
import { http } from 'msw';
import type {
  AppVulnerabilities,
  VulnerabilityFinding,
  VulnerabilitySeverity,
  VulnerabilitySummary,
} from '../api/generated/model';
import { findApp } from './db';
import { json, notFound, signedIn, unauthorized } from './http';
import {
  appFindings,
  buildSummary,
  countsOf,
  inventoryFor,
  orderFindings,
  reportDataFor,
} from './vulnerabilities';

const SEVERITIES = new Set<VulnerabilitySeverity>([
  'critical',
  'high',
  'medium',
  'low',
  'unknown',
]);

const severityWeight = (severity: VulnerabilitySeverity): number => {
  switch (severity) {
    case 'critical':
      return 5;
    case 'high':
      return 4;
    case 'medium':
      return 3;
    case 'low':
      return 2;
    case 'unknown':
      return 1;
  }
};

const startIndex = (cursor: string | null): number => {
  const parsed = Number(cursor ?? '0');
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
};

const pageSize = (limit: string | null): number => {
  const parsed = Number(limit ?? '50');
  return Number.isInteger(parsed) && parsed > 0 ? Math.min(parsed, 500) : 50;
};

const pageOf = <T>(
  items: T[],
  cursor: string | null,
  limit: string | null,
): { items: T[]; next_cursor: string | null } => {
  const start = startIndex(cursor);
  const page = items.slice(start, start + pageSize(limit));
  const next = start + page.length;
  return { items: page, next_cursor: next < items.length ? String(next) : null };
};

const filterFindings = (
  findings: VulnerabilityFinding[],
  url: URL,
): VulnerabilityFinding[] => {
  const severityParam = url.searchParams.get('severity');
  const severity =
    severityParam !== null && SEVERITIES.has(severityParam as VulnerabilitySeverity)
      ? (severityParam as VulnerabilitySeverity)
      : null;
  const kevOnly = url.searchParams.get('kev') === 'true';
  const includeResolved = url.searchParams.get('include_resolved') === 'true';

  return orderFindings(
    findings.filter((finding) => {
      if (!includeResolved && finding.resolved_at !== null) return false;
      if (kevOnly && !finding.kev) return false;
      if (
        severity !== null &&
        severityWeight(finding.severity) < severityWeight(severity)
      ) {
        return false;
      }
      return true;
    }),
  );
};

export const vulnerabilitiesHandlers = [
  http.get('*/api/v1/apps/:appId/vulnerabilities', ({ request, params }) => {
    if (!signedIn()) return unauthorized();
    const app = findApp(String(params.appId));
    if (!app) return notFound('That app');

    const url = new URL(request.url);
    const filtered = filterFindings(appFindings(app), url);
    const page = pageOf(
      filtered,
      url.searchParams.get('cursor'),
      url.searchParams.get('limit'),
    );

    const report: AppVulnerabilities = {
      app_id: app.id,
      inventory: inventoryFor(app),
      counts: countsOf(appFindings(app)),
      data: reportDataFor(app),
      items: page.items,
      next_cursor: page.next_cursor,
    };
    return json(report);
  }),

  http.get('*/api/v1/vulnerabilities', ({ request }) => {
    if (!signedIn()) return unauthorized();

    const url = new URL(request.url);
    const summary = buildSummary(url.searchParams.get('project_id'));
    const page = pageOf(
      summary.apps,
      url.searchParams.get('cursor'),
      url.searchParams.get('limit'),
    );

    const body: VulnerabilitySummary = {
      ...summary,
      apps: page.items,
      next_cursor: page.next_cursor,
    };
    return json(body);
  }),
];
