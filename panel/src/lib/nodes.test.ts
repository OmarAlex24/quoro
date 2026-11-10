import { describe, expect, it } from 'vitest';
import type { Node } from '../api/generated/model';
import { clusterSummary, connectedLine, logLagLine, nodeReadout, primaryLine } from './nodes';

const node = (name: string, fields: Partial<Node>): Node =>
  ({ id: `nod_${name}`, name, status: 'connected', cp_status: null, ...fields }) as Node;

describe('clusterSummary', () => {
  it('finds the primary and counts the connected nodes', () => {
    const summary = clusterSummary([
      node('lab-1', { cp_status: 'replica' }),
      node('lab-2', { cp_status: 'primary' }),
      node('lab-3', { status: 'disconnected' }),
    ]);
    expect(summary.primary?.name).toBe('lab-2');
    expect(connectedLine(summary)).toBe('2 of 3 nodes connected');
  });

  it('leaves removed machines out and falls back to the first node', () => {
    const summary = clusterSummary([
      node('old', { status: 'removed', cp_status: 'primary' }),
      node('lab-1', {}),
    ]);
    expect(summary.primary?.name).toBe('lab-1');
    expect(connectedLine(summary)).toBe('1 of 1 node connected');
  });

  it('says so when nothing is enrolled', () => {
    const summary = clusterSummary([]);
    expect(summary.primary).toBeUndefined();
    expect(connectedLine(summary)).toBe('No node enrolled');
  });

  it('names the primary with its control-plane status', () => {
    expect(primaryLine(node('lab-2', { cp_status: 'primary' }))).toBe('lab-2 · primary');
    expect(primaryLine(node('lab-1', {}))).toBe('lab-1 · node');
  });
});

describe('nodeReadout', () => {
  it('maps every session state to a lamp tone', () => {
    expect(nodeReadout('connected')).toEqual({ tone: 'run', words: 'Connected', pulse: false });
    expect(nodeReadout('pending').pulse).toBe(true);
    expect(nodeReadout('fenced').tone).toBe('fault');
    expect(nodeReadout('removed').tone).toBe('idle');
  });
});

describe('logLagLine', () => {
  it('says the node is in sync at no lag', () => {
    expect(logLagLine(0)).toBe('Change log: in sync');
  });

  it('counts the entries the node is behind', () => {
    expect(logLagLine(1)).toBe('Change log: 1 entry behind');
    expect(logLagLine(2)).toBe('Change log: 2 entries behind');
    expect(logLagLine(742)).toBe('Change log: 742 entries behind');
  });

  it('says nothing without a lag', () => {
    expect(logLagLine(null)).toBeNull();
    expect(logLagLine(undefined)).toBeNull();
  });
});
