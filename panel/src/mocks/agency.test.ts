import { describe, expect, it } from 'vitest';
import { agency, appendAudit, canonicalJson, sha256Hex, tamperAudit, verifyAuditChain } from './agency';
import { db, mockId, now } from './db';

describe('agency mock', () => {
  it('hashes with SHA-256', () => {
    expect(sha256Hex('')).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
    expect(sha256Hex('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
    expect(sha256Hex('a'.repeat(1000))).toBe(
      '41edece42d63e8d9bf515a9ba6932e1c20cbc9f5a5d134645adb5db1b9737ea3',
    );
  });

  it('serializes with sorted keys and no undefined fields', () => {
    expect(canonicalJson({ b: 1, a: { d: [1, 'x'], c: undefined } })).toBe('{"a":{"d":[1,"x"]},"b":1}');
  });

  it('keeps a verifiable chain and reports the exact tampered seq', () => {
    db.owner = {
      id: mockId('usr'),
      email: 'owner@example.com',
      name: 'Owner',
      role: 'owner',
      disabled: false,
      created_at: now(),
    };
    const state = agency();
    expect(state.audit.length).toBeGreaterThan(3);
    const record = appendAudit({ operation: 'POST /api/v1/audit/verify', params: {}, scope: 'audit:read' });
    expect(record.prev_hash).toBe(state.audit[state.audit.length - 2]?.hash);
    expect(Object.values(record)).not.toContain(null);

    const clean = verifyAuditChain();
    expect(clean.ok).toBe(true);
    expect(clean.checked).toBe(state.audit.length);

    expect(tamperAudit(3)).toBe(true);
    const broken = verifyAuditChain();
    expect(broken.ok).toBe(false);
    expect(broken.broken_at_seq).toBe(3);
  });
});
