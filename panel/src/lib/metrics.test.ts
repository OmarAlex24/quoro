import { describe, expect, it } from 'vitest';
import {
  areaPath,
  chartPoints,
  cpuDomain,
  formatBytes,
  formatPercent,
  linePath,
  memoryDomain,
  mergeSamples,
  METRICS_WINDOW_MS,
} from './metrics';

interface Sample {
  time: string;
  value: number;
}

const at = (secondsAgo: number, now: number): string =>
  new Date(now - secondsAgo * 1000).toISOString();

const NOW = Date.parse('2026-11-03T12:00:00.000Z');

const sample = (secondsAgo: number, value: number): Sample => ({
  time: at(secondsAgo, NOW),
  value,
});

describe('mergeSamples', () => {
  it('orders history and live samples oldest first', () => {
    const merged = mergeSamples(
      [sample(30, 0.3), sample(10, 0.1)],
      [sample(20, 0.2)],
      NOW,
    );
    expect(merged.map((item) => item.value)).toEqual([0.3, 0.2, 0.1]);
  });

  it('dedupes by time with the incoming sample winning', () => {
    const merged = mergeSamples(
      [sample(10, 0.1), sample(20, 0.2)],
      [sample(10, 0.9)],
      NOW,
    );
    expect(merged).toHaveLength(2);
    expect(merged.find((item) => item.value === 0.9)).toBeDefined();
    expect(merged.some((item) => item.value === 0.1)).toBe(false);
  });

  it('keeps only the last ten minutes', () => {
    const merged = mergeSamples(
      [sample(700, 0.7), sample(300, 0.3)],
      [sample(5, 0.05)],
      NOW,
    );
    expect(merged).toHaveLength(2);
    expect(merged[0]?.value).toBe(0.3);
    expect(merged[1]?.value).toBe(0.05);
    expect(METRICS_WINDOW_MS).toBe(600_000);
  });

  it('drops samples without a readable time', () => {
    const merged = mergeSamples(
      [{ time: 'not a date', value: 0.1 }, sample(5, 0.2)],
      [],
      NOW,
    );
    expect(merged).toHaveLength(1);
    expect(merged[0]?.value).toBe(0.2);
  });

  it('dedupes per key when samples share a time', () => {
    const a = { time: at(5, NOW), vm: 'vm_a', value: 0.1 };
    const b = { time: at(5, NOW), vm: 'vm_b', value: 0.2 };
    const merged = mergeSamples([a], [b], NOW, (item) => item.vm);
    expect(merged).toHaveLength(2);
  });
});

describe('formatters', () => {
  it('prints a fraction as a percentage with one decimal', () => {
    expect(formatPercent(0.124)).toBe('12.4%');
    expect(formatPercent(1)).toBe('100.0%');
    expect(formatPercent(1.04)).toBe('104.0%');
  });

  it('prints bytes in MiB and GiB with sensible precision', () => {
    expect(formatBytes(0)).toBe('0 MiB');
    expect(formatBytes(12 * 1024)).toBe('0.0 MiB');
    expect(formatBytes(512 * 1024 * 1024)).toBe('512 MiB');
    expect(formatBytes(1.25 * 1024 * 1024 * 1024)).toBe('1.3 GiB');
    expect(formatBytes(64 * 1024 * 1024 * 1024)).toBe('64.0 GiB');
  });
});

describe('domains', () => {
  it('keeps a node CPU domain fixed at 0..1', () => {
    expect(cpuDomain([{ cpu_usage: 0.2 }, { cpu_usage: 0.8 }])).toEqual({
      min: 0,
      max: 1,
    });
  });

  it('grows a VM CPU domain to its peak, never below 1', () => {
    expect(cpuDomain([{ cpu_usage: 0.2 }], true)).toEqual({ min: 0, max: 1 });
    expect(cpuDomain([{ cpu_usage: 1.25 }], true)).toEqual({ min: 0, max: 1.25 });
  });

  it('uses the memory total as the top of the domain', () => {
    expect(memoryDomain(2048)).toEqual({ min: 0, max: 2048 });
    expect(memoryDomain(0)).toEqual({ min: 0, max: 1 });
  });
});

describe('chart geometry', () => {
  const geometry = {
    width: 320,
    height: 120,
    padTop: 10,
    padRight: 8,
    padBottom: 18,
    padLeft: 44,
  };

  it('puts now at the right edge and ten minutes ago at the left pad', () => {
    const points = chartPoints(
      [
        { time: at(600, NOW), value: 0 },
        { time: at(0, NOW), value: 1 },
      ],
      (item) => item.value,
      { min: 0, max: 1 },
      geometry,
      NOW,
    );
    expect(points[0]?.x).toBeCloseTo(44);
    expect(points[0]?.y).toBeCloseTo(102);
    expect(points[1]?.x).toBeCloseTo(312);
    expect(points[1]?.y).toBeCloseTo(10);
  });

  it('clamps values outside the fixed domain', () => {
    const points = chartPoints(
      [{ time: at(0, NOW), value: 4 }],
      (item) => item.value,
      { min: 0, max: 1 },
      geometry,
      NOW,
    );
    expect(points[0]?.y).toBeCloseTo(10);
  });

  it('builds a line and a closed area', () => {
    const points = [
      { x: 0, y: 10 },
      { x: 10, y: 20 },
    ];
    expect(linePath(points)).toBe('M0.00 10.00 L10.00 20.00');
    expect(areaPath(points, 100)).toBe(
      'M0.00 10.00 L10.00 20.00 L10.00 100.00 L0.00 100.00 Z',
    );
    expect(areaPath([], 100)).toBe('');
  });
});
