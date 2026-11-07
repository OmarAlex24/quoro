import { cn } from '../lib/utils';
import {
  areaPath,
  chartPoints,
  linePath,
  type MetricDomain,
  type TimedSample,
} from '../lib/metrics';

const WIDTH = 320;
const HEIGHT = 120;
const PAD_TOP = 10;
const PAD_RIGHT = 8;
const PAD_BOTTOM = 18;
const PAD_LEFT = 44;
const BASELINE = HEIGHT - PAD_BOTTOM;

const GRID_STEPS = [0.25, 0.5, 0.75];

const STROKE = {
  accent: 'var(--accent)',
  ion: 'var(--ion)',
} as const;

export interface MetricChartProps<T extends TimedSample> {
  title: string;
  /** The current value said in full, e.g. "12.4%" or "1.2 GiB of 2.0 GiB". */
  currentText: string;
  samples: readonly T[];
  value: (sample: T) => number;
  domain: MetricDomain;
  formatValue: (value: number) => string;
  tone?: keyof typeof STROKE;
  emptyText?: string;
  /** A refusal or an unreachable node, shown over the plot instead of a blank. */
  notice?: string | null;
  className?: string;
}

/**
 * One series over the last ten minutes. A thin line and a light area fill on
 * a faint line grid; the current value sits large beside the title, so the
 * chart reads at a glance before the shape does.
 */
export function MetricChart<T extends TimedSample>({
  title,
  currentText,
  samples,
  value,
  domain,
  formatValue,
  tone = 'accent',
  emptyText = 'Collecting samples — first one within 5 s',
  notice,
  className,
}: MetricChartProps<T>) {
  // The right edge is the newest sample, so a node clock that differs from the
  // browser's does not push the line off the chart.
  const newest = samples.length > 0 ? Date.parse(samples[samples.length - 1].time) : NaN;
  const now = Number.isFinite(newest) ? newest : Date.now();
  const points = chartPoints(
    samples,
    value,
    domain,
    {
      width: WIDTH,
      height: HEIGHT,
      padTop: PAD_TOP,
      padRight: PAD_RIGHT,
      padBottom: PAD_BOTTOM,
      padLeft: PAD_LEFT,
    },
    now,
  );
  const line = linePath(points);
  const area = areaPath(points, BASELINE);
  const stroke = STROKE[tone];
  const last = points[points.length - 1];
  const ariaLabel =
    samples.length === 0
      ? `${title}: no samples yet, collecting`
      : `${title}: ${currentText}`;

  return (
    <div className={cn('min-w-0', className)}>
      <div className="flex flex-wrap items-baseline gap-x-2.5 gap-y-1">
        <h3 className="type-bay-title text-[13px] text-ink-2">{title}</h3>
        <p className="type-data text-[20px] leading-none text-ink">{currentText}</p>
      </div>

      {notice && samples.length > 0 ? (
        <p className="mt-1 text-[12px] leading-snug text-fault">{notice}</p>
      ) : null}

      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={ariaLabel}
        className="mt-2 block w-full"
      >
        {GRID_STEPS.map((step) => {
          const y = PAD_TOP + (1 - step) * (BASELINE - PAD_TOP);
          const gridValue = domain.min + (domain.max - domain.min) * step;
          return (
            <g key={step}>
              <line
                x1={PAD_LEFT}
                x2={WIDTH - PAD_RIGHT}
                y1={y}
                y2={y}
                stroke="var(--line)"
                strokeWidth={1}
              />
              <text
                x={PAD_LEFT - 6}
                y={y + 3}
                textAnchor="end"
                fontSize={9.5}
                fill="var(--ink-3)"
              >
                {formatValue(gridValue)}
              </text>
            </g>
          );
        })}

        <line
          x1={PAD_LEFT}
          x2={WIDTH - PAD_RIGHT}
          y1={BASELINE}
          y2={BASELINE}
          stroke="var(--line)"
          strokeWidth={1}
        />

        {points.length > 0 ? (
          <>
            <path d={area} fill={stroke} fillOpacity={0.12} stroke="none" />
            <path
              d={line}
              fill="none"
              stroke={stroke}
              strokeWidth={1.6}
              strokeLinecap="round"
              strokeLinejoin="round"
              vectorEffect="non-scaling-stroke"
            />
            {last ? <circle cx={last.x} cy={last.y} r={2.1} fill={stroke} /> : null}
          </>
        ) : (
          <text
            x={(PAD_LEFT + WIDTH - PAD_RIGHT) / 2}
            y={(PAD_TOP + BASELINE) / 2}
            textAnchor="middle"
            fontSize={10.5}
            fill="var(--ink-3)"
          >
            {notice ?? emptyText}
          </text>
        )}

        <text x={PAD_LEFT} y={HEIGHT - 4} fontSize={9} fill="var(--ink-3)">
          -10 min
        </text>
        <text
          x={WIDTH - PAD_RIGHT}
          y={HEIGHT - 4}
          textAnchor="end"
          fontSize={9}
          fill="var(--ink-3)"
        >
          now
        </text>
      </svg>
    </div>
  );
}
